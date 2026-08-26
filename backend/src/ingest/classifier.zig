//! Autonomous bulk-mirror classifier — the labeler's brain.
//!
//! The labeler watches the firehose and labels on its own: every ingested
//! document is fed here via observe(), which keeps a rolling per-DID aggregate
//! in its OWN SQLite (never turso, never the frozen replica, never blocks the
//! firehose). When a DID crosses a volume floor it is scored in-process; if it
//! looks like a bulk-generated registry/feed mirror, the labeler emits a
//! signed `bulk-generated` account label — no human in the loop.
//!
//! Scoring mirrors scripts/classify-bulk-mirror (validated offline): the
//! decisive axis is authorship/content SHAPE (templated titles, thin/empty
//! content), NOT volume — volume only corroborates, because the corpus's
//! highest-volume authors are real humans. A mislabel is corrected by a
//! negation (see labeler.emit neg=true), so autonomous emission is safe.

const std = @import("std");
const zqlite = @import("zqlite");
const logfire = @import("logfire");
const labeler = @import("../labeler.zig");
const policy = @import("../policy.zig");
const db = @import("../db.zig");

const http = std.http;
const json = std.json;
const Io = std.Io;
const Allocator = std.mem.Allocator;
const log = std.log.scoped(.classifier);

// tuned against the known keep/flag set. THRESHOLD is conservative (offline
// separation put flags at 0.52-0.81, real humans <0.27) because emission is
// autonomous and there's no curation veto in the firehose path.
const FLOOR: i64 = 50; // min docs before a DID can be judged
const EVAL_EVERY: i64 = 25; // re-score every N docs past the floor until labeled
const EXTREME_VOLUME_REVIEW_FLOOR: i64 = 5000;
// Precision comes from the signal fixes (date/empty/non-ASCII titles score ~0.3),
// not the threshold — so 0.50 catches genuine mirrors (transit feeds ~0.54)
// while staying FP-safe. The curation veto is the backstop for prolific humans.
const THRESHOLD: f64 = 0.50;
// bump when scoring logic OR threshold changes → bootstrap negates old labels +
// re-scores the whole corpus from a clean slate. v2 = precision fix (content-word
// scaffold only, curation veto). v3 = threshold 0.55→0.50. v4 = model-pass gate
// (heuristic flags → LLM confirms content is bulk-generated before emit).
// v11 = rotating-slot template fix (short mostly-scaffold titles count fully
// templated) + thin-volume promotion (eligundry mood tracker sat at 0.47).
const SCORING_VERSION: i64 = 11;
const AGGREGATION_VERSION: i64 = 1; // v1: count unique document URIs, not put/update events

// review pipeline states. The heuristic is a cheap PRE-FILTER: it never emits
// directly (titles can't tell a branded real blog from a registry mirror). It
// flags PENDING; a background worker reads sample CONTENT, asks an LLM, and only
// then emits. So a false positive from the heuristic costs an LLM call, not a
// mislabeled human.
const STATE_OBSERVING: i64 = 0;
const STATE_PENDING: i64 = 1; // heuristic flagged; awaiting model review
const STATE_LABELED: i64 = 2; // model confirmed bulk-generated → emitted
const STATE_REJECTED: i64 = 3; // model said human → not labeled
const STATE_VETOED: i64 = 4; // had curation → never labeled
const MAX_REVIEW_ATTEMPTS: i64 = 5; // give up after this many inconclusive reviews
// the model-pass runs on co/core (cocore.dev) — an AT-Protocol-native, OpenAI-
// compatible decentralized inference exchange. Fitting: the labeler is an AT
// Proto thing, so its content judge runs on AT-Proto-native inference.
// review provider: any OpenAI-compatible chat-completions endpoint. Defaults
// to co/core; override all three via env (REVIEW_API_URL / REVIEW_MODEL /
// REVIEW_API_KEY) to switch providers with a secrets change, no deploy of code.
// picked by scripts/judge-eval: gemma-4-12B went 19/19 conclusive votes
// correct across two runs on the composed-vs-generated prompt (2026-07-02),
// and it's the model our own provider machine serves — reliability and
// economics we control. Qwen2.5-7B (the original judge) was 0/3, confidently
// inverted. Reasoning-style + slow is fine: the worker is a durable queue,
// candidates just stay PENDING longer.
const DEFAULT_REVIEW_MODEL = "mlx-community/gemma-4-12B-it-8bit";
const DEFAULT_REVIEW_URL = "https://console.cocore.dev/api/v1/chat/completions";

const ReviewCfg = struct {
    url: []const u8,
    model: []const u8,
    key: []const u8,
};
const SAMPLE_CAP: i64 = 64; // normalized titles kept per DID for template scoring

var g_conn: ?zqlite.Conn = null;
var g_rebuilding = std.atomic.Value(bool).init(false);

const STOPWORDS = [_][]const u8{
    "the", "a",   "an",   "and", "or",   "but", "of",   "to", "in",  "on", "for",  "with",
    "at",  "by",  "from", "is",  "are",  "was", "were", "be", "as",  "it", "this", "that",
    "i",   "you", "we",   "my",  "your", "our", "me",   "no", "not", "do",
};

fn isStopword(w: []const u8) bool {
    for (STOPWORDS) |s| if (std.mem.eql(u8, s, w)) return true;
    return false;
}

/// Open the author-stats db (separate file from labels.db; this is working
/// state, not emitted labels). No-op safe to call once at boot.
pub fn init() void {
    const path: [*:0]const u8 = if (std.c.getenv("CLASSIFIER_DB")) |p| @ptrCast(p) else "/data/author-stats.db";
    const conn = zqlite.open(path, zqlite.OpenFlags.Create | zqlite.OpenFlags.ReadWrite) catch |err| {
        logfire.err("classifier: open failed: {s}", .{@errorName(err)});
        return;
    };
    conn.execNoArgs("PRAGMA journal_mode=WAL") catch {};
    conn.execNoArgs("PRAGMA busy_timeout=5000") catch {};
    conn.execNoArgs(
        \\CREATE TABLE IF NOT EXISTS author_stats (
        \\  did TEXT PRIMARY KEY,
        \\  doc_count INTEGER NOT NULL DEFAULT 0,
        \\  len_sum INTEGER NOT NULL DEFAULT 0,
        \\  empty_titles INTEGER NOT NULL DEFAULT 0,
        \\  digit_titles INTEGER NOT NULL DEFAULT 0,
        \\  title_sample TEXT NOT NULL DEFAULT '',
        \\  sample_count INTEGER NOT NULL DEFAULT 0,
        \\  labeled INTEGER NOT NULL DEFAULT 0
        \\)
    ) catch |err| {
        logfire.err("classifier: schema failed: {s}", .{@errorName(err)});
        return;
    };
    conn.execNoArgs("CREATE TABLE IF NOT EXISTS classifier_meta (k TEXT PRIMARY KEY, v INTEGER NOT NULL)") catch {};
    // review-pipeline state (added after the initial schema; ALTER is a no-op if
    // the column already exists).
    conn.execNoArgs("ALTER TABLE author_stats ADD COLUMN state INTEGER NOT NULL DEFAULT 0") catch {};
    conn.execNoArgs("ALTER TABLE author_stats ADD COLUMN review_attempts INTEGER NOT NULL DEFAULT 0") catch {};
    conn.execNoArgs("ALTER TABLE author_stats ADD COLUMN reason TEXT NOT NULL DEFAULT ''") catch {};
    // display identity for authors with no replica presence (seeded bans):
    // the replica can't resolve a site for a DID it has zero docs for.
    conn.execNoArgs("ALTER TABLE author_stats ADD COLUMN site TEXT NOT NULL DEFAULT ''") catch {};
    conn.execNoArgs("CREATE TABLE IF NOT EXISTS observed_documents (uri TEXT PRIMARY KEY)") catch {};
    g_conn = conn;
    logfire.info("classifier: author-stats ready (floor={d} threshold={d:.2})", .{ FLOOR, THRESHOLD });
}

/// Feed one ingested document. Cheap upsert; scores + maybe emits when a DID
/// crosses the floor. Called from the firehose path — must never block on
/// anything but local sqlite.
pub fn observe(uri: []const u8, did: []const u8, title: []const u8, content: []const u8) void {
    observeLen(uri, did, title, content.len);
}

/// One-time backfill: feed the existing corpus through the aggregate so the
/// classifier evaluates every author already indexed (not just ones that
/// publish after deploy). Reads the local replica (frozen, no turso); pulls only
/// did/title/LENGTH(content), never the content blobs. Idempotent via a marker.
/// Runs once, in a background thread.
pub fn bootstrap() void {
    const conn = g_conn orelse return;
    if (getMeta(conn, "scoring_version") == SCORING_VERSION) {
        if (getMeta(conn, "aggregation_version") != AGGREGATION_VERSION) rebuildUniqueAggregation(conn);
        // no re-score needed, but a DID newly added to banned-dids.txt still
        // needs its seeded label without waiting for a version bump.
        seedBannedRegistry(conn);
        return;
    }
    const local = db.getLocalDbRaw() orelse return;
    if (!local.isReady()) return;

    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena.deinit();
    const a = arena.allocator();

    // scoring changed since last bootstrap: negate every label we previously
    // emitted, so the corrected scoring starts from a clean slate (otherwise a
    // now-below-threshold author would keep a stale positive label). No-op on
    // the very first boot (nothing labeled yet).
    {
        var prev: std.ArrayList([]const u8) = .empty;
        var rows = conn.rows("SELECT did FROM author_stats WHERE labeled = 1", .{}) catch return;
        while (rows.next()) |row| prev.append(a, a.dupe(u8, row.text(0)) catch continue) catch {};
        rows.deinit();
        for (prev.items) |did| {
            _ = labeler.emit(did, labeler.LABEL_BULK_GENERATED, true) catch {};
            // negation matches on (src, uri, val) — labels emitted before the
            // v10 rename carry the old value and need their own retraction.
            _ = labeler.emit(did, "machine-generated", true) catch {};
        }
        if (prev.items.len > 0)
            logfire.info("classifier: cleared {d} prior labels for re-scoring (v{d})", .{ prev.items.len, SCORING_VERSION });
        conn.execNoArgs("DELETE FROM author_stats") catch {};
        conn.execNoArgs("DELETE FROM observed_documents") catch {};
    }

    const Entry = struct { uri: []const u8, did: []const u8, title: []const u8, len: usize };
    var entries: std.ArrayList(Entry) = .empty;

    // materialize first so we don't hold a replica read connection across the
    // (slower) per-row upserts below.
    {
        var rows = local.query("SELECT uri, did, title, LENGTH(content) FROM documents", .{}) catch |err| {
            logfire.err("classifier: bootstrap query failed: {s}", .{@errorName(err)});
            return;
        };
        defer rows.deinit();
        while (rows.next()) |row| {
            entries.append(a, .{
                .uri = a.dupe(u8, row.text(0)) catch continue,
                .did = a.dupe(u8, row.text(1)) catch continue,
                .title = a.dupe(u8, row.text(2)) catch continue,
                .len = @intCast(@max(row.int(3), 0)),
            }) catch continue;
        }
    }

    for (entries.items) |e| observeLen(e.uri, e.did, e.title, e.len);
    setMeta(conn, "scoring_version", SCORING_VERSION);
    setMeta(conn, "aggregation_version", AGGREGATION_VERSION);
    logfire.info("classifier: bootstrap (re)scored {d} existing docs at v{d}", .{ entries.items.len, SCORING_VERSION });
    seedBannedRegistry(conn);
}

/// v1 counted every put/update as another document. Rebuild the aggregate from
/// unique corpus URIs while retaining terminal operator/model decisions. Pending
/// nominations are intentionally discarded and must qualify again from their
/// real document count.
fn rebuildUniqueAggregation(conn: zqlite.Conn) void {
    const local = db.getLocalDbRaw() orelse return;
    if (!local.isReady()) return;

    g_rebuilding.store(true, .release);
    defer g_rebuilding.store(false, .release);

    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena.deinit();
    const a = arena.allocator();

    const Decision = struct {
        did: []const u8,
        state: i64,
        attempts: i64,
        reason: []const u8,
        site: []const u8,
    };
    var decisions: std.ArrayList(Decision) = .empty;
    var decided_rows = conn.rows(
        "SELECT did, state, review_attempts, reason, site FROM author_stats WHERE state IN (?, ?, ?)",
        .{ STATE_LABELED, STATE_REJECTED, STATE_VETOED },
    ) catch return;
    while (decided_rows.next()) |row| {
        decisions.append(a, .{
            .did = a.dupe(u8, row.text(0)) catch continue,
            .state = row.int(1),
            .attempts = row.int(2),
            .reason = a.dupe(u8, row.text(3)) catch continue,
            .site = a.dupe(u8, row.text(4)) catch continue,
        }) catch continue;
    }
    decided_rows.deinit();

    const Entry = struct { uri: []const u8, did: []const u8, title: []const u8, len: usize };
    var entries: std.ArrayList(Entry) = .empty;
    var rows = local.query("SELECT uri, did, title, LENGTH(content) FROM documents", .{}) catch return;
    while (rows.next()) |row| {
        entries.append(a, .{
            .uri = a.dupe(u8, row.text(0)) catch continue,
            .did = a.dupe(u8, row.text(1)) catch continue,
            .title = a.dupe(u8, row.text(2)) catch continue,
            .len = @intCast(@max(row.int(3), 0)),
        }) catch continue;
    }
    rows.deinit();

    conn.execNoArgs("DELETE FROM author_stats") catch return;
    conn.execNoArgs("DELETE FROM observed_documents") catch return;
    for (entries.items) |entry| observeLen(entry.uri, entry.did, entry.title, entry.len);

    for (decisions.items) |decision| {
        conn.exec(
            \\INSERT INTO author_stats (did, labeled, state, review_attempts, reason, site)
            \\VALUES (?, 1, ?, ?, ?, ?)
            \\ON CONFLICT(did) DO UPDATE SET labeled = 1, state = excluded.state,
            \\  review_attempts = excluded.review_attempts, reason = excluded.reason, site = excluded.site
        , .{ decision.did, decision.state, decision.attempts, decision.reason, decision.site }) catch {};
    }
    setMeta(conn, "aggregation_version", AGGREGATION_VERSION);
    logfire.info("classifier: rebuilt {d} unique documents; preserved {d} terminal decisions", .{ entries.items.len, decisions.items.len });
}

/// The hand-banned registry (banned-dids.txt / docs/exclusions.md) must be a
/// SUBSET of the labeled set: banned DIDs are dropped at the firehose and
/// excluded from the replica, so the classifier can never observe them — but
/// they're our best-evidenced bulk-generated cases, and the label stream is
/// how other consumers learn about them. Operator-attested: no model review.
/// Idempotent per scoring version (re-emits after each version's negate+wipe).
fn seedBannedRegistry(conn: zqlite.Conn) void {
    for (policy.BANNED_ENTRIES) |entry| {
        const was_labeled = blk: {
            const row = (conn.row("SELECT state FROM author_stats WHERE did = ?", .{entry.did}) catch break :blk false) orelse break :blk false;
            defer row.deinit();
            break :blk row.int(0) == STATE_LABELED;
        };
        // note format: "drivepatents.com patent bot" — first token is the site,
        // the rest describes what it is.
        const site = if (std.mem.indexOfScalar(u8, entry.note, ' ')) |sp| entry.note[0..sp] else entry.note;
        const kind = if (std.mem.indexOfScalar(u8, entry.note, ' ')) |sp| entry.note[sp + 1 ..] else "bulk-generated mirror";
        var reason_buf: [256]u8 = undefined;
        const reason = std.fmt.bufPrint(&reason_buf, "hand-banned {s} — evidence in docs/exclusions.md", .{kind}) catch entry.note;
        // row upsert is unconditional (refreshes site/reason wording); the
        // stream emit happens once — re-emitting on every boot would be spam.
        conn.exec(
            "INSERT OR REPLACE INTO author_stats (did, doc_count, len_sum, empty_titles, digit_titles, title_sample, sample_count, state, reason, site) VALUES (?, 0, 0, 0, 0, '', 0, ?, ?, ?)",
            .{ entry.did, STATE_LABELED, reason, site },
        ) catch continue;
        if (!was_labeled) {
            _ = labeler.emit(entry.did, labeler.LABEL_BULK_GENERATED, false) catch {};
            logfire.info("classifier: seeded banned-registry label for {s} ({s})", .{ entry.did, entry.note });
        }
    }
}

fn getMeta(conn: zqlite.Conn, key: []const u8) i64 {
    const row = (conn.row("SELECT v FROM classifier_meta WHERE k = ?", .{key}) catch return 0) orelse return 0;
    defer row.deinit();
    return row.int(0);
}

fn setMeta(conn: zqlite.Conn, key: []const u8, v: i64) void {
    conn.exec("INSERT OR REPLACE INTO classifier_meta (k, v) VALUES (?, ?)", .{ key, v }) catch {};
}

fn observeLen(uri: []const u8, did: []const u8, title: []const u8, content_len: usize) void {
    const conn = g_conn orelse return;
    if (!claimDocument(conn, uri)) return;

    var norm_buf: [256]u8 = undefined;
    const norm = normalizeTitle(title, &norm_buf);
    const has_digit: i64 = if (hasDigit(title)) 1 else 0;
    const is_empty: i64 = if (std.mem.trim(u8, title, " \t\n").len == 0) 1 else 0;

    // append the normalized title to the sample (newline-joined) until capped.
    conn.exec(
        \\INSERT INTO author_stats (did, doc_count, len_sum, empty_titles, digit_titles, title_sample, sample_count)
        \\VALUES (?, 1, ?, ?, ?, ?, 1)
        \\ON CONFLICT(did) DO UPDATE SET
        \\  doc_count = doc_count + 1,
        \\  len_sum = len_sum + excluded.len_sum,
        \\  empty_titles = empty_titles + excluded.empty_titles,
        \\  digit_titles = digit_titles + excluded.digit_titles,
        \\  title_sample = CASE WHEN sample_count < ? THEN title_sample || char(10) || ? ELSE title_sample END,
        \\  sample_count = CASE WHEN sample_count < ? THEN sample_count + 1 ELSE sample_count END
    , .{ did, @as(i64, @intCast(content_len)), is_empty, has_digit, norm, SAMPLE_CAP, norm, SAMPLE_CAP }) catch |err| {
        log.debug("observe upsert: {s}", .{@errorName(err)});
        return;
    };

    maybeEvaluate(conn, did);
}

fn claimDocument(conn: zqlite.Conn, uri: []const u8) bool {
    if (conn.row("SELECT 1 FROM observed_documents WHERE uri = ?", .{uri}) catch return false) |seen| {
        seen.deinit();
        return false;
    }
    conn.exec("INSERT OR IGNORE INTO observed_documents (uri) VALUES (?)", .{uri}) catch return false;
    return true;
}

fn maybeEvaluate(conn: zqlite.Conn, did: []const u8) void {
    const row = (conn.row(
        "SELECT doc_count, len_sum, empty_titles, digit_titles, title_sample, labeled FROM author_stats WHERE did = ?",
        .{did},
    ) catch return) orelse return;
    defer row.deinit();

    const doc_count = row.int(0);
    const labeled = row.int(5) != 0;
    if (labeled or doc_count < FLOOR) return;
    if (@rem(doc_count, EVAL_EVERY) != 0) return; // only re-score on cadence

    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena.deinit();

    const stats = Stats{
        .doc_count = doc_count,
        .len_sum = row.int(1),
        .empty_titles = row.int(2),
        .digit_titles = row.int(3),
        .sample = row.text(4),
    };
    const score = stats.score(arena.allocator());
    if (!stats.shouldReview(score)) return;

    // curation veto: any recommends/subscriptions → never label (human signal a
    // mirror won't have). Decided; stop re-scoring.
    if (hasCuration(did)) {
        setDecided(conn, did, STATE_VETOED);
        logfire.info("classifier: vetoed {s} (score={d:.3} but has curation)", .{ did, score });
        return;
    }

    // the heuristic NEVER emits directly — it can't tell a branded real blog from
    // a registry mirror. Flag for the model-pass; the review worker reads sample
    // content, asks an LLM, and emits only on confirmation.
    setDecided(conn, did, STATE_PENDING);
    logfire.info("classifier: queued {s} for model review (score={d:.3} docs={d})", .{ did, score, doc_count });
}

/// The account's site domain (base_path) from the replica — these are
/// standard.site/leaflet publishers, so the domain (prideraiser.org) is the
/// human identity, not a bsky handle (which getProfiles can't resolve).
fn authorSite(allocator: Allocator, did: []const u8) ?[]const u8 {
    const local = db.getLocalDbRaw() orelse return null;
    if (!local.isReady()) return null;
    var rows = local.query("SELECT base_path FROM documents WHERE did = ? AND base_path != '' LIMIT 1", .{did}) catch return null;
    defer rows.deinit();
    if (rows.next()) |row| {
        const bp = row.text(0);
        if (bp.len > 0) return allocator.dupe(u8, bp) catch null;
    }
    return null;
}

/// Is this DID currently labeled bulk-generated? Read by the search path to
/// filter/annotate results (author-stats sqlite is serialized; a point lookup
/// per result row is sub-microsecond). Kept-ness is policy.isKept, comptime.
pub fn isLabeledDid(did: []const u8) bool {
    const conn = g_conn orelse return false;
    const row = (conn.row("SELECT 1 FROM author_stats WHERE did = ? AND state = ?", .{ did, STATE_LABELED }) catch return false) orelse return false;
    row.deinit();
    return true;
}

/// Operator negated a label (via /admin/label neg=1): record the human verdict
/// so the /labels page and the review pipeline agree with the retraction.
/// Keeps state=REJECTED (terminal) so the classifier never re-flags the DID.
pub fn markNegated(did: []const u8) void {
    const conn = g_conn orelse return;
    setOperatorState(conn, did, STATE_REJECTED, "label negated by operator");
}

/// Keep the classifier's enforcement state in sync with a positive label
/// emitted through the operator endpoint.
pub fn markLabeled(did: []const u8) void {
    const conn = g_conn orelse return;
    setOperatorState(conn, did, STATE_LABELED, "label emitted by operator");
}

fn setOperatorState(conn: zqlite.Conn, did: []const u8, state: i64, reason: []const u8) void {
    conn.exec(
        \\INSERT INTO author_stats (did, labeled, state, reason) VALUES (?, 1, ?, ?)
        \\ON CONFLICT(did) DO UPDATE SET labeled = 1, state = excluded.state, reason = excluded.reason
    , .{ did, state, reason }) catch {};
}

fn stateName(s: i64) []const u8 {
    return switch (s) {
        STATE_PENDING => "pending",
        STATE_LABELED => "labeled",
        STATE_REJECTED => "rejected",
        STATE_VETOED => "vetoed",
        else => "observing",
    };
}

/// Read-only admin summary: counts by state + every decided author (state != 0)
/// with its score and a few normalized title patterns (the "why"). JSON for the
/// /labels heads-up page. caller owns the result.
pub fn writeSummaryJson(allocator: Allocator) ![]u8 {
    const conn = g_conn orelse return allocator.dupe(u8, "{\"counts\":{},\"authors\":[]}");

    var out: std.Io.Writer.Allocating = .init(allocator);
    errdefer out.deinit();
    var jw: json.Stringify = .{ .writer = &out.writer, .options = .{} };

    try jw.beginObject();

    // counts by state
    var counts = [_]i64{0} ** 5;
    {
        var r = try conn.rows("SELECT state, COUNT(*) FROM author_stats GROUP BY state", .{});
        defer r.deinit();
        while (r.next()) |row| {
            const s = row.int(0);
            if (s >= 0 and s < 5) counts[@intCast(s)] = row.int(1);
        }
    }
    try jw.objectField("counts");
    try jw.beginObject();
    inline for (.{ .{ "observing", STATE_OBSERVING }, .{ "pending", STATE_PENDING }, .{ "labeled", STATE_LABELED }, .{ "rejected", STATE_REJECTED }, .{ "vetoed", STATE_VETOED } }) |pair| {
        try jw.objectField(pair[0]);
        try jw.write(counts[pair[1]]);
    }
    try jw.endObject();

    // decided authors (labeled first, then pending, vetoed, rejected)
    try jw.objectField("authors");
    try jw.beginArray();
    var r = try conn.rows(
        \\SELECT did, doc_count, len_sum, empty_titles, digit_titles, title_sample, state, reason, site
        \\FROM author_stats WHERE state != 0
        \\ORDER BY CASE state WHEN 2 THEN 0 WHEN 1 THEN 1 WHEN 4 THEN 2 ELSE 3 END, doc_count DESC
        \\LIMIT 500
    , .{});
    defer r.deinit();
    while (r.next()) |row| {
        var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
        defer arena.deinit();
        const stats = Stats{
            .doc_count = row.int(1),
            .len_sum = row.int(2),
            .empty_titles = row.int(3),
            .digit_titles = row.int(4),
            .sample = row.text(5),
        };
        try jw.beginObject();
        try jw.objectField("did");
        try jw.write(row.text(0));
        try jw.objectField("site");
        const stored_site = row.text(8);
        if (stored_site.len > 0) {
            try jw.write(stored_site);
        } else {
            try jw.write(authorSite(arena.allocator(), row.text(0)) orelse "");
        }
        try jw.objectField("state");
        try jw.write(stateName(row.int(6)));
        try jw.objectField("kept");
        try jw.write(policy.isKept(row.text(0)));
        try jw.objectField("reason");
        try jw.write(row.text(7));
        try jw.objectField("docs");
        try jw.write(row.int(1));
        try jw.objectField("score");
        try jw.write(stats.score(arena.allocator()));
        // up to 3 normalized title patterns — shows WHY it scored as templated
        try jw.objectField("patterns");
        try jw.beginArray();
        var it = std.mem.splitScalar(u8, stats.sample, '\n');
        var n: usize = 0;
        while (it.next()) |t| {
            if (t.len == 0) continue;
            try jw.write(t);
            n += 1;
            if (n >= 3) break;
        }
        try jw.endArray();
        try jw.endObject();
    }
    try jw.endArray();

    try jw.endObject();
    return out.toOwnedSlice();
}

fn setDecided(conn: zqlite.Conn, did: []const u8, state: i64) void {
    conn.exec("UPDATE author_stats SET labeled = 1, state = ? WHERE did = ?", .{ state, did }) catch {};
}

const Stats = struct {
    doc_count: i64,
    len_sum: i64,
    empty_titles: i64,
    digit_titles: i64,
    sample: []const u8, // newline-joined normalized titles

    fn frac(n: i64, d: i64) f64 {
        return if (d == 0) 0 else @as(f64, @floatFromInt(n)) / @as(f64, @floatFromInt(d));
    }

    fn clamp01(x: f64) f64 {
        return @max(0.0, @min(1.0, x));
    }

    /// Prolific author with near-empty bodies — worth model review even when
    /// the title scaffold misses. The model gate is the precision layer; this
    /// only decides who's worth tokens.
    fn thinPromote(self: Stats) bool {
        const avg_len = frac(self.len_sum, self.doc_count);
        const dc: f64 = @floatFromInt(self.doc_count);
        const volume = clamp01((std.math.log10(@max(dc, 1.0)) - 2.0) / 2.0);
        return (1.0 - avg_len / 800.0) > 0.9 and volume > 0.3;
    }

    /// Route exceptionally prolific publishers through the model gate even
    /// when they generate fluent, uniquely titled documents. This does not
    /// label on volume; it only spends review tokens so feed-to-article farms
    /// cannot remain permanently invisible to the content judge.
    fn extremeVolumePromote(self: Stats) bool {
        return self.doc_count >= EXTREME_VOLUME_REVIEW_FLOOR;
    }

    fn shouldReview(self: Stats, score_value: f64) bool {
        return score_value >= THRESHOLD or self.thinPromote() or self.extremeVolumePromote();
    }

    /// composite 0..1 — same shape as scripts/classify-bulk-mirror.
    fn score(self: Stats, alloc: Allocator) f64 {
        const avg_len = frac(self.len_sum, self.doc_count);
        const thinness = clamp01(1.0 - avg_len / 800.0);
        const structural = @max(frac(self.empty_titles, self.doc_count), frac(self.digit_titles, self.doc_count));
        const dc: f64 = @floatFromInt(self.doc_count);
        const volume = clamp01((std.math.log10(@max(dc, 1.0)) - 2.0) / 2.0);
        const template = self.templateScore(alloc);
        return 0.45 * template + 0.20 * structural + 0.20 * thinness + 0.15 * volume;
    }

    /// Scaffold coverage: how much of each title is shared CONTENT words
    /// (tokens appearing in >=50% of titles). Titles with no content words —
    /// dates, symbols, non-ASCII (Japanese) — contribute 0, NOT 1.0: a human
    /// journal titled by date is not "templated spam". This is the precision
    /// fix; the old `max(1 - distinct-ratio, …)` + `toks==0 -> 1.0` flagged real
    /// people. Empty/date titles are caught by the structural signal instead.
    fn templateScore(self: Stats, alloc: Allocator) f64 {
        var titles: std.ArrayList([]const u8) = .empty;
        var it = std.mem.splitScalar(u8, self.sample, '\n');
        while (it.next()) |t| {
            if (t.len == 0) continue;
            titles.append(alloc, t) catch return 0;
        }
        const n = titles.items.len;
        if (n == 0) return 0;

        // scaffold: tokens appearing in >= 50% of titles
        var doc_freq = std.StringHashMap(usize).init(alloc);
        for (titles.items) |t| {
            var seen_in_title = std.StringHashMap(void).init(alloc);
            var tit = std.mem.splitScalar(u8, t, ' ');
            while (tit.next()) |w| {
                if (w.len <= 1 or isStopword(w)) continue;
                if (seen_in_title.contains(w)) continue;
                seen_in_title.put(w, {}) catch {};
                const e = doc_freq.getOrPut(w) catch continue;
                e.value_ptr.* = if (e.found_existing) e.value_ptr.* + 1 else 1;
            }
        }
        const half = @as(f64, @floatFromInt(n)) * 0.5;

        var coverage_sum: f64 = 0;
        for (titles.items) |t| {
            var toks: usize = 0;
            var scaffold_toks: usize = 0;
            var tit = std.mem.splitScalar(u8, t, ' ');
            while (tit.next()) |w| {
                if (w.len <= 1 or isStopword(w)) continue;
                toks += 1;
                const f = doc_freq.get(w) orelse 0;
                if (@as(f64, @floatFromInt(f)) >= half) scaffold_toks += 1;
            }
            // no content words (date/symbol/non-ASCII) → NOT templated.
            // short titles where at least half the words are scaffold ("i felt
            // rad" / "i felt meh") are fully templated: a rotating fill-in slot
            // splits unigram doc-frequency so the slot word never crosses the
            // 50% bar, halving coverage on exactly the spammiest shape.
            coverage_sum += if (toks == 0)
                0.0
            else if (toks <= 3 and scaffold_toks * 2 >= toks)
                1.0
            else
                frac(@intCast(scaffold_toks), @intCast(toks));
        }
        return coverage_sum / @as(f64, @floatFromInt(n));
    }
};

// ── model-pass gate (background review worker) ──────────────────────────────

/// Start the review worker if a provider key is set (REVIEW_API_KEY, falling
/// back to COCORE_API_KEY). Without one, flagged authors stay PENDING
/// (unlabeled) — emission is paused, not faked.
pub fn startReview(allocator: Allocator, io: Io) void {
    const key = if (std.c.getenv("REVIEW_API_KEY")) |p|
        std.mem.span(p)
    else if (std.c.getenv("COCORE_API_KEY")) |p|
        std.mem.span(p)
    else {
        logfire.info("classifier: model-pass disabled (no REVIEW_API_KEY/COCORE_API_KEY) — flagged authors queue unlabeled", .{});
        return;
    };
    const cfg = ReviewCfg{
        .url = if (std.c.getenv("REVIEW_API_URL")) |p| std.mem.span(p) else DEFAULT_REVIEW_URL,
        .model = if (std.c.getenv("REVIEW_MODEL")) |p| std.mem.span(p) else DEFAULT_REVIEW_MODEL,
        .key = key,
    };
    const t = std.Thread.spawn(.{}, reviewWorker, .{ allocator, cfg, io }) catch |err| {
        logfire.err("classifier: review worker spawn failed: {s}", .{@errorName(err)});
        return;
    };
    t.detach();
    logfire.info("classifier: model-pass review worker started ({s} @ {s})", .{ cfg.model, cfg.url });
}

fn reviewWorker(allocator: Allocator, cfg: ReviewCfg, io: Io) void {
    // authors that exhausted their attempts stay PENDING but invisible to
    // nextPending — without this reset they'd wedge forever. A fresh set of
    // attempts per boot keeps retries bounded (the process restarts on each
    // snapshot adoption) while still never rejecting on provider flakiness.
    var reset_done = false;
    while (true) {
        const conn = g_conn orelse {
            io.sleep(Io.Duration.fromSeconds(30), .awake) catch {};
            continue;
        };
        if (!reset_done) {
            resetExhaustedAttempts(conn);
            reset_done = true;
        }
        const did = nextPending(allocator, conn) orelse {
            io.sleep(Io.Duration.fromSeconds(30), .awake) catch {};
            continue;
        };
        defer allocator.free(did);

        // null verdict = inconclusive (empty/garbled reply, transient provider
        // error, or out of CC) → leave PENDING and retry, never reject. Only a
        // CLEAR machine/human answer decides. The model's reason (free text — the
        // protocol can't carry it on the label, so we keep it ourselves) is
        // stored for the dashboard.
        const verdict: ?Verdict = reviewAuthor(allocator, cfg, io, did) catch |err| blk: {
            if (err == error.NoCapacity) {
                // nobody is serving the model (cocore has no queue — we are
                // the queue). Pause; the candidate stays PENDING at its
                // current attempt count.
                logfire.info("classifier: no review capacity — queue paused 10m ({s} stays pending)", .{did});
                io.sleep(Io.Duration.fromSeconds(600), .awake) catch {};
                continue;
            }
            logfire.warn("classifier: review error for {s}: {s} (will retry)", .{ did, @errorName(err) });
            break :blk null;
        };
        if (verdict) |v| {
            defer allocator.free(v.reason);
            const state = if (v.machine) STATE_LABELED else STATE_REJECTED;
            conn.exec("UPDATE author_stats SET state = ?, reason = ? WHERE did = ?", .{ state, v.reason, did }) catch {};
            if (v.machine) {
                _ = labeler.emit(did, labeler.LABEL_BULK_GENERATED, false) catch {};
                logfire.info("classifier: model CONFIRMED {s} → bulk-generated ({s})", .{ did, v.reason });
                // DRAFT notification — NOT posted yet. We draft to the log (review
                // on /labels first) the heads-up the labeled account would get.
                const site = authorSite(allocator, did);
                defer if (site) |s| allocator.free(s);
                logfire.info("labeler DRAFT notify (NOT posted) → @{s}: pub-search indexes writing composed by an author; this account was classified bulk-generated ({s}) and excluded from search. reply to appeal · pub-search.waow.tech/labels", .{ site orelse did, v.reason });
            } else {
                logfire.info("classifier: model REJECTED {s} ({s})", .{ did, v.reason });
            }
        } else {
            // inconclusive → bump attempts; nextPending stops picking it past the
            // cap (abandoned, NOT labeled — precision-first). Slower backoff.
            conn.exec("UPDATE author_stats SET review_attempts = review_attempts + 1 WHERE did = ?", .{did}) catch {};
            logfire.info("classifier: review inconclusive for {s} (will retry)", .{did});
            io.sleep(Io.Duration.fromSeconds(10), .awake) catch {};
            continue;
        }
        io.sleep(Io.Duration.fromSeconds(1), .awake) catch {};
    }
}

fn resetExhaustedAttempts(conn: zqlite.Conn) void {
    conn.exec(
        "UPDATE author_stats SET review_attempts = 0 WHERE state = ? AND review_attempts >= ?",
        .{ STATE_PENDING, MAX_REVIEW_ATTEMPTS },
    ) catch {};
}

fn nextPending(allocator: Allocator, conn: zqlite.Conn) ?[]const u8 {
    if (g_rebuilding.load(.acquire)) return null;
    const row = (conn.row(
        "SELECT did FROM author_stats WHERE state = ? AND review_attempts < ? ORDER BY review_attempts ASC LIMIT 1",
        .{ STATE_PENDING, MAX_REVIEW_ATTEMPTS },
    ) catch return null) orelse return null;
    defer row.deinit();
    return allocator.dupe(u8, row.text(0)) catch null;
}

const Verdict = struct { machine: bool, reason: []const u8 };

// majority-of-N model gate: one review flipped verdicts on the same account
// across scoring versions (sksksketch: human at v4, machine at v6) because the
// evidence was whatever 8 rows the replica returned first. Each vote now sees a
// DIFFERENT deterministic slice of the corpus, so the majority averages over
// which part of an author's history you look at — sampling variance becomes
// signal instead of a coin flip.
const VOTES: usize = 3;
const VOTE_MAJORITY: usize = 2;
const TITLES_PER_VOTE: usize = 30;
const EXCERPTS_PER_VOTE: usize = 5;
const EXCERPT_LEN: usize = 800;

/// Ask the LLM whether the author is a bulk-generated mirror: VOTES
/// independent reviews over different corpus slices, VOTE_MAJORITY clear
/// agreeing answers decide. Returns null = no majority (retry). The returned
/// reason (from the first vote on the winning side) is owned by `allocator`.
fn reviewAuthor(allocator: Allocator, cfg: ReviewCfg, io: Io, did: []const u8) !?Verdict {
    var machine_reason: ?[]const u8 = null;
    var human_reason: ?[]const u8 = null;
    errdefer if (machine_reason) |r| allocator.free(r);
    errdefer if (human_reason) |r| allocator.free(r);
    var machine_votes: usize = 0;
    var human_votes: usize = 0;

    for (0..VOTES) |vote| {
        const v = (reviewVote(allocator, cfg, io, did, vote) catch |err| {
            // capacity errors abort the whole review — the worker pauses the
            // queue instead of counting an offline provider as a failed attempt
            if (err == error.NoCapacity) return err;
            logfire.warn("classifier: vote {d} error for {s}: {s}", .{ vote, did, @errorName(err) });
            continue;
        }) orelse continue;
        if (v.machine) {
            machine_votes += 1;
            if (machine_reason == null) machine_reason = v.reason else allocator.free(v.reason);
        } else {
            human_votes += 1;
            if (human_reason == null) human_reason = v.reason else allocator.free(v.reason);
        }
        logfire.info("classifier: vote {d}/{d} for {s}: {s}", .{ vote + 1, VOTES, did, if (v.machine) "machine" else "human" });
        // remaining votes can't change the outcome → stop spending tokens
        if (machine_votes >= VOTE_MAJORITY or human_votes >= VOTE_MAJORITY) break;
    }

    if (machine_votes >= VOTE_MAJORITY) {
        if (human_reason) |r| allocator.free(r);
        return .{ .machine = true, .reason = machine_reason.? };
    }
    if (human_votes >= VOTE_MAJORITY) {
        if (machine_reason) |r| allocator.free(r);
        return .{ .machine = false, .reason = human_reason.? };
    }
    // split / too many inconclusive votes → no verdict, worker retries
    logfire.info("classifier: no majority for {s} (machine={d} human={d})", .{ did, machine_votes, human_votes });
    if (machine_reason) |r| allocator.free(r);
    if (human_reason) |r| allocator.free(r);
    return null;
}

/// One vote: build this vote's evidence slice, ask the model once.
fn reviewVote(allocator: Allocator, cfg: ReviewCfg, io: Io, did: []const u8, vote: usize) !?Verdict {
    const samples = try fetchVoteMaterial(allocator, did, vote);
    defer allocator.free(samples);

    // the line is COMPOSED vs GENERATED, not human vs machine: most banned
    // content was human-written at the source (patents, recall notices,
    // transcripts) and original AI writing is welcome. Keep this text in
    // lockstep with scripts/judge-eval PROMPT_HEAD, and re-validate any change
    // there against the known-truth set before shipping (19/19 conclusive
    // votes correct on gemma-4-12B + Qwen3.6-35B, 2026-07-02).
    var prompt: std.Io.Writer.Allocating = .init(allocator);
    defer prompt.deinit();
    try prompt.writer.writeAll(
        \\A search engine indexes writing: documents COMPOSED by an author — a person or
        \\an AI — who chose what to say. It must EXCLUDE accounts that GENERATE
        \\documents from a data source: one document per database row, feed event,
        \\catalog entry, or result, where a template plus the data determines the text.
        \\
        \\The test is NOT whether the text is fluent, and NOT whether a human once wrote
        \\the underlying material — patents, recall notices, episode summaries, and
        \\transcripts were all written by people, but republishing them one-per-record
        \\is still generation. The test: does each document exist because its author had
        \\something to say, OR because a record exists in some dataset?
        \\
        \\machine=true examples: a patent-database mirror, vehicle-recall summaries (one
        \\per recall), a TV-episode catalog (one per episode), transit alerts (one per
        \\service event), chart/stats dumps (one per chart-day), tournament results (one
        \\per event), fundraiser announcements stamped from campaign records — even when
        \\the underlying data belongs to the account itself.
        \\machine=false examples: a personal blog or essay series (even branded,
        \\numbered, templated-looking titles, or extremely prolific), a daily journal
        \\(the date is a schedule, not a data source), original writing by an AI agent.
        \\
        \\Evidence from ONE account (facts, a title sample spanning its whole
        \\history, and excerpts from the middle of several documents):
        \\
        \\
    );
    try prompt.writer.writeAll(samples);
    try prompt.writer.writeAll(
        \\
        \\
        \\Respond with ONLY JSON: {"machine": true|false, "reason": "<one sentence>"}
    );

    const reply = try callModel(allocator, cfg, io, prompt.written());
    defer allocator.free(reply);
    const verdict = parseVerdict(allocator, reply);
    if (verdict == null) {
        // surface WHAT came back — repeated inconclusives are otherwise
        // undiagnosable from the logs (see the sksksketch wedge).
        logfire.warn("classifier: unparseable model reply for {s}: {s}", .{ did, reply[0..@min(reply.len, 300)] });
    }
    return verdict;
}

/// The evidence for one vote: corpus facts (doc count + activity span), a
/// broad title sample, and a few longer excerpts from the MIDDLE of documents
/// (openings are the most template-like part of anyone's writing). Everything
/// is ordered by created_at and picked by deterministic index, phased by
/// `vote`, so each vote sees a different — but reproducible — slice.
fn fetchVoteMaterial(allocator: Allocator, did: []const u8, vote: usize) ![]const u8 {
    const local = db.getLocalDbRaw() orelse return error.NoReplica;
    var out: std.Io.Writer.Allocating = .init(allocator);
    errdefer out.deinit();

    const total: usize = blk: {
        var rows = try local.query("SELECT COUNT(*), COALESCE(MIN(created_at),''), COALESCE(MAX(created_at),'') FROM documents WHERE did = ?", .{did});
        defer rows.deinit();
        const row = rows.next() orelse return error.NoSamples;
        const n: usize = @intCast(@max(row.int(0), 0));
        if (n == 0) return error.NoSamples;
        try out.writer.print("Account facts: {d} documents, first {s}, latest {s}\n\nTitles across the account's history:\n", .{ n, row.text(1), row.text(2) });
        break :blk n;
    };

    {
        var rows = try local.query("SELECT title FROM documents WHERE did = ? ORDER BY created_at, rkey", .{did});
        defer rows.deinit();
        const stride = @max(total / TITLES_PER_VOTE, 1);
        const phase = vote % stride;
        var i: usize = 0;
        var picked: usize = 0;
        while (rows.next()) |row| : (i += 1) {
            if (picked >= TITLES_PER_VOTE) break;
            if (i % stride != phase) continue;
            picked += 1;
            try out.writer.print("- {s}\n", .{row.text(0)});
        }
    }

    try out.writer.writeAll("\nContent excerpts (from the middle of documents):\n\n");
    for (0..EXCERPTS_PER_VOTE) |j| {
        const idx = excerptIndex(total, j, vote);
        var rows = try local.query(
            "SELECT title, content FROM documents WHERE did = ? ORDER BY created_at, rkey LIMIT 1 OFFSET ?",
            .{ did, @as(i64, @intCast(idx)) },
        );
        defer rows.deinit();
        const row = rows.next() orelse continue;
        try out.writer.print("- title: {s}\n  content: {s}\n\n", .{ row.text(0), utf8Middle(row.text(1), EXCERPT_LEN) });
    }

    return out.toOwnedSlice();
}

/// Excerpt j of k for a vote: evenly spaced across the corpus (midpoints of k
/// equal buckets), shifted by the vote index so votes read different documents.
fn excerptIndex(total: usize, j: usize, vote: usize) usize {
    const base = (total * (2 * j + 1)) / (2 * EXCERPTS_PER_VOTE);
    return (base + vote) % total;
}

/// A bounded window from the middle of `s`, both edges on UTF-8 boundaries.
fn utf8Middle(s: []const u8, max: usize) []const u8 {
    if (s.len <= max) return s;
    var start = (s.len - max) / 2;
    while (start < s.len and s[start] & 0xC0 == 0x80) start += 1;
    return utf8Excerpt(s[start..], max);
}

/// Bounded prefix trimmed to a UTF-8 codepoint boundary — a mid-codepoint
/// slice puts invalid UTF-8 in the JSON payload and the model reply comes
/// back garbled (this kept CJK-content authors permanently "inconclusive").
fn utf8Excerpt(s: []const u8, max: usize) []const u8 {
    var end = @min(s.len, max);
    while (end > 0 and end < s.len and s[end] & 0xC0 == 0x80) end -= 1;
    return s[0..end];
}

/// POST /chat/completions against any OpenAI-compatible provider (cfg.url).
fn callModel(allocator: Allocator, cfg: ReviewCfg, io: Io, prompt: []const u8) ![]const u8 {
    var client: http.Client = .{ .allocator = allocator, .io = io };
    defer client.deinit();

    var body: std.Io.Writer.Allocating = .init(allocator);
    defer body.deinit();
    var jw: json.Stringify = .{ .writer = &body.writer, .options = .{} };
    try jw.beginObject();
    try jw.objectField("model");
    try jw.write(cfg.model);
    // reasoning models think out loud before the JSON verdict; 150 truncated
    // them mid-thought and every reply parsed as inconclusive
    try jw.objectField("max_tokens");
    try jw.write(2000);
    try jw.objectField("temperature");
    try jw.write(0);
    try jw.objectField("messages");
    try jw.beginArray();
    try jw.beginObject();
    try jw.objectField("role");
    try jw.write("user");
    try jw.objectField("content");
    try jw.write(prompt);
    try jw.endObject();
    try jw.endArray();
    try jw.endObject();
    const payload = try body.toOwnedSlice();
    defer allocator.free(payload);

    var auth_buf: [256]u8 = undefined;
    const auth = std.fmt.bufPrint(&auth_buf, "Bearer {s}", .{cfg.key}) catch return error.AuthTooLong;

    var resp: std.Io.Writer.Allocating = .init(allocator);
    errdefer resp.deinit();
    const res = client.fetch(.{
        .location = .{ .url = cfg.url },
        .method = .POST,
        .headers = .{
            .content_type = .{ .override = "application/json" },
            .authorization = .{ .override = auth },
        },
        .payload = payload,
        .response_writer = &resp.writer,
    }) catch return error.CocoreRequestFailed;

    const text = try resp.toOwnedSlice();
    if (res.status != .ok) {
        defer allocator.free(text);
        logfire.err("classifier: review provider {}: {s}", .{ res.status, text[0..@min(text.len, 200)] });
        // 503 = no provider serving the model right now (cocore dispatch is
        // fail-fast, no server-side queue — WE are the queue). Distinct error
        // so the worker pauses instead of burning review attempts.
        // 404 = the model left cocore's roster entirely (2026-08-13:
        // gemma-4-12B vanished and the worker hot-looped 3 failing TLS calls
        // per author on the serving vCPU). Same treatment: pause, stay
        // PENDING, resume when the model returns or REVIEW_MODEL changes.
        if (res.status == .service_unavailable or res.status == .not_found) return error.NoCapacity;
        return error.CocoreApiError;
    }
    return text;
}

/// Pull the verdict + reason out of the OpenAI envelope. The content is
/// {"machine": bool, "reason": "..."}. Returns null when the reply is
/// empty/garbled/missing (transient failure or out of CC) so the worker retries
/// instead of falsely rejecting. The reason is owned by `allocator`.
fn parseVerdict(allocator: Allocator, response: []const u8) ?Verdict {
    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const parsed = json.parseFromSliceLeaky(json.Value, a, response, .{}) catch return null;
    if (parsed != .object) return null;
    const choices = parsed.object.get("choices") orelse return null;
    if (choices != .array or choices.array.items.len == 0) return null;
    const msg = choices.array.items[0].object.get("message") orelse return null;
    const content = msg.object.get("content") orelse return null;
    if (content != .string or content.string.len == 0) return null;
    const s = content.string;

    // the verdict is the LAST "machine": true/false in the reply — reasoning
    // models emit a thought stream (which may draft verdicts) before the final
    // JSON answer. Tolerates prose around it; a model may not return pure JSON.
    var pos: ?usize = null;
    var machine = false;
    inline for (.{ "\"machine\": true", "\"machine\":true" }) |needle| {
        if (std.mem.lastIndexOf(u8, s, needle)) |i| {
            if (pos == null or i > pos.?) {
                pos = i;
                machine = true;
            }
        }
    }
    inline for (.{ "\"machine\": false", "\"machine\":false" }) |needle| {
        if (std.mem.lastIndexOf(u8, s, needle)) |i| {
            if (pos == null or i > pos.?) {
                pos = i;
                machine = false;
            }
        }
    }
    if (pos == null) return null; // unexpected shape → retry

    // reason — from the JSON object holding that final verdict: parse from its
    // opening brace, extending past '}' chars that turn out to be inside the
    // reason text. Best-effort; missing reason still returns the verdict.
    var reason: []const u8 = "";
    if (std.mem.lastIndexOfScalar(u8, s[0..pos.?], '{')) |start| {
        var from = pos.?;
        while (std.mem.indexOfScalarPos(u8, s, from, '}')) |close| {
            if (json.parseFromSliceLeaky(json.Value, a, s[start .. close + 1], .{})) |inner| {
                if (inner == .object) {
                    if (inner.object.get("reason")) |rv| {
                        if (rv == .string) reason = rv.string;
                    }
                }
                break;
            } else |_| from = close + 1;
        }
    }
    const capped = reason[0..@min(reason.len, 280)];
    return .{ .machine = machine, .reason = allocator.dupe(u8, capped) catch "" };
}

/// True if anyone recommends a doc by this author, or subscribes to one of
/// their publications. Read from the frozen local replica (indexed existence
/// checks; rare — only on a threshold crossing).
fn hasCuration(did: []const u8) bool {
    const local = db.getLocalDbRaw() orelse return false;
    if (!local.isReady()) return false;
    var rows = local.query(
        \\SELECT EXISTS(SELECT 1 FROM recommends r JOIN documents d ON r.document_uri = d.uri WHERE d.did = ?)
        \\    OR EXISTS(SELECT 1 FROM subscriptions s JOIN publications p ON s.publication_uri = p.uri WHERE p.did = ?)
    , .{ did, did }) catch return false;
    defer rows.deinit();
    if (rows.next()) |row| return row.int(0) != 0;
    return false;
}

fn hasDigit(s: []const u8) bool {
    for (s) |c| if (std.ascii.isDigit(c)) return true;
    return false;
}

/// lowercase, digit-runs → '#', non-alnum → space, collapse whitespace.
fn normalizeTitle(title: []const u8, buf: *[256]u8) []const u8 {
    var w: usize = 0;
    var last_space = true; // strip leading
    var i: usize = 0;
    while (i < title.len and w < buf.len) {
        const c = title[i];
        if (std.ascii.isDigit(c)) {
            if (w < buf.len) {
                buf[w] = '#';
                w += 1;
            }
            while (i < title.len and std.ascii.isDigit(title[i])) i += 1; // collapse digit run
            last_space = false;
            continue;
        }
        if (std.ascii.isAlphabetic(c)) {
            buf[w] = std.ascii.toLower(c);
            w += 1;
            last_space = false;
        } else {
            if (!last_space and w < buf.len) {
                buf[w] = ' ';
                w += 1;
                last_space = true;
            }
        }
        i += 1;
    }
    var out = buf[0..w];
    if (out.len > 0 and out[out.len - 1] == ' ') out = out[0 .. out.len - 1];
    return out;
}

// === tests ===

test "normalizeTitle collapses digits + punctuation" {
    var buf: [256]u8 = undefined;
    try std.testing.expectEqualStrings("train # stopped", normalizeTitle("Train 807 Stopped", &buf));
    try std.testing.expectEqualStrings("i felt rad", normalizeTitle("I felt rad!", &buf));
    try std.testing.expectEqualStrings("s#e# the cadillac", normalizeTitle("S07E14: The Cadillac", &buf));
}

test "templateScore: templated high, diverse low" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    // transit-alert style: shared scaffold "line/delays/route/detour"
    const templated = Stats{
        .doc_count = 6,
        .len_sum = 600,
        .empty_titles = 0,
        .digit_titles = 0,
        .sample = "green line delays\nblue line delays\nred line delays\nroute # detour\nroute # detour\ngold line delays",
    };
    // diverse human titles
    const diverse = Stats{
        .doc_count = 5,
        .len_sum = 4000,
        .empty_titles = 0,
        .digit_titles = 0,
        .sample = "new sleigh bells album\nthe dreaming void\nwe lost the thread\nsimple data fetching\ndangerous animals",
    };
    try std.testing.expect(diverse.templateScore(a) < 0.3);
    // templated scaffolding scores well above diverse human titles
    try std.testing.expect(templated.templateScore(a) > 0.4);
    try std.testing.expect(templated.templateScore(a) > diverse.templateScore(a) + 0.2);
}

test "score: flagged vs kept separation" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    const transit = Stats{
        .doc_count = 555,
        .len_sum = 555 * 183,
        .empty_titles = 0,
        .digit_titles = 400,
        .sample = "train # stopped\ntrain # delayed\nline service delayed\ntrain # annulled\nline delay\ntrain # delayed",
    };
    try std.testing.expect(transit.score(a) >= THRESHOLD);

    // real human, even with short content (mikebifulco-like): diverse titles save it
    const human = Stats{
        .doc_count = 203,
        .len_sum = 203 * 143,
        .empty_titles = 0,
        .digit_titles = 5,
        .sample = "struggling with typescript\nbreaking the cycle\ntiny improvements\nwhat to do when\nserendipity isnt\nopen source your work",
    };
    try std.testing.expect(human.score(a) < THRESHOLD);
}

test "parseVerdict: clear verdicts decide + carry reason, empty/garbled retries" {
    const a = std.testing.allocator;
    const machine = "{\"choices\":[{\"message\":{\"content\":\"{\\\"machine\\\": true, \\\"reason\\\":\\\"automated feed\\\"}\"}}]}";
    const human = "{\"choices\":[{\"message\":{\"content\":\"{\\\"machine\\\": false, \\\"reason\\\":\\\"personal blog\\\"}\"}}]}";

    const m = parseVerdict(a, machine).?;
    defer a.free(m.reason);
    try std.testing.expect(m.machine);
    try std.testing.expectEqualStrings("automated feed", m.reason);

    const h = parseVerdict(a, human).?;
    defer a.free(h.reason);
    try std.testing.expect(!h.machine);
    try std.testing.expectEqualStrings("personal blog", h.reason);

    try std.testing.expect(parseVerdict(a, "{\"choices\":[{\"message\":{\"content\":\"\"}}]}") == null); // → retry
    try std.testing.expect(parseVerdict(a, "not json") == null); // → retry
    try std.testing.expect(parseVerdict(a, "{\"choices\":[]}") == null); // → retry
}

test "parseVerdict: reasoning-model thought stream — LAST verdict wins, reason from final object" {
    const a = std.testing.allocator;
    // a thought stream that drafts the opposite verdict (with stray braces)
    // before the final JSON answer — the shape Qwen3.6/gemma-4 actually emit.
    const thinking =
        \\{"choices":[{"message":{"content":"Here's a thinking process:\n1. Task: {\"machine\": true|false}. Could be {\"machine\": true} if a feed...\n2. But the excerpts are varied prose.\n\n{\"machine\": false, \"reason\": \"varied personal prose across years\"}"}}]}
    ;
    const v = parseVerdict(a, thinking).?;
    defer a.free(v.reason);
    try std.testing.expect(!v.machine);
    try std.testing.expectEqualStrings("varied personal prose across years", v.reason);
}

test "rotating-slot template is flagged (regression: eligundry mood tracker)" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    // "I felt <mood>" — the mood enum splits unigram doc-frequency so no slot
    // word reaches the 50% scaffold bar; old coverage maxed out at ~0.5 and the
    // composite sat at 0.47, just under THRESHOLD.
    const mood = Stats{
        .doc_count = 643,
        .len_sum = 643 * 66, // one-line bodies
        .empty_titles = 0,
        .digit_titles = 13,
        .sample = "i felt rad\ni felt good\ni felt meh\ni felt rad\ni felt bad\ni felt good\n# days ago i felt rad",
    };
    try std.testing.expect(mood.score(a) >= THRESHOLD);
    // and the thin-volume promotion catches it independently of title shape
    try std.testing.expect(mood.thinPromote());

    // diverse short-titled human must NOT be swept up by the short-title rule
    const human = Stats{
        .doc_count = 203,
        .len_sum = 203 * 143,
        .empty_titles = 0,
        .digit_titles = 5,
        .sample = "struggling with typescript\nbreaking the cycle\ntiny improvements\nwhat to do when\nserendipity isnt\nopen source your work",
    };
    try std.testing.expect(human.score(a) < THRESHOLD);
    try std.testing.expect(!human.thinPromote());
}

test "date-titled journal is NOT flagged (precision regression: firstwaterbottle)" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    // observe() stores normalized titles; "1/27/2026" → "# # #" (no content words).
    const journal = Stats{
        .doc_count = 98,
        .len_sum = 98 * 400,
        .empty_titles = 0,
        .digit_titles = 98, // every title has digits
        .sample = "# # #\n# # #\n# # #\n# # #\n# # #\n# # #",
    };
    // the old code scored this maximally templated (1.0) → false positive.
    try std.testing.expectEqual(@as(f64, 0), journal.templateScore(a));
    try std.testing.expect(journal.score(a) < THRESHOLD);
}

test "extreme volume reaches review despite fluent unique titles (regression: trendingonweibo)" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    const feed_articles = Stats{
        .doc_count = 12_424,
        .len_sum = 47_538_291,
        .empty_titles = 0,
        .digit_titles = 2353,
        .sample = "faye wong makes a rare public appearance\nhow a one yuan note became china s most shared rebuttal\nchina s hotel chain made its pillows popular\nwhy a blockbuster is dominating weibo",
    };
    const score_value = feed_articles.score(a);
    try std.testing.expect(score_value < THRESHOLD);
    try std.testing.expect(!feed_articles.thinPromote());
    try std.testing.expect(feed_articles.extremeVolumePromote());
    try std.testing.expect(feed_articles.shouldReview(score_value));

    const prolific_human = Stats{
        .doc_count = 1195,
        .len_sum = 1195 * 1800,
        .empty_titles = 0,
        .digit_titles = 20,
        .sample = feed_articles.sample,
    };
    try std.testing.expect(!prolific_human.extremeVolumePromote());
}

test "operator positive and negated labels update enforcement state" {
    const conn = try zqlite.open(":memory:", zqlite.OpenFlags.Create | zqlite.OpenFlags.ReadWrite);
    defer conn.close();
    try conn.execNoArgs(
        \\CREATE TABLE author_stats (
        \\  did TEXT PRIMARY KEY,
        \\  labeled INTEGER NOT NULL DEFAULT 0,
        \\  state INTEGER NOT NULL DEFAULT 0,
        \\  reason TEXT NOT NULL DEFAULT ''
        \\)
    );

    setOperatorState(conn, "did:new", STATE_LABELED, "label emitted by operator");
    setOperatorState(conn, "did:existing", STATE_REJECTED, "label negated by operator");
    setOperatorState(conn, "did:existing", STATE_LABELED, "label emitted by operator");

    const labeled = (try conn.row("SELECT state, reason FROM author_stats WHERE did = 'did:new'", .{})).?;
    defer labeled.deinit();
    try std.testing.expectEqual(STATE_LABELED, labeled.int(0));
    try std.testing.expectEqualStrings("label emitted by operator", labeled.text(1));

    const relabeled = (try conn.row("SELECT state, reason FROM author_stats WHERE did = 'did:existing'", .{})).?;
    defer relabeled.deinit();
    try std.testing.expectEqual(STATE_LABELED, relabeled.int(0));
    try std.testing.expectEqualStrings("label emitted by operator", relabeled.text(1));
}

test "repeated puts for one URI count as one document" {
    const conn = try zqlite.open(":memory:", zqlite.OpenFlags.Create | zqlite.OpenFlags.ReadWrite);
    defer conn.close();
    try conn.execNoArgs("CREATE TABLE observed_documents (uri TEXT PRIMARY KEY)");

    try std.testing.expect(claimDocument(conn, "at://did:example/site.standard.document/one"));
    try std.testing.expect(!claimDocument(conn, "at://did:example/site.standard.document/one"));
    try std.testing.expect(claimDocument(conn, "at://did:example/site.standard.document/two"));

    const row = (try conn.row("SELECT COUNT(*) FROM observed_documents", .{})).?;
    defer row.deinit();
    try std.testing.expectEqual(@as(i64, 2), row.int(0));
}

test "utf8Excerpt never splits a codepoint (regression: CJK authors stuck inconclusive)" {
    // "스케치" is 9 bytes (3 per codepoint); cutting at 4/5 must fall back to 3.
    const kr = "스케치";
    try std.testing.expectEqualStrings("스", utf8Excerpt(kr, 4));
    try std.testing.expectEqualStrings("스", utf8Excerpt(kr, 5));
    try std.testing.expectEqualStrings("스케", utf8Excerpt(kr, 6));
    try std.testing.expectEqualStrings(kr, utf8Excerpt(kr, 100)); // shorter than max: untouched
    try std.testing.expectEqualStrings("abc", utf8Excerpt("abcdef", 3)); // ascii: plain cut
    try std.testing.expect(std.unicode.utf8ValidateSlice(utf8Excerpt(kr, 4)));
}

test "utf8Middle: bounded middle window, valid UTF-8 on both edges" {
    // 30 Korean codepoints = 90 bytes; a 20-byte middle window lands mid-codepoint
    // on both sides without alignment.
    const kr = "스케치" ** 10;
    const mid = utf8Middle(kr, 20);
    try std.testing.expect(mid.len <= 20 and mid.len > 0);
    try std.testing.expect(std.unicode.utf8ValidateSlice(mid));
    try std.testing.expectEqualStrings("short", utf8Middle("short", 20)); // shorter than max: whole string
    try std.testing.expectEqualStrings("cde", utf8Middle("abcdefg", 3)); // ascii: centered
}

test "excerptIndex: votes read different, evenly spread, in-range documents" {
    const total: usize = 92;
    for (0..VOTES) |vote| {
        var prev: usize = 0;
        for (0..EXCERPTS_PER_VOTE) |j| {
            const idx = excerptIndex(total, j, vote);
            try std.testing.expect(idx < total);
            if (j > 0) try std.testing.expect(idx > prev); // strictly advancing across the corpus
            prev = idx;
        }
        // consecutive votes see adjacent-but-different documents
        if (vote > 0) try std.testing.expect(excerptIndex(total, 0, vote) != excerptIndex(total, 0, vote - 1));
    }
    // tiny corpora stay in range
    try std.testing.expect(excerptIndex(1, 4, 2) == 0);
    try std.testing.expect(excerptIndex(3, 4, 2) < 3);
}

test "exhausted pending authors get fresh attempts on worker start (regression: sksksketch wedge)" {
    const conn = try zqlite.open(":memory:", zqlite.OpenFlags.Create | zqlite.OpenFlags.ReadWrite);
    defer conn.close();
    try conn.execNoArgs("CREATE TABLE author_stats (did TEXT PRIMARY KEY, state INTEGER NOT NULL, review_attempts INTEGER NOT NULL)");
    try conn.exec("INSERT INTO author_stats VALUES ('did:stuck', ?, ?)", .{ STATE_PENDING, MAX_REVIEW_ATTEMPTS });
    try conn.exec("INSERT INTO author_stats VALUES ('did:fresh', ?, 2)", .{STATE_PENDING});
    try conn.exec("INSERT INTO author_stats VALUES ('did:done', ?, ?)", .{ STATE_LABELED, MAX_REVIEW_ATTEMPTS });

    resetExhaustedAttempts(conn);

    const attempts = struct {
        fn of(c: zqlite.Conn, did: []const u8) !i64 {
            const row = (try c.row("SELECT review_attempts FROM author_stats WHERE did = ?", .{did})).?;
            defer row.deinit();
            return row.int(0);
        }
    };
    try std.testing.expectEqual(@as(i64, 0), try attempts.of(conn, "did:stuck")); // visible to nextPending again
    try std.testing.expectEqual(@as(i64, 2), try attempts.of(conn, "did:fresh")); // in-flight count untouched
    try std.testing.expectEqual(MAX_REVIEW_ATTEMPTS, try attempts.of(conn, "did:done")); // non-pending untouched
}
