//! Serving-time visibility policy: which publications asked to stay out of
//! discovery surfaces (`site.standard.publication` preferences.showInDiscover).
//!
//! This is deliberately a SET, not a per-row lookup, and that distinction is
//! the whole point. The previous design asked the local replica "is THIS uri
//! hidden?" — a question the replica cannot answer for a row it does not have
//! (a document indexed above the snapshot watermark, or a stale rkey that only
//! turso and turbopuffer still carry). A missing row returned "not hidden", so
//! absence read as permission and opted-out documents ranked in anonymous
//! search. See docs/visibility.md.
//!
//! The set of opted-out *publications* is complete even when the document
//! index is not: ~8.5k publications total, the opted-out subset is tiny, and
//! every retrieval path already carries enough identity (publication uri, or
//! did + base_path) to test membership. So every row gets a real answer.
//!
//! Freshness without putting turso on the hot path — the invariant this
//! codebase repeats in eight places (six background caches, the turso slot
//! shedder, the local replica itself):
//!   - seed from the local replica at startup: no network, complete for
//!     everything below the snapshot watermark
//!   - refresh from turso in the background: picks up opt-outs newer than the
//!     snapshot, plus preference flips on existing publications
//!   - fail static: a failed refresh keeps the last known good set
//! A read is a hash lookup under a mutex. No I/O, ever, on the request path.

const std = @import("std");
const mem = std.mem;
const Allocator = mem.Allocator;
const Io = std.Io;
const logfire = @import("logfire");
const db = @import("db.zig");

/// How often to re-read the opted-out set from turso.
const REFRESH_INTERVAL_SECS: u64 = 300;

/// Publications that opted out of discovery. Both queries are bounded by the
/// publication count (~8.5k), not the document count, and run off-path.
const UNDISCOVERABLE_SQL =
    \\SELECT uri, did, COALESCE(base_path, '') FROM publications
    \\WHERE COALESCE(show_in_discover, 1) = 0
;

// zig 0.16 moved locks onto std.Io, so taking one needs the Io handle —
// same shape as server/cache.zig. `global_io` is null only in unit tests,
// which are single-threaded, so the lock degrades to a no-op there.
var lock: Io.Mutex = Io.Mutex.init;

fn acquire() void {
    if (global_io) |io| lock.lockUncancelable(io);
}

fn release() void {
    if (global_io) |io| lock.unlock(io);
}

/// publication uri -> {} and "did\x00base_path" -> {}. Two keyings of one set:
/// SQL paths know the publication uri; turbopuffer results only carry
/// did + base_path (vectors predate any publication_uri attribute, and we do
/// not want a 64k-vector backfill to be load-bearing for a policy check).
var by_uri: std.StringHashMapUnmanaged(void) = .empty;
var by_did_path: std.StringHashMapUnmanaged(void) = .empty;
var arena: ?std.heap.ArenaAllocator = null;
var loaded: bool = false;
var global_io: ?Io = null;

/// True once the set has been populated at least once. Until then callers
/// cannot get a trustworthy answer; `isUndiscoverable*` fails CLOSED so a
/// boot-time gap can never publish an opted-out document.
pub fn isLoaded() bool {
    acquire();
    defer release();
    return loaded;
}

fn didPathKey(buf: []u8, did: []const u8, base_path: []const u8) ?[]const u8 {
    if (did.len + 1 + base_path.len > buf.len) return null;
    @memcpy(buf[0..did.len], did);
    buf[did.len] = 0;
    @memcpy(buf[did.len + 1 ..][0..base_path.len], base_path);
    return buf[0 .. did.len + 1 + base_path.len];
}

/// Does this publication uri belong to an author who opted out of discovery?
/// Fails CLOSED: before the first successful load we do not know, and "I do
/// not know" must never render as "allowed" (stream's invariant: an internal
/// failure must never look like an absence).
pub fn isUndiscoverablePub(uri: []const u8) bool {
    acquire();
    defer release();
    if (!loaded) return true;
    return by_uri.contains(uri);
}

/// Same question for a document, keyed by the identity every retrieval path
/// carries. A document with no publication (looseleaf: empty base_path,
/// has_publication = false) has no publication preference to honor.
pub fn isUndiscoverableDoc(publication_uri: []const u8, did: []const u8, base_path: []const u8) bool {
    acquire();
    defer release();
    if (!loaded) return true;
    if (publication_uri.len > 0 and by_uri.contains(publication_uri)) return true;
    if (base_path.len == 0) return false;
    var buf: [512]u8 = undefined;
    const key = didPathKey(&buf, did, base_path) orelse return false;
    return by_did_path.contains(key);
}

/// Number of publications currently opted out (observability / tests).
pub fn count() usize {
    acquire();
    defer release();
    return by_uri.count();
}

const Entry = struct { uri: []const u8, did: []const u8, base_path: []const u8 };

/// Swap in a freshly built set. Takes the write lock only for the pointer
/// swap; building happens outside it.
fn install(entries: []const Entry) !void {
    var next_arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    errdefer next_arena.deinit();
    const a = next_arena.allocator();

    var next_uri: std.StringHashMapUnmanaged(void) = .empty;
    var next_did_path: std.StringHashMapUnmanaged(void) = .empty;

    for (entries) |e| {
        try next_uri.put(a, try a.dupe(u8, e.uri), {});
        if (e.base_path.len == 0) continue;
        var buf: [512]u8 = undefined;
        const key = didPathKey(&buf, e.did, e.base_path) orelse continue;
        try next_did_path.put(a, try a.dupe(u8, key), {});
    }

    acquire();
    defer release();
    const old = arena;
    by_uri = next_uri;
    by_did_path = next_did_path;
    arena = next_arena;
    loaded = true;
    if (old) |*o| {
        var mutable = o.*;
        mutable.deinit();
    }
}

/// Seed from the local replica: no network, available as soon as the replica
/// opens, and complete for every publication at or below the snapshot
/// watermark. Publications that opted out after the snapshot are picked up by
/// the first turso refresh.
pub fn seedFromLocal() void {
    const local = db.getLocalDb() orelse {
        logfire.warn("visibility: local replica unavailable, cannot seed", .{});
        return;
    };

    var gpa = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer gpa.deinit();
    const a = gpa.allocator();

    var entries: std.ArrayList(Entry) = .empty;
    var rows = local.query(UNDISCOVERABLE_SQL, .{}) catch |err| {
        logfire.warn("visibility: local seed query failed: {s}", .{@errorName(err)});
        return;
    };
    defer rows.deinit();
    while (rows.next()) |row| {
        entries.append(a, .{
            .uri = a.dupe(u8, row.text(0)) catch continue,
            .did = a.dupe(u8, row.text(1)) catch continue,
            .base_path = a.dupe(u8, row.text(2)) catch continue,
        }) catch continue;
    }

    install(entries.items) catch |err| {
        logfire.err("visibility: seed install failed: {s}", .{@errorName(err)});
        return;
    };
    logfire.info("visibility: seeded {d} undiscoverable publications from replica", .{entries.items.len});
}

/// Re-read from turso — the authority, and the only source that knows about
/// publications newer than the snapshot. Keeps the previous set on failure.
fn refreshFromTurso() void {
    const client = db.getClient() orelse return;

    var gpa = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer gpa.deinit();
    const a = gpa.allocator();

    var result = client.query(UNDISCOVERABLE_SQL, &.{}) catch |err| {
        logfire.warn("visibility: turso refresh failed, keeping previous set: {s}", .{@errorName(err)});
        return;
    };
    defer result.deinit();

    var entries: std.ArrayList(Entry) = .empty;
    for (result.rows) |row| {
        entries.append(a, .{
            .uri = a.dupe(u8, row.text(0)) catch continue,
            .did = a.dupe(u8, row.text(1)) catch continue,
            .base_path = a.dupe(u8, row.text(2)) catch continue,
        }) catch continue;
    }

    const before = count();

    // A set that was non-empty and is now empty means nothing is filtered
    // anymore — the exact silent fail-open this module exists to prevent.
    // Every author opting back in simultaneously is possible but far less
    // likely than a schema or query fault, so make it loud and alertable.
    // We still install it: the data is the data, and refusing would strand an
    // author who genuinely opted back in.
    if (before > 0 and entries.items.len == 0) {
        logfire.err(
            "visibility: undiscoverable set collapsed {d} -> 0; nothing is being filtered — verify publications.show_in_discover",
            .{before},
        );
        logfire.counter("visibility.set_collapsed", 1);
    }

    install(entries.items) catch |err| {
        logfire.err("visibility: refresh install failed: {s}", .{@errorName(err)});
        return;
    };
    if (entries.items.len != before) {
        logfire.info("visibility: undiscoverable set {d} -> {d}", .{ before, entries.items.len });
    }
    logfire.gaugeInt("visibility.undiscoverable_publications", @intCast(entries.items.len));
}

fn worker() void {
    const io = global_io orelse return;
    // Turso first, and immediately: it is initialized before the listener
    // starts accepting, whereas the replica takes ~1.4s to open. Until the set
    // loads, search answers 503, so this query is the length of that window —
    // a turso point read (p50 10ms) rather than a replica open.
    refreshFromTurso();
    while (true) {
        io.sleep(Io.Duration.fromSeconds(REFRESH_INTERVAL_SECS), .awake) catch {};
        refreshFromTurso();
    }
}

/// Start the refresher. Non-blocking: the seed happens on the spawned thread
/// so a slow turso cannot stall the rest of initServices behind it.
///
/// Call this FIRST in initServices. Everything after it (migrations, opening
/// the replica) is slower, and the search paths fail closed until the set
/// exists — so anything ordered before this is added directly to the window
/// where search answers 503.
pub fn start(io: Io) void {
    global_io = io;
    const thread = std.Thread.spawn(.{}, worker, .{}) catch |err| {
        logfire.err("visibility: refresher failed to start: {s}", .{@errorName(err)});
        return;
    };
    thread.detach();
}

/// Test-only: install a known set without touching a database.
pub fn installForTest(entries: []const Entry) !void {
    try install(entries);
}

/// Test-only: return to the never-loaded state.
pub fn resetForTest() void {
    acquire();
    defer release();
    loaded = false;
    by_uri = .empty;
    by_did_path = .empty;
    if (arena) |*o| {
        var mutable = o.*;
        mutable.deinit();
        arena = null;
    }
}

test "unloaded set fails closed" {
    resetForTest();
    // Before the first load nothing is known — and an unknown must never
    // render as permission. This is the exact bug the module exists to kill.
    try std.testing.expect(isUndiscoverablePub("at://did:plc:x/site.standard.publication/notes"));
    try std.testing.expect(isUndiscoverableDoc("", "did:plc:x", "notes.example"));
}

test "membership by publication uri" {
    resetForTest();
    try installForTest(&.{
        .{ .uri = "at://did:plc:x/site.standard.publication/notes", .did = "did:plc:x", .base_path = "notes.example" },
    });
    try std.testing.expect(isUndiscoverablePub("at://did:plc:x/site.standard.publication/notes"));
    try std.testing.expect(!isUndiscoverablePub("at://did:plc:y/site.standard.publication/blog"));
    try std.testing.expectEqual(@as(usize, 1), count());
}

test "membership by did+base_path covers rows with no publication uri" {
    resetForTest();
    try installForTest(&.{
        .{ .uri = "at://did:plc:x/site.standard.publication/notes", .did = "did:plc:x", .base_path = "notes.example" },
    });
    // A turbopuffer hit carries did + base_path but no publication uri. This
    // is the ghost-rkey case: the document is absent from the replica, so the
    // old per-uri lookup returned "not hidden" and leaked it.
    try std.testing.expect(isUndiscoverableDoc("", "did:plc:x", "notes.example"));
    try std.testing.expect(!isUndiscoverableDoc("", "did:plc:x", "other.example"));
    try std.testing.expect(!isUndiscoverableDoc("", "did:plc:z", "notes.example"));
}

test "looseleaf documents have no publication preference to honor" {
    resetForTest();
    try installForTest(&.{
        .{ .uri = "at://did:plc:x/site.standard.publication/notes", .did = "did:plc:x", .base_path = "notes.example" },
    });
    // empty base_path + no publication uri = whitewind-style looseleaf
    try std.testing.expect(!isUndiscoverableDoc("", "did:plc:x", ""));
}

test "a document of an opted-out publication is hidden even when its uri is unknown" {
    resetForTest();
    try installForTest(&.{
        .{ .uri = "at://did:plc:x/site.standard.publication/notes", .did = "did:plc:x", .base_path = "notes.example" },
    });
    // Superseded rkey: turso and turbopuffer still carry it, the replica does
    // not. The set answers anyway, which is the property the old point lookup
    // lacked.
    try std.testing.expect(isUndiscoverableDoc(
        "at://did:plc:x/site.standard.publication/notes",
        "did:plc:x",
        "notes.example",
    ));
}

/// The opt-in is scoped to ONE identity per request.
///
/// `include_undiscoverable=true` exists so an author can read their own
/// unlisted writing. Honored on a global query it is a corpus-wide switch
/// instead: anyone can flip one boolean and read every opted-out publication,
/// including other people's. Verified 2026-08-08 against a third party's
/// publication, so this is not hypothetical.
///
/// Requiring an author does not make it authenticated — a caller can still
/// name someone else — but it removes the enumeration: you must already know
/// whose writing you are asking for, and you get only theirs.
pub fn optInIsScoped(author_filter: ?[]const u8) bool {
    const author = author_filter orelse return false;
    return author.len > 0;
}

test "the opt-in requires an author" {
    try std.testing.expect(!optInIsScoped(null));
    try std.testing.expect(!optInIsScoped(""));
    try std.testing.expect(optInIsScoped("did:plc:x"));
    try std.testing.expect(optInIsScoped("nate.bsky.social"));
}

/// Every uri in a batch fetch must belong to the same repo when the opt-in is
/// set, for the same reason: one identity per request.
pub fn urisShareOneDid(uris: []const []const u8) bool {
    var first: ?[]const u8 = null;
    for (uris) |uri| {
        const rest = if (mem.startsWith(u8, uri, "at://")) uri["at://".len..] else uri;
        const slash = mem.indexOfScalar(u8, rest, '/') orelse return false;
        const did = rest[0..slash];
        if (first) |f| {
            if (!mem.eql(u8, f, did)) return false;
        } else first = did;
    }
    return first != null;
}

test "batch opt-in is limited to a single repo" {
    try std.testing.expect(urisShareOneDid(&.{
        "at://did:plc:x/site.standard.document/a",
        "at://did:plc:x/site.standard.document/b",
    }));
    try std.testing.expect(!urisShareOneDid(&.{
        "at://did:plc:x/site.standard.document/a",
        "at://did:plc:y/site.standard.document/b",
    }));
    try std.testing.expect(!urisShareOneDid(&.{}));
    try std.testing.expect(!urisShareOneDid(&.{"garbage"}));
}
