//! Background worker for reconciling stale documents.
//!
//! Two checks per doc per cycle:
//!  1. Does the PDS record still exist? If 400/404 → delete from turso + tpuf.
//!  2. Does the destination URL we'd link to still resolve? If 404 →
//!     soft-hide (set documents.url_dead=1) so search excludes it without
//!     deleting the row. We can't delete on URL-dead because the ingester re-adds
//!     the doc on the next resync (insertDocument doesn't consult
//!     tombstones), which would flap.
//!
//! Per-host throttle: max 1 HEAD/sec to any single destination host so we
//! don't burst when a batch happens to be from one publisher.

const std = @import("std");
const http = std.http;
const json = std.json;
const mem = std.mem;
const Allocator = mem.Allocator;
const Io = std.Io;
const logfire = @import("logfire");
const db = @import("../db.zig");
const tpuf = @import("../tpuf.zig");
const indexer = @import("indexer.zig");
const search = @import("../server/search.zig");

// config (env vars with defaults)
fn getenv(key: [*:0]const u8) ?[]const u8 {
    return if (std.c.getenv(key)) |p| std.mem.span(p) else null;
}

fn getIntervalSecs() u64 {
    const val = getenv("RECONCILE_INTERVAL_SECS") orelse "1800";
    return std.fmt.parseInt(u64, val, 10) catch 1800;
}

/// Documents per cycle. 64k docs / 7-day reverify = ~9,200/day; at 48
/// cycles/day that needs ~192. Only meaningful because the network phase runs
/// concurrently — serially, cycle time scaled with this and throughput did not.
fn getBatchSize() usize {
    const val = getenv("RECONCILE_BATCH_SIZE") orelse "200";
    return std.fmt.parseInt(usize, val, 10) catch 200;
}

fn getReverifyDays() u64 {
    const val = getenv("RECONCILE_REVERIFY_DAYS") orelse "7";
    return std.fmt.parseInt(u64, val, 10) catch 7;
}

/// Wall-clock bound per outbound PDS/PLC request. zig's http client has no
/// read timeout, so an unanswering peer blocks forever without one.
const DEFAULT_HTTP_TIMEOUT_SECS: u64 = 10;

fn getHttpTimeoutSecs() u64 {
    const val = getenv("RECONCILE_HTTP_TIMEOUT_SECS") orelse "10";
    return std.fmt.parseInt(u64, val, 10) catch DEFAULT_HTTP_TIMEOUT_SECS;
}

fn isEnabled() bool {
    const val = getenv("RECONCILE_ENABLED") orelse "true";
    return !mem.eql(u8, val, "false") and !mem.eql(u8, val, "0");
}

/// Kill switch for the per-doc destination URL HEAD check (the url_dead
/// feature). zig 0.16 std.http.Client.fetch panics with "attempt to use
/// null value" on certain redirect chains (observed: blog.karashiiro.moe's
/// auth-callback bounce, 3 hops cross-domain → relative). The panic
/// bypasses our `catch return .url_skip`, crash-loops the reconciler, and
/// blocks PDS verification — which is the reconciler's primary job.
/// Default off until the stdlib bug is worked around.
fn isUrlCheckEnabled() bool {
    const val = getenv("RECONCILE_URL_CHECK_ENABLED") orelse "false";
    return mem.eql(u8, val, "true") or mem.eql(u8, val, "1");
}

var global_io: ?Io = null;

/// AT-URI components parsed from "at://{did}/{collection}/{rkey}"
const UriParts = struct {
    did: []const u8,
    collection: []const u8,
    rkey: []const u8,
};

fn parseAtUri(uri: []const u8) ?UriParts {
    const prefix = "at://";
    if (!mem.startsWith(u8, uri, prefix)) return null;
    const rest = uri[prefix.len..];

    const first_slash = mem.indexOf(u8, rest, "/") orelse return null;
    const did = rest[0..first_slash];
    const after_did = rest[first_slash + 1 ..];

    const second_slash = mem.indexOf(u8, after_did, "/") orelse return null;
    const collection = after_did[0..second_slash];
    const rkey = after_did[second_slash + 1 ..];

    if (did.len == 0 or collection.len == 0 or rkey.len == 0) return null;
    return .{ .did = did, .collection = collection, .rkey = rkey };
}

/// Start the reconciler background worker.
pub fn start(allocator: Allocator, io: Io) void {
    if (!isEnabled()) {
        logfire.info("reconcile: disabled via RECONCILE_ENABLED", .{});
        return;
    }

    global_io = io;
    const thread = std.Thread.spawn(.{}, worker, .{ allocator, io }) catch |err| {
        logfire.err("reconcile: failed to start thread: {}", .{err});
        return;
    };
    thread.detach();
    logfire.info("reconcile: background worker started", .{});
}

fn worker(allocator: Allocator, io: Io) void {
    // wait for db to be ready
    io.sleep(Io.Duration.fromSeconds(10), .awake) catch {};

    // PDS cache: DID → PDS endpoint URL (persists across cycles)
    var pds_cache = std.StringHashMap([]const u8).init(allocator);
    defer {
        var it = pds_cache.iterator();
        while (it.next()) |entry| {
            allocator.free(entry.key_ptr.*);
            allocator.free(entry.value_ptr.*);
        }
        pds_cache.deinit();
    }

    // Per-host throttle for destination URL HEADs: tracks the last
    // monotonic timestamp (ns) we hit each host. Used to enforce
    // HEAD_HOST_MIN_GAP_MS regardless of batch composition — keeps us from
    // bursting a single publisher when their docs cluster at the front of
    // the queue.
    var last_head_ns = std.StringHashMap(i128).init(allocator);
    defer {
        var it2 = last_head_ns.iterator();
        while (it2.next()) |entry| allocator.free(entry.key_ptr.*);
        last_head_ns.deinit();
    }

    var consecutive_errors: u32 = 0;

    while (true) {
        const result = runCycle(allocator, &pds_cache, &last_head_ns);
        if (result) |counts| {
            consecutive_errors = 0;
            if (counts.verified > 0 or counts.deleted > 0) {
                logfire.info("reconcile: verified {d} documents, deleted {d}", .{ counts.verified, counts.deleted });
            }
            // per-cycle counts look healthy even when the sweep can never
            // catch up; only the lag numbers show that.
            reportSweepLag();
        } else |err| {
            consecutive_errors += 1;
            logfire.warn("reconcile: cycle error: {}, consecutive: {d}", .{ err, consecutive_errors });
        }

        const interval = getIntervalSecs();
        const backoff_secs: u64 = if (consecutive_errors > 0)
            @min(interval * consecutive_errors, 3600)
        else
            interval;
        io.sleep(Io.Duration.fromSeconds(@intCast(backoff_secs)), .awake) catch {};
    }
}

const CycleCounts = struct {
    verified: usize,
    deleted: usize,
};

/// One row of the verification queue. Hoisted to file scope so the parallel
/// network phase can take a slice of them.
const DocInfo = struct {
    uri: []const u8,
    did: []const u8,
    base_path: []const u8,
    path: []const u8,
    platform: []const u8,
    rkey: []const u8,
    has_publication: bool,
};

/// Decided in the network phase, applied serially afterwards so turso keeps
/// its single-writer pattern.
const Outcome = union(enum) {
    skip, // unparseable uri, or never reached
    no_pds, // DID deactivated / PLC unknown — verify, don't delete
    bridgy, // brid.gy-hosted; mark excluded
    checked: RecordStatus,
};

/// Throughput is per-doc latency bound (cycle = batch x per_doc), so only
/// concurrency moves it. Each worker pauses 200ms between documents, capping
/// us at ~5 req/s per worker against any one publisher.
const DEFAULT_CONCURRENCY: usize = 8;
const MAX_CONCURRENCY: usize = 32;

fn getConcurrency() usize {
    const val = getenv("RECONCILE_CONCURRENCY") orelse "8";
    const n = std.fmt.parseInt(usize, val, 10) catch DEFAULT_CONCURRENCY;
    return std.math.clamp(n, 1, MAX_CONCURRENCY);
}

/// Shared state for the network phase. Workers pull documents off `next`, so
/// a slow host stalls one worker instead of the whole sweep.
const CheckCtx = struct {
    allocator: Allocator,
    io: Io,
    docs: []const DocInfo,
    outcomes: []Outcome,
    next: std.atomic.Value(usize),
    cache: *std.StringHashMap([]const u8),
    cache_lock: Io.Mutex,
};

/// HTTP miss happens outside the lock; holding it across a network call would
/// serialize every worker behind the slowest DID.
fn resolvePdsShared(ctx: *CheckCtx, did: []const u8) ?[]const u8 {
    ctx.cache_lock.lockUncancelable(ctx.io);
    const cached = ctx.cache.get(did);
    ctx.cache_lock.unlock(ctx.io);
    if (cached) |pds| return pds;

    const span = logfire.span("reconcile.resolve_pds", .{});
    defer span.end();
    const pds = resolvePdsHttp(ctx.allocator, did) orelse return null;

    ctx.cache_lock.lockUncancelable(ctx.io);
    defer ctx.cache_lock.unlock(ctx.io);
    // Two workers can miss the same DID concurrently. Keep the first winner
    // rather than overwriting it — overwriting would leak the value that
    // other threads may already be holding a pointer to.
    if (ctx.cache.get(did)) |existing| {
        ctx.allocator.free(pds);
        return existing;
    }
    const key = ctx.allocator.dupe(u8, did) catch return pds;
    ctx.cache.put(key, pds) catch {
        ctx.allocator.free(key);
    };
    return pds;
}

/// Claim the next index, or null when exhausted. Factored out to test the
/// risk directly: a doc claimed twice is deleted twice, one never claimed is
/// silently never verified.
fn claimNext(next: *std.atomic.Value(usize), len: usize) ?usize {
    const i = next.fetchAdd(1, .monotonic);
    if (i >= len) return null;
    return i;
}

fn checkWorker(ctx: *CheckCtx) void {
    while (true) {
        const i = claimNext(&ctx.next, ctx.docs.len) orelse return;
        const doc = ctx.docs[i];

        const parts = parseAtUri(doc.uri) orelse {
            logfire.warn("reconcile: invalid AT-URI: {s}", .{doc.uri});
            ctx.outcomes[i] = .skip;
            continue;
        };

        // counters fire before the call, spans only on completion, so
        // attempts-minus-completions names a stuck phase.
        logfire.counter("reconcile.resolve_attempt", 1);
        const pds = resolvePdsShared(ctx, parts.did) orelse {
            ctx.outcomes[i] = .no_pds;
            continue;
        };

        if (std.mem.indexOf(u8, pds, "brid.gy") != null) {
            ctx.outcomes[i] = .bridgy;
            continue;
        }

        logfire.counter("reconcile.check_attempt", 1);
        ctx.outcomes[i] = .{ .checked = checkRecord(ctx.allocator, pds, parts.did, parts.collection, parts.rkey) };

        // Politeness, per worker: at most ~5 requests/second each.
        ctx.io.sleep(Io.Duration.fromMilliseconds(200), .awake) catch {};
    }
}

/// Emit how far behind the verification sweep is: the age in days of the
/// oldest document still awaiting (re)verification. This is the number that
/// says whether the reconciler can meet RECONCILE_REVERIFY_DAYS at all —
/// per-cycle counts cannot, because a starved sweep and a healthy one both
/// log the same "verified 50".
///
/// One aggregate over an indexed column, once per cycle (default every 30
/// min), off the request path.
fn reportSweepLag() void {
    const client = db.getClient() orelse return;
    // Two separate facts: coalescing NULL to an epoch date reported the age of
    // 1970 (20673d) rather than any real verification.
    var result = client.query(
        \\SELECT
        \\  CAST(COALESCE((julianday('now') - julianday(MIN(verified_at))), 0) AS INTEGER),
        \\  SUM(CASE WHEN verified_at IS NULL THEN 1 ELSE 0 END)
        \\FROM documents
    , &.{}) catch |err| {
        logfire.warn("reconcile: sweep-lag probe failed: {s}", .{@errorName(err)});
        return;
    };
    defer result.deinit();
    if (result.rows.len == 0) return;

    const lag_days = result.rows[0].int(0);
    const never_verified = result.rows[0].int(1);
    logfire.gaugeInt("reconcile.oldest_verified_days", lag_days);
    logfire.gaugeInt("reconcile.never_verified_docs", never_verified);

    const target = getReverifyDays();
    if (lag_days > 0 and @as(u64, @intCast(lag_days)) > target * 2) {
        logfire.warn(
            "reconcile: oldest verification is {d}d old against a {d}d target ({d} documents never verified)",
            .{ lag_days, target, never_verified },
        );
    }
}

fn runCycle(allocator: Allocator, pds_cache: *std.StringHashMap([]const u8), last_head_ns: *std.StringHashMap(i128)) !CycleCounts {
    const span = logfire.span("reconcile.cycle", .{});
    defer span.end();

    const client = db.getClient() orelse return error.NoClient;
    const batch_size = getBatchSize();
    const reverify_days = getReverifyDays();

    // fetch docs ordered by verified_at (NULLs first = never verified = highest priority)
    // re-verify docs older than RECONCILE_REVERIFY_DAYS
    // compute cutoff timestamp in Zig (avoids strftime with parameterized modifiers)
    var batch_str: [10]u8 = undefined;
    const batch_str_val = std.fmt.bufPrint(&batch_str, "{d}", .{batch_size}) catch "200";

    const io = global_io.?;
    const now_s: i64 = @intCast(@divFloor(Io.Timestamp.now(io, .real).nanoseconds, std.time.ns_per_s));
    const cutoff_ts = formatTimestamp(now_s - @as(i64, @intCast(reverify_days * 86400)));
    const cutoff = cutoff_ts.slice();

    // Also pull URL-construction columns so we can HEAD the destination URL
    // in the same cycle without a second round-trip per doc.
    var result = try client.query(
        \\SELECT uri, did, COALESCE(base_path, '') AS base_path,
        \\  COALESCE(path, '') AS path, platform, rkey, has_publication
        \\FROM documents
        \\WHERE verified_at IS NULL
        \\   OR verified_at < ?
        \\ORDER BY verified_at ASC NULLS FIRST
        \\LIMIT ?
    ,
        &.{ cutoff, batch_str_val },
    );
    defer result.deinit();

    if (result.rows.len == 0) return .{ .verified = 0, .deleted = 0 };

    // Logged at cycle START, not end: a cycle that never finishes emits no
    // reconcile.cycle span, so without this there is no record it ever ran.
    logfire.info("reconcile: cycle start, {d} documents (http bound {d}s)", .{
        result.rows.len,
        getHttpTimeoutSecs(),
    });

    // collect URIs + URL-construction fields (copy since result owns the memory)
    var docs: std.ArrayList(DocInfo) = .empty;
    defer {
        for (docs.items) |doc| {
            allocator.free(doc.uri);
            allocator.free(doc.did);
            allocator.free(doc.base_path);
            allocator.free(doc.path);
            allocator.free(doc.platform);
            allocator.free(doc.rkey);
        }
        docs.deinit(allocator);
    }

    for (result.rows) |row| {
        const uri = allocator.dupe(u8, row.text(0)) catch continue;
        errdefer allocator.free(uri);
        const did = allocator.dupe(u8, row.text(1)) catch continue;
        errdefer allocator.free(did);
        const base_path = allocator.dupe(u8, row.text(2)) catch continue;
        errdefer allocator.free(base_path);
        const path = allocator.dupe(u8, row.text(3)) catch continue;
        errdefer allocator.free(path);
        const platform = allocator.dupe(u8, row.text(4)) catch continue;
        errdefer allocator.free(platform);
        const rkey = allocator.dupe(u8, row.text(5)) catch continue;
        errdefer allocator.free(rkey);
        docs.append(allocator, .{
            .uri = uri,
            .did = did,
            .base_path = base_path,
            .path = path,
            .platform = platform,
            .rkey = rkey,
            .has_publication = row.int(6) != 0,
        }) catch continue;
    }

    var verified: usize = 0;
    var deleted: usize = 0;

    // collect hashed IDs of stale docs for batch tpuf delete
    var stale_ids: std.ArrayList([32]u8) = .empty;
    defer stale_ids.deinit(allocator);

    // ---- phase 1: network, concurrent (no turso/tpuf) ----
    const outcomes = allocator.alloc(Outcome, docs.items.len) catch {
        return error.OutOfMemory;
    };
    defer allocator.free(outcomes);
    @memset(outcomes, .skip);

    var ctx = CheckCtx{
        .allocator = allocator,
        .io = io,
        .docs = docs.items,
        .outcomes = outcomes,
        .next = .init(0),
        .cache = pds_cache,
        .cache_lock = Io.Mutex.init,
    };

    {
        const net_span = logfire.span("reconcile.network_phase", .{});
        defer net_span.end();

        const worker_count = @min(getConcurrency(), docs.items.len);
        var threads: [MAX_CONCURRENCY]?std.Thread = @splat(null);
        for (0..worker_count) |w| {
            threads[w] = std.Thread.spawn(.{}, checkWorker, .{&ctx}) catch |err| blk: {
                logfire.warn("reconcile: worker {d} spawn failed: {s}", .{ w, @errorName(err) });
                break :blk null;
            };
        }
        // all spawns failing would report a clean cycle having verified
        // nothing, so fall back to this thread.
        var spawned: usize = 0;
        for (threads[0..worker_count]) |t| {
            if (t != null) spawned += 1;
        }
        if (spawned == 0) checkWorker(&ctx);
        for (threads[0..worker_count]) |t| {
            if (t) |thread| thread.join();
        }
    }

    // ---- phase 2: database, serial ----
    for (docs.items, outcomes) |doc, outcome| {
        switch (outcome) {
            .skip => {},
            // PDS unknown or DID deactivated — verify anyway so these don't
            // permanently clog the head of the queue.
            .no_pds => updateVerifiedAt(client, doc.uri),
            // brid.gy-hosted: bridged content we exclude from search.
            .bridgy => markBridgyfed(client, doc.uri),
            .checked => |status| switch (status) {
                .exists => {
                // PDS record is good — also check the destination URL we'd
                // link to. Per-host throttled inside checkDocUrl. 404 →
                // soft-hide; 2xx → reset url_dead (in case it came back).
                // Guarded by RECONCILE_URL_CHECK_ENABLED — std.http.Client
                // currently panics on some redirect chains, see isUrlCheckEnabled.
                if (isUrlCheckEnabled()) {
                    const doc_type: []const u8 = if (doc.has_publication) "article" else "looseleaf";
                    const url = search.buildDocUrl(allocator, doc_type, doc.platform, doc.base_path, doc.path, doc.rkey, doc.did);
                    defer allocator.free(url);
                    if (url.len > 0) {
                        switch (checkDocUrl(allocator, url, last_head_ns)) {
                            .url_dead => {
                                updateUrlDead(client, doc.uri, true);
                                logfire.info("reconcile: marked url_dead: {s} → {s}", .{ doc.uri, url });
                            },
                            .url_ok => updateUrlDead(client, doc.uri, false),
                            .url_skip => {}, // transient / 405 / timeout — leave alone
                        }
                    }
                }
                    updateVerifiedAt(client, doc.uri);
                    verified += 1;
                },
                .deleted => {
                    // record gone — delete from turso + queue for tpuf batch delete
                    indexer.deleteDocument(doc.uri);
                    const hashed = tpuf.hashId(doc.uri);
                    stale_ids.append(allocator, hashed) catch {};
                    deleted += 1;
                    logfire.info("reconcile: deleted stale document: {s}", .{doc.uri});
                },
                .error_skip => {
                    // 5xx / timeout / network error — don't update verified_at, retry next cycle
                },
            },
        }
        // politeness pause lives in the network phase now, where the remote
        // requests are.
    }

    if (stale_ids.items.len > 0 and tpuf.isEnabled()) {
        var id_ptrs = allocator.alloc([]const u8, stale_ids.items.len) catch {
            logfire.warn("reconcile: alloc failed for tpuf delete batch", .{});
            return .{ .verified = verified, .deleted = deleted };
        };
        defer allocator.free(id_ptrs);

        for (stale_ids.items, 0..) |*id, i| {
            id_ptrs[i] = id;
        }

        tpuf.delete(allocator, id_ptrs) catch |err| {
            logfire.warn("reconcile: tpuf batch delete failed: {}", .{err});
        };
    }

    return .{ .verified = verified, .deleted = deleted };
}

fn updateVerifiedAt(client: *db.Client, uri: []const u8) void {
    // exec already spans as db.query, so this is not a hidden cost — the span
    // is here to attribute turso time to the reconciler specifically rather
    // than leaving it pooled with every other db.query on the box.
    const span = logfire.span("reconcile.update_verified", .{});
    defer span.end();

    const ts: i64 = @intCast(@divFloor(Io.Timestamp.now(global_io.?, .real).nanoseconds, std.time.ns_per_s));
    const now = formatTimestamp(ts);
    client.exec(
        "UPDATE documents SET verified_at = ? WHERE uri = ?",
        &.{ now.slice(), uri },
    ) catch |err| {
        logfire.warn("reconcile: failed to update verified_at for {s}: {}", .{ uri, err });
    };
}

/// Mark a doc as bridgy-fed (excluded from all search paths) in Turso, the
/// source of truth. Bumps indexed_at so the next snapshot build excludes it
/// (the build is watermark-pinned on indexed_at and filters is_bridgyfed), and
/// stamps verified_at so the doc leaves the reconcile queue. The serving
/// replica is immutable between snapshot adoptions, so this is NOT visible
/// in-place — it takes effect when the next snapshot is adopted.
fn markBridgyfed(client: *db.Client, uri: []const u8) void {
    const ts: i64 = @intCast(@divFloor(Io.Timestamp.now(global_io.?, .real).nanoseconds, std.time.ns_per_s));
    const now = formatTimestamp(ts);
    client.exec(
        "UPDATE documents SET is_bridgyfed = '1', verified_at = ?, indexed_at = ? WHERE uri = ?",
        &.{ now.slice(), now.slice(), uri },
    ) catch |err| {
        logfire.warn("reconcile: failed to mark bridgyfed for {s}: {}", .{ uri, err });
    };
}

fn updateUrlDead(client: *db.Client, uri: []const u8, dead: bool) void {
    const val: []const u8 = if (dead) "1" else "0";
    client.exec(
        "UPDATE documents SET url_dead = ? WHERE uri = ?",
        &.{ val, uri },
    ) catch |err| {
        logfire.warn("reconcile: failed to update url_dead for {s}: {}", .{ uri, err });
    };
}

const UrlStatus = enum { url_ok, url_dead, url_skip };

// Min gap between HEAD requests to the same destination host. Keeps a
// reconcile batch from bursting one publisher when their docs cluster at
// the front of the verify queue. Effective ceiling: 1 req/sec per host.
const HEAD_HOST_MIN_GAP_NS: i128 = 1_000_000_000;

/// HEAD the destination URL; classify the outcome:
///   404            → .url_dead (definitive — record points at a gone URL)
///   2xx / 3xx      → .url_ok   (working — reset url_dead in case it flipped)
///   405 / 5xx /
///   timeout / err  → .url_skip (don't change url_dead)
/// Per-host throttled: sleeps if we've hit this host within
/// HEAD_HOST_MIN_GAP_NS, then updates the last-hit timestamp.
fn checkDocUrl(allocator: Allocator, url: []const u8, last_head_ns: *std.StringHashMap(i128)) UrlStatus {
    const io = global_io.?;
    const host = extractHost(url) orelse return .url_skip;

    // throttle: if we hit this host recently, sleep until the gap is satisfied.
    const now_ns: i128 = Io.Timestamp.now(io, .real).nanoseconds;
    if (last_head_ns.get(host)) |prev| {
        const elapsed = now_ns - prev;
        if (elapsed < HEAD_HOST_MIN_GAP_NS) {
            const wait_ns: u64 = @intCast(HEAD_HOST_MIN_GAP_NS - elapsed);
            io.sleep(Io.Duration.fromNanoseconds(@intCast(wait_ns)), .awake) catch {};
        }
    }
    // record the post-sleep timestamp. Use getOrPut so we don't leak the
    // duped key on second-and-subsequent hits to the same host.
    const stamp_ns: i128 = Io.Timestamp.now(io, .real).nanoseconds;
    if (last_head_ns.getOrPut(host)) |gop| {
        if (!gop.found_existing) {
            // first sighting — replace the borrowed (temporary) key slot with an
            // allocator-owned dupe so it survives past the current cycle.
            if (allocator.dupe(u8, host)) |owned| {
                gop.key_ptr.* = owned;
            } else |_| {
                _ = last_head_ns.remove(host);
            }
        }
        gop.value_ptr.* = stamp_ns;
    } else |_| {}

    var http_client: http.Client = .{ .allocator = allocator, .io = io };
    defer http_client.deinit();

    var sink: std.Io.Writer.Allocating = .init(allocator);
    defer sink.deinit();

    const res = http_client.fetch(.{
        .location = .{ .url = url },
        .method = .HEAD,
        .response_writer = &sink.writer,
    }) catch return .url_skip;

    const code: u10 = @intFromEnum(res.status);
    if (code == 404) return .url_dead;
    if (code >= 200 and code < 400) return .url_ok;
    // 405 (HEAD not allowed), 403, 429, 5xx, anything else — don't penalize
    return .url_skip;
}

/// Parse the host portion out of a URL: "https://example.com/foo" → "example.com".
/// Returns null for malformed input. Strips userinfo / port to normalize the
/// throttle key (so "example.com:443" and "example.com" share a bucket).
fn extractHost(url: []const u8) ?[]const u8 {
    const scheme_end = mem.indexOf(u8, url, "://") orelse return null;
    const after_scheme = url[scheme_end + 3 ..];
    const path_start = mem.indexOfAny(u8, after_scheme, "/?#") orelse after_scheme.len;
    var host = after_scheme[0..path_start];
    if (mem.indexOfScalar(u8, host, '@')) |at_idx| host = host[at_idx + 1 ..];
    if (mem.indexOfScalar(u8, host, ':')) |colon_idx| host = host[0..colon_idx];
    if (host.len == 0) return null;
    return host;
}

/// Format a unix timestamp as ISO 8601 string (same approach as embedder.zig).
const TimestampBuf = struct {
    buf: [20]u8,
    len: usize,

    fn slice(self: *const TimestampBuf) []const u8 {
        return self.buf[0..self.len];
    }
};

fn formatTimestamp(ts: i64) TimestampBuf {
    const epoch_secs: u64 = @intCast(@max(ts, 0));
    const epoch = std.time.epoch.EpochSeconds{ .secs = epoch_secs };
    const day = epoch.getDaySeconds();
    const yd = epoch.getEpochDay().calculateYearDay();
    const md = yd.calculateMonthDay();
    var result: TimestampBuf = undefined;
    const formatted = std.fmt.bufPrint(&result.buf, "{d:0>4}-{d:0>2}-{d:0>2}T{d:0>2}:{d:0>2}:{d:0>2}", .{
        yd.year,               md.month.numeric(),       @as(u32, md.day_index) + 1,
        day.getHoursIntoDay(), day.getMinutesIntoHour(), day.getSecondsIntoMinute(),
    }) catch {
        // fallback: epoch (will cause re-verify, which is safe)
        const fallback = "1970-01-01T00:00:00";
        @memcpy(result.buf[0..fallback.len], fallback);
        result.len = fallback.len;
        return result;
    };
    result.len = formatted.len;
    return result;
}

const RecordStatus = enum { exists, deleted, error_skip };

fn checkRecord(allocator: Allocator, pds: []const u8, did: []const u8, collection: []const u8, rkey: []const u8) RecordStatus {
    const span = logfire.span("reconcile.check_record", .{});
    defer span.end();

    // build URL: {pds}/xrpc/com.atproto.repo.getRecord?repo={did}&collection={collection}&rkey={rkey}
    var url_buf: [512]u8 = undefined;
    const url = std.fmt.bufPrint(&url_buf, "{s}/xrpc/com.atproto.repo.getRecord?repo={s}&collection={s}&rkey={s}", .{ pds, did, collection, rkey }) catch {
        return .error_skip;
    };

    var http_client: http.Client = .{ .allocator = allocator, .io = global_io.? };
    defer http_client.deinit();

    var response_body: std.Io.Writer.Allocating = .init(allocator);
    defer response_body.deinit();

    const res = db.Client.fetchBounded(&http_client, global_io.?, .{
        .location = .{ .url = url },
        .method = .GET,
        .response_writer = &response_body.writer,
    }, getHttpTimeoutSecs()) catch |err| {
        if (err == error.RequestTimeout) {
            logfire.warn("reconcile: PDS check timed out for {s}", .{did});
            logfire.counter("reconcile.pds_timeout", 1);
        }
        // A timeout is NOT evidence the record is gone — leave verified_at
        // alone so the document is retried, and never delete on a hang.
        return .error_skip;
    };

    const status_int: u10 = @intFromEnum(res.status);
    if (status_int >= 200 and status_int < 300) return .exists;
    if (status_int == 400 or status_int == 404) return .deleted;
    // 5xx, rate limit, or unexpected status — skip
    return .error_skip;
}

fn resolvePdsHttp(allocator: Allocator, did: []const u8) ?[]const u8 {
    var url_buf: [256]u8 = undefined;
    const url = std.fmt.bufPrint(&url_buf, "https://plc.directory/{s}", .{did}) catch return null;

    var http_client: http.Client = .{ .allocator = allocator, .io = global_io.? };
    defer http_client.deinit();

    var response_body: std.Io.Writer.Allocating = .init(allocator);
    defer response_body.deinit();

    const res = db.Client.fetchBounded(&http_client, global_io.?, .{
        .location = .{ .url = url },
        .method = .GET,
        .response_writer = &response_body.writer,
    }, getHttpTimeoutSecs()) catch |err| {
        logfire.warn("reconcile: PLC lookup failed for {s}: {s}", .{ did, @errorName(err) });
        if (err == error.RequestTimeout) logfire.counter("reconcile.plc_timeout", 1);
        return null;
    };

    if (res.status != .ok) {
        logfire.warn("reconcile: PLC lookup {s} returned {}", .{ did, res.status });
        return null;
    }

    const body = response_body.toOwnedSlice() catch return null;
    defer allocator.free(body);

    const parsed = json.parseFromSlice(json.Value, allocator, body, .{}) catch return null;
    defer parsed.deinit();

    // look for service[].serviceEndpoint where type == "AtprotoPersonalDataServer"
    const services = parsed.value.object.get("service") orelse return null;
    if (services != .array) return null;

    for (services.array.items) |svc| {
        if (svc != .object) continue;
        const svc_type = svc.object.get("type") orelse continue;
        if (svc_type != .string) continue;
        if (!mem.eql(u8, svc_type.string, "AtprotoPersonalDataServer")) continue;
        const endpoint = svc.object.get("serviceEndpoint") orelse continue;
        if (endpoint != .string) continue;

        // dupe the endpoint — it's owned by the parsed json which we're about to free
        return allocator.dupe(u8, endpoint.string) catch null;
    }

    return null;
}

test "concurrency is clamped to a sane range" {
    // A typo in the env must not spawn 10,000 threads at a PDS, and must not
    // spawn zero workers (which would report clean cycles that verified
    // nothing).
    try std.testing.expect(getConcurrency() >= 1);
    try std.testing.expect(getConcurrency() <= MAX_CONCURRENCY);
}

test "every document is claimed exactly once under concurrent workers" {
    // The parallel phase's correctness risk: a document claimed twice gets
    // deleted twice, and one never claimed is silently never verified while
    // the cycle still reports success.
    const N = 1000;
    var counts = [_]std.atomic.Value(u32){.init(0)} ** N;
    var next: std.atomic.Value(usize) = .init(0);

    const Runner = struct {
        fn run(nxt: *std.atomic.Value(usize), seen: []std.atomic.Value(u32)) void {
            while (claimNext(nxt, seen.len)) |i| {
                _ = seen[i].fetchAdd(1, .monotonic);
            }
        }
    };

    var threads: [8]std.Thread = undefined;
    for (&threads) |*t| t.* = try std.Thread.spawn(.{}, Runner.run, .{ &next, counts[0..] });
    for (threads) |t| t.join();

    for (&counts) |*c| try std.testing.expectEqual(@as(u32, 1), c.load(.monotonic));
}

test "claimNext stops at the batch end" {
    var next: std.atomic.Value(usize) = .init(0);
    try std.testing.expectEqual(@as(?usize, 0), claimNext(&next, 2));
    try std.testing.expectEqual(@as(?usize, 1), claimNext(&next, 2));
    try std.testing.expectEqual(@as(?usize, null), claimNext(&next, 2));
    // and stays exhausted rather than wrapping
    try std.testing.expectEqual(@as(?usize, null), claimNext(&next, 2));
}

test "an empty batch claims nothing" {
    var next: std.atomic.Value(usize) = .init(0);
    try std.testing.expectEqual(@as(?usize, null), claimNext(&next, 0));
}
