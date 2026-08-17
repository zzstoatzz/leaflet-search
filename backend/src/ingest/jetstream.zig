//! Jetstream V2 live-ingest consumer — the /channel replacement.
//!
//! Transport is the zat.dev/jetstream SDK's unified client
//! (`jetstream.subscribe`): archive sweep from `after_seq`, gapless seq-dedup
//! cutover to the live subscribeEvents tail, CursorTooOld re-entering
//! backfill — recovery is gap-free at ANY outage length, not bounded by a
//! subscribe replay window. This file owns only the pub-search concerns:
//! host policy, durable cursor persistence, corpus policy (banned DIDs,
//! bridgy fed), dispatch into the same path the /channel consumer uses, and
//! wedge escalation.
//!
//! Hosts: stream.waow.tech only by default. Its ingest runs full Sync 1.1
//! signature/MST verification ("no event is served or archived unverified"),
//! so records arriving here are operator-verified even though the blocks
//! needed to re-check are not on the wire. Multi-host failover exists in the
//! SDK (witnessed-time re-anchoring) but needs a per-host archive bearer
//! key, and only verified-V2 instances qualify — extend JETSTREAM_HOSTS
//! deliberately, never with a v1 instance.
//!
//! at-least-once contract: events are processed synchronously in onEvent
//! and the durable cursor (the instance seq) is persisted only after
//! dispatch returns (2s time gate). The SDK's seq dedup makes the
//! archive→live seam exact; process restarts resume from the persisted seq
//! with no rewind margin needed. Slow turso propagates as websocket
//! backpressure; a server-side drop re-dials inside the SDK. No ack
//! protocol, no outbox, no shedding.
//!
//! cursor migration: the file used to hold a v1 time_us (≥1e15); a v2 seq
//! is ~1e10. On boot a time_us value is converted once via the archive's
//! segment metadata (`fetchAnchor`), same magnitude split the server uses.
//!
//! policy parity with the fly-app ingester this replaces:
//!   - banned DIDs dropped via policy.isBanned (was the ingester's reader)
//!   - bridgy fed dropped via a cached DID→PDS check (was verifier.zig's
//!     bridged verdict); resolution failure admits the event — bridgy repos
//!     are did:plc and resolve reliably, and the reconciler re-checks PDS
//!     hosting later, so an open failure mode never blocks did:web authors.

const std = @import("std");
const mem = std.mem;
const json = std.json;
const Allocator = mem.Allocator;
const Io = std.Io;
const zat = @import("zat");
const jetstream = @import("jetstream");
const logfire = @import("logfire");
const ingester = @import("ingester.zig");
const policy = @import("../policy.zig");

/// The collections we subscribe to — mirrors the fly-app ingester's tracked
/// set (ingester/src/pipeline.zig COLLECTIONS) plus nothing else.
const COLLECTIONS = [_][]const u8{
    "pub.leaflet.document",
    "pub.leaflet.publication",
    "pub.leaflet.interactions.recommend",
    "site.standard.document",
    "site.standard.publication",
    "site.standard.graph.recommend",
    "site.standard.graph.subscription",
    "blog.pckt.publication",
    "com.whtwnd.blog.entry",
};

const DEFAULT_HOSTS = [_][]const u8{
    "https://stream.waow.tech",
};

/// v1→v2 cursor magnitude split (the server's own convention): at or above
/// this the persisted value is a time_us needing anchor conversion; below
/// it, an instance seq.
const TIME_US_THRESHOLD: i64 = 1_000_000_000_000_000;

/// rewind applied when deriving a replay floor from wall-clock time (first
/// boot, or v1 cursor conversion): witnessed-time skew cover.
const REWIND_US: i64 = 5_000_000;

/// persist the cursor at most this often. event-count gating (the first cut
/// used every-200) left the whole session unpersisted at our volume — the
/// 2026-08-16 wedge lost its replay position because 200 events never
/// accumulated. one atomic rename per interval is cheap.
const CURSOR_PERSIST_INTERVAL_NS: i96 = 2 * std.time.ns_per_s;

/// wedge escalation threshold. identity/account events flow on the
/// unfiltered kinds stream every few seconds, so minutes of true silence
/// means the read loop is stuck (a hung outbound fetch inside processing)
/// or the host is unreachable; either way a clean restart that resumes
/// from the persisted seq is the recovery.
const STALE_SECONDS_DEFAULT: u32 = 900;

/// bridged-DID cache bound; evict-one on overflow, never clear-all
/// (verifier.zig's resolve-stampede lesson).
const MAX_CACHED_BRIDGED: usize = 100_000;

fn cursorPath() [:0]const u8 {
    return if (std.c.getenv("JETSTREAM_CURSOR_PATH")) |p| std.mem.span(p) else "/data/jetstream-cursor";
}

fn staleSeconds() u32 {
    const raw = if (std.c.getenv("JETSTREAM_STALE_SECS")) |p| std.mem.span(p) else return STALE_SECONDS_DEFAULT;
    return std.fmt.parseInt(u32, raw, 10) catch STALE_SECONDS_DEFAULT;
}

fn apiKey() ?[]const u8 {
    return if (std.c.getenv("JETSTREAM_API_KEY")) |p| std.mem.span(p) else null;
}

fn readCursor(path: [:0]const u8) ?i64 {
    const fd = std.c.open(path.ptr, .{ .ACCMODE = .RDONLY }, @as(std.c.mode_t, 0));
    if (fd < 0) return null;
    defer _ = std.c.close(fd);
    var buf: [32]u8 = undefined;
    const n = std.c.read(fd, &buf, buf.len);
    if (n <= 0) return null;
    const trimmed = mem.trim(u8, buf[0..@intCast(n)], &std.ascii.whitespace);
    return std.fmt.parseInt(i64, trimmed, 10) catch null;
}

fn persistCursor(path: [:0]const u8, value: i64) void {
    var tmp_buf: [256]u8 = undefined;
    const tmp = std.fmt.bufPrintZ(&tmp_buf, "{s}.tmp", .{path}) catch return;
    const fd = std.c.open(tmp.ptr, .{ .ACCMODE = .WRONLY, .CREAT = true, .TRUNC = true }, @as(std.c.mode_t, 0o644));
    if (fd < 0) return;
    var num_buf: [32]u8 = undefined;
    const body = std.fmt.bufPrint(&num_buf, "{d}", .{value}) catch {
        _ = std.c.close(fd);
        return;
    };
    var total: usize = 0;
    while (total < body.len) {
        const w = std.c.write(fd, body[total..].ptr, body.len - total);
        if (w <= 0) break;
        total += @intCast(w);
    }
    _ = std.c.close(fd);
    _ = std.c.rename(tmp.ptr, path.ptr);
}

/// Hosts from JETSTREAM_HOSTS (comma-separated base URLs) or the default.
/// Leaks its allocation once at startup — process-lifetime.
fn getHosts(allocator: Allocator) []const []const u8 {
    const raw = std.c.getenv("JETSTREAM_HOSTS") orelse return &DEFAULT_HOSTS;
    const spanned = std.mem.span(raw);
    var list: std.ArrayList([]const u8) = .empty;
    var it = mem.splitScalar(u8, spanned, ',');
    while (it.next()) |h| {
        const trimmed = mem.trim(u8, h, " \t");
        if (trimmed.len > 0) list.append(allocator, trimmed) catch return &DEFAULT_HOSTS;
    }
    if (list.items.len == 0) return &DEFAULT_HOSTS;
    return list.toOwnedSlice(allocator) catch &DEFAULT_HOSTS;
}

/// hard wall-clock bound on one PDS resolution. the resolve runs on a
/// detached thread and we poll for at most this long (verifier.zig's abandon
/// pattern): fetchBounded's own bound is NOT trustworthy here — its
/// io.concurrent fallback runs the request UNBOUNDED, and one hung
/// plc.directory connection on the read loop froze ingestion at the
/// 2026-08-16 cutover. an abandoned task leaks a stuck thread until the
/// kernel gives up on its socket; the read loop must never inherit that wait.
const RESOLVE_DEADLINE_MS: u64 = 10_000;
const RESOLVE_POLL_MS: u64 = 50;

const TASK_RUNNING: u8 = 0;
const TASK_DONE: u8 = 1;
const TASK_ABANDONED: u8 = 2;

const ResolveTask = struct {
    io: Io,
    allocator: Allocator,
    did_buf: [512]u8 = undefined,
    did_len: usize = 0,
    state: std.atomic.Value(u8) = std.atomic.Value(u8).init(TASK_RUNNING),
    pds: ?[]u8 = null,

    fn run(task: *ResolveTask) void {
        task.pds = ingester.resolvePds(task.allocator, task.io, task.did_buf[0..task.did_len]) catch null;
        if (task.state.swap(TASK_DONE, .acq_rel) == TASK_ABANDONED) {
            if (task.pds) |p| task.allocator.free(p);
            task.allocator.destroy(task);
        }
    }
};

/// cached brid.gy hosting check. keys are owned dupes; values true = bridged.
/// only definitive resolutions are cached — a failed PLC lookup stays uncached
/// so the next event from that DID retries.
const BridgeGate = struct {
    allocator: Allocator,
    io: Io,
    cache: std.StringHashMapUnmanaged(bool) = .empty,
    lookups: u64 = 0,
    bridged_dropped: u64 = 0,

    /// resolve with a hard deadline; null = unresolved (timeout or failure).
    fn resolvePdsBounded(self: *BridgeGate, did: []const u8) ?[]u8 {
        if (did.len > 512) return null;
        const task = self.allocator.create(ResolveTask) catch return null;
        task.* = .{ .io = self.io, .allocator = self.allocator, .did_len = did.len };
        @memcpy(task.did_buf[0..did.len], did);

        const thread = std.Thread.spawn(.{}, ResolveTask.run, .{task}) catch {
            self.allocator.destroy(task);
            return null;
        };
        thread.detach();

        var waited: u64 = 0;
        while (task.state.load(.acquire) != TASK_DONE and waited < RESOLVE_DEADLINE_MS) {
            self.io.sleep(Io.Duration.fromMilliseconds(RESOLVE_POLL_MS), .awake) catch {};
            waited += RESOLVE_POLL_MS;
        }
        if (task.state.load(.acquire) != TASK_DONE) {
            if (task.state.swap(TASK_ABANDONED, .acq_rel) != TASK_DONE) {
                logfire.warn("jetstream: PDS resolve timed out for {s} after {d}ms", .{ did, RESOLVE_DEADLINE_MS });
                return null;
            }
        }
        defer self.allocator.destroy(task);
        return task.pds;
    }

    fn isBridged(self: *BridgeGate, did: []const u8) bool {
        if (self.cache.get(did)) |bridged| return bridged;

        const pds = self.resolvePdsBounded(did) orelse {
            logfire.warn("jetstream: PDS resolve failed for {s} — admitting (reconciler re-checks)", .{did});
            return false;
        };
        defer self.allocator.free(pds);
        self.lookups += 1;

        const bridged = ingester.isBridgyPds(pds);
        if (self.cache.count() >= MAX_CACHED_BRIDGED) {
            var it = self.cache.iterator();
            if (it.next()) |victim| {
                const victim_key = victim.key_ptr.*;
                _ = self.cache.remove(victim_key);
                self.allocator.free(victim_key);
            }
        }
        const key = self.allocator.dupe(u8, did) catch return bridged;
        self.cache.put(self.allocator, key, bridged) catch self.allocator.free(key);
        return bridged;
    }
};

/// unified-client handler: policy gates + dispatch + cursor persistence.
const EventHandler = struct {
    allocator: Allocator,
    io: Io,
    gate: *BridgeGate,
    cursor_path: [:0]const u8,
    last_seq: u64 = 0,
    last_persist_ns: i96 = 0,
    // atomic: read by the staleness watchdog thread
    event_count: std.atomic.Value(usize) = std.atomic.Value(usize).init(0),

    pub fn onEvent(self: *EventHandler, event: jetstream.livedecode.Event) bool {
        const count = self.event_count.fetchAdd(1, .monotonic) + 1;
        if (count % 1000 == 0) {
            logfire.info("jetstream: recv {d}, seq {d}, pds lookups {d}, bridged dropped {d}", .{
                count, self.last_seq, self.gate.lookups, self.gate.bridged_dropped,
            });
        }
        defer self.advanceCursor(event.seq);

        const commit = switch (event.payload) {
            .commit => |c| c,
            // identity/account/sync: nothing to evict without a key cache
            else => return true,
        };

        if (policy.isBanned(event.did)) {
            logfire.span("ingest.dropped", .{ .reason = "banned_did", .collection = commit.collection }).end();
            return true;
        }
        // upserts only: deleting a bridged repo's leftovers is harmless, and
        // deletes shouldn't cost a PLC roundtrip
        if (commit.operation != .delete and self.gate.isBridged(event.did)) {
            self.gate.bridged_dropped += 1;
            logfire.span("ingest.dropped", .{ .reason = "bridged_repo", .collection = commit.collection }).end();
            return true;
        }

        const rec = ingester.IngesterRecord{
            .collection = commit.collection,
            .action = @tagName(commit.operation), // same vocabulary: create/update/delete
            .did = event.did,
            .rkey = commit.rkey,
            .cid = commit.cid,
        };
        const inner: ?json.ObjectMap = if (commit.record) |r| switch (r) {
            .object => |o| o,
            else => null,
        } else null;
        ingester.dispatchRecord(self.allocator, self.io, rec, inner);
        return true;
    }

    pub fn onError(_: *EventHandler, err: anyerror) bool {
        logfire.warn("jetstream: recoverable stream error: {s}", .{@errorName(err)});
        return true;
    }

    pub fn onInfo(_: *EventHandler, info: jetstream.livedecode.Info) void {
        logfire.info("jetstream: server info {s}: {s}", .{ info.name, info.message });
    }

    fn advanceCursor(self: *EventHandler, seq: u64) void {
        if (seq <= self.last_seq) return;
        self.last_seq = seq;
        const now_ns = Io.Timestamp.now(self.io, .awake).nanoseconds;
        if (now_ns - self.last_persist_ns >= CURSOR_PERSIST_INTERVAL_NS) {
            persistCursor(self.cursor_path, @intCast(self.last_seq));
            self.last_persist_ns = now_ns;
        }
    }
};

/// The SDK re-dials, dedups, and re-enters backfill on its own; what it
/// cannot recover is a read loop wedged INSIDE event processing (e.g. a
/// hung outbound fetch — the 2026-08-16 stall). Frames flow every few
/// seconds when healthy (identity/account events on the unfiltered kinds
/// stream), so prolonged silence = wedged or fully partitioned: persist
/// the cursor and exit for a clean restart that resumes from the seq.
const Watchdog = struct {
    io: Io,
    handler: *EventHandler,

    fn run(self: *Watchdog) void {
        const stale_limit = staleSeconds();
        var last: usize = 0;
        var stale: u32 = 0;
        while (true) {
            self.io.sleep(Io.Duration.fromSeconds(1), .awake) catch {};
            const n = self.handler.event_count.load(.monotonic);
            if (n != last) {
                last = n;
                stale = 0;
                continue;
            }
            stale += 1;
            if (stale >= stale_limit) {
                logfire.err("jetstream: no events for {d}s — exiting for a clean restart", .{stale_limit});
                // the loop is frozen, so last_seq is stable to read here
                if (self.handler.last_seq > 0) persistCursor(self.handler.cursor_path, @intCast(self.handler.last_seq));
                std.process.exit(1);
            }
        }
    }
};

/// Resolve the replay start seq. Persisted seq → use it directly (the SDK's
/// dedup makes exact resume safe). Persisted v1 time_us, or nothing → derive
/// a seq from the archive's segment metadata for (value|now) − rewind.
fn resolveAfterSeq(allocator: Allocator, io: Io, host: []const u8, path: [:0]const u8, key: ?[]const u8) ?u64 {
    const persisted = readCursor(path);
    if (persisted) |v| {
        if (v < TIME_US_THRESHOLD) return @intCast(v);
    }
    const now_us: i64 = @intCast(@divFloor(Io.Timestamp.now(io, .real).nanoseconds, std.time.ns_per_us));
    const floor_us = (persisted orelse now_us - 60 * std.time.us_per_s) - REWIND_US;
    const anchor = jetstream.ArchiveBackfill.fetchAnchor(io, allocator, host, floor_us, key) catch |err| {
        logfire.err("jetstream: seq anchor fetch failed ({s}) — starting live from the tip", .{@errorName(err)});
        return null;
    };
    logfire.info("jetstream: anchored floor_us={d} → after_seq={?d}", .{ floor_us, anchor.after_seq });
    return anchor.after_seq;
}

pub fn consumer(allocator: Allocator, io: Io) void {
    const hosts = getHosts(allocator);
    const path = cursorPath();
    const key = apiKey();

    if (key == null) {
        logfire.warn("jetstream: no JETSTREAM_API_KEY — archive sweep/anchor unavailable, gap recovery degrades to the live replay window", .{});
    }

    const after_seq = resolveAfterSeq(allocator, io, hosts[0], path, key);

    logfire.info("jetstream: consumer starting, {d} host(s), primary={s}, after_seq={?d} ({s})", .{
        hosts.len, hosts[0], after_seq, path,
    });

    var gate = BridgeGate{ .allocator = allocator, .io = io };
    var handler = EventHandler{ .allocator = allocator, .io = io, .gate = &gate, .cursor_path = path };

    var watchdog = Watchdog{ .io = io, .handler = &handler };
    if (std.Thread.spawn(.{}, Watchdog.run, .{&watchdog})) |t| t.detach() else |err| {
        logfire.err("jetstream: failed to spawn watchdog: {}", .{err});
    }

    // blocks until a terminal error; the SDK owns reconnects, seq dedup,
    // and CursorTooOld → re-backfill
    jetstream.subscribe(io, allocator, .{
        .hosts = hosts,
        .after_seq = after_seq,
        .collections = &COLLECTIONS,
        .api_key = key,
    }, &handler) catch |err| {
        logfire.err("jetstream: subscribe ended: {} — exiting for restart", .{err});
    };
    if (handler.last_seq > 0) persistCursor(path, @intCast(handler.last_seq));
    std.process.exit(1);
}

test "commit events map onto the shared record envelope" {
    // livedecode's typed contract is what dispatch consumes; pin the pieces
    // dispatchRecord relies on (operation tag names, payload union shape).
    try std.testing.expectEqualStrings("create", @tagName(jetstream.livedecode.Operation.create));
    try std.testing.expectEqualStrings("update", @tagName(jetstream.livedecode.Operation.update));
    try std.testing.expectEqualStrings("delete", @tagName(jetstream.livedecode.Operation.delete));

    const ev = jetstream.livedecode.Event{
        .seq = 42,
        .time_us = 1755230000000000,
        .did = "did:plc:abc123",
        .payload = .{ .commit = .{
            .operation = .create,
            .collection = "site.standard.document",
            .rkey = "3mn3z7u7jgsgl",
            .rev = "3m",
            .cid = "bafyabc",
        } },
    };
    try std.testing.expectEqual(@as(u64, 42), ev.seq);
    switch (ev.payload) {
        .commit => |c| try std.testing.expectEqualStrings("site.standard.document", c.collection),
        else => unreachable,
    }
}

test "v1 time_us cursors are detected for anchor conversion" {
    try std.testing.expect(1786850647440646 >= TIME_US_THRESHOLD); // a real v1 cursor
    try std.testing.expect(23417264498 < TIME_US_THRESHOLD); // a real v2 seq
}

test "cursor round-trips through the persist file" {
    var path_buf: [128]u8 = undefined;
    const path = try std.fmt.bufPrintZ(&path_buf, "/tmp/jetstream-cursor-test-{d}", .{std.testing.random_seed});
    defer _ = std.c.unlink(path.ptr);

    try std.testing.expectEqual(@as(?i64, null), readCursor(path));
    persistCursor(path, 23417264498);
    try std.testing.expectEqual(@as(?i64, 23417264498), readCursor(path));
    persistCursor(path, 23417264999);
    try std.testing.expectEqual(@as(?i64, 23417264999), readCursor(path));
}
