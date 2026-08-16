//! Jetstream V2 live-ingest consumer — the /channel replacement.
//!
//! Transport is zat.JetstreamClient (host rotation, typed event parsing,
//! reconnect backoff, 10s cursor rewind on host switch, TCP keepalive); this
//! file owns only the pub-search concerns: which hosts we trust, durable
//! cursor persistence, corpus policy (banned DIDs, bridgy fed), dispatch into
//! the same path the /channel consumer uses, and wedge escalation.
//!
//! Hosts: stream.waow.tech primary, Bluesky's hosted V2 instances as
//! failover. Every host in the list runs Sync 1.1 signature/MST verification
//! at its own ingest, so records arriving here are operator-verified even
//! though the blocks needed to re-check are not on the wire. Jetstream v1
//! instances do NOT verify — never add one (which is also why we override
//! zat's default host list).
//!
//! at-least-once contract: events are processed synchronously in onEvent and
//! the durable cursor (time_us) is persisted only after dispatch returns
//! (2s time gate), then rewound REWIND_US on process start. Slow turso
//! propagates as websocket backpressure; if a server drops us as a slow
//! consumer, zat re-dials and resumes from its in-memory cursor. No ack
//! protocol, no outbox, no shedding — redelivery + idempotent upserts
//! replace all three.
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

/// Verified-V2-only. Overrides zat's default host list, which includes v1 and
/// third-party instances we don't trust to have verified.
const DEFAULT_HOSTS = [_][]const u8{
    "stream.waow.tech",
    "jetstream2.us-east.bsky.network",
    "jetstream2.us-west.bsky.network",
};

/// rewind applied to the persisted cursor on process start: redelivered
/// events are absorbed by idempotent upserts, missed events are not absorbed
/// by anything. (In-process reconnects use zat's cursor handling.)
const REWIND_US: i64 = 5_000_000;

/// persist the cursor at most this often. event-count gating (the first cut
/// used every-200) left the whole session unpersisted at our volume — the
/// 2026-08-16 wedge lost its replay position because 200 events never
/// accumulated. one atomic rename per interval is cheap.
const CURSOR_PERSIST_INTERVAL_NS: i96 = 2 * std.time.ns_per_s;

/// wedge escalation threshold. identity/account events bypass
/// wantedCollections, so a healthy connection sees frames every few seconds
/// — minutes of true silence means the read loop is stuck (a hung outbound
/// fetch inside processing) or every host is unreachable; either way a clean
/// restart that replays from the cursor is the recovery.
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

fn persistCursor(path: [:0]const u8, time_us: i64) void {
    var tmp_buf: [256]u8 = undefined;
    const tmp = std.fmt.bufPrintZ(&tmp_buf, "{s}.tmp", .{path}) catch return;
    const fd = std.c.open(tmp.ptr, .{ .ACCMODE = .WRONLY, .CREAT = true, .TRUNC = true }, @as(std.c.mode_t, 0o644));
    if (fd < 0) return;
    var num_buf: [32]u8 = undefined;
    const body = std.fmt.bufPrint(&num_buf, "{d}", .{time_us}) catch {
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

/// Hosts to rotate through on reconnect, from JETSTREAM_HOSTS (comma-separated)
/// or the default list. Leaks its allocation once at startup — process-lifetime.
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

/// zat.JetstreamClient handler: policy gates + dispatch + cursor persistence.
const EventHandler = struct {
    allocator: Allocator,
    io: Io,
    gate: *BridgeGate,
    cursor_path: [:0]const u8,
    last_time_us: i64 = 0,
    last_persist_ns: i96 = 0,
    // atomic: read by the staleness watchdog thread
    event_count: std.atomic.Value(usize) = std.atomic.Value(usize).init(0),

    pub fn onConnect(_: *EventHandler, host: []const u8) void {
        logfire.info("jetstream: connected to {s}", .{host});
    }

    pub fn onError(_: *EventHandler, err: anyerror) void {
        logfire.warn("jetstream: connection error: {s}, zat reconnecting", .{@errorName(err)});
    }

    pub fn onEvent(self: *EventHandler, event: zat.JetstreamEvent) void {
        const count = self.event_count.fetchAdd(1, .monotonic) + 1;
        if (count % 1000 == 0) {
            logfire.info("jetstream: recv {d}, cursor {d}, pds lookups {d}, bridged dropped {d}", .{
                count, self.last_time_us, self.gate.lookups, self.gate.bridged_dropped,
            });
        }
        defer self.advanceCursor(event.timeUs());

        const commit = switch (event) {
            .commit => |c| c,
            // identity/account: nothing to evict without a key cache
            else => return,
        };

        if (policy.isBanned(commit.did)) {
            logfire.span("ingest.dropped", .{ .reason = "banned_did", .collection = commit.collection }).end();
            return;
        }
        // upserts only: deleting a bridged repo's leftovers is harmless, and
        // deletes shouldn't cost a PLC roundtrip
        if (commit.operation != .delete and self.gate.isBridged(commit.did)) {
            self.gate.bridged_dropped += 1;
            logfire.span("ingest.dropped", .{ .reason = "bridged_repo", .collection = commit.collection }).end();
            return;
        }

        const rec = ingester.IngesterRecord{
            .collection = commit.collection,
            .action = @tagName(commit.operation), // same vocabulary: create/update/delete
            .did = commit.did,
            .rkey = commit.rkey,
            .cid = commit.cid,
        };
        const inner: ?json.ObjectMap = if (commit.record) |r| switch (r) {
            .object => |o| o,
            else => null,
        } else null;
        ingester.dispatchRecord(self.allocator, self.io, rec, inner);
    }

    fn advanceCursor(self: *EventHandler, time_us: i64) void {
        if (time_us <= self.last_time_us) return;
        self.last_time_us = time_us;
        const now_ns = Io.Timestamp.now(self.io, .awake).nanoseconds;
        if (now_ns - self.last_persist_ns >= CURSOR_PERSIST_INTERVAL_NS) {
            persistCursor(self.cursor_path, self.last_time_us);
            self.last_persist_ns = now_ns;
        }
    }
};

/// zat's subscribe loop already re-dials half-open sockets (TCP keepalive)
/// and rotates hosts; what it cannot recover is a read loop wedged INSIDE
/// event processing (e.g. a hung outbound fetch — the 2026-08-16 stall).
/// Frames flow every few seconds when healthy (identity/account events
/// bypass wantedCollections), so prolonged silence = wedged or fully
/// partitioned: persist the cursor and exit for a clean restart that
/// replays the gap.
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
                // the loop is frozen, so last_time_us is stable to read here
                if (self.handler.last_time_us > 0) persistCursor(self.handler.cursor_path, self.handler.last_time_us);
                std.process.exit(1);
            }
        }
    }
};

pub fn consumer(allocator: Allocator, io: Io) void {
    const hosts = getHosts(allocator);
    const path = cursorPath();

    // no cursor file yet (first cutover boot): seed from wall clock minus a
    // minute so the deploy window between the old consumer stopping and this
    // one connecting is replayed instead of skipped (time_us is the stream's
    // witness clock, ~wall time).
    const now_us: i64 = @intCast(@divFloor(Io.Timestamp.now(io, .real).nanoseconds, std.time.ns_per_us));
    const persisted = readCursor(path) orelse now_us - 60 * std.time.us_per_s;
    const start_cursor = persisted - REWIND_US;

    logfire.info("jetstream: consumer starting, {d} host(s), primary={s}, cursor={d} ({s})", .{
        hosts.len, hosts[0], start_cursor, path,
    });

    var gate = BridgeGate{ .allocator = allocator, .io = io };
    var handler = EventHandler{ .allocator = allocator, .io = io, .gate = &gate, .cursor_path = path };

    var watchdog = Watchdog{ .io = io, .handler = &handler };
    if (std.Thread.spawn(.{}, Watchdog.run, .{&watchdog})) |t| t.detach() else |err| {
        logfire.err("jetstream: failed to spawn watchdog: {}", .{err});
    }

    var client = zat.JetstreamClient.init(io, allocator, .{
        .hosts = hosts,
        .wanted_collections = &COLLECTIONS,
        .cursor = start_cursor,
        .max_message_size = 5 * 1024 * 1024,
    });
    defer client.deinit();

    // blocks forever: zat owns reconnect backoff + host rotation
    client.subscribe(&handler) catch |err| {
        logfire.err("jetstream: subscribe loop ended: {} — exiting for restart", .{err});
        if (handler.last_time_us > 0) persistCursor(path, handler.last_time_us);
        std.process.exit(1);
    };
}

test "zat jetstream event maps onto the shared record envelope" {
    const allocator = std.testing.allocator;
    const event_json =
        \\{"did":"did:plc:abc123","time_us":1755230000000000,"kind":"commit","commit":{"rev":"3m","operation":"create","collection":"site.standard.document","rkey":"3mn3z7u7jgsgl","record":{"$type":"site.standard.document","title":"hi"},"cid":"bafyabc"}}
    ;
    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const event = try zat.jetstream.parseEvent(arena.allocator(), event_json);

    const commit = event.commit;
    try std.testing.expectEqual(zat.jetstream.CommitAction.create, commit.operation);
    try std.testing.expectEqualStrings("create", @tagName(commit.operation));
    try std.testing.expectEqualStrings("site.standard.document", commit.collection);
    try std.testing.expectEqualStrings("bafyabc", commit.cid.?);
    try std.testing.expect(commit.record.? == .object);
    try std.testing.expectEqual(@as(i64, 1755230000000000), event.timeUs());
}

test "zat jetstream delete event has no record and maps to delete action" {
    const allocator = std.testing.allocator;
    const event_json =
        \\{"did":"did:plc:abc123","time_us":1755230000000001,"kind":"commit","commit":{"rev":"3m","operation":"delete","collection":"pub.leaflet.document","rkey":"3mn3z7u7jgsgl"}}
    ;
    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const event = try zat.jetstream.parseEvent(arena.allocator(), event_json);

    const commit = event.commit;
    try std.testing.expectEqual(zat.jetstream.CommitAction.delete, commit.operation);
    try std.testing.expectEqual(@as(?json.Value, null), commit.record);
    try std.testing.expectEqual(@as(?[]const u8, null), commit.cid);
}

test "cursor round-trips through the persist file" {
    var path_buf: [128]u8 = undefined;
    const path = try std.fmt.bufPrintZ(&path_buf, "/tmp/jetstream-cursor-test-{d}", .{std.testing.random_seed});
    defer _ = std.c.unlink(path.ptr);

    try std.testing.expectEqual(@as(?i64, null), readCursor(path));
    persistCursor(path, 1755230000000000);
    try std.testing.expectEqual(@as(?i64, 1755230000000000), readCursor(path));
    persistCursor(path, 1755230000000005);
    try std.testing.expectEqual(@as(?i64, 1755230000000005), readCursor(path));
}
