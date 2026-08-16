//! Jetstream V2 live-ingest consumer — the /channel replacement.
//!
//! Subscribes to a Jetstream instance (stream.waow.tech primary; Bluesky's
//! hosted V2 instances as failover — every host in the list runs Sync 1.1
//! signature/MST verification at its own ingest, so records arriving here are
//! operator-verified even though the blocks needed to re-check are not on the
//! wire) and normalizes each commit event into the same dispatchRecord path
//! the /channel consumer uses, so both sources index identically.
//!
//! at-least-once contract: events are processed synchronously on the read
//! loop and the durable cursor (time_us) is persisted only after dispatch
//! returns, then rewound REWIND_US on every reconnect. Slow turso propagates
//! as websocket backpressure; if the server drops us as a slow consumer we
//! re-dial and replay from the cursor. No ack protocol, no outbox, no
//! shedding — redelivery + idempotent upserts replace all three.
//!
//! policy parity with the fly-app ingester it replaces:
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
const websocket = @import("websocket");
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

/// rewind applied to the persisted cursor on every (re)connect: redelivered
/// events are absorbed by idempotent upserts, missed events are not absorbed
/// by anything.
const REWIND_US: i64 = 5_000_000;

const CURSOR_PERSIST_EVERY: u64 = 200;

/// our collections are quiet enough that minutes of silence are normal — a
/// firehose-style 90s watchdog would false-trigger constantly. 15 min of
/// nothing (jetstream emits nothing when filters match nothing) still bounds
/// a half-open socket to one rewind's worth of extra replay.
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

const DEFAULT_HOSTS = [_][]const u8{
    "stream.waow.tech",
    "jetstream2.us-east.bsky.network",
    "jetstream2.us-west.bsky.network",
};

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

/// cached brid.gy hosting check. keys are owned dupes; values true = bridged.
/// only definitive resolutions are cached — a failed PLC lookup stays uncached
/// so the next event from that DID retries.
const BridgeGate = struct {
    allocator: Allocator,
    io: Io,
    cache: std.StringHashMapUnmanaged(bool) = .empty,
    lookups: u64 = 0,
    bridged_dropped: u64 = 0,

    fn isBridged(self: *BridgeGate, did: []const u8) bool {
        if (self.cache.get(did)) |bridged| return bridged;

        const pds = ingester.resolvePds(self.allocator, self.io, did) catch |err| {
            logfire.warn("jetstream: PDS resolve failed for {s}: {s} — admitting (reconciler re-checks)", .{ did, @errorName(err) });
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

pub fn consumer(allocator: Allocator, io: Io) void {
    const hosts = getHosts(allocator);
    var gate = BridgeGate{ .allocator = allocator, .io = io };
    var backoff: u64 = 1;
    const max_backoff: u64 = 30;
    var host_idx: usize = 0;

    logfire.info("jetstream: consumer starting, {d} host(s), primary={s}, cursor_path={s}", .{ hosts.len, hosts[0], cursorPath() });

    while (true) {
        const host = hosts[host_idx];
        if (connect(allocator, io, host, &gate)) |_| {
            backoff = 1;
            host_idx = 0; // clean close: go back to the primary
            logfire.info("jetstream: connection closed, reconnecting", .{});
        } else |err| {
            logfire.warn("jetstream: {s} error: {}, next host in {d}s", .{ host, err, backoff });
            host_idx = (host_idx + 1) % hosts.len;
            io.sleep(Io.Duration.fromSeconds(@intCast(backoff)), .awake) catch {};
            backoff = @min(backoff * 2, max_backoff);
        }
    }
}

const Handler = struct {
    allocator: Allocator,
    io: Io,
    gate: *BridgeGate,
    cursor_path: [:0]const u8,
    last_time_us: i64 = 0,
    events_since_persist: u64 = 0,
    // atomic: read by the staleness watchdog thread
    msg_count: std.atomic.Value(usize) = std.atomic.Value(usize).init(0),

    pub fn serverMessage(self: *Handler, data: []const u8) !void {
        const count = self.msg_count.fetchAdd(1, .monotonic) + 1;
        if (count % 1000 == 0) {
            logfire.info("jetstream: recv {d}, cursor {d}, pds lookups {d}, bridged dropped {d}", .{
                count, self.last_time_us, self.gate.lookups, self.gate.bridged_dropped,
            });
        }
        self.processEvent(data) catch |err| {
            logfire.err("jetstream: event processing error: {}", .{err});
            // cursor does NOT advance past a failed event; the reconnect
            // rewind re-delivers it.
            return;
        };
    }

    fn processEvent(self: *Handler, data: []const u8) !void {
        const parsed = json.parseFromSlice(json.Value, self.allocator, data, .{}) catch {
            logfire.err("jetstream: JSON parse failed, first 100 bytes: {s}", .{data[0..@min(data.len, 100)]});
            return;
        };
        defer parsed.deinit();

        const time_us = zat.json.getInt(parsed.value, "time_us") orelse return;
        defer self.advanceCursor(time_us);

        const kind = zat.json.getString(parsed.value, "kind") orelse return;
        if (!mem.eql(u8, kind, "commit")) return; // identity/account: nothing to evict without a key cache

        const did = zat.json.getString(parsed.value, "did") orelse return;
        const operation = zat.json.getString(parsed.value, "commit.operation") orelse return;
        const collection = zat.json.getString(parsed.value, "commit.collection") orelse return;
        const rkey = zat.json.getString(parsed.value, "commit.rkey") orelse return;

        if (policy.isBanned(did)) {
            logfire.span("ingest.dropped", .{ .reason = "banned_did", .collection = collection }).end();
            return;
        }
        // upserts only: deleting a bridged repo's leftovers is harmless, and
        // deletes shouldn't cost a PLC roundtrip
        const is_delete = mem.eql(u8, operation, "delete");
        if (!is_delete and self.gate.isBridged(did)) {
            self.gate.bridged_dropped += 1;
            logfire.span("ingest.dropped", .{ .reason = "bridged_repo", .collection = collection }).end();
            return;
        }

        const rec = ingester.IngesterRecord{
            .collection = collection,
            .action = operation, // jetstream operations are create/update/delete — same vocabulary
            .did = did,
            .rkey = rkey,
            .cid = zat.json.getString(parsed.value, "commit.cid"),
        };
        const inner = zat.json.getObject(parsed.value, "commit.record");
        ingester.dispatchRecord(self.allocator, self.io, rec, inner);
    }

    fn advanceCursor(self: *Handler, time_us: i64) void {
        if (time_us <= self.last_time_us) return;
        self.last_time_us = time_us;
        self.events_since_persist += 1;
        if (self.events_since_persist >= CURSOR_PERSIST_EVERY) {
            persistCursor(self.cursor_path, self.last_time_us);
            self.events_since_persist = 0;
        }
    }

    pub fn close(self: *Handler) void {
        // flush the cursor so a clean shutdown doesn't replay a whole batch
        if (self.last_time_us > 0) persistCursor(self.cursor_path, self.last_time_us);
    }
};

const Watchdog = struct {
    io: Io,
    client: *websocket.Client,
    handler: *Handler,
    stop: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),

    fn run(self: *Watchdog) void {
        const stale_limit = staleSeconds();
        var last: usize = 0;
        var stale: u32 = 0;
        while (!self.stop.load(.acquire)) {
            self.io.sleep(Io.Duration.fromSeconds(1), .awake) catch {};
            const n = self.handler.msg_count.load(.monotonic);
            if (n != last) {
                last = n;
                stale = 0;
                continue;
            }
            stale += 1;
            if (stale >= stale_limit) {
                logfire.warn("jetstream: no events for {d}s, closing connection to force reconnect", .{stale_limit});
                self.client.close(.{}) catch {};
                return;
            }
        }
    }
};

fn connect(allocator: Allocator, io: Io, host: []const u8, gate: *BridgeGate) !void {
    const path = cursorPath();

    var path_buf: [1024]u8 = undefined;
    var w: std.Io.Writer = .fixed(&path_buf);
    w.writeAll("/subscribe") catch return error.PathTooLong;
    var sep: u8 = '?';
    inline for (COLLECTIONS) |c| {
        w.print("{c}wantedCollections={s}", .{ sep, c }) catch return error.PathTooLong;
        sep = '&';
    }
    // no cursor file yet (first cutover boot): seed from wall clock minus a
    // minute so the deploy window between the old consumer stopping and this
    // one connecting is replayed instead of skipped (time_us is the stream's
    // witness clock, ~wall time).
    const now_us: i64 = @intCast(@divFloor(Io.Timestamp.now(io, .real).nanoseconds, std.time.ns_per_us));
    const persisted = readCursor(path) orelse now_us - 60 * std.time.us_per_s;
    w.print("&cursor={d}", .{persisted - REWIND_US}) catch return error.PathTooLong;
    const subscribe_path = w.buffered();

    logfire.info("jetstream: connecting to wss://{s}{s}", .{ host, subscribe_path });

    var client = websocket.Client.init(io, allocator, .{
        .host = host,
        .port = 443,
        .tls = true,
        .max_size = 5 * 1024 * 1024,
    }) catch |err| {
        logfire.err("jetstream: websocket client init failed: {}", .{err});
        return err;
    };
    defer client.deinit();

    var host_header_buf: [256]u8 = undefined;
    const host_header = std.fmt.bufPrint(&host_header_buf, "Host: {s}\r\n", .{host}) catch return error.PathTooLong;

    client.handshake(subscribe_path, .{ .headers = host_header }) catch |err| {
        logfire.err("jetstream: handshake with {s} failed: {}", .{ host, err });
        return err;
    };

    logfire.info("jetstream: connected to {s}", .{host});

    var handler = Handler{ .allocator = allocator, .io = io, .gate = gate, .cursor_path = path };

    var watchdog = Watchdog{ .io = io, .client = &client, .handler = &handler };
    const wd_thread = std.Thread.spawn(.{}, Watchdog.run, .{&watchdog}) catch |err| {
        logfire.err("jetstream: failed to spawn watchdog: {}", .{err});
        return err;
    };
    defer {
        watchdog.stop.store(true, .release);
        wd_thread.join();
    }

    client.readLoop(&handler) catch |err| {
        logfire.err("jetstream: read loop error: {}", .{err});
        return err;
    };
}

test "jetstream commit event maps onto the shared record envelope" {
    const allocator = std.testing.allocator;
    const event =
        \\{"did":"did:plc:abc123","time_us":1755230000000000,"kind":"commit","commit":{"rev":"3m","operation":"create","collection":"site.standard.document","rkey":"3mn3z7u7jgsgl","record":{"$type":"site.standard.document","title":"hi"},"cid":"bafyabc"}}
    ;
    var parsed = try json.parseFromSlice(json.Value, allocator, event, .{});
    defer parsed.deinit();

    try std.testing.expectEqualStrings("commit", zat.json.getString(parsed.value, "kind").?);
    try std.testing.expectEqualStrings("create", zat.json.getString(parsed.value, "commit.operation").?);
    try std.testing.expectEqualStrings("site.standard.document", zat.json.getString(parsed.value, "commit.collection").?);
    try std.testing.expectEqualStrings("bafyabc", zat.json.getString(parsed.value, "commit.cid").?);
    try std.testing.expect(zat.json.getObject(parsed.value, "commit.record") != null);
    try std.testing.expectEqual(@as(i64, 1755230000000000), zat.json.getInt(parsed.value, "time_us").?);
}

test "jetstream delete event has no record and maps to delete action" {
    const allocator = std.testing.allocator;
    const event =
        \\{"did":"did:plc:abc123","time_us":1755230000000001,"kind":"commit","commit":{"rev":"3m","operation":"delete","collection":"pub.leaflet.document","rkey":"3mn3z7u7jgsgl"}}
    ;
    var parsed = try json.parseFromSlice(json.Value, allocator, event, .{});
    defer parsed.deinit();

    try std.testing.expectEqualStrings("delete", zat.json.getString(parsed.value, "commit.operation").?);
    try std.testing.expect(zat.json.getObject(parsed.value, "commit.record") == null);
    try std.testing.expect(zat.json.getString(parsed.value, "commit.cid") == null);
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
