//! pub-search firehose ingester.
//!
//! Standalone service: our own verified firehose consumer. It consumes the
//! real firehose (com.atproto.sync.subscribeRepos), filters to pub-search's
//! collections, verifies each matched commit (signature + MST, see
//! verifier.zig) in process, and re-emits verified records over the `/channel`
//! websocket the backend consumes (live path since the 2026-06-09 cutover).
//! Every matched record also logs `ingester.captured` (with a verified flag)
//! so coverage stays auditable in logfire.

const std = @import("std");
const Io = std.Io;
const logfire = @import("logfire");
const zat = @import("zat");
const ch = @import("channel.zig");
const vf = @import("verifier.zig");
const pl = @import("pipeline.zig");

// banned repos: bulk-archive bots that flood the corpus. Single source of
// truth is /banned-dids.txt (repo root), shared with backend/src/policy.zig
// and scripts/purge-*; wired in via build.zig, parsed here at comptime.
// registry of who/why/evidence: docs/exclusions.md.
const BANNED_DIDS = parseBannedDids(@embedFile("banned_dids"));

fn parseBannedDids(comptime data: []const u8) []const []const u8 {
    comptime {
        @setEvalBranchQuota(100_000);
        var list: []const []const u8 = &.{};
        var lines = std.mem.splitScalar(u8, data, '\n');
        while (lines.next()) |raw| {
            const code = if (std.mem.indexOfScalar(u8, raw, '#')) |h| raw[0..h] else raw;
            const did = std.mem.trim(u8, code, " \t\r");
            if (did.len == 0) continue;
            list = list ++ &[_][]const u8{did};
        }
        return list;
    }
}

fn isBanned(did: []const u8) bool {
    for (BANNED_DIDS) |b| {
        if (std.mem.eql(u8, b, did)) return true;
    }
    return false;
}


// Persist the firehose cursor every this many events so we resume across our
// OWN restarts (zat only keeps last_seq in memory). ~every few seconds at
// firehose volume; cheap atomic file write.
const CURSOR_PERSIST_EVERY: u64 = 500;

// std.fs was removed in zig 0.16; use POSIX std.c (matches backend timing.zig).
fn cursorPath() [:0]const u8 {
    return if (std.c.getenv("CURSOR_PATH")) |p| std.mem.span(p) else "/data/cursor";
}

fn readCursor(path: [:0]const u8) ?i64 {
    const fd = std.c.open(path.ptr, .{ .ACCMODE = .RDONLY }, @as(std.c.mode_t, 0));
    if (fd < 0) return null;
    defer _ = std.c.close(fd);
    var buf: [32]u8 = undefined;
    const n = std.c.read(fd, &buf, buf.len);
    if (n <= 0) return null;
    const trimmed = std.mem.trim(u8, buf[0..@intCast(n)], &std.ascii.whitespace);
    return std.fmt.parseInt(i64, trimmed, 10) catch null;
}

fn persistCursor(path: [:0]const u8, seq: i64) void {
    var tmp_buf: [256]u8 = undefined;
    const tmp = std.fmt.bufPrintZ(&tmp_buf, "{s}.tmp", .{path}) catch return;
    const fd = std.c.open(tmp.ptr, .{ .ACCMODE = .WRONLY, .CREAT = true, .TRUNC = true }, @as(std.c.mode_t, 0o644));
    if (fd < 0) return;
    var num_buf: [32]u8 = undefined;
    const body = std.fmt.bufPrint(&num_buf, "{d}", .{seq}) catch {
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

/// Thin reader (stream's live-scheduler shape, docs/scale-300k-plan.md §1e):
/// decode for CLASSIFICATION only — banned/tracked checks — then hand the raw
/// frame bytes to the verify pipeline. All expensive work (signature + MST
/// verification, DID key resolution) happens on the worker pool, so nothing
/// here ever blocks on the network.
const Handler = struct {
    allocator: std.mem.Allocator,
    verifier: *vf.Verifier,
    pipeline: *pl.Pipeline,
    channel: *ch.Channel,
    matched: u64 = 0,
    events: u64 = 0,
    last_seq: i64 = 0,
    cursor_path: [:0]const u8,

    pub fn onRawFrame(self: *Handler, data: []const u8) void {
        self.events += 1;

        var arena = std.heap.ArenaAllocator.init(self.allocator);
        defer arena.deinit();
        const event = zat.firehose.decodeFrame(arena.allocator(), data) catch |err| {
            logfire.debug("frame decode error: {s}", .{@errorName(err)});
            return;
        };
        if (event.seq()) |s| self.last_seq = s;

        switch (event) {
            .commit => |commit| {
                if (isBanned(commit.repo)) return;
                var tracked_ops: usize = 0;
                for (commit.ops) |op| {
                    if (pl.isTracked(op.collection)) tracked_ops += 1;
                }
                if (tracked_ops > 0) {
                    self.matched += tracked_ops;
                    self.pipeline.submit(commit.repo, commit.seq, data);
                }
            },
            .identity => |id| self.verifier.evict(id.did),
            else => {},
        }

        if (self.events % CURSOR_PERSIST_EVERY == 0) {
            self.checkpoint();
        }
    }

    /// checkpoint = delivery position, not read position (event-stream spec:
    /// "last sequence number received and successfully processed"). Two kinds
    /// of work may still be behind the read position: commits inflight in the
    /// verify pipeline, and frames buffered undelivered in the channel ring.
    /// Pin the durable cursor just before the oldest of either so a restart
    /// replays them from the relay; the backend's upserts absorb duplicates
    /// (stream's cursor = min(inflight)−1 rule).
    fn checkpoint(self: *Handler) void {
        var floor: i64 = self.last_seq + 1;
        if (self.pipeline.minInflight()) |s| floor = @min(floor, s);
        if (self.channel.pendingSeq()) |s| floor = @min(floor, s);
        const cp = floor - 1;
        persistCursor(self.cursor_path, cp);
        logfire.debug("ingester progress: events={d} matched={d} seq={d} checkpoint={d} verified={d} sig_only={d} bridged={d} rejected={d} unresolvable={d} pool_dropped={d}", .{
            self.events,
            self.matched,
            self.last_seq,
            cp,
            self.verifier.verified.load(.monotonic),
            self.verifier.sig_only.load(.monotonic),
            self.verifier.bridged.load(.monotonic),
            self.verifier.rejected.load(.monotonic),
            self.verifier.unresolvable.load(.monotonic),
            self.pipeline.dropped.load(.monotonic),
        });
    }

    pub fn onError(_: *Handler, err: anyerror) void {
        logfire.warn("firehose error: {s}, reconnecting...", .{@errorName(err)});
    }
};

pub fn main() !void {
    const allocator = std.heap.smp_allocator;

    // match zlay's io options — concurrent_limit caps io.concurrent tasks
    // (firehose + server accept loop + one per connection). Defaults can be too
    // tight for a server that spawns a task per connection.
    var threaded = Io.Threaded.init(allocator, .{
        .concurrent_limit = Io.Limit.limited(4096),
    });
    const io = threaded.io();

    _ = logfire.configure(.{
        .service_name = "leaflet-ingester",
        .service_version = "0.0.1",
        .environment = if (std.c.getenv("FLY_APP_NAME")) |p| std.mem.span(p) else "development",
    }) catch |err| {
        std.debug.print("logfire init failed: {}, continuing without observability\n", .{err});
    };

    // Relay failover list. zat rotates through these on reconnect (exponential
    // backoff, reset on host switch) and resumes from ?cursor=last_seq, so a
    // downed relay fails over and a blip replays. Override with RELAY_HOSTS.
    var hosts_buf: [8][]const u8 = undefined;
    const hosts: []const []const u8 = if (std.c.getenv("RELAY_HOSTS")) |p| blk: {
        var n: usize = 0;
        var it = std.mem.tokenizeScalar(u8, std.mem.span(p), ',');
        while (it.next()) |h| {
            if (n >= hosts_buf.len) break;
            hosts_buf[n] = h;
            n += 1;
        }
        break :blk hosts_buf[0..n];
    } else &.{
        "relay1.us-east.bsky.network",
        "relay1.us-west.bsky.network",
        "zlay.waow.tech",
        "bsky.network",
    };

    const path = cursorPath();
    const cursor = readCursor(path);
    const port: u16 = blk: {
        const s = if (std.c.getenv("PORT")) |p| std.mem.span(p) else "2480";
        break :blk std.fmt.parseInt(u16, s, 10) catch 2480;
    };

    var channel = ch.Channel{ .allocator = allocator };

    // Both the firehose consumer and the /channel server run as Io-native
    // concurrent tasks sharing one io — zlay's pattern (relay + firehose
    // consumer in one process). The server uses runIo (NOT the internal
    // listen() loop, which doesn't tolerate other threads under Io.Threaded).
    const fctx = FirehoseCtx{
        .allocator = allocator,
        .io = io,
        .channel = &channel,
        .hosts = hosts,
        .cursor = cursor,
        .cursor_path = path,
    };
    if (std.c.getenv("SKIP_FIREHOSE") == null) {
        const fh_thread = try std.Thread.spawn(.{}, runFirehose, .{fctx});
        fh_thread.detach();
    } else {
        logfire.info("SKIP_FIREHOSE set — /channel server only", .{});
    }

    const hb_thread = try std.Thread.spawn(.{}, runHeartbeat, .{ io, &channel });
    hb_thread.detach();

    logfire.info("leaflet-ingester starting, /channel on :{d}, {d} relay host(s), primary={s}, resume_cursor={?d}", .{ port, hosts.len, hosts[0], cursor });

    // websocket server blocks on main; karlseguin's worker pool coexists with
    // the firehose thread fine.
    try ch.serve(allocator, io, &channel, port);
}

const FirehoseCtx = struct {
    allocator: std.mem.Allocator,
    io: Io,
    channel: *ch.Channel,
    hosts: []const []const u8,
    cursor: ?i64,
    cursor_path: [:0]const u8,
};

// 20s heartbeat: paired with the backend's ~90s staleness watchdog, so a
// half-open socket (our restart leaves no RST behind for an idle peer) gets
// detected and re-dialed instead of hanging the backend's read loop forever.
fn runHeartbeat(io: Io, channel: *ch.Channel) void {
    while (true) {
        io.sleep(Io.Duration.fromSeconds(20), .awake) catch {};
        channel.ping();
    }
}

fn runFirehose(ctx: FirehoseCtx) void {
    var client = zat.FirehoseClient.init(ctx.io, ctx.allocator, .{ .hosts = ctx.hosts, .cursor = ctx.cursor });
    defer client.deinit();
    var verifier = vf.Verifier.init(ctx.io, ctx.allocator);
    defer verifier.deinit();
    var pipeline = pl.Pipeline{
        .allocator = ctx.allocator,
        .io = ctx.io,
        .channel = ctx.channel,
        .verifier = &verifier,
    };
    pipeline.start() catch |err| {
        logfire.err("pipeline start failed: {s}", .{@errorName(err)});
        return;
    };
    var handler = Handler{
        .allocator = ctx.allocator,
        .verifier = &verifier,
        .pipeline = &pipeline,
        .channel = ctx.channel,
        .cursor_path = ctx.cursor_path,
    };
    client.subscribe(&handler) catch |err| {
        logfire.err("firehose subscribe ended: {s}", .{@errorName(err)});
    };
}

// test root: `_ = @import` forces analysis of each file's test blocks (lazy
// analysis skips them otherwise — same pattern as backend/src/main.zig).
test {
    _ = @import("channel.zig");
    _ = @import("pipeline.zig");
    _ = @import("verifier.zig");
    _ = @import("cbor_json.zig");
}
