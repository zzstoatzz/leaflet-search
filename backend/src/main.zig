const std = @import("std");
const Io = std.Io;
const Thread = std.Thread;
const logfire = @import("logfire");
const db = @import("db.zig");
const tpuf = @import("tpuf.zig");
const metrics = @import("metrics.zig");
const server = @import("server.zig");
const ingest = @import("ingest.zig");
const builder = @import("builder.zig");
const promote = @import("promote.zig");
const labeler = @import("labeler.zig");
const visibility = @import("visibility.zig");
const labeler_classifier = @import("ingest/classifier.zig");

const SOCKET_TIMEOUT_SECS = 5;

// multi-threaded debug_io — required for safe std.debug.print from worker threads
var threaded_io: Io.Threaded = undefined;
pub const std_options_debug_threaded_io: ?*Io.Threaded = &threaded_io;

// route every `std.log.*` call through logfire's OTEL log pipeline so
// stdlib + dependency log output is queryable in logfire alongside spans.
// active once `logfire.configure(...)` runs (it calls std_log_bridge.init);
// before that the bridge falls back to std.log.defaultLog (stderr).
pub const std_options: std.Options = .{
    .log_level = .info,
    .logFn = logfire.logFn,
};

pub fn main() !void {
    const allocator = std.heap.smp_allocator;

    // init Io backend for networking
    threaded_io = Io.Threaded.init(allocator, .{});
    const io = threaded_io.io();

    // configure logfire (reads LOGFIRE_WRITE_TOKEN from env)
    _ = logfire.configure(.{
        .service_name = "leaflet-search",
        .service_version = "0.1.0",
        .environment = if (std.c.getenv("FLY_APP_NAME")) |p| std.mem.span(p) else "development",
    }) catch |err| {
        std.debug.print("logfire init failed: {}, continuing without observability\n", .{err});
    };

    // builder mode: offline snapshot build + R2 publish, then exit.
    // never starts the server, never touches /data — scaling-plan invariant
    // #1 (background data movement stays off the serving box).
    if (std.c.getenv("BUILDER_MODE") != null) {
        builder.run(allocator, io) catch |err| {
            logfire.err("builder failed: {}", .{err});
            std.process.exit(1);
        };
        std.process.exit(0);
    }

    // start http server FIRST so Fly proxy doesn't timeout
    const port: u16 = blk: {
        const port_str = if (std.c.getenv("PORT")) |p| std.mem.span(p) else "3000";
        break :blk std.fmt.parseInt(u16, port_str, 10) catch 3000;
    };

    const address = Io.net.Ip4Address.unspecified(port);
    var listener = (Io.net.IpAddress{ .ip4 = address }).listen(io, .{ .reuse_address = true }) catch |err| {
        logfire.err("failed to listen on port {d}: {}", .{ port, err });
        return err;
    };
    defer listener.deinit(io);

    const app_name = if (std.c.getenv("APP_NAME")) |p| std.mem.span(p) else "leaflet-search";
    logfire.info("{s} listening on port {d}", .{ app_name, port });

    // init turso client synchronously (fast, needed for search fallback)
    try db.initTurso(io);

    // metrics modules just need `io` stashed so per-request handlers can
    // record latency / activity safely. these MUST be initialized before
    // the listener accept loop starts, otherwise the first poll of /activity
    // or /stats hits `global_io.?` and SIGABRTs the process. the heavier
    // services that depend on the local replica stay in initServices.
    metrics.activity.init(io);
    metrics.timing.setIo(io);
    metrics.buffer.init(io);

    // read vector-store config (env-only, no I/O) before the accept loop.
    // isSemanticEnabled() gates semantic search on these keys; if init ran
    // later in the background initServices thread (behind slow schema
    // migrations + local-db sync), every deploy served "semantic search not
    // available" for the whole startup window. keepalive (network) stays async.
    tpuf.init(io);

    // labeler: serves com.atproto.label.* on its own port + emits bulk-generated
    // account labels. No-op unless LABELER_DID is set, so this is safe to ship
    // before the labeler identity is provisioned.
    labeler.start(allocator, io);

    // autonomous bulk-generated classifier — fed per-document from the firehose
    // (see ingest/ingester.zig processDocument); emits via the labeler on its own.
    labeler_classifier.init();

    // init local db and other services in background (slow)
    const init_thread = try Thread.spawn(.{}, initServices, .{ allocator, io });
    init_thread.detach();

    // thread-per-connection (Thread.Pool removed in 0.16)
    while (true) {
        const stream = listener.accept(io) catch |err| {
            logfire.err("accept error: {}", .{err});
            continue;
        };

        setSocketTimeout(stream.socket.handle, SOCKET_TIMEOUT_SECS) catch |err| {
            logfire.warn("failed to set socket timeout: {}", .{err});
        };

        const accepted_at = Io.Timestamp.now(io, .real).toMicroseconds();
        const thread = Thread.spawn(.{}, server.handleConnection, .{ stream, io, accepted_at }) catch |err| {
            logfire.err("spawn error: {}", .{err});
            stream.close(io);
            continue;
        };
        thread.detach();
    }
}

fn initServices(allocator: std.mem.Allocator, io: Io) void {
    // run schema migrations first (idempotent, but may be slow if turso is laggy)
    db.initSchema();

    // hydrate the hourly request-metrics ring buffer from turso (durable across
    // restarts + endpoint-enum resets). Runs after migrations so the table exists.
    metrics.timing.loadFromTurso();

    // init local db (slow - turso already initialized)
    db.initLocalDb(io);
    db.startSync(io);

    // serving-time visibility policy. Seeds from the replica we just opened
    // (no network) and then refreshes from turso in the background, so a
    // showInDiscover preference is never resolved on the request path. Must
    // come straight after the replica opens: until the set loads, the search
    // paths fail closed, which is correct but hides opted-in results too.
    visibility.start(io);

    // one-time: feed the existing corpus through the classifier so it evaluates
    // every already-indexed author, not just ones publishing after deploy.
    // Background thread — the replica is open, this just reads it.
    if (Thread.spawn(.{}, labeler_classifier.bootstrap, .{})) |t| t.detach() else |_| {}

    // model-pass gate: background worker that confirms flagged authors are
    // bulk-generated (reads content, asks an LLM) before the labeler emits.
    // No-op without COCORE_API_KEY — flagged authors just queue unlabeled.
    labeler_classifier.startReview(allocator, io);

    // snapshot promote watcher (inert unless ENABLE_SNAPSHOT_PROMOTE is set)
    promote.start(allocator, io);

    // metrics.activity / metrics.buffer / metrics.timing are now initialized
    // up in main() before the listener starts so request handlers can safely
    // call record() on them.

    // tpuf.init() ran synchronously in main() before the accept loop (it is
    // env-only and gates semantic search). keepalive does network I/O, so it
    // stays here in the background.
    tpuf.startKeepalive(allocator);

    // keep turso connection warm (avoids ~1s TLS handshake on first query after idle)
    db.startKeepalive();

    // seed + start the background refresh for /recommended and /curators
    // so leaderboard pages never block user requests on a remote Turso query.
    server.initRecommendedCache(io);
    server.initCuratorsCache(io);
    server.initSubscribedCache(io);
    server.initDashboardCache(io);
    server.initTagsCache(io);
    server.initPopularCache(io);

    // prune search_events older than 90 days on each boot. Bounded
    // growth + natural privacy hygiene; the popular-searches window is
    // only 7 days so anything older has no read consumer.
    if (db.getClient()) |c| {
        c.exec("DELETE FROM search_events WHERE at < strftime('%s', 'now') - 90 * 86400", &.{}) catch {};
    }

    // start reconciler (verifies documents still exist at source PDS)
    ingest.reconciler.start(allocator, io);

    // start embedder (voyage-4-lite, 1024 dims, 1 worker)
    ingest.embedder.start(allocator, io);

    // start ingester consumer
    ingest.ingester.consumer(allocator, io);
}

fn setSocketTimeout(fd: std.posix.fd_t, secs: u32) !void {
    const timeout = std.mem.toBytes(std.posix.timeval{
        .sec = @intCast(secs),
        .usec = 0,
    });
    try std.posix.setsockopt(fd, std.posix.SOL.SOCKET, std.posix.SO.RCVTIMEO, &timeout);
    try std.posix.setsockopt(fd, std.posix.SOL.SOCKET, std.posix.SO.SNDTIMEO, &timeout);
}

// Force the test runner to include `test "..."` blocks from non-root modules.
// Without this, only tests literally inside main.zig would run — referencing
// a module via `pub const X = @import("x.zig").X` does not pull in `x.zig`'s
// test blocks. Add new test-bearing files here as they appear.
test {
    _ = @import("builder.zig");
    _ = @import("db/Client.zig");
    _ = @import("db/zug_conn.zig");
    _ = @import("db/migrations.zig");
    _ = @import("ingest/extractor.zig");
    _ = @import("ingest/ingester.zig");
    _ = @import("server/search.zig");
    _ = @import("server/documents.zig");
    _ = @import("server.zig");
    _ = @import("policy.zig");
    _ = @import("visibility.zig");
    _ = @import("promote.zig");
    _ = @import("db/LocalDb.zig");
    _ = @import("server/pubkey.zig");
    _ = @import("server/cache.zig");
    _ = @import("labeler.zig");
    _ = @import("labeler/label.zig");
    _ = @import("labeler/store.zig");
    _ = @import("labeler/server.zig");
    _ = @import("ingest/classifier.zig");
    _ = @import("metrics/timing.zig");
}
