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

    // Fly process-group role (plyr.fm's app/worker split; see fly.toml).
    // `app` serves HTTP + ingest + labeler (everything touching /data);
    // `worker` runs the noisy stateless tenants (reconciler, embedder) on
    // their own machine so background storms can't sit on the serving vCPU
    // (2026-08-13: a review-loop + reconcile burst made 'what' take 14s).
    // Default `all` keeps dev and un-updated deploys byte-identical.
    const role = processRole();

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

    if (role != .worker) {
        // labeler: serves com.atproto.label.* on its own port + emits bulk-generated
        // account labels. No-op unless LABELER_DID is set, so this is safe to ship
        // before the labeler identity is provisioned.
        labeler.start(allocator, io);

        // autonomous bulk-generated classifier — fed per-document from the firehose
        // (see ingest/ingester.zig processDocument); emits via the labeler on its own.
        // State (author-stats.db) lives on the serving volume, so it stays here.
        labeler_classifier.init();
    }

    // init local db and other services in background (slow)
    const init_thread = try Thread.spawn(.{}, initServices, .{ allocator, io, role });
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

/// Which services this process runs. `all` (no PROCESS_ROLE env) is the
/// pre-split behavior — dev and any deploy without updated fly.toml are
/// unchanged. `app` and `worker` partition the tenants; see fly.toml.
const Role = enum { all, app, worker };

fn processRole() Role {
    const v = std.c.getenv("PROCESS_ROLE") orelse return .all;
    const s = std.mem.span(v);
    if (std.mem.eql(u8, s, "app")) return .app;
    if (std.mem.eql(u8, s, "worker")) return .worker;
    return .all;
}

fn initServices(allocator: std.mem.Allocator, io: Io, role: Role) void {
    if (role != .worker) {
        // FIRST: the search paths fail closed until the visibility set loads, so
        // anything ordered ahead of this lands directly in the window where
        // /search answers 503. Non-blocking — it seeds from turso (already
        // initialized, before the listener) on its own thread.
        visibility.start(io);
    }

    // run schema migrations first (idempotent, but may be slow if turso is laggy).
    // App-only: zug is checksum-serialized, but two groups racing the same
    // migration list on every deploy is pointless — the worker assumes schema.
    if (role != .worker) db.initSchema();

    if (role != .worker) {
        // hydrate the hourly request-metrics ring buffer from turso (durable across
        // restarts + endpoint-enum resets). Runs after migrations so the table exists.
        metrics.timing.loadFromTurso();

        // init local db (slow - turso already initialized). The worker has no
        // volume: everything below here that touches /data is app-only.
        db.initLocalDb(io);
        db.startSync(io);

        // live overlay beside the frozen snapshot (inert unless OVERLAY_WRITE=1)
        db.initOverlay(io);

        // Backstop for the visibility set: if turso was unreachable at boot the
        // refresher is still unloaded, and the replica can answer instead — its
        // publications table is complete at or below the snapshot watermark, which
        // is enough to serve while turso recovers.
        if (!visibility.isLoaded()) visibility.seedFromLocal();

        // one-time: feed the existing corpus through the classifier so it evaluates
        // every already-indexed author, not just ones publishing after deploy.
        // Background thread — the replica is open, this just reads it.
        if (Thread.spawn(.{}, labeler_classifier.bootstrap, .{})) |t| t.detach() else |_| {}

        // model-pass gate: background worker that confirms flagged authors are
        // bulk-generated (reads content, asks an LLM) before the labeler emits.
        // No-op without COCORE_API_KEY — flagged authors just queue unlabeled.
        // Stays on app: its queue lives in author-stats.db on the volume.
        labeler_classifier.startReview(allocator, io);

        // snapshot promote watcher (inert unless ENABLE_SNAPSHOT_PROMOTE is set)
        promote.start(allocator, io);

        // warm the replica's pages in the background — a deploy boot serves
        // from a file the page cache has never seen, and a cold common-word
        // FTS query reads thousands of scattered volume pages (14s vs 0.2s
        // warm, 2026-08-16). Serving proceeds during the warm, just slower.
        promote.startBootWarm(io);

        // tpuf.init() ran synchronously in main() before the accept loop (it is
        // env-only and gates semantic search). keepalive does network I/O, so it
        // stays here in the background.
        tpuf.startKeepalive(allocator);
    }

    // keep turso connection warm (avoids ~1s TLS handshake on first query after idle)
    db.startKeepalive();

    if (role != .worker) {
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
    }

    if (role != .app) {
        // the noisy stateless tenants — parallel PDS checks and embedding
        // batches — run on the worker machine so their bursts never share a
        // vCPU with search. Both talk only to turso/voyage/tpuf, no /data.

        // start reconciler (verifies documents still exist at source PDS)
        ingest.reconciler.start(allocator, io);

        // start embedder (voyage-4-lite, 1024 dims, 1 worker — exactly ONE
        // process group instance runs this: turso DiskANN tolerates a single
        // embedder writer, so the worker group must stay at one machine)
        ingest.embedder.start(allocator, io);
    }

    if (role != .worker) {
        // start the live-ingest consumer (writes turso + the overlay on /data).
        // INGEST_SOURCE=jetstream consumes a verified Jetstream V2 instance
        // directly (ingest/jetstream.zig); default is the fly-app /channel
        // path, kept as the rollback until the jetstream cutover soaks clean.
        const source = if (std.c.getenv("INGEST_SOURCE")) |p| std.mem.span(p) else "channel";
        if (std.mem.eql(u8, source, "jetstream")) {
            ingest.jetstream.consumer(allocator, io);
        } else {
            ingest.ingester.consumer(allocator, io);
        }
    }
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
    _ = @import("ingest/reconciler.zig");
    _ = @import("ingest/ingester.zig");
    _ = @import("ingest/jetstream.zig");
    _ = @import("server/search.zig");
    _ = @import("server/documents.zig");
    _ = @import("server.zig");
    _ = @import("policy.zig");
    _ = @import("visibility.zig");
    _ = @import("promote.zig");
    _ = @import("db/LocalDb.zig");
    _ = @import("db/OverlayDb.zig");
    _ = @import("server/pubkey.zig");
    _ = @import("server/cache.zig");
    _ = @import("labeler.zig");
    _ = @import("labeler/label.zig");
    _ = @import("labeler/store.zig");
    _ = @import("labeler/server.zig");
    _ = @import("ingest/classifier.zig");
    _ = @import("metrics/timing.zig");
}
