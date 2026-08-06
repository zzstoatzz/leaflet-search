//! Builder mode: offline snapshot build + verified R2 publish.
//!
//! Invariant #1 of docs/scaling-plan.md: background data movement never
//! touches the serving box. This runs as a separate process (BUILDER_MODE=1
//! on an ephemeral machine or a cron), builds a fresh replica from turso into
//! a scratch path, refuses to publish unless sentinels pass, and uploads
//! artifact + manifest to R2 — latest pointer last, so a reader can never see
//! a pointer to an incomplete artifact (typeahead's channel discipline).
//!
//! Channels: staging by default; prod requires BUILDER_ALLOW_PROD=1 so a
//! scratch run can never overwrite the production pointer.
//!
//! Env: TURSO_URL, TURSO_TOKEN, INDEX_R2_{ENDPOINT,BUCKET,ACCESS_KEY_ID,
//! SECRET_ACCESS_KEY}; optional BUILD_DIR (default /tmp/leaflet-build),
//! BUILDER_CHANNEL (staging|prod), BUILDER_VERSION (git sha), SKIP_UPLOAD=1
//! (build + verify only).

const std = @import("std");
const Io = std.Io;
const Allocator = std.mem.Allocator;
const logfire = @import("logfire");
const zqlite = @import("zqlite");
const db = @import("db.zig");
const LocalDb = @import("db/LocalDb.zig");
const sync = @import("db/sync.zig");
const r2 = @import("r2.zig");

pub const MANIFEST_VERSION = 1;

const Channel = enum { staging, prod };
// builds were ~40min at a 60k-doc corpus with a 45min default — no headroom.
// export, VACUUM, sha256, and upload all scale ~linearly with corpus size, so
// give the watchdog room for 5x growth; BUILDER_TIMEOUT_SECS still overrides.
const DEFAULT_TIMEOUT_SECS: u64 = 120 * 60;
// turso pages complete in seconds normally, ~1-3min when turso is degraded
// (2026-08-06 00:40 run: 850s per 5k docs = ~85s/page and still finished).
// 10min of NO completed request is a hang, not slowness.
const DEFAULT_STALL_SECS: u64 = 10 * 60;

/// wall-clock seconds since `since_s`, for per-phase timing logs
fn secondsSince(io: Io, since_s: i64) i64 {
    const now_s: i64 = @intCast(@divFloor(Io.Timestamp.now(io, .real).nanoseconds, std.time.ns_per_s));
    return now_s - since_s;
}

fn getenv(name: [*:0]const u8) ?[]const u8 {
    return if (std.c.getenv(name)) |v| std.mem.span(v) else null;
}

fn parseSeconds(raw: ?[]const u8, default: u64) u64 {
    const value = std.fmt.parseInt(u64, raw orelse return default, 10) catch
        return default;
    return if (value == 0) default else value;
}

fn timeoutWatchdog(io: Io, timeout_s: u64) void {
    io.sleep(Io.Duration.fromSeconds(@intCast(timeout_s)), .awake) catch return;
    logfire.err("builder: exceeded {d}s wall-clock timeout; terminating", .{timeout_s});
    std.process.exit(1);
}

/// Armed only while the build is talking to turso (export + remote gates).
/// Zig's HTTP client has no read timeout, so a hung turso request blocks
/// forever with zero log output; without this, the only bound was the
/// wall-clock watchdog — a 94min silent stall on 2026-08-06. Exiting fast
/// instead lets the prefect task retry into fresh scratch within minutes.
var stall_armed: std.atomic.Value(bool) = .init(false);

fn stallWatchdog(io: Io, stall_s: u64) void {
    while (true) {
        io.sleep(Io.Duration.fromSeconds(30), .awake) catch return;
        if (!stall_armed.load(.monotonic)) continue;
        const last = db.Client.last_request_completed_s.load(.monotonic);
        if (last == 0) continue;
        const now_s: i64 = @intCast(@divFloor(Io.Timestamp.now(io, .real).nanoseconds, std.time.ns_per_s));
        if (now_s - last > @as(i64, @intCast(stall_s))) {
            logfire.err("builder: no turso request completed in {d}s (last at epoch {d}) — hung connection; terminating for retry", .{ stall_s, last });
            std.process.exit(1);
        }
    }
}

pub fn run(allocator: Allocator, io: Io) !void {
    const started_s: i64 = @intCast(@divFloor(Io.Timestamp.now(io, .real).nanoseconds, std.time.ns_per_s));
    const timeout_s = parseSeconds(getenv("BUILDER_TIMEOUT_SECS"), DEFAULT_TIMEOUT_SECS);
    const watchdog = try std.Thread.spawn(.{}, timeoutWatchdog, .{ io, timeout_s });
    watchdog.detach();
    const stall_s = parseSeconds(getenv("BUILDER_STALL_SECS"), DEFAULT_STALL_SECS);
    const stall_watchdog = try std.Thread.spawn(.{}, stallWatchdog, .{ io, stall_s });
    stall_watchdog.detach();
    logfire.info("builder: watchdog armed ({d}s wall-clock, {d}s turso stall)", .{ timeout_s, stall_s });

    const channel: Channel = blk: {
        const raw = getenv("BUILDER_CHANNEL") orelse "staging";
        if (std.mem.eql(u8, raw, "prod")) {
            if (getenv("BUILDER_ALLOW_PROD") == null) {
                logfire.err("builder: BUILDER_CHANNEL=prod requires BUILDER_ALLOW_PROD=1", .{});
                return error.ProdNotArmed;
            }
            break :blk .prod;
        }
        break :blk .staging;
    };

    const build_dir = getenv("BUILD_DIR") orelse "/tmp/leaflet-build";
    _ = std.c.mkdir(@ptrCast((std.fmt.allocPrintSentinel(allocator, "{s}", .{build_dir}, 0) catch return error.OutOfMemory).ptr), 0o755);

    var id_buf: [48]u8 = undefined;
    const nanos: u64 = @intCast(@mod(Io.Timestamp.now(io, .real).nanoseconds, 65536));
    const build_id = try std.fmt.bufPrint(&id_buf, "b{d}-{x:0>4}", .{ started_s, nanos });

    const db_path = try std.fmt.allocPrint(allocator, "{s}/replica.db", .{build_dir});
    const manifest_path = try std.fmt.allocPrint(allocator, "{s}/manifest.json", .{build_dir});

    // fresh scratch file every run — stale partial builds must not leak in
    for ([_][]const u8{ "", "-wal", "-shm" }) |suffix| {
        const path = try std.fmt.allocPrintSentinel(allocator, "{s}{s}", .{ db_path, suffix }, 0);
        _ = std.c.unlink(path.ptr);
    }

    logfire.info("builder: starting build {s} (channel={s}) -> {s}", .{ build_id, @tagName(channel), db_path });

    try db.initTurso(io);
    const turso = db.getClient() orelse return error.TursoNotInitialized;

    // arm the stall watchdog for everything that talks to turso (export +
    // remote count gates); seed the heartbeat so a hang on the very first
    // request still trips it
    db.Client.last_request_completed_s.store(@intCast(@divFloor(Io.Timestamp.now(io, .real).nanoseconds, std.time.ns_per_s)), .monotonic);
    stall_armed.store(true, .monotonic);

    // watermark BEFORE paging: docs indexed mid-build land in the next
    // snapshot; the overlay/promote side uses this as its freshness cutoff
    const watermark = blk: {
        var result = try turso.query("SELECT COALESCE(MAX(indexed_at), '') FROM documents", &.{});
        defer result.deinit();
        if (result.first()) |row| {
            break :blk try allocator.dupe(u8, row.text(0));
        }
        break :blk try allocator.dupe(u8, "");
    };

    var local = LocalDb.init(allocator, io);
    try local.openAt(db_path);

    const counts = try sync.buildSnapshot(turso, &local, watermark);
    const export_s = secondsSince(io, started_s);
    logfire.info("builder: built {d} docs, {d} pubs, {d} tags, {d} popular, {d} recommends, {d} subscriptions", .{
        counts.documents, counts.publications, counts.tags, counts.popular, counts.recommends, counts.subscriptions,
    });

    // ------------------------------------------------------------------
    // verification gates: refuse to publish a snapshot that fails any
    // ------------------------------------------------------------------
    const conn = local.getConn() orelse return error.LocalNotOpen;

    // sync_meta: a server adopting this snapshot (or offline catchup) resumes
    // incrementally from the build start, not from epoch
    {
        var ts_buf: [20]u8 = undefined;
        const ts_str = std.fmt.bufPrint(&ts_buf, "{d}", .{started_s}) catch "0";
        try conn.exec("INSERT OR REPLACE INTO sync_meta (key, value) VALUES ('last_sync', ?)", .{ts_str});
    }

    // gate 1: row count vs turso, both sides pinned to the watermark + the
    // same policy filters. The set is immutable below the watermark, so any
    // mismatch beyond deletes-during-build is a real bug; keep a small
    // tolerance for those deletes only.
    {
        logfire.info("builder: phase=verify_remote", .{});
        var result = try turso.query(sync.BUILD_DOC_COUNT_SQL, &.{watermark});
        defer result.deinit();
        const expected: usize = if (result.first()) |row| @intCast(row.int(0)) else 0;
        const tolerance = @max(expected / 1000, 10); // 0.1%, floor 10 — deletes mid-build only
        const diff = if (expected > counts.documents) expected - counts.documents else counts.documents - expected;
        if (expected == 0 or diff > tolerance) {
            logfire.err("builder: GATE FAIL doc count: built {d}, turso expects {d} (±{d})", .{ counts.documents, expected, tolerance });
            return error.DocCountGate;
        }
    }

    // gate 1b: recommends + subscriptions counts vs turso, watermark-pinned
    // like documents. these walks are keyset-paginated (sync.copyPaged); the
    // gate is what proves a paging bug can't silently ship a partial copy.
    inline for (.{
        .{ sync.BUILD_REC_COUNT_SQL, "recommends" },
        .{ sync.BUILD_SUB_COUNT_SQL, "subscriptions" },
    }, .{ counts.recommends, counts.subscriptions }) |gate, built| {
        var result = try turso.query(gate[0], &.{watermark});
        defer result.deinit();
        const expected: usize = if (result.first()) |row| @intCast(row.int(0)) else 0;
        const tolerance = @max(expected / 1000, 10); // deletes mid-build only
        const diff = if (expected > built) expected - built else built - expected;
        if (diff > tolerance) {
            logfire.err("builder: GATE FAIL {s} count: built {d}, turso expects {d} (±{d})", .{ gate[1], built, expected, tolerance });
            return error.CountGate;
        }
    }

    // turso is done — everything below is local (or rclone, which bounds
    // itself); an unarmed watchdog can't false-fire during a long VACUUM/upload
    stall_armed.store(false, .monotonic);

    // gate 2: FTS answers a query
    {
        const row = try conn.row("SELECT COUNT(*) FROM documents_fts WHERE documents_fts MATCH 'the'", .{});
        if (row) |r| {
            defer r.deinit();
            if (r.int(0) == 0) {
                logfire.err("builder: GATE FAIL fts sentinel returned 0 rows", .{});
                return error.FtsSentinelGate;
            }
        } else return error.FtsSentinelGate;
    }

    // leaving WAL mode requires being the ONLY connection — close LocalDb's
    // write conn + read pool first, then compact on a fresh solo connection
    local.deinit();
    const verify_done_s = secondsSince(io, started_s);

    {
        logfire.info("builder: phase=compact", .{});
        const path_z = try std.fmt.allocPrintSentinel(allocator, "{s}", .{db_path}, 0);
        const solo = try zqlite.open(path_z.ptr, zqlite.OpenFlags.ReadWrite);
        defer solo.close();

        // compact to a single file: checkpoint WAL, drop to DELETE journal,
        // VACUUM, then back to WAL (serving re-asserts WAL at open anyway)
        try solo.exec("PRAGMA wal_checkpoint(TRUNCATE)", .{});
        try solo.exec("PRAGMA journal_mode=DELETE", .{});
        try solo.exec("VACUUM", .{});
        try solo.exec("PRAGMA journal_mode=WAL", .{});
        try solo.exec("PRAGMA wal_checkpoint(TRUNCATE)", .{});

        // gate 3: page-structure integrity on the final file
        const row = try solo.row("PRAGMA quick_check", .{});
        if (row) |r| {
            defer r.deinit();
            if (!std.mem.eql(u8, r.text(0), "ok")) {
                logfire.err("builder: GATE FAIL quick_check: {s}", .{r.text(0)});
                return error.QuickCheckGate;
            }
        } else return error.QuickCheckGate;
    }

    const compact_done_s = secondsSince(io, started_s);

    // hash + size of the exact bytes that will be served
    var sha_hex: [64]u8 = undefined;
    const byte_size = try sha256File(allocator, io, db_path, &sha_hex);
    const hash_done_s = secondsSince(io, started_s);
    logfire.info("builder: replica.db {d} bytes sha256={s}", .{ byte_size, sha_hex });

    const prefix = switch (channel) {
        .prod => "",
        .staging => "staging/",
    };
    const snapshot_key = try std.fmt.allocPrint(allocator, "{s}builds/{s}/replica.db", .{ prefix, build_id });
    const manifest_key = try std.fmt.allocPrint(allocator, "{s}builds/{s}/manifest.json", .{ prefix, build_id });
    const latest_key: []const u8 = switch (channel) {
        .prod => "latest.json",
        .staging => "latest.staging.json",
    };

    const manifest = try std.fmt.allocPrint(allocator,
        \\{{
        \\  "manifest_version": {d},
        \\  "schema_version": {d},
        \\  "build_id": "{s}",
        \\  "channel": "{s}",
        \\  "snapshot_key": "{s}",
        \\  "byte_size": {d},
        \\  "sha256": "{s}",
        \\  "source_watermark": "{s}",
        \\  "doc_count": {d},
        \\  "pub_count": {d},
        \\  "tag_count": {d},
        \\  "rec_count": {d},
        \\  "created_at": {d},
        \\  "builder_version": "{s}"
        \\}}
        \\
    , .{
        MANIFEST_VERSION,        LocalDb.SCHEMA_VERSION, build_id,    @tagName(channel),
        snapshot_key,            byte_size,              &sha_hex,    watermark,
        counts.documents,        counts.publications,    counts.tags, counts.recommends,
        started_s,               getenv("BUILDER_VERSION") orelse "dev",
    });

    try writeFile(io, manifest_path, manifest);

    if (getenv("SKIP_UPLOAD") != null) {
        logfire.info("builder: SKIP_UPLOAD set — artifacts left at {s} ({s})", .{ build_dir, build_id });
        std.debug.print("builder: done (no upload). replica={s} manifest={s}\n", .{ db_path, manifest_path });
        return;
    }

    const cfg = try r2.configure(allocator, io);
    logfire.info("builder: phase=upload", .{});
    try r2.upload(allocator, io, cfg, db_path, snapshot_key);
    try r2.upload(allocator, io, cfg, manifest_path, manifest_key);
    // latest pointer LAST — readers can never see a pointer to a partial build
    try r2.upload(allocator, io, cfg, manifest_path, latest_key);

    const took_s = secondsSince(io, started_s);
    // per-phase wall-clock: which stage grows as the corpus does. deltas
    // between the checkpoints above; upload is everything after the hash.
    logfire.info("builder: timings export={d}s verify={d}s compact={d}s hash={d}s upload={d}s total={d}s", .{
        export_s, verify_done_s - export_s, compact_done_s - verify_done_s, hash_done_s - compact_done_s, took_s - hash_done_s, took_s,
    });
    logfire.info("builder: published {s} to {s} channel ({d} docs, {d}s)", .{ build_id, @tagName(channel), counts.documents, took_s });
}

test "builder timeout parser uses a safe nonzero default" {
    try std.testing.expectEqual(DEFAULT_TIMEOUT_SECS, parseSeconds(null, DEFAULT_TIMEOUT_SECS));
    try std.testing.expectEqual(DEFAULT_TIMEOUT_SECS, parseSeconds("", DEFAULT_TIMEOUT_SECS));
    try std.testing.expectEqual(DEFAULT_TIMEOUT_SECS, parseSeconds("nope", DEFAULT_TIMEOUT_SECS));
    try std.testing.expectEqual(DEFAULT_TIMEOUT_SECS, parseSeconds("0", DEFAULT_TIMEOUT_SECS));
    try std.testing.expectEqual(@as(u64, 90), parseSeconds("90", DEFAULT_TIMEOUT_SECS));
    try std.testing.expectEqual(DEFAULT_STALL_SECS, parseSeconds(null, DEFAULT_STALL_SECS));
}

fn writeFile(io: Io, path: []const u8, content: []const u8) !void {
    const file = try Io.Dir.createFileAbsolute(io, path, .{ .truncate = true });
    defer file.close(io);
    var wbuf: [4096]u8 = undefined;
    var fw = Io.File.Writer.init(file, io, &wbuf);
    try fw.interface.writeAll(content);
    try fw.interface.flush();
}

/// Streaming sha256 of a file; returns byte count, writes lowercase hex into out.
fn sha256File(allocator: Allocator, io: Io, path: []const u8, out_hex: *[64]u8) !u64 {
    _ = allocator;
    const file = try Io.Dir.openFileAbsolute(io, path, .{});
    defer file.close(io);

    var hasher = std.crypto.hash.sha2.Sha256.init(.{});
    var total: u64 = 0;
    var buf: [256 * 1024]u8 = undefined;
    while (true) {
        const n = file.readStreaming(io, &.{&buf}) catch |err| switch (err) {
            error.EndOfStream => break,
            else => return err,
        };
        if (n == 0) break;
        hasher.update(buf[0..n]);
        total += n;
    }
    var digest: [32]u8 = undefined;
    hasher.final(&digest);
    const hex = std.fmt.bytesToHex(digest, .lower);
    @memcpy(out_hex, &hex);
    return total;
}
