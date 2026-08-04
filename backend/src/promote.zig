//! Promote watcher: adopt verified snapshots from R2.
//!
//! Behind ENABLE_SNAPSHOT_PROMOTE (absent = inert). Polls the channel's
//! latest pointer, and when a new build appears: downloads to a `.part`
//! file on the volume, verifies EVERYTHING the manifest claims (size,
//! sha256, doc count) plus semantic sentinels the manifest can't claim
//! (no banned DIDs, no bridgy rows, FTS answers, platforms present),
//! stages it as `local.db.new` + manifest sidecar, and exits 0 so the
//! machine restarts into LocalDb.adoptPending() — the same boot-adopt
//! path every deploy already exercises. The previous snapshot is kept as
//! `local.db.prev` for one-command rollback.
//!
//! Promotion REJECTS, it never best-effort attaches: any gate failure
//! deletes the .part and logs; serving continues on the current snapshot.
//!
//! Channel discipline (Codex review, 2026-06-11): the watcher is pinned to
//! PROMOTE_CHANNEL (default staging) and refuses a manifest whose channel
//! field disagrees — a staging build can never be adopted by a prod
//! watcher, even if someone uploads it to the wrong key.

const std = @import("std");
const Io = std.Io;
const Allocator = std.mem.Allocator;
const zqlite = @import("zqlite");
const logfire = @import("logfire");
const r2 = @import("r2.zig");
const policy = @import("policy.zig");
const LocalDb = @import("db/LocalDb.zig");

pub const MANIFEST_VERSION = 1;

pub const Manifest = struct {
    manifest_version: u32,
    schema_version: u32 = 0, // 0 = legacy manifest from before the field existed
    build_id: []const u8,
    channel: []const u8,
    snapshot_key: []const u8,
    byte_size: u64,
    sha256: []const u8,
    source_watermark: []const u8 = "",
    doc_count: u64 = 0,
    pub_count: u64 = 0,
    tag_count: u64 = 0,
    rec_count: u64 = 0,
    created_at: i64 = 0,
    builder_version: []const u8 = "dev",
};

fn getenv(name: [*:0]const u8) ?[]const u8 {
    return if (std.c.getenv(name)) |v| std.mem.span(v) else null;
}

pub fn enabled() bool {
    return std.c.getenv("ENABLE_SNAPSHOT_PROMOTE") != null;
}

pub fn channelName() []const u8 {
    return getenv("PROMOTE_CHANNEL") orelse "staging";
}

pub fn latestKeyForChannel(channel: []const u8) []const u8 {
    return if (std.mem.eql(u8, channel, "prod")) "latest.json" else "latest.staging.json";
}

fn pollIntervalSecs() u64 {
    const raw = getenv("PROMOTE_POLL_SECS") orelse return 300;
    return std.fmt.parseInt(u64, raw, 10) catch 300;
}

/// Spawn the watcher thread if the flag is set. Call from initServices once
/// the local db is open (the watcher compares against the live manifest).
pub fn start(allocator: Allocator, io: Io) void {
    if (!enabled()) return;
    const thread = std.Thread.spawn(.{}, watchLoop, .{ allocator, io }) catch |err| {
        logfire.err("promote: failed to spawn watcher: {}", .{err});
        return;
    };
    thread.detach();
    logfire.info("promote: watcher started (channel={s}, poll={d}s)", .{ channelName(), pollIntervalSecs() });
}

fn watchLoop(allocator: Allocator, io: Io) void {
    const interval = pollIntervalSecs();
    while (true) {
        // sleep FIRST: at boot we just (possibly) adopted; no point
        // re-checking R2 before the world has moved
        io.sleep(Io.Duration.fromSeconds(@intCast(interval)), .awake) catch return;
        checkOnce(allocator, io) catch |err| {
            logfire.warn("promote: check failed: {s} (will retry)", .{@errorName(err)});
        };
    }
}

fn localDbPath() []const u8 {
    return getenv("LOCAL_DB_PATH") orelse "/data/local.db";
}

fn readFileAlloc(allocator: Allocator, io: Io, path: []const u8, max: usize) ![]u8 {
    const file = try Io.Dir.openFileAbsolute(io, path, .{});
    defer file.close(io);
    var out: std.ArrayList(u8) = .empty;
    errdefer out.deinit(allocator);
    var buf: [16 * 1024]u8 = undefined;
    while (out.items.len < max) {
        const n = file.readStreaming(io, &.{&buf}) catch |err| switch (err) {
            error.EndOfStream => break,
            else => return err,
        };
        if (n == 0) break;
        try out.appendSlice(allocator, buf[0..n]);
    }
    return out.toOwnedSlice(allocator);
}

fn writeFile(io: Io, path: []const u8, content: []const u8) !void {
    const file = try Io.Dir.createFileAbsolute(io, path, .{ .truncate = true });
    defer file.close(io);
    var wbuf: [4096]u8 = undefined;
    var fw = Io.File.Writer.init(file, io, &wbuf);
    try fw.interface.writeAll(content);
    try fw.interface.flush();
}

fn unlinkZ(allocator: Allocator, path: []const u8) void {
    const z = std.fmt.allocPrintSentinel(allocator, "{s}", .{path}, 0) catch return;
    defer allocator.free(z);
    _ = std.c.unlink(z.ptr);
}

/// Read a sidecar manifest's build_id ("" if absent/unparseable).
fn sidecarBuildId(allocator: Allocator, io: Io, path: []const u8, buf: []u8) []const u8 {
    const bytes = readFileAlloc(allocator, io, path, 64 * 1024) catch return "";
    defer allocator.free(bytes);
    const parsed = std.json.parseFromSlice(Manifest, allocator, bytes, .{
        .ignore_unknown_fields = true,
        .allocate = .alloc_always,
    }) catch return "";
    defer parsed.deinit();
    const id = parsed.value.build_id;
    if (id.len > buf.len) return "";
    @memcpy(buf[0..id.len], id);
    return buf[0..id.len];
}

/// Read a sidecar manifest's doc_count (0 if absent/unparseable).
fn sidecarDocCount(allocator: Allocator, io: Io, path: []const u8) u64 {
    const bytes = readFileAlloc(allocator, io, path, 64 * 1024) catch return 0;
    defer allocator.free(bytes);
    const parsed = std.json.parseFromSlice(Manifest, allocator, bytes, .{
        .ignore_unknown_fields = true,
        .allocate = .alloc_always,
    }) catch return 0;
    defer parsed.deinit();
    return parsed.value.doc_count;
}

/// Shrink floor: refuse a candidate whose doc_count is less than half the
/// currently-served build's. Every gate downstream checks the snapshot
/// against ITS OWN manifest — internally consistent but drastically smaller
/// (stale pointer, wrong-channel copy, a builder bug the build-side gates
/// missed) would still pass. Legitimate mass shrinks (a ban-wave purge) set
/// PROMOTE_ALLOW_SHRINK=1 for one adoption. (typeahead's "stale-but-correct
/// over fresh-but-suspect" count floor, docs/scale-300k-plan.md §4.)
fn passesShrinkFloor(candidate_docs: u64, live_docs: u64) bool {
    if (live_docs == 0) return true; // nothing served yet / legacy sidecar
    if (std.c.getenv("PROMOTE_ALLOW_SHRINK") != null) return true;
    return candidate_docs >= live_docs / 2;
}

fn checkOnce(allocator: Allocator, io: Io) !void {
    var arena_state = std.heap.ArenaAllocator.init(allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    const cfg = try r2.configure(arena, io);
    const channel = channelName();
    const latest_key = latestKeyForChannel(channel);

    // 1. fetch the pointer (tiny — safe to re-pull every poll)
    const latest_path = "/tmp/promote-latest.json";
    try r2.download(arena, io, cfg, latest_key, latest_path);
    const manifest_bytes = try readFileAlloc(arena, io, latest_path, 64 * 1024);

    const parsed = std.json.parseFromSlice(Manifest, arena, manifest_bytes, .{
        .ignore_unknown_fields = true,
        .allocate = .alloc_always,
    }) catch |err| {
        logfire.warn("promote: latest manifest unparseable: {}", .{err});
        return error.BadManifest;
    };
    const m = parsed.value;

    // 2. gates that need no download
    if (m.manifest_version != MANIFEST_VERSION) {
        logfire.warn("promote: unsupported manifest_version {d}", .{m.manifest_version});
        return error.UnsupportedManifestVersion;
    }
    if (!std.mem.eql(u8, m.channel, channel)) {
        logfire.warn("promote: channel mismatch: manifest={s} watcher={s} — refusing", .{ m.channel, channel });
        return error.ChannelMismatch;
    }
    // schema compatibility: a snapshot from an out-of-date builder image must
    // stall freshness, never break serving. 0 = legacy manifest (pre-field),
    // accepted with a warning until those age out of the channel.
    if (m.schema_version != 0 and m.schema_version != LocalDb.SCHEMA_VERSION) {
        logfire.warn("promote: schema mismatch: manifest v{d}, serving v{d} — refusing (recreate the builder machine)", .{ m.schema_version, LocalDb.SCHEMA_VERSION });
        return error.SchemaMismatch;
    }
    if (m.schema_version == 0) {
        logfire.warn("promote: legacy manifest without schema_version — accepting (build {s})", .{m.build_id});
    }

    // 3. already current or already staged?
    const live = localDbPath();
    const live_sidecar = try std.fmt.allocPrint(arena, "{s}.manifest.json", .{live});
    const staged_sidecar = try std.fmt.allocPrint(arena, "{s}.new.manifest.json", .{live});
    var id_buf_a: [64]u8 = undefined;
    var id_buf_b: [64]u8 = undefined;
    if (std.mem.eql(u8, sidecarBuildId(arena, io, live_sidecar, &id_buf_a), m.build_id)) return;
    if (std.mem.eql(u8, sidecarBuildId(arena, io, staged_sidecar, &id_buf_b), m.build_id)) {
        logfire.info("promote: build {s} already staged, awaiting restart", .{m.build_id});
        return;
    }

    const live_docs = sidecarDocCount(arena, io, live_sidecar);
    if (!passesShrinkFloor(m.doc_count, live_docs)) {
        logfire.err("promote: GATE FAIL shrink floor: candidate {s} has {d} docs, serving {d} — refusing (set PROMOTE_ALLOW_SHRINK=1 if intentional)", .{ m.build_id, m.doc_count, live_docs });
        return error.ShrinkFloorGate;
    }

    logfire.info("promote: new build {s} on {s} channel ({d} docs, watermark {s})", .{
        m.build_id, channel, m.doc_count, m.source_watermark,
    });

    // 4. download to .part on the volume (same filesystem as live — the
    //    final rename must be atomic)
    const part_path = try std.fmt.allocPrint(arena, "{s}.part", .{live});
    unlinkZ(arena, part_path); // stale partials from prior failed attempts
    try r2.download(arena, io, cfg, m.snapshot_key, part_path);
    errdefer unlinkZ(arena, part_path);

    // 5. verify bytes: exact size + sha256 against the manifest
    var sha_hex: [64]u8 = undefined;
    const actual_size = try sha256File(io, part_path, &sha_hex);
    if (actual_size != m.byte_size) {
        logfire.err("promote: GATE FAIL size: got {d}, manifest says {d}", .{ actual_size, m.byte_size });
        return error.SizeGate;
    }
    if (!std.mem.eql(u8, &sha_hex, m.sha256)) {
        logfire.err("promote: GATE FAIL sha256: got {s}, manifest says {s}", .{ &sha_hex, m.sha256 });
        return error.Sha256Gate;
    }

    // 6. verify contents: open read-only, run structural + semantic sentinels
    try verifySnapshot(arena, part_path, m);

    // 7. stage atomically: sidecar first, then the rename that makes it real
    try writeFile(io, staged_sidecar, manifest_bytes);
    {
        const part_z = try std.fmt.allocPrintSentinel(arena, "{s}", .{part_path}, 0);
        const new_z = try std.fmt.allocPrintSentinel(arena, "{s}.new", .{live}, 0);
        if (std.c.rename(part_z.ptr, new_z.ptr) != 0) return error.StageRenameFailed;
    }
    // the read-only verify left .part-wal/-shm behind; the .part is renamed
    // away, so they're pure litter
    inline for (.{ "-wal", "-shm" }) |suffix| {
        const aux = try std.fmt.allocPrint(arena, "{s}{s}", .{ part_path, suffix });
        unlinkZ(arena, aux);
    }

    // 8. adopt via the boot path every deploy already exercises. Requires
    //    the machine's restart policy to be `always` — exit(0) under
    //    `on-failure` strands the machine.
    logfire.info("promote: staged build {s}; exiting to adopt (boot takes ~25s)", .{m.build_id});
    std.process.exit(0);
}

/// Structural + semantic sentinels on the downloaded snapshot. The manifest
/// is the builder's claim; this is the consumer's independent check — and
/// the semantic gates (banned/bridgy zero, platforms present, FTS answers)
/// catch what byte equality can't: a faithfully-delivered bad build.
fn verifySnapshot(arena: Allocator, path: []const u8, m: Manifest) !void {
    const path_z = try std.fmt.allocPrintSentinel(arena, "{s}", .{path}, 0);
    const conn = zqlite.open(path_z.ptr, zqlite.OpenFlags.ReadOnly) catch return error.OpenGate;
    defer conn.close();

    // quick_check: page-structure integrity (seconds, not minutes)
    {
        const row = conn.row("PRAGMA quick_check", .{}) catch return error.QuickCheckGate;
        if (row) |r| {
            defer r.deinit();
            if (!std.mem.eql(u8, r.text(0), "ok")) {
                logfire.err("promote: GATE FAIL quick_check: {s}", .{r.text(0)});
                return error.QuickCheckGate;
            }
        } else return error.QuickCheckGate;
    }

    // doc count must match the manifest exactly — the build is pinned to its
    // watermark, so there is no race to tolerate here
    {
        const row = conn.row("SELECT COUNT(*) FROM documents", .{}) catch return error.CountGate;
        if (row) |r| {
            defer r.deinit();
            const got: u64 = @intCast(r.int(0));
            if (got != m.doc_count) {
                logfire.err("promote: GATE FAIL doc count: snapshot has {d}, manifest says {d}", .{ got, m.doc_count });
                return error.CountGate;
            }
        } else return error.CountGate;
    }

    // recommends count must match when the manifest claims one (v2+ builds;
    // 0 = pre-recommends manifest, nothing to check)
    if (m.rec_count > 0) {
        const row = conn.row("SELECT COUNT(*) FROM recommends", .{}) catch return error.CountGate;
        if (row) |r| {
            defer r.deinit();
            const got: u64 = @intCast(r.int(0));
            if (got != m.rec_count) {
                logfire.err("promote: GATE FAIL rec count: snapshot has {d}, manifest says {d}", .{ got, m.rec_count });
                return error.CountGate;
            }
        } else return error.CountGate;
    }

    // policy: zero banned-DID rows, zero bridgy rows — a snapshot that
    // resurrects either is rejected no matter how valid its bytes are
    inline for (policy.BANNED_DIDS) |banned| {
        const row = conn.row("SELECT COUNT(*) FROM documents WHERE did = ?", .{banned}) catch return error.PolicyGate;
        if (row) |r| {
            defer r.deinit();
            if (r.int(0) != 0) {
                logfire.err("promote: GATE FAIL banned DID present: {s} ({d} rows)", .{ banned, r.int(0) });
                return error.PolicyGate;
            }
        } else return error.PolicyGate;
    }
    {
        const row = conn.row("SELECT COUNT(*) FROM documents WHERE COALESCE(is_bridgyfed, 0) IN (1, '1')", .{}) catch return error.PolicyGate;
        if (row) |r| {
            defer r.deinit();
            if (r.int(0) != 0) {
                logfire.err("promote: GATE FAIL bridgy rows present: {d}", .{r.int(0)});
                return error.PolicyGate;
            }
        } else return error.PolicyGate;
    }

    // semantic sentinels: FTS answers a real query, and the corpus spans the
    // platforms we index (an FTS table that is non-empty but missing whole
    // platforms is a bad build, not a valid snapshot)
    {
        const row = conn.row("SELECT COUNT(*) FROM documents_fts WHERE documents_fts MATCH 'leaflet'", .{}) catch return error.FtsGate;
        if (row) |r| {
            defer r.deinit();
            if (r.int(0) == 0) {
                logfire.err("promote: GATE FAIL fts sentinel returned 0 rows", .{});
                return error.FtsGate;
            }
        } else return error.FtsGate;
    }
    inline for (.{ "leaflet", "whitewind" }) |platform| {
        const row = conn.row("SELECT COUNT(*) FROM documents WHERE platform = ?", .{platform}) catch return error.PlatformGate;
        if (row) |r| {
            defer r.deinit();
            if (r.int(0) == 0) {
                logfire.err("promote: GATE FAIL platform {s} has 0 docs", .{platform});
                return error.PlatformGate;
            }
        } else return error.PlatformGate;
    }

    // the snapshot must carry its sync watermark so incremental tooling
    // (and the future overlay) knows where it stands
    {
        const row = conn.row("SELECT value FROM sync_meta WHERE key = 'last_sync'", .{}) catch return error.MetaGate;
        if (row) |r| {
            defer r.deinit();
            if (r.text(0).len == 0) return error.MetaGate;
        } else return error.MetaGate;
    }
}

/// Streaming sha256; returns byte count, writes lowercase hex into out_hex.
pub fn sha256File(io: Io, path: []const u8, out_hex: *[64]u8) !u64 {
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

test "shrink floor: refuses <50% of serving count, tolerates growth and empty live" {
    try std.testing.expect(passesShrinkFloor(60_000, 60_000)); // steady
    try std.testing.expect(passesShrinkFloor(300_000, 60_000)); // influx growth
    try std.testing.expect(passesShrinkFloor(30_000, 60_000)); // exactly half: allowed
    try std.testing.expect(!passesShrinkFloor(29_999, 60_000)); // below half: refused
    try std.testing.expect(!passesShrinkFloor(0, 60_000)); // empty candidate: refused
    try std.testing.expect(passesShrinkFloor(10, 0)); // no live sidecar: allowed
}

test "latestKeyForChannel maps channels to pointer keys" {
    try std.testing.expectEqualStrings("latest.json", latestKeyForChannel("prod"));
    try std.testing.expectEqualStrings("latest.staging.json", latestKeyForChannel("staging"));
    try std.testing.expectEqualStrings("latest.staging.json", latestKeyForChannel("anything-else"));
}

test "manifest parses builder output and ignores unknown fields" {
    const json =
        \\{
        \\  "manifest_version": 1,
        \\  "schema_version": 1,
        \\  "build_id": "b1781159205-d85f",
        \\  "channel": "staging",
        \\  "snapshot_key": "staging/builds/b1781159205-d85f/replica.db",
        \\  "byte_size": 344346624,
        \\  "sha256": "eb62104b653ee7673d592db3d981cd9e6a28f144218b4a3eaddc5e874d5af6e4",
        \\  "source_watermark": "2026-06-11T06:26:20",
        \\  "doc_count": 25394,
        \\  "pub_count": 5731,
        \\  "tag_count": 89026,
        \\  "created_at": 1781159205,
        \\  "builder_version": "dev",
        \\  "some_future_field": true
        \\}
    ;
    const parsed = try std.json.parseFromSlice(Manifest, std.testing.allocator, json, .{
        .ignore_unknown_fields = true,
        .allocate = .alloc_always,
    });
    defer parsed.deinit();
    try std.testing.expectEqualStrings("b1781159205-d85f", parsed.value.build_id);
    try std.testing.expectEqual(@as(u64, 344346624), parsed.value.byte_size);
    try std.testing.expectEqualStrings("staging", parsed.value.channel);
}
