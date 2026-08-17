//! Origin-side response memo + ETag for GET /search.
//!
//! The replica is frozen between snapshot adoptions and the overlay adds a
//! trickle of live rows, so a search response is stable for minutes at a
//! time. The edge cache exploits that per-colo; this memo exploits it at the
//! origin, so N colos (or fly-direct callers) asking the same query cost one
//! search instead of N.
//!
//! Validity token = (snapshot adoption generation, 300s wall-clock bucket).
//! The bucket bounds overlay staleness and recency-score drift to ≤5min —
//! well inside the edge's own 10min fresh window. Adoption bumps the
//! generation, invalidating everything at once.
//!
//! The same token doubles as the ETag: any /search URL's body is stable
//! while the token holds, so an If-None-Match match answers 304 with zero
//! search work (the edge revalidates instead of re-fetching bodies).
//!
//! Bounded: MAX_ENTRIES, clear-all on overflow or token change. Bodies are
//! page_allocator-owned copies; entries never outlive the map.

const std = @import("std");
const Io = std.Io;

const MAX_ENTRIES = 512;

// Generation = boot-time seconds (lazily seeded) + adoption count. Seeding
// from the wall clock instead of a constant keeps tokens unique across
// process restarts: a restart plus an adoption inside one 5-min bucket must
// not reuse a pre-adoption token, or a held ETag turns into a wrong 304.
// Monotone because uptime seconds grow far faster than adoptions (~1 per 2h).
var boot_seed = std.atomic.Value(u64).init(0);
var adoptions = std.atomic.Value(u64).init(0);

var mutex: Io.Mutex = Io.Mutex.init;
var entries: ?std.StringHashMap(Entry) = null;
var stored_token: u64 = 0;

const Entry = struct {
    body: []u8,
};

/// Called when a new snapshot is adopted (boot + promote watcher).
pub fn bumpGeneration() void {
    _ = adoptions.fetchAdd(1, .monotonic);
}

fn generationValue(io: Io) u64 {
    var s = boot_seed.load(.monotonic);
    if (s == 0) {
        const secs: u64 = @intCast(@divTrunc(Io.Timestamp.now(io, .real).toMicroseconds(), 1_000_000));
        _ = boot_seed.cmpxchgStrong(0, secs, .monotonic, .monotonic);
        s = boot_seed.load(.monotonic);
    }
    return s +% adoptions.load(.monotonic);
}

/// Opaque validity token: changes on adoption and every 300s.
pub fn token(io: Io) u64 {
    const secs = @divTrunc(Io.Timestamp.now(io, .real).toMicroseconds(), 1_000_000);
    const bucket: u64 = @intCast(@divTrunc(secs, 300));
    return generationValue(io) *% 0x1_0000_0000 +% bucket;
}

/// Render the current token as an ETag value into `buf`.
pub fn etag(io: Io, buf: []u8) []const u8 {
    return std.fmt.bufPrint(buf, "\"ps-{x}\"", .{token(io)}) catch "\"ps\"";
}

fn ensureLocked(alloc: std.mem.Allocator) *std.StringHashMap(Entry) {
    if (entries == null) entries = std.StringHashMap(Entry).init(alloc);
    return &entries.?;
}

fn clearLocked() void {
    if (entries) |*map| {
        var it = map.iterator();
        while (it.next()) |e| {
            std.heap.page_allocator.free(e.key_ptr.*);
            std.heap.page_allocator.free(e.value_ptr.body);
        }
        map.clearRetainingCapacity();
    }
}

/// Duped copy of the memoized body for `target`, or null. `alloc` owns the
/// returned slice (pass the request arena).
pub fn get(io: Io, alloc: std.mem.Allocator, target: []const u8) ?[]u8 {
    const now = token(io);
    mutex.lockUncancelable(io);
    defer mutex.unlock(io);
    if (stored_token != now) {
        clearLocked();
        stored_token = now;
        return null;
    }
    if (entries) |*map| {
        const e = map.get(target) orelse return null;
        return alloc.dupe(u8, e.body) catch null;
    }
    return null;
}

/// Store a successful response body for `target`. Best-effort: allocation
/// failure just skips memoization.
pub fn put(io: Io, target: []const u8, body: []const u8) void {
    const now = token(io);
    const pa = std.heap.page_allocator;
    mutex.lockUncancelable(io);
    defer mutex.unlock(io);
    const map = ensureLocked(pa);
    if (stored_token != now or map.count() >= MAX_ENTRIES) {
        clearLocked();
        stored_token = now;
    }
    if (map.contains(target)) return;
    const key = pa.dupe(u8, target) catch return;
    const val = pa.dupe(u8, body) catch {
        pa.free(key);
        return;
    };
    map.put(key, .{ .body = val }) catch {
        pa.free(key);
        pa.free(val);
    };
}

/// Test hook: drop everything.
pub fn reset(io: Io) void {
    mutex.lockUncancelable(io);
    defer mutex.unlock(io);
    clearLocked();
    stored_token = 0;
}

test "memo: put/get round-trip, arena-owned copy" {
    var threaded = std.Io.Threaded.init(std.testing.allocator, .{});
    defer threaded.deinit();
    const tio = threaded.io();
    reset(tio);
    defer reset(tio);
    put(tio, "/search?q=zig", "[{\"a\":1}]");
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const got = get(tio, arena.allocator(), "/search?q=zig") orelse return error.TestExpectedHit;
    try std.testing.expectEqualStrings("[{\"a\":1}]", got);
    try std.testing.expect(get(tio, arena.allocator(), "/search?q=other") == null);
}

test "memo: generation bump invalidates" {
    var threaded = std.Io.Threaded.init(std.testing.allocator, .{});
    defer threaded.deinit();
    const tio = threaded.io();
    reset(tio);
    defer reset(tio);
    put(tio, "/search?q=zig", "old");
    bumpGeneration();
    try std.testing.expect(get(tio, std.testing.allocator, "/search?q=zig") == null);
}

test "memo: etag renders and changes with generation" {
    var threaded = std.Io.Threaded.init(std.testing.allocator, .{});
    defer threaded.deinit();
    const tio = threaded.io();
    var buf: [64]u8 = undefined;
    var buf2: [64]u8 = undefined;
    const a = etag(tio, &buf);
    try std.testing.expect(std.mem.startsWith(u8, a, "\"ps-"));
    bumpGeneration();
    const b = etag(tio, &buf2);
    try std.testing.expect(!std.mem.eql(u8, a, b));
}
