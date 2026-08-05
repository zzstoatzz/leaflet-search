const std = @import("std");
const Io = std.Io;
const logfire = @import("logfire");

const schema = @import("db/schema.zig");
const result = @import("db/result.zig");

// re-exports
pub const Client = @import("db/Client.zig");
pub const LocalDb = @import("db/LocalDb.zig");
pub const Row = result.Row;
pub const Result = result.Result;
pub const BatchResult = result.BatchResult;

// global state
const allocator = std.heap.smp_allocator;
var client: ?Client = null;
// Heap-allocated behind an atomic pointer so the promote watcher can hot-swap
// an adopted snapshot without a restart: readers load the pointer once per
// query and the displaced instance drains in the background (retireAsync).
var local_db: std.atomic.Value(?*LocalDb) = .init(null);

/// Initialize Turso client only (fast, call synchronously at startup).
/// Schema migrations run separately via initSchema() in the background thread
/// so a slow/unreachable turso doesn't block the accept loop.
pub fn initTurso(io: Io) !void {
    client = try Client.init(allocator, io);
}

/// Run schema migrations via zug. Call from background thread.
///
/// On an existing turso DB, `schema.bootstrapIfNeeded` seeds the
/// `zug_migrations` table on first run so no migrations actually execute.
/// On a fresh DB, the full migration list runs from scratch.
pub fn initSchema() void {
    if (client) |*c| {
        schema.init(allocator, c) catch |err| {
            logfire.err("schema init failed: {s}", .{@errorName(err)});
        };
    }
}

/// Initialize local SQLite replica (slow, call in background thread)
pub fn initLocalDb(io: Io) void {
    initLocal(io) catch |err| {
        std.debug.print("local db init failed (will use turso only): {}\n", .{err});
    };
}

fn initLocal(io: Io) !void {
    // check if local db is disabled
    if (std.c.getenv("LOCAL_DB_ENABLED")) |p| {
        const val = std.mem.span(p);
        if (std.mem.eql(u8, val, "false") or std.mem.eql(u8, val, "0")) {
            std.debug.print("local db disabled via LOCAL_DB_ENABLED\n", .{});
            return;
        }
    }

    const l = try allocator.create(LocalDb);
    errdefer allocator.destroy(l);
    l.* = LocalDb.init(allocator, io);
    try l.open();
    local_db.store(l, .release);
}

/// Hot snapshot adoption (promote watcher): open a fresh instance at the same
/// path — openAt's adoptPending renames the staged `.new` over the live file
/// first, while the displaced instance keeps serving its open FDs — then swap
/// the pointer and retire the old instance once its readers drain. No restart,
/// no serving gap.
pub fn swapLocalDb(io: Io) !void {
    const fresh = try allocator.create(LocalDb);
    errdefer allocator.destroy(fresh);
    fresh.* = LocalDb.init(allocator, io);
    try fresh.open();
    fresh.setReady(true);
    const old = local_db.swap(fresh, .acq_rel);
    if (old) |o| o.retireAsync();
}

pub fn getClient() ?*Client {
    if (client) |*c| return c;
    return null;
}

pub fn startKeepalive() void {
    if (client) |*c| c.startKeepalive();
}

/// Get local db if ready (synced and available)
pub fn getLocalDb() ?*LocalDb {
    const l = local_db.load(.acquire) orelse return null;
    if (l.isReady()) return l;
    return null;
}

/// Get local db even if not ready (for sync operations)
pub fn getLocalDbRaw() ?*LocalDb {
    return local_db.load(.acquire);
}

/// Mark the local replica ready to serve. In-place Turso→replica sync was
/// deleted (2026-06-26): the serving box's replica is refreshed only by
/// verified snapshot adoption at boot, never by background data movement
/// (the 2026-06-10 invariant). Kept as a named call so main.zig's startup
/// sequence reads unchanged.
pub fn startSync(io: Io) void {
    _ = io;
    if (getLocalDbRaw()) |l| l.setReady(true);
    std.debug.print("sync: in-place sync removed — serving adopted snapshot as-is\n", .{});
}
