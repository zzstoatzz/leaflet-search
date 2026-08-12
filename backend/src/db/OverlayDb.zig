//! Live overlay over the frozen serving snapshot (typeahead's snapshot+overlay
//! design, see docs/scaling-plan.md). The snapshot replica is replaced
//! wholesale on adoption; this file persists across adoptions and carries the
//! documents ingested since the adopted snapshot's watermark, so search
//! freshness no longer depends on adoption cadence.
//!
//! Consistency: writes are at-least-once and idempotent, applied AFTER the
//! turso commit; a lost overlay write is healed by the next snapshot. The
//! overlay is a freshness cache, never a source of truth — every row here is
//! either also in turso or newer than the serving snapshot.
//!
//! Compaction key is `indexed_at` in turso's timestamp domain: the builder
//! records MAX(documents.indexed_at) as the manifest `source_watermark`, and
//! rows at or below the adopted watermark are already baked into the snapshot.
//! Tombstones ≤ watermark are equally safe to drop: the builder exports
//! current state, so a delete committed before the watermark is absent from
//! that snapshot.

const std = @import("std");
const Io = std.Io;
const zqlite = @import("zqlite");
const Allocator = std.mem.Allocator;
const logfire = @import("logfire");

const OverlayDb = @This();

/// Same shedding rationale as the ingest queue: a backfill re-touching old
/// docs is harmless to lose from the overlay (the snapshot already has them),
/// so beyond the cap the oldest rows go first.
pub const DEFAULT_MAX_ROWS: i64 = 50_000;

const READ_POOL_SIZE = 4;

conn: ?zqlite.Conn = null,
read_pool: [READ_POOL_SIZE]?zqlite.Conn = .{null} ** READ_POOL_SIZE,
read_in_use: [READ_POOL_SIZE]bool = .{false} ** READ_POOL_SIZE,
pool_mutex: Io.Mutex = Io.Mutex.init,
pool_cond: Io.Condition = Io.Condition.init,
mutex: Io.Mutex = Io.Mutex.init, // guards write conn
allocator: Allocator,
io: Io,
max_rows: i64 = DEFAULT_MAX_ROWS,

pub fn init(allocator: Allocator, io: Io) OverlayDb {
    return .{ .allocator = allocator, .io = io };
}

/// Everything the overlay stores for one document — the same surface
/// searchLocal projects from the snapshot, so an overlay hit can fill a
/// result row (and hydrate semantic results) without touching the snapshot.
pub const DocRow = struct {
    uri: []const u8,
    did: []const u8,
    rkey: []const u8,
    title: []const u8,
    content: []const u8,
    created_at: []const u8,
    publication_uri: []const u8,
    publication_name: []const u8 = "",
    platform: []const u8,
    path: []const u8,
    base_path: []const u8,
    has_publication: []const u8, // "0"/"1", mirrors DocWriteParams.has_pub
    cover_image: []const u8,
    is_bridgyfed: []const u8, // "0"/"1"
    tags: []const []const u8 = &.{},
};

pub fn open(self: *OverlayDb) !void {
    const path_env = if (std.c.getenv("OVERLAY_DB_PATH")) |p| std.mem.span(p) else "/data/overlay.db";
    try self.openAt(path_env);
}

pub fn openAt(self: *OverlayDb, path_env: []const u8) !void {
    var path_buf: [256]u8 = undefined;
    if (path_env.len >= path_buf.len) return error.PathTooLong;
    @memcpy(path_buf[0..path_env.len], path_env);
    path_buf[path_env.len] = 0;
    const path: [*:0]const u8 = path_buf[0..path_env.len :0];

    self.conn = try zqlite.open(path, zqlite.OpenFlags.Create | zqlite.OpenFlags.ReadWrite);
    _ = self.conn.?.exec("PRAGMA journal_mode=WAL", .{}) catch {};
    _ = self.conn.?.exec("PRAGMA busy_timeout=5000", .{}) catch {};

    // corruption handling is blunt on purpose: the overlay is a cache, so a
    // failed quick_check means delete and start empty (the snapshot backstops)
    if (!self.quickCheckOk()) {
        logfire.warn("overlay: quick_check failed — recreating empty overlay", .{});
        if (self.conn) |c| c.close();
        self.conn = null;
        deleteDbFiles(path_env);
        self.conn = try zqlite.open(path, zqlite.OpenFlags.Create | zqlite.OpenFlags.ReadWrite);
        _ = self.conn.?.exec("PRAGMA journal_mode=WAL", .{}) catch {};
        _ = self.conn.?.exec("PRAGMA busy_timeout=5000", .{}) catch {};
    }

    try self.createSchema();

    for (&self.read_pool) |*slot| {
        const rc = try zqlite.open(path, zqlite.OpenFlags.ReadOnly);
        _ = rc.exec("PRAGMA busy_timeout=1000", .{}) catch {};
        slot.* = rc;
    }

    if (std.c.getenv("MAX_OVERLAY_ROWS")) |v| {
        self.max_rows = std.fmt.parseInt(i64, std.mem.span(v), 10) catch DEFAULT_MAX_ROWS;
    }
}

fn quickCheckOk(self: *OverlayDb) bool {
    const c = self.conn orelse return false;
    const row = c.row("PRAGMA quick_check", .{}) catch return false;
    if (row) |r| {
        defer r.deinit();
        return std.mem.eql(u8, r.text(0), "ok");
    }
    return false;
}

fn deleteDbFiles(path: []const u8) void {
    var buf: [280]u8 = undefined;
    inline for (.{ "", "-wal", "-shm" }) |sfx| {
        const z = std.fmt.bufPrintZ(&buf, "{s}{s}", .{ path, sfx }) catch return;
        _ = std.c.unlink(z.ptr);
    }
}

fn createSchema(self: *OverlayDb) !void {
    const c = self.conn orelse return error.NotOpen;
    try c.exec(
        \\CREATE TABLE IF NOT EXISTS documents_overlay (
        \\  uri TEXT PRIMARY KEY,
        \\  did TEXT NOT NULL DEFAULT '',
        \\  rkey TEXT NOT NULL DEFAULT '',
        \\  title TEXT NOT NULL DEFAULT '',
        \\  content TEXT NOT NULL DEFAULT '',
        \\  created_at TEXT NOT NULL DEFAULT '',
        \\  publication_uri TEXT NOT NULL DEFAULT '',
        \\  publication_name TEXT NOT NULL DEFAULT '',
        \\  platform TEXT NOT NULL DEFAULT '',
        \\  path TEXT NOT NULL DEFAULT '',
        \\  base_path TEXT NOT NULL DEFAULT '',
        \\  has_publication INTEGER NOT NULL DEFAULT 0,
        \\  cover_image TEXT NOT NULL DEFAULT '',
        \\  is_bridgyfed INTEGER NOT NULL DEFAULT 0,
        \\  deleted INTEGER NOT NULL DEFAULT 0,
        \\  indexed_at TEXT NOT NULL
        \\)
    , .{});
    try c.exec("CREATE INDEX IF NOT EXISTS idx_overlay_indexed_at ON documents_overlay(indexed_at)", .{});
    // tokenizer MUST match the snapshot's documents_fts (LocalDb doctrine:
    // divergence serves wrong results with no error)
    try c.exec(
        \\CREATE VIRTUAL TABLE IF NOT EXISTS overlay_fts USING fts5(
        \\  uri UNINDEXED, title, content, tokenize='unicode61'
        \\)
    , .{});
    try c.exec("CREATE TABLE IF NOT EXISTS overlay_doc_tags (uri TEXT NOT NULL, tag TEXT NOT NULL, PRIMARY KEY (uri, tag))", .{});
    try c.exec("CREATE INDEX IF NOT EXISTS idx_overlay_tags_tag ON overlay_doc_tags(tag)", .{});
    try c.exec("CREATE TABLE IF NOT EXISTS overlay_meta (key TEXT PRIMARY KEY, value TEXT NOT NULL)", .{});
}

pub fn deinit(self: *OverlayDb) void {
    for (&self.read_pool) |*slot| {
        if (slot.*) |c| c.close();
        slot.* = null;
    }
    if (self.conn) |c| c.close();
    self.conn = null;
}

/// Upsert one document. Idempotent: replaying the same row leaves one
/// documents_overlay row, one overlay_fts row, and one tag set. indexed_at is
/// stamped here in the same format turso uses so compaction watermarks (which
/// come from turso MAX(indexed_at)) compare in one domain.
pub fn upsert(self: *OverlayDb, row: DocRow) !void {
    self.mutex.lockUncancelable(self.io);
    defer self.mutex.unlock(self.io);
    const c = self.conn orelse return error.NotOpen;

    try c.exec("BEGIN IMMEDIATE", .{});
    errdefer c.exec("ROLLBACK", .{}) catch {};

    try c.exec(
        \\INSERT OR REPLACE INTO documents_overlay
        \\  (uri, did, rkey, title, content, created_at, publication_uri, publication_name,
        \\   platform, path, base_path, has_publication, cover_image, is_bridgyfed, deleted, indexed_at)
        \\VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, strftime('%Y-%m-%dT%H:%M:%S', 'now'))
    , .{
        row.uri,             row.did,      row.rkey,      row.title,
        row.content,         row.created_at, row.publication_uri, row.publication_name,
        row.platform,        row.path,     row.base_path, row.has_publication,
        row.cover_image,     row.is_bridgyfed,
    });
    try c.exec("DELETE FROM overlay_fts WHERE uri = ?", .{row.uri});
    try c.exec("INSERT INTO overlay_fts (uri, title, content) VALUES (?, ?, ?)", .{ row.uri, row.title, row.content });
    try c.exec("DELETE FROM overlay_doc_tags WHERE uri = ?", .{row.uri});
    for (row.tags) |tag| {
        try c.exec("INSERT OR IGNORE INTO overlay_doc_tags (uri, tag) VALUES (?, ?)", .{ row.uri, tag });
    }
    try c.exec("COMMIT", .{});
}

/// Record a delete. The tombstone row suppresses any snapshot hit for this
/// uri at merge time until a snapshot built after the delete is adopted.
pub fn tombstone(self: *OverlayDb, uri: []const u8) !void {
    self.mutex.lockUncancelable(self.io);
    defer self.mutex.unlock(self.io);
    const c = self.conn orelse return error.NotOpen;

    try c.exec("BEGIN IMMEDIATE", .{});
    errdefer c.exec("ROLLBACK", .{}) catch {};
    try c.exec(
        \\INSERT INTO documents_overlay (uri, deleted, indexed_at)
        \\VALUES (?, 1, strftime('%Y-%m-%dT%H:%M:%S', 'now'))
        \\ON CONFLICT(uri) DO UPDATE SET
        \\  deleted = 1, title = '', content = '',
        \\  indexed_at = strftime('%Y-%m-%dT%H:%M:%S', 'now')
    , .{uri});
    try c.exec("DELETE FROM overlay_fts WHERE uri = ?", .{uri});
    try c.exec("DELETE FROM overlay_doc_tags WHERE uri = ?", .{uri});
    try c.exec("COMMIT", .{});
}

/// Prune everything the newly adopted snapshot already covers. Called by the
/// promote watcher AFTER a successful swap (adopt-then-compact: a crash
/// before this leaves shadowed-but-correct rows for the next adoption).
pub fn compact(self: *OverlayDb, watermark: []const u8) !void {
    if (watermark.len == 0) return; // no watermark, nothing provable to prune
    self.mutex.lockUncancelable(self.io);
    defer self.mutex.unlock(self.io);
    const c = self.conn orelse return error.NotOpen;

    try c.exec("BEGIN IMMEDIATE", .{});
    errdefer c.exec("ROLLBACK", .{}) catch {};
    try c.exec("DELETE FROM overlay_fts WHERE uri IN (SELECT uri FROM documents_overlay WHERE indexed_at <= ?)", .{watermark});
    try c.exec("DELETE FROM overlay_doc_tags WHERE uri IN (SELECT uri FROM documents_overlay WHERE indexed_at <= ?)", .{watermark});
    try c.exec("DELETE FROM documents_overlay WHERE indexed_at <= ?", .{watermark});
    try c.exec("INSERT OR REPLACE INTO overlay_meta (key, value) VALUES ('compacted_watermark', ?)", .{watermark});
    try c.exec("COMMIT", .{});
}

/// Shed oldest rows beyond the cap (backfill-flood guard). Returns pruned count.
pub fn enforceCap(self: *OverlayDb) !i64 {
    self.mutex.lockUncancelable(self.io);
    defer self.mutex.unlock(self.io);
    const c = self.conn orelse return error.NotOpen;

    const over: i64 = blk: {
        const row = try c.row("SELECT MAX(0, COUNT(*) - ?) FROM documents_overlay", .{self.max_rows});
        if (row) |r| {
            defer r.deinit();
            break :blk r.int(0);
        }
        break :blk 0;
    };
    if (over == 0) return 0;

    try c.exec("BEGIN IMMEDIATE", .{});
    errdefer c.exec("ROLLBACK", .{}) catch {};
    try c.exec(
        \\CREATE TEMP TABLE IF NOT EXISTS _prune AS
        \\SELECT uri FROM documents_overlay ORDER BY indexed_at ASC LIMIT 0
    , .{});
    try c.exec("DELETE FROM _prune", .{});
    try c.exec("INSERT INTO _prune SELECT uri FROM documents_overlay ORDER BY indexed_at ASC LIMIT ?", .{over});
    try c.exec("DELETE FROM overlay_fts WHERE uri IN (SELECT uri FROM _prune)", .{});
    try c.exec("DELETE FROM overlay_doc_tags WHERE uri IN (SELECT uri FROM _prune)", .{});
    try c.exec("DELETE FROM documents_overlay WHERE uri IN (SELECT uri FROM _prune)", .{});
    try c.exec("DELETE FROM _prune", .{});
    try c.exec("COMMIT", .{});
    return over;
}

pub const Stats = struct {
    rows: i64 = 0,
    tombstones: i64 = 0,
    min_indexed_at: [32]u8 = .{0} ** 32,
    max_indexed_at: [32]u8 = .{0} ** 32,
    min_len: usize = 0,
    max_len: usize = 0,
    compacted_watermark: [32]u8 = .{0} ** 32,
    watermark_len: usize = 0,
};

pub fn stats(self: *OverlayDb) Stats {
    var out: Stats = .{};
    const idx = self.acquireRead();
    defer self.releaseRead(idx);
    const c = self.read_pool[idx].?;

    if (c.row("SELECT COUNT(*), SUM(deleted), COALESCE(MIN(indexed_at),''), COALESCE(MAX(indexed_at),'') FROM documents_overlay", .{}) catch null) |r| {
        defer r.deinit();
        out.rows = r.int(0);
        out.tombstones = r.int(1);
        const mn = r.text(2);
        const mx = r.text(3);
        out.min_len = @min(mn.len, out.min_indexed_at.len);
        @memcpy(out.min_indexed_at[0..out.min_len], mn[0..out.min_len]);
        out.max_len = @min(mx.len, out.max_indexed_at.len);
        @memcpy(out.max_indexed_at[0..out.max_len], mx[0..out.max_len]);
    }
    if (c.row("SELECT value FROM overlay_meta WHERE key = 'compacted_watermark'", .{}) catch null) |r| {
        defer r.deinit();
        const wm = r.text(0);
        out.watermark_len = @min(wm.len, out.compacted_watermark.len);
        @memcpy(out.compacted_watermark[0..out.watermark_len], wm[0..out.watermark_len]);
    }
    return out;
}

fn acquireRead(self: *OverlayDb) usize {
    self.pool_mutex.lockUncancelable(self.io);
    defer self.pool_mutex.unlock(self.io);
    while (true) {
        for (0..READ_POOL_SIZE) |i| {
            if (self.read_pool[i] != null and !self.read_in_use[i]) {
                self.read_in_use[i] = true;
                return i;
            }
        }
        self.pool_cond.waitUncancelable(self.io, &self.pool_mutex);
    }
}

fn releaseRead(self: *OverlayDb, idx: usize) void {
    self.pool_mutex.lockUncancelable(self.io);
    self.read_in_use[idx] = false;
    self.pool_mutex.unlock(self.io);
    self.pool_cond.signal(self.io);
}

/// Run a read-only query on a pool connection. Unlike LocalDb there is no
/// nested-checkout bookkeeping: overlay reads are single self-contained
/// statements, and callers must deinit the Rows before issuing another
/// overlay query on the same thread.
pub fn query(self: *OverlayDb, comptime sql: []const u8, args: anytype) !Rows {
    const idx = self.acquireRead();
    const c = self.read_pool[idx].?;
    const rows = c.rows(sql, args) catch |e| {
        self.releaseRead(idx);
        logfire.err("overlay.query failed: {s}", .{@errorName(e)});
        return e;
    };
    return .{ .inner = rows, .db = self, .pool_idx = idx };
}

pub const Rows = struct {
    inner: zqlite.Rows,
    db: ?*OverlayDb = null,
    pool_idx: usize = 0,

    pub fn next(self: *Rows) ?Row {
        if (self.inner.next()) |r| return .{ .stmt = r };
        return null;
    }
    pub fn deinit(self: *Rows) void {
        self.inner.deinit();
        if (self.db) |d| {
            d.releaseRead(self.pool_idx);
            self.db = null;
        }
    }
    pub fn err(self: *Rows) ?anyerror {
        return self.inner.err;
    }
};

pub const Row = struct {
    stmt: zqlite.Row,
    pub fn text(self: Row, index: usize) []const u8 {
        return self.stmt.text(index);
    }
    pub fn int(self: Row, index: usize) i64 {
        return self.stmt.int(index);
    }
    pub fn nullableText(self: Row, index: usize) ?[]const u8 {
        return self.stmt.nullableText(index);
    }
};

// ---------------- tests ----------------

const testing = std.testing;

fn testOverlay(io: Io, path: [:0]const u8) !OverlayDb {
    var o = OverlayDb.init(testing.allocator, io);
    try o.openAt(path);
    return o;
}

fn testDoc(uri: []const u8, title: []const u8, content: []const u8) DocRow {
    return .{
        .uri = uri,
        .did = "did:plc:test",
        .rkey = "rkey1",
        .title = title,
        .content = content,
        .created_at = "2026-08-12T00:00:00",
        .publication_uri = "",
        .platform = "leaflet",
        .path = "",
        .base_path = "test.leaflet.pub",
        .has_publication = "0",
        .cover_image = "",
        .is_bridgyfed = "0",
    };
}

fn countRows(o: *OverlayDb, comptime sql: []const u8) !i64 {
    var rows = try o.query(sql, .{});
    defer rows.deinit();
    if (rows.next()) |r| return r.int(0);
    return error.NoRow;
}

fn rmTestDb(path: [:0]const u8) void {
    deleteDbFiles(path);
}

test "overlay upsert is idempotent: replay leaves one row, one fts entry, one tag set" {
    var threaded = Io.Threaded.init(testing.allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();
    const path = "/tmp/overlay-test-idempotent.db";
    rmTestDb(path);
    defer rmTestDb(path);
    var o = try testOverlay(io, path);
    defer o.deinit();

    var doc = testDoc("at://did:plc:test/site.standard.document/1", "hello atproto", "body text");
    doc.tags = &.{ "zig", "search" };
    try o.upsert(doc);
    try o.upsert(doc);
    try o.upsert(doc);

    try testing.expectEqual(@as(i64, 1), try countRows(&o, "SELECT COUNT(*) FROM documents_overlay"));
    try testing.expectEqual(@as(i64, 1), try countRows(&o, "SELECT COUNT(*) FROM overlay_fts WHERE overlay_fts MATCH 'atproto'"));
    try testing.expectEqual(@as(i64, 2), try countRows(&o, "SELECT COUNT(*) FROM overlay_doc_tags"));
}

test "overlay tombstone suppresses fts and survives as a deleted row" {
    var threaded = Io.Threaded.init(testing.allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();
    const path = "/tmp/overlay-test-tombstone.db";
    rmTestDb(path);
    defer rmTestDb(path);
    var o = try testOverlay(io, path);
    defer o.deinit();

    const uri = "at://did:plc:test/site.standard.document/2";
    try o.upsert(testDoc(uri, "doomed doc", "content"));
    try o.tombstone(uri);

    try testing.expectEqual(@as(i64, 0), try countRows(&o, "SELECT COUNT(*) FROM overlay_fts WHERE overlay_fts MATCH 'doomed'"));
    try testing.expectEqual(@as(i64, 1), try countRows(&o, "SELECT COUNT(*) FROM documents_overlay WHERE deleted = 1"));
    // tombstone of a never-seen uri also works (delete arriving before create)
    try o.tombstone("at://did:plc:test/site.standard.document/never-seen");
    try testing.expectEqual(@as(i64, 2), try countRows(&o, "SELECT COUNT(*) FROM documents_overlay WHERE deleted = 1"));
}

test "overlay compact drops rows at-or-below the watermark, keeps newer" {
    var threaded = Io.Threaded.init(testing.allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();
    const path = "/tmp/overlay-test-compact.db";
    rmTestDb(path);
    defer rmTestDb(path);
    var o = try testOverlay(io, path);
    defer o.deinit();

    try o.upsert(testDoc("at://old", "old doc", "old"));
    try o.upsert(testDoc("at://new", "new doc", "new"));
    try o.tombstone("at://old-tombstone");
    // force distinct indexed_at values: pin "old" rows into the past
    {
        o.mutex.lockUncancelable(o.io);
        defer o.mutex.unlock(o.io);
        try o.conn.?.exec("UPDATE documents_overlay SET indexed_at = '2026-01-01T00:00:00' WHERE uri IN ('at://old','at://old-tombstone')", .{});
    }

    try o.compact("2026-06-01T00:00:00");

    try testing.expectEqual(@as(i64, 1), try countRows(&o, "SELECT COUNT(*) FROM documents_overlay"));
    try testing.expectEqual(@as(i64, 0), try countRows(&o, "SELECT COUNT(*) FROM documents_overlay WHERE deleted = 1"));
    try testing.expectEqual(@as(i64, 0), try countRows(&o, "SELECT COUNT(*) FROM overlay_fts WHERE overlay_fts MATCH 'old'"));
    try testing.expectEqual(@as(i64, 1), try countRows(&o, "SELECT COUNT(*) FROM overlay_fts WHERE overlay_fts MATCH 'new'"));

    var rows = try o.query("SELECT value FROM overlay_meta WHERE key = 'compacted_watermark'", .{});
    defer rows.deinit();
    const r = rows.next() orelse return error.NoRow;
    try testing.expectEqualStrings("2026-06-01T00:00:00", r.text(0));
}

test "overlay compact with empty watermark is a no-op" {
    var threaded = Io.Threaded.init(testing.allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();
    const path = "/tmp/overlay-test-compact-empty.db";
    rmTestDb(path);
    defer rmTestDb(path);
    var o = try testOverlay(io, path);
    defer o.deinit();

    try o.upsert(testDoc("at://kept", "kept doc", "kept"));
    try o.compact("");
    try testing.expectEqual(@as(i64, 1), try countRows(&o, "SELECT COUNT(*) FROM documents_overlay"));
}

test "overlay enforceCap sheds oldest rows first" {
    var threaded = Io.Threaded.init(testing.allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();
    const path = "/tmp/overlay-test-cap.db";
    rmTestDb(path);
    defer rmTestDb(path);
    var o = try testOverlay(io, path);
    defer o.deinit();
    o.max_rows = 2;

    try o.upsert(testDoc("at://a", "doc a", "a"));
    try o.upsert(testDoc("at://b", "doc b", "b"));
    try o.upsert(testDoc("at://c", "doc c", "c"));
    {
        o.mutex.lockUncancelable(o.io);
        defer o.mutex.unlock(o.io);
        try o.conn.?.exec("UPDATE documents_overlay SET indexed_at = '2026-01-01T00:00:00' WHERE uri = 'at://a'", .{});
        try o.conn.?.exec("UPDATE documents_overlay SET indexed_at = '2026-02-01T00:00:00' WHERE uri = 'at://b'", .{});
    }

    const pruned = try o.enforceCap();
    try testing.expectEqual(@as(i64, 1), pruned);
    try testing.expectEqual(@as(i64, 2), try countRows(&o, "SELECT COUNT(*) FROM documents_overlay"));
    try testing.expectEqual(@as(i64, 0), try countRows(&o, "SELECT COUNT(*) FROM documents_overlay WHERE uri = 'at://a'"));
}

test "overlay stats reports rows, tombstones, and watermark" {
    var threaded = Io.Threaded.init(testing.allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();
    const path = "/tmp/overlay-test-stats.db";
    rmTestDb(path);
    defer rmTestDb(path);
    var o = try testOverlay(io, path);
    defer o.deinit();

    try o.upsert(testDoc("at://x", "x", "x"));
    try o.tombstone("at://y");
    try o.compact("2020-01-01T00:00:00");

    const s = o.stats();
    try testing.expectEqual(@as(i64, 2), s.rows);
    try testing.expectEqual(@as(i64, 1), s.tombstones);
    try testing.expectEqualStrings("2020-01-01T00:00:00", s.compacted_watermark[0..s.watermark_len]);
}
