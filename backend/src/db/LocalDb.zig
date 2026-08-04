//! Local SQLite read replica using zqlite
//! Provides fast FTS5 queries while Turso remains source of truth

const std = @import("std");
const Io = std.Io;
const zqlite = @import("zqlite");
const Allocator = std.mem.Allocator;
const logfire = @import("logfire");
const pubkey = @import("../server/pubkey.zig");

const LocalDb = @This();

/// Local replica schema generation. Bump when createSchema changes shape
/// (new column/table/index the serving code depends on). The builder stamps
/// it into the manifest; the promote watcher rejects a mismatch — a snapshot
/// built by an out-of-date builder image must stall freshness, not break
/// serving. (Scheduled builder machines pin their creation image: recreate
/// them after bumping this.)
pub const SCHEMA_VERSION: u32 = 4; // v4: documents.source_cid for source-attested reconciliation

const READ_POOL_SIZE = 12;

// Per-thread checkout state. A thread holds at most one pool connection; nested
// and sibling queries on the same thread share it, counted by `tl_depth`. The
// connection is acquired when depth goes 0→1 and returned when it falls 1→0.
threadlocal var tl_conn_idx: ?usize = null;
threadlocal var tl_depth: usize = 0;

conn: ?zqlite.Conn = null,
read_conn: ?zqlite.Conn = null, // legacy single read conn (kept as pool[0] alias)
// READ POOL — one connection per thread, reference-counted.
//
// A single SQLite connection runs one *thread's* statements; a query() holds
// its connection open for the whole row iteration. The old round-robin scheme
// let two threads share a connection and serialize (a PK lookup measured at 10s
// behind a content scan, 2026-06-25). A naive per-query checkout fixed that but
// DEADLOCKED: search holds its result rows open while running per-result
// is_bridgyfed / content queries, so one request needs two connections at once;
// 12 such requests each grabbed one and waited forever for a second.
//
// The fix removes hold-and-wait entirely: a thread checks out ONE connection on
// its outermost query() and every nested or sibling query on that thread reuses
// it (SQLite is happy to run several statements on one connection from one
// thread). The connection returns to the pool when the thread's last Rows
// closes. A thread therefore blocks (in acquireRead) only while holding zero
// connections, so a deadlock cycle cannot form. Distinct threads get distinct
// connections → true WAL-parallel reads.
read_pool: [READ_POOL_SIZE]?zqlite.Conn = .{null} ** READ_POOL_SIZE,
read_in_use: [READ_POOL_SIZE]bool = .{false} ** READ_POOL_SIZE,
pool_mutex: Io.Mutex = Io.Mutex.init, // guards read_in_use
pool_cond: Io.Condition = Io.Condition.init, // signaled when a connection is returned
allocator: Allocator,
io: Io,
is_ready: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),
needs_resync: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),
mutex: Io.Mutex = Io.Mutex.init, // protects write conn only
path: []const u8 = "",
consecutive_errors: std.atomic.Value(u32) = std.atomic.Value(u32).init(0),

pub fn init(allocator: Allocator, io: Io) LocalDb {
    return .{ .allocator = allocator, .io = io };
}

/// Check database integrity and return false if corrupt. quick_check, not
/// integrity_check: the full check scans every index b-tree, which on a
/// 365MB+ file on a fly volume is minutes of is_ready=false at every boot
/// (keyword falls back to slow turso the whole time). Adopted snapshots are
/// already sha256-gated before they become local.db, so the full scan is
/// redundant; quick_check still catches page-level corruption in seconds.
fn checkIntegrity(self: *LocalDb) bool {
    const c = self.conn orelse return false;
    const row = c.row("PRAGMA quick_check", .{}) catch return false;
    if (row) |r| {
        defer r.deinit();
        const result = r.text(0);
        if (std.mem.eql(u8, result, "ok")) {
            return true;
        }
        std.debug.print("local db: integrity check failed: {s}\n", .{result});
        return false;
    }
    return false;
}

/// Delete the database file and WAL/SHM files
fn deleteDbFiles(path: []const u8) void {
    unlinkPath(path);
    // also delete WAL and SHM files
    var wal_buf: [260]u8 = undefined;
    var shm_buf: [260]u8 = undefined;
    if (path.len < 252) {
        const wal_path = std.fmt.bufPrint(&wal_buf, "{s}-wal", .{path}) catch return;
        const shm_path = std.fmt.bufPrint(&shm_buf, "{s}-shm", .{path}) catch return;
        unlinkPath(wal_path);
        unlinkPath(shm_path);
    }
}

fn hasManifestSidecar(path_env: []const u8) bool {
    var buf: [280]u8 = undefined;
    const sidecar = std.fmt.bufPrintZ(&buf, "{s}.manifest.json", .{path_env}) catch return false;
    const fd = std.c.open(sidecar.ptr, .{ .ACCMODE = .RDONLY }, @as(std.c.mode_t, 0));
    if (fd < 0) return false;
    _ = std.c.close(fd);
    return true;
}

/// Delete a file by path using C unlink (std.fs.cwd removed in 0.16)
fn unlinkPath(path: []const u8) void {
    var buf: [260]u8 = undefined;
    if (path.len >= buf.len) return;
    @memcpy(buf[0..path.len], path);
    buf[path.len] = 0;
    _ = std.c.unlink(@ptrCast(&buf));
}

pub fn open(self: *LocalDb) !void {
    const path_env = if (std.c.getenv("LOCAL_DB_PATH")) |p| std.mem.span(p) else "/data/local.db";
    try self.openAt(path_env);
}

/// Open at an explicit path — the snapshot builder targets a scratch file,
/// never the serving replica.
pub fn openAt(self: *LocalDb, path: []const u8) !void {
    self.path = path;
    adoptPending(path);
    try self.openDb(path, false);
}

/// Snapshot adoption: if `<path>.new` exists at boot, rename it over the live
/// file (plus clear stale WAL/SHM) before opening. This is how offline-built
/// replicas ship: stage as .new while the old process serves, restart, and
/// the swap is atomic — the serving path never coexists with a bulk writer.
/// The displaced live file is kept as `<path>.prev` (with its manifest
/// sidecar) so rollback is `mv .prev .new` + restart, no re-download.
fn adoptPending(path_env: []const u8) void {
    var new_buf: [256]u8 = undefined;
    const new_path = std.fmt.bufPrintZ(&new_buf, "{s}.new", .{path_env}) catch return;
    var cur_buf: [256]u8 = undefined;
    const cur_path = std.fmt.bufPrintZ(&cur_buf, "{s}", .{path_env}) catch return;

    const fd = std.c.open(new_path.ptr, .{ .ACCMODE = .RDONLY }, @as(std.c.mode_t, 0));
    if (fd < 0) return; // nothing pending
    _ = std.c.close(fd);

    var aux_buf: [264]u8 = undefined;
    inline for (.{ "-wal", "-shm" }) |suffix| {
        const aux = std.fmt.bufPrintZ(&aux_buf, "{s}{s}", .{ path_env, suffix }) catch return;
        _ = std.c.unlink(aux.ptr);
    }

    // keep the displaced snapshot for rollback: live -> .prev (best-effort)
    var prev_buf: [256]u8 = undefined;
    if (std.fmt.bufPrintZ(&prev_buf, "{s}.prev", .{path_env})) |prev_path| {
        _ = std.c.rename(cur_path.ptr, prev_path.ptr);
    } else |_| {}

    // manifest sidecars follow their snapshots (promote watcher writes them;
    // manual swaps without sidecars are fine — these renames just fail)
    var sc_a: [280]u8 = undefined;
    var sc_b: [280]u8 = undefined;
    if (std.fmt.bufPrintZ(&sc_a, "{s}.manifest.json", .{path_env})) |live_sc| {
        if (std.fmt.bufPrintZ(&sc_b, "{s}.prev.manifest.json", .{path_env})) |prev_sc| {
            _ = std.c.rename(live_sc.ptr, prev_sc.ptr);
        } else |_| {}
    } else |_| {}
    if (std.fmt.bufPrintZ(&sc_a, "{s}.new.manifest.json", .{path_env})) |new_sc| {
        if (std.fmt.bufPrintZ(&sc_b, "{s}.manifest.json", .{path_env})) |live_sc| {
            _ = std.c.rename(new_sc.ptr, live_sc.ptr);
        } else |_| {}
    } else |_| {}

    if (std.c.rename(new_path.ptr, cur_path.ptr) == 0) {
        std.debug.print("local db: adopted pending snapshot {s}\n", .{new_path});
    } else {
        std.debug.print("local db: snapshot adopt FAILED, serving existing file\n", .{});
    }
}

fn openDb(self: *LocalDb, path_env: []const u8, is_retry: bool) !void {
    // convert to null-terminated for zqlite
    var path_buf: [256]u8 = undefined;
    if (path_env.len >= path_buf.len) return error.PathTooLong;
    @memcpy(path_buf[0..path_env.len], path_env);
    path_buf[path_env.len] = 0;
    const path: [*:0]const u8 = path_buf[0..path_env.len :0];

    std.debug.print("local db: opening {s}\n", .{path_env});

    const flags = zqlite.OpenFlags.Create | zqlite.OpenFlags.ReadWrite;
    self.conn = zqlite.open(path, flags) catch |err| {
        std.debug.print("local db: failed to open write conn: {}\n", .{err});
        return err;
    };

    // enable WAL for better concurrency
    _ = self.conn.?.exec("PRAGMA journal_mode=WAL", .{}) catch {};
    _ = self.conn.?.exec("PRAGMA busy_timeout=5000", .{}) catch {};

    // open the read POOL — WAL mode allows concurrent reads + writes
    for (&self.read_pool) |*slot| {
        const rc = zqlite.open(path, zqlite.OpenFlags.ReadOnly) catch |err| {
            std.debug.print("local db: failed to open read conn: {}\n", .{err});
            return err;
        };
        _ = rc.exec("PRAGMA busy_timeout=1000", .{}) catch {};
        // mmap for fast reads — avoids pread() syscalls, uses OS page cache directly
        _ = rc.exec("PRAGMA mmap_size=268435456", .{}) catch {};
        // 8MB page cache per conn. Smaller than before because the pool is now
        // 12 connections (12×8=96MB heap); mmap already serves hot pages from
        // the shared OS page cache, so the per-conn cache matters less.
        _ = rc.exec("PRAGMA cache_size=-8000", .{}) catch {};
        slot.* = rc;
    }
    self.read_conn = self.read_pool[0];

    // integrity: if the manifest sidecar exists, this exact file was
    // sha256-verified + sentinel-checked at stage time and is never written
    // between adoptions (sync disabled; adoption is an atomic rename), so a
    // boot-time scan re-reads 350MB of cold volume for nothing — measured
    // ~3.5-4 min of is_ready=false per adoption (2026-06-11 first prod
    // adopt), during which keyword falls back to turso. Attested files skip
    // it; unattested files (manual swaps, fresh creates) keep the check.
    if (hasManifestSidecar(path_env)) {
        std.debug.print("local db: integrity attested by manifest sidecar; skipping boot quick_check\n", .{});
    } else if (!self.checkIntegrity()) {
        if (is_retry) {
            std.debug.print("local db: still corrupt after recreation, giving up\n", .{});
            return error.DatabaseCorrupt;
        }
        std.debug.print("local db: corrupt, deleting and recreating\n", .{});
        if (self.conn) |c| c.close();
        self.conn = null;
        deleteDbFiles(path_env);
        return self.openDb(path_env, true);
    }

    try self.createSchema();
    std.debug.print("local db: initialized\n", .{});
}

pub fn deinit(self: *LocalDb) void {
    for (&self.read_pool) |*slot| {
        if (slot.*) |c| c.close();
        slot.* = null;
    }
    self.read_conn = null;
    if (self.conn) |c| c.close();
    self.conn = null;
}

pub fn isReady(self: *LocalDb) bool {
    return self.is_ready.load(.acquire);
}

pub fn setReady(self: *LocalDb, ready: bool) void {
    self.is_ready.store(ready, .release);
}

fn createSchema(self: *LocalDb) !void {
    const c = self.conn orelse return error.NotOpen;

    // documents table (no embedding/embedded_at — vectors are in turbopuffer)
    c.exec(
        \\CREATE TABLE IF NOT EXISTS documents (
        \\  uri TEXT PRIMARY KEY,
        \\  did TEXT NOT NULL,
        \\  rkey TEXT NOT NULL,
        \\  title TEXT NOT NULL,
        \\  content TEXT NOT NULL,
        \\  created_at TEXT,
        \\  publication_uri TEXT,
        \\  platform TEXT DEFAULT 'leaflet',
        \\  source_collection TEXT,
        \\  path TEXT,
        \\  base_path TEXT DEFAULT '',
        \\  has_publication INTEGER DEFAULT 0,
        \\  indexed_at TEXT,
        \\  cover_image TEXT DEFAULT '',
        \\  source_cid TEXT DEFAULT ''
        \\)
    , .{}) catch |err| {
        std.debug.print("local db: failed to create documents table: {}\n", .{err});
        return err;
    };

    // FTS5 index (unicode61 tokenizer to match Turso)
    c.exec(
        \\CREATE VIRTUAL TABLE IF NOT EXISTS documents_fts USING fts5(
        \\  uri UNINDEXED,
        \\  title,
        \\  content,
        \\  tokenize='unicode61'
        \\)
    , .{}) catch |err| {
        std.debug.print("local db: failed to create documents_fts: {}\n", .{err});
        return err;
    };

    // publications table (no created_at - matches Turso schema)
    c.exec(
        \\CREATE TABLE IF NOT EXISTS publications (
        \\  uri TEXT PRIMARY KEY,
        \\  did TEXT NOT NULL,
        \\  rkey TEXT NOT NULL,
        \\  name TEXT NOT NULL,
        \\  description TEXT,
        \\  base_path TEXT,
        \\  platform TEXT DEFAULT 'leaflet',
        \\  source_collection TEXT,
        \\  indexed_at TEXT,
        \\  show_in_discover INTEGER NOT NULL DEFAULT 1
        \\)
    , .{}) catch |err| {
        std.debug.print("local db: failed to create publications table: {}\n", .{err});
        return err;
    };

    // publications FTS
    c.exec(
        \\CREATE VIRTUAL TABLE IF NOT EXISTS publications_fts USING fts5(
        \\  uri UNINDEXED,
        \\  name,
        \\  description,
        \\  base_path,
        \\  tokenize='unicode61'
        \\)
    , .{}) catch |err| {
        std.debug.print("local db: failed to create publications_fts: {}\n", .{err});
        return err;
    };

    // document_tags table
    c.exec(
        \\CREATE TABLE IF NOT EXISTS document_tags (
        \\  document_uri TEXT NOT NULL,
        \\  tag TEXT NOT NULL,
        \\  PRIMARY KEY (document_uri, tag)
        \\)
    , .{}) catch |err| {
        std.debug.print("local db: failed to create document_tags table: {}\n", .{err});
        return err;
    };

    // index for tag queries
    c.exec("CREATE INDEX IF NOT EXISTS idx_document_tags_tag ON document_tags(tag)", .{}) catch {};

    // index for base_path join (documents → publications via publication_uri)
    c.exec("CREATE INDEX IF NOT EXISTS idx_documents_publication_uri ON documents(publication_uri)", .{}) catch {};

    // index for per-author lookups (/wrapped author lens). without this every
    // author query full-scans the whole documents table — 13s on /wrapped.
    c.exec("CREATE INDEX IF NOT EXISTS idx_documents_did ON documents(did)", .{}) catch {};

    // index for the (did, rkey) publication join used by /wrapped, /subscribed,
    // /subscribers. The join matches a publication to an at-uri by did+rkey
    // PARSED out of the uri (collection-agnostic dual-write), so it can't use
    // the `uri` PRIMARY KEY. Without this index every such join FULL-SCANS
    // publications: one query is sub-ms cached, but 16 concurrent full-scans
    // saturate the single vCPU → 45s (measured). With the index: 0.39s.
    c.exec("CREATE INDEX IF NOT EXISTS idx_publications_did_rkey ON publications(did, rkey)", .{}) catch {};

    // sync metadata table
    c.exec(
        \\CREATE TABLE IF NOT EXISTS sync_meta (
        \\  key TEXT PRIMARY KEY,
        \\  value TEXT
        \\)
    , .{}) catch |err| {
        std.debug.print("local db: failed to create sync_meta table: {}\n", .{err});
        return err;
    };

    // stats table for local counters
    c.exec(
        \\CREATE TABLE IF NOT EXISTS stats (
        \\  id INTEGER PRIMARY KEY CHECK (id = 1),
        \\  total_searches INTEGER DEFAULT 0,
        \\  total_errors INTEGER DEFAULT 0,
        \\  service_started_at INTEGER
        \\)
    , .{}) catch {};
    c.exec("INSERT OR IGNORE INTO stats (id) VALUES (1)", .{}) catch {};

    // popular searches
    c.exec(
        \\CREATE TABLE IF NOT EXISTS popular_searches (
        \\  query TEXT PRIMARY KEY,
        \\  count INTEGER DEFAULT 1
        \\)
    , .{}) catch {};

    // recommends (schema v2): lets the top-author cascade and (eventually)
    // the recommended/curators cache refreshes run against the replica
    // instead of walking ~30k turso rows per request
    c.exec(
        \\CREATE TABLE IF NOT EXISTS recommends (
        \\  uri TEXT PRIMARY KEY,
        \\  did TEXT NOT NULL,
        \\  rkey TEXT NOT NULL,
        \\  document_uri TEXT NOT NULL,
        \\  created_at TEXT,
        \\  indexed_at TEXT
        \\)
    , .{}) catch |err| {
        std.debug.print("local db: failed to create recommends table: {}\n", .{err});
    };
    c.exec("CREATE INDEX IF NOT EXISTS idx_recommends_document_uri ON recommends(document_uri)", .{}) catch {};
    c.exec("CREATE INDEX IF NOT EXISTS idx_recommends_did ON recommends(did)", .{}) catch {};

    // subscriptions (schema v3): the /subscribed leaderboard + /subscribers
    // drill-down serve from the replica, same as recommends. Empty on pre-v3
    // snapshots — endpoints fall back to turso until the first v3 build.
    c.exec(
        \\CREATE TABLE IF NOT EXISTS subscriptions (
        \\  uri TEXT PRIMARY KEY,
        \\  did TEXT NOT NULL,
        \\  rkey TEXT NOT NULL,
        \\  publication_uri TEXT NOT NULL,
        \\  created_at TEXT,
        \\  indexed_at TEXT
        \\)
    , .{}) catch |err| {
        std.debug.print("local db: failed to create subscriptions table: {}\n", .{err});
    };
    c.exec("CREATE INDEX IF NOT EXISTS idx_subscriptions_publication_uri ON subscriptions(publication_uri)", .{}) catch {};
    c.exec("CREATE INDEX IF NOT EXISTS idx_subscriptions_did ON subscriptions(did)", .{}) catch {};

    // Idempotent column-add for existing local DBs. SQLite returns an error
    // when the column already exists ("duplicate column name"), which is
    // the common case — we'd flood logs if every boot warned on that. So
    // detect the column first via PRAGMA table_info and skip if present,
    // and on the rare ACTUAL failure log loudly.
    addColumnIfMissing(c, "documents", "indexed_at", "TEXT");
    addColumnIfMissing(c, "documents", "embedded_at", "TEXT");
    addColumnIfMissing(c, "documents", "cover_image", "TEXT DEFAULT ''");
    addColumnIfMissing(c, "documents", "is_bridgyfed", "INTEGER DEFAULT 0");
    addColumnIfMissing(c, "documents", "url_dead", "INTEGER DEFAULT 0");
    addColumnIfMissing(c, "publications", "indexed_at", "TEXT");
    addColumnIfMissing(c, "publications", "show_in_discover", "INTEGER NOT NULL DEFAULT 1");

    // Materialized (did, rkey) for subscriptions so the publication join is a
    // sargable indexed equijoin instead of parsing publication_uri per row (see
    // pubkey.joinOnStored). createSchema runs at every boot before serving, so
    // backfilling here makes the columns correct on the currently-adopted
    // snapshot immediately — no multi-step rollout. New rows are populated at
    // build time (sync.zig); this UPDATE only touches rows still NULL.
    addColumnIfMissing(c, "subscriptions", "publication_did", "TEXT");
    addColumnIfMissing(c, "subscriptions", "publication_rkey", "TEXT");
    const sub_backfill_sql = comptime "UPDATE subscriptions SET publication_did = " ++ pubkey.didExpr("publication_uri") ++
        ", publication_rkey = " ++ pubkey.rkeyExpr("publication_uri") ++
        " WHERE publication_did IS NULL AND publication_uri LIKE 'at://%/%/%'";
    c.exec(sub_backfill_sql, .{}) catch |err| std.debug.print("local db: subscriptions pubkey backfill failed: {}\n", .{err});
    c.exec("CREATE INDEX IF NOT EXISTS idx_subscriptions_pub_did_rkey ON subscriptions(publication_did, publication_rkey)", .{}) catch {};

    // Materialized per-document recommender counts. Tag browse ranks by
    // ln(1+recommenders); computing that with a GROUP BY over ALL of
    // recommends on every request was the last O(corpus) request path
    // (docs/scale-300k-plan.md §3a — typeahead's rule: ranking work belongs
    // at build time). The replica is frozen between adoptions and adoption
    // always restarts through here, so one rebuild per boot keeps the table
    // exactly consistent with the snapshot's recommends at request-time cost
    // zero. No schema-version bump: the table is server-generated, never
    // shipped in snapshots.
    c.exec("CREATE TABLE IF NOT EXISTS recommend_counts (document_uri TEXT PRIMARY KEY, rc INTEGER NOT NULL)", .{}) catch |err| {
        std.debug.print("local db: failed to create recommend_counts table: {}\n", .{err});
    };
    c.exec("DELETE FROM recommend_counts", .{}) catch {};
    c.exec("INSERT INTO recommend_counts SELECT document_uri, COUNT(DISTINCT did) FROM recommends GROUP BY document_uri", .{}) catch |err| {
        std.debug.print("local db: recommend_counts rebuild failed: {}\n", .{err});
    };
}

fn hasColumn(c: zqlite.Conn, table: []const u8, column: []const u8) bool {
    var buf: [128]u8 = undefined;
    const sql = std.fmt.bufPrint(&buf, "PRAGMA table_info({s})", .{table}) catch return false;
    var rows = c.rows(sql, .{}) catch return false;
    defer rows.deinit();
    while (rows.next()) |row| {
        if (std.mem.eql(u8, row.text(1), column)) return true;
    }
    return false;
}

fn addColumnIfMissing(c: zqlite.Conn, table: []const u8, column: []const u8, decl: []const u8) void {
    if (hasColumn(c, table, column)) return;
    var buf: [256]u8 = undefined;
    const sql = std.fmt.bufPrint(&buf, "ALTER TABLE {s} ADD COLUMN {s} {s}", .{ table, column, decl }) catch {
        std.debug.print("local db: ALTER format failed for {s}.{s}\n", .{ table, column });
        return;
    };
    c.exec(sql, .{}) catch |err| {
        // Hit only on actual failures (lock contention, disk full) since
        // we pre-checked for duplicate-column. Worth knowing about.
        std.debug.print("local db: ALTER TABLE {s} ADD COLUMN {s} failed: {}\n", .{ table, column, err });
    };
}

/// Row adapter matching result.Row interface (column-indexed access)
pub const Row = struct {
    stmt: zqlite.Row,

    pub fn text(self: Row, index: usize) []const u8 {
        return self.stmt.text(index);
    }

    pub fn int(self: Row, index: usize) i64 {
        return self.stmt.int(index);
    }
};

/// Iterator for query results
pub const Rows = struct {
    inner: zqlite.Rows,
    db: ?*LocalDb = null, // non-null => deinit must call endQuery() to balance beginQuery()

    pub fn next(self: *Rows) ?Row {
        if (self.inner.next()) |r| {
            return .{ .stmt = r };
        }
        return null;
    }

    pub fn deinit(self: *Rows) void {
        self.inner.deinit();
        // balance the beginQuery() that produced this Rows, exactly once
        if (self.db) |d| {
            d.endQuery();
            self.db = null;
        }
    }

    pub fn err(self: *Rows) ?anyerror {
        return self.inner.err;
    }
};

/// Check out an idle read connection, blocking until one is free. Only ever
/// called while this thread holds NO connection (tl_depth 0→1), so a blocked
/// thread holds nothing and no deadlock cycle can form.
fn acquireRead(self: *LocalDb) usize {
    self.pool_mutex.lockUncancelable(self.io);
    defer self.pool_mutex.unlock(self.io);
    while (true) {
        for (0..READ_POOL_SIZE) |i| {
            if (self.read_pool[i] != null and !self.read_in_use[i]) {
                self.read_in_use[i] = true;
                return i;
            }
        }
        self.pool_cond.waitUncancelable(self.io, &self.pool_mutex); // releases mutex while parked
    }
}

fn releaseRead(self: *LocalDb, idx: usize) void {
    self.pool_mutex.lockUncancelable(self.io);
    self.read_in_use[idx] = false;
    self.pool_mutex.unlock(self.io);
    self.pool_cond.signal(self.io);
}

/// Begin a query on this thread's connection, acquiring one from the pool if
/// this is the outermost query. Returns the connection to run on.
fn beginQuery(self: *LocalDb) zqlite.Conn {
    if (tl_depth == 0) tl_conn_idx = self.acquireRead();
    tl_depth += 1;
    return self.read_pool[tl_conn_idx.?].?;
}

/// End a query; return this thread's connection to the pool when its last
/// open Rows closes (depth 1→0).
fn endQuery(self: *LocalDb) void {
    tl_depth -= 1;
    if (tl_depth == 0) {
        if (tl_conn_idx) |idx| {
            self.releaseRead(idx);
            tl_conn_idx = null;
        }
    }
}

/// Execute a SELECT on this thread's pool connection (shared across the thread's
/// nested/sibling queries, never across threads). The returned Rows holds the
/// connection until deinit.
pub fn query(self: *LocalDb, comptime sql: []const u8, args: anytype) !Rows {
    const span = logfire.span("db.local.query", .{
        .sql = truncateSql(sql),
    });
    defer span.end();

    const c = self.beginQuery();
    const rows = c.rows(sql, args) catch |e| {
        self.endQuery(); // unwind the depth bump; release if it was the outermost
        logfire.err("db.local.query failed: {s} | sql: {s}", .{ @errorName(e), truncateSql(sql) });
        return e;
    };
    return .{ .inner = rows, .db = self };
}

/// Execute a statement (INSERT, UPDATE, DELETE)
pub fn exec(self: *LocalDb, comptime sql: []const u8, args: anytype) !void {
    self.mutex.lockUncancelable(self.io);
    defer self.mutex.unlock(self.io);

    const c = self.conn orelse return error.NotOpen;
    c.exec(sql, args) catch |e| {
        logfire.err("db.local.exec failed: {s} | sql: {s}", .{ @errorName(e), truncateSql(sql) });
        return e;
    };
}

/// Get raw connection for batch operations (caller must handle locking)
pub fn getConn(self: *LocalDb) ?zqlite.Conn {
    return self.conn;
}

/// Lock for batch operations
pub fn lock(self: *LocalDb) void {
    self.mutex.lockUncancelable(self.io);
}

/// Unlock after batch operations
pub fn unlock(self: *LocalDb) void {
    self.mutex.unlock(self.io);
}

fn truncateSql(sql: []const u8) []const u8 {
    const max_len = 100;
    return if (sql.len > max_len) sql[0..max_len] else sql;
}


// remove a sqlite db file and its WAL/SHM sidecars (test helper)
fn unlinkDbFiles(path: [:0]const u8) void {
    _ = std.c.unlink(path.ptr);
    var b: [80]u8 = undefined;
    inline for (.{ "-wal", "-shm" }) |sfx| {
        const z = std.fmt.bufPrintZ(&b, "{s}{s}", .{ path, sfx }) catch return;
        _ = std.c.unlink(z.ptr);
    }
}

test "read pool: concurrent nested queries never deadlock and never leak" {
    // Real OS threads contending for the pool, each running the NESTED pattern
    // (outer rows held open while inner queries run) that deadlocked a per-query
    // checkout. With far more threads than connections, a hold-and-wait design
    // would wedge here forever; the thread-local reuse must let every thread
    // finish. (If the design regressed, this test HANGS — that is the signal.)
    var threaded = Io.Threaded.init(std.testing.allocator, .{});
    defer threaded.deinit();
    const tio = threaded.io();

    // Per-process path + defer cleanup: never shares state with a concurrent
    // run or a prior crashed run, and tidies up even if an assertion fails.
    var path_buf: [64]u8 = undefined;
    const zpath = std.fmt.bufPrintZ(&path_buf, "/tmp/leaflet-pool-{d}.db", .{std.c.getpid()}) catch unreachable;
    unlinkDbFiles(zpath);
    defer unlinkDbFiles(zpath);
    {
        const w = try zqlite.open(zpath.ptr, zqlite.OpenFlags.Create | zqlite.OpenFlags.ReadWrite);
        defer w.close();
        try w.exec("CREATE TABLE t(x INTEGER)", .{});
        try w.exec("INSERT INTO t(x) VALUES (1),(2),(3),(4),(5)", .{});
    }

    var ldb = LocalDb.init(std.testing.allocator, tio);
    for (&ldb.read_pool) |*slot| slot.* = try zqlite.open(zpath.ptr, zqlite.OpenFlags.ReadOnly);
    defer for (&ldb.read_pool) |*slot| {
        if (slot.*) |c| c.close();
        slot.* = null;
    };

    const Worker = struct {
        fn run(db_: *LocalDb, ok: *std.atomic.Value(u32)) void {
            var iter: usize = 0;
            while (iter < 25) : (iter += 1) {
                var outer = db_.query("SELECT x FROM t ORDER BY x", .{}) catch return;
                defer outer.deinit();
                var count: usize = 0;
                while (outer.next()) |row| {
                    const x = row.int(0);
                    // nested query while `outer` is still open
                    var inner = db_.query("SELECT x FROM t WHERE x = ?", .{x}) catch return;
                    defer inner.deinit();
                    if (inner.next()) |r2| {
                        if (r2.int(0) == x) count += 1;
                    }
                }
                if (count != 5) return; // wrong result → leave ok unbumped, test fails
            }
            _ = ok.fetchAdd(1, .monotonic);
        }
    };

    const N = 4 * READ_POOL_SIZE; // 4× more nesting threads than connections
    var ok = std.atomic.Value(u32).init(0);
    var threads: [N]std.Thread = undefined;
    for (&threads) |*t| t.* = try std.Thread.spawn(.{}, Worker.run, .{ &ldb, &ok });
    for (threads) |t| t.join();

    try std.testing.expectEqual(@as(u32, N), ok.load(.monotonic)); // all finished correctly
    for (ldb.read_in_use) |u| try std.testing.expect(!u); // every connection returned
}

test "adoptPending keeps previous snapshot and moves manifest sidecars" {
    const tio = std.Options.debug_io;

    // Per-process dir + defer cleanup: hermetic against concurrent runs, prior
    // crashed runs, and assertion failures partway through this test.
    var dir_buf: [64]u8 = undefined;
    const dir = std.fmt.bufPrintZ(&dir_buf, "/tmp/leaflet-adopt-{d}", .{std.c.getpid()}) catch unreachable;
    _ = std.c.mkdir(dir.ptr, 0o755);
    const jp = struct {
        fn f(buf: []u8, d: []const u8, suffix: []const u8) [:0]const u8 {
            return std.fmt.bufPrintZ(buf, "{s}/local.db{s}", .{ d, suffix }) catch unreachable;
        }
    }.f;
    defer {
        var b: [160]u8 = undefined;
        inline for (.{ "", ".new", ".prev", ".manifest.json", ".new.manifest.json", ".prev.manifest.json" }) |sfx| {
            _ = std.c.unlink(jp(&b, dir, sfx).ptr);
        }
        _ = std.c.rmdir(dir.ptr);
    }
    var pb: [160]u8 = undefined; // paths are used sequentially, so one reused buffer is safe

    const writeStr = struct {
        fn f(io_: Io, path: []const u8, content: []const u8) !void {
            const file = try Io.Dir.createFileAbsolute(io_, path, .{ .truncate = true });
            defer file.close(io_);
            var wbuf: [64]u8 = undefined;
            var fw = Io.File.Writer.init(file, io_, &wbuf);
            try fw.interface.writeAll(content);
            try fw.interface.flush();
        }
    }.f;
    const readStr = struct {
        fn f(io_: Io, path: []const u8, buf: []u8) ![]const u8 {
            const file = try Io.Dir.openFileAbsolute(io_, path, .{});
            defer file.close(io_);
            const n = file.readStreaming(io_, &.{buf}) catch |err| switch (err) {
                error.EndOfStream => return buf[0..0],
                else => return err,
            };
            return buf[0..n];
        }
    }.f;

    try writeStr(tio, jp(&pb, dir, ""), "OLD");
    try writeStr(tio, jp(&pb, dir, ".manifest.json"), "{\"old\":1}");
    try writeStr(tio, jp(&pb, dir, ".new"), "NEW");
    try writeStr(tio, jp(&pb, dir, ".new.manifest.json"), "{\"new\":1}");

    adoptPending(jp(&pb, dir, ""));

    var buf: [64]u8 = undefined;
    try std.testing.expectEqualStrings("NEW", try readStr(tio, jp(&pb, dir, ""), &buf));
    try std.testing.expectEqualStrings("OLD", try readStr(tio, jp(&pb, dir, ".prev"), &buf));
    try std.testing.expectEqualStrings("{\"new\":1}", try readStr(tio, jp(&pb, dir, ".manifest.json"), &buf));
    try std.testing.expectEqualStrings("{\"old\":1}", try readStr(tio, jp(&pb, dir, ".prev.manifest.json"), &buf));

    // .new is consumed
    const fd = std.c.open(jp(&pb, dir, ".new").ptr, .{ .ACCMODE = .RDONLY }, @as(std.c.mode_t, 0));
    try std.testing.expect(fd < 0);
}
