//! Offline snapshot builder: reads Turso and populates a FRESH local SQLite
//! file off-box (builder.zig). In-place serving-box sync was deleted — the
//! replica is refreshed only by verified snapshot adoption (see
//! docs/snapshot-pipeline.md). Background data movement never touches the
//! serving box (2026-06-10 invariant).

const std = @import("std");
const Io = std.Io;
const zqlite = @import("zqlite");
const logfire = @import("logfire");
const Allocator = std.mem.Allocator;
const Client = @import("Client.zig");
const LocalDb = @import("LocalDb.zig");
const policy = @import("../policy.zig");
const pubkey = @import("../server/pubkey.zig");

pub const BuildCounts = struct {
    documents: usize = 0,
    publications: usize = 0,
    tags: usize = 0,
    popular: usize = 0,
    recommends: usize = 0,
    subscriptions: usize = 0,
};

const BUILD_PAGE_SIZE = 500;

// Snapshot-build page query. Two non-negotiable filters (policy.zig): turso
// still holds historical rows for banned DIDs and bridgy-flagged docs until
// the paced cleanup finishes, and a snapshot must never resurrect them.
// Keyset pagination on the uri PK keeps turso row reads linear (an OFFSET
// walk re-scans from the start every page — the 2026-06 access-pattern
// lesson).
const BUILD_DOC_PAGE_SQL =
    "SELECT uri, did, rkey, title, content, created_at, publication_uri, " ++
    "platform, source_collection, path, base_path, has_publication, indexed_at, embedded_at, " ++
    "COALESCE(cover_image, '') as cover_image, COALESCE(is_bridgyfed, 0) as is_bridgyfed, " ++
    "COALESCE(url_dead, 0) as url_dead, COALESCE(source_cid, '') as source_cid " ++
    "FROM documents WHERE uri > ? " ++
    "AND indexed_at <= ? " ++
    "AND COALESCE(is_bridgyfed, 0) NOT IN (1, '1') " ++
    "AND did NOT IN (" ++ policy.banned_dids_sql ++ ") " ++
    "ORDER BY uri LIMIT 500";

pub const BUILD_DOC_COUNT_SQL =
    "SELECT COUNT(*) FROM documents " ++
    "WHERE indexed_at <= ? " ++
    "AND COALESCE(is_bridgyfed, 0) NOT IN (1, '1') " ++
    "AND did NOT IN (" ++ policy.banned_dids_sql ++ ")";

// every table copy below is keyset-paginated like documents: the old
// one-shot SELECTs pulled the whole table into a single hrana response —
// unbounded builder memory and exactly the giant-turso-scan pattern that
// has wedged prod before. at 5x corpus, document_tags is the dangerous one.
const BUILD_PUB_PAGE_SQL =
    "SELECT uri, did, rkey, name, description, base_path, platform, indexed_at, COALESCE(show_in_discover, 1) " ++
    "FROM publications WHERE uri > ? AND did NOT IN (" ++ policy.banned_dids_sql ++ ") " ++
    "ORDER BY uri LIMIT 500";

const BUILD_TAG_PAGE_SQL =
    "SELECT document_uri, tag FROM document_tags " ++
    "WHERE (document_uri, tag) > (?, ?) " ++
    "ORDER BY document_uri, tag LIMIT 500";

const BUILD_POPULAR_PAGE_SQL =
    "SELECT query, count FROM popular_searches WHERE query > ? ORDER BY query LIMIT 500";

// watermark-pinned like documents (NULL indexed_at sorts as old → included)
const BUILD_REC_PAGE_SQL =
    "SELECT uri, did, rkey, document_uri, COALESCE(created_at, ''), COALESCE(indexed_at, '') " ++
    "FROM recommends WHERE uri > ? AND COALESCE(indexed_at, '') <= ? " ++
    "AND did NOT IN (" ++ policy.banned_dids_sql ++ ") " ++
    "ORDER BY uri LIMIT 500";

pub const BUILD_REC_COUNT_SQL =
    "SELECT COUNT(*) FROM recommends WHERE COALESCE(indexed_at, '') <= ? " ++
    "AND did NOT IN (" ++ policy.banned_dids_sql ++ ")";

// watermark-pinned like recommends. did = subscriber; banned subscribers are
// excluded the same way (a banned repo's signal never lands in the snapshot).
const BUILD_SUB_PAGE_SQL =
    "SELECT uri, did, rkey, publication_uri, COALESCE(created_at, ''), COALESCE(indexed_at, '') " ++
    "FROM subscriptions WHERE uri > ? AND COALESCE(indexed_at, '') <= ? " ++
    "AND did NOT IN (" ++ policy.banned_dids_sql ++ ") " ++
    "ORDER BY uri LIMIT 500";

pub const BUILD_SUB_COUNT_SQL =
    "SELECT COUNT(*) FROM subscriptions WHERE COALESCE(indexed_at, '') <= ? " ++
    "AND did NOT IN (" ++ policy.banned_dids_sql ++ ")";

/// Offline snapshot build: populate a FRESH local db from turso. The target
/// must never be the serving replica — the builder runs off-box (builder.zig)
/// and pacing between pages keeps turso comfortable while production reads it
/// (2026-06-10 wedge lesson: bulk ops own their blast radius).
///
/// The build is PINNED to `indexed_at <= watermark`: docs written mid-build
/// are excluded, so the manifest's source_watermark is an exact contract —
/// the snapshot contains every (policy-passing) doc at or before it and
/// nothing after. The overlay/promote side depends on this for its freshness
/// cutoff. (Watermark semantics cover documents; publications/tags are small
/// and copied whole.)
pub fn buildSnapshot(turso: *Client, local: *LocalDb, watermark: []const u8) !BuildCounts {
    const conn = local.getConn() orelse return error.LocalNotOpen;
    var counts: BuildCounts = .{};

    var cursor_buf: [512]u8 = undefined;
    var cursor: []const u8 = "";
    while (true) {
        var result = turso.query(BUILD_DOC_PAGE_SQL, &.{ cursor, watermark }) catch |err| {
            logfire.err("build: turso document page failed at cursor {s}: {}", .{ cursor, err });
            return err;
        };
        defer result.deinit();

        if (result.rows.len == 0) break;

        conn.exec("BEGIN", .{}) catch {};
        for (result.rows) |row| {
            try insertDocumentLocal(conn, row);
            counts.documents += 1;
        }
        conn.exec("COMMIT", .{}) catch {};

        const last_uri = result.rows[result.rows.len - 1].text(0);
        if (last_uri.len >= cursor_buf.len) return error.UriTooLong;
        @memcpy(cursor_buf[0..last_uri.len], last_uri);
        cursor = cursor_buf[0..last_uri.len];

        if (counts.documents % 5000 < BUILD_PAGE_SIZE) {
            std.debug.print("build: {d} documents...\n", .{counts.documents});
        }
        if (result.rows.len < BUILD_PAGE_SIZE) break;

        // pacing: the builder shares turso with production reads
        turso.io.sleep(Io.Duration.fromMilliseconds(150), .awake) catch {};
    }

    try copyPaged(BUILD_PUB_PAGE_SQL, 1, false, insertPublicationLocal, turso, conn, watermark, &counts.publications, "publications");

    try copyPaged(BUILD_TAG_PAGE_SQL, 2, false, insertTagLocal, turso, conn, watermark, &counts.tags, "tags");
    // tags were copied unfiltered; drop the ones whose documents the
    // policy filters excluded (local-side, cheap)
    conn.exec("DELETE FROM document_tags WHERE document_uri NOT IN (SELECT uri FROM documents)", .{}) catch {};

    try copyPaged(BUILD_REC_PAGE_SQL, 1, true, insertRecommendLocal, turso, conn, watermark, &counts.recommends, "recommends");

    try copyPaged(BUILD_SUB_PAGE_SQL, 1, true, insertSubscriptionLocal, turso, conn, watermark, &counts.subscriptions, "subscriptions");
    // Ship snapshots with the materialized join key already populated so a
    // freshly-adopted replica is fast on first query (createSchema's boot
    // backfill is the safety net for older snapshots). See pubkey.joinOnStored.
    const sub_backfill_sql = comptime "UPDATE subscriptions SET publication_did = " ++ pubkey.didExpr("publication_uri") ++
        ", publication_rkey = " ++ pubkey.rkeyExpr("publication_uri") ++
        " WHERE publication_did IS NULL AND publication_uri LIKE 'at://%/%/%'";
    conn.exec(sub_backfill_sql, .{}) catch {};

    try copyPaged(BUILD_POPULAR_PAGE_SQL, 1, false, insertPopularLocal, turso, conn, watermark, &counts.popular, "popular_searches");

    // merge the incrementally-built FTS index into one b-tree: fewer, more
    // contiguous posting-list pages, which is what cold reads on the serving
    // volume pay for
    conn.exec("INSERT INTO documents_fts (documents_fts) VALUES ('optimize')", .{}) catch |err| {
        logfire.warn("build: fts optimize failed ({s}) — snapshot still valid", .{@errorName(err)});
    };

    return counts;
}

/// Keyset-paginated table copy from turso into the local build target.
/// `page_sql` must select the keyset column(s) FIRST, filter with
/// `key > ?` (or a `(k1, k2) > (?, ?)` row-value compare when
/// `key_cols == 2`), and end with `ORDER BY <keys> LIMIT 500`. When
/// `has_watermark` the watermark binds as the parameter after the cursor.
/// Pages share turso with production reads, so pacing matches the document
/// walk (150ms between pages).
fn copyPaged(
    comptime page_sql: []const u8,
    comptime key_cols: comptime_int,
    comptime has_watermark: bool,
    comptime insertRow: anytype,
    turso: *Client,
    conn: zqlite.Conn,
    watermark: []const u8,
    counter: *usize,
    comptime label: []const u8,
) !void {
    var cur1_buf: [512]u8 = undefined;
    var cur2_buf: [512]u8 = undefined;
    var cur1: []const u8 = "";
    var cur2: []const u8 = "";
    while (true) {
        var result = (if (key_cols == 1)
            (if (has_watermark) turso.query(page_sql, &.{ cur1, watermark }) else turso.query(page_sql, &.{cur1}))
        else
            (if (has_watermark) turso.query(page_sql, &.{ cur1, cur2, watermark }) else turso.query(page_sql, &.{ cur1, cur2 }))) catch |err| {
            logfire.err("build: turso " ++ label ++ " page failed at cursor {s}: {}", .{ cur1, err });
            return err;
        };
        defer result.deinit();

        if (result.rows.len == 0) break;

        conn.exec("BEGIN", .{}) catch {};
        for (result.rows) |row| {
            try insertRow(conn, row);
            counter.* += 1;
        }
        conn.exec("COMMIT", .{}) catch {};

        const last = result.rows[result.rows.len - 1];
        const k1 = last.text(0);
        if (k1.len >= cur1_buf.len) return error.UriTooLong;
        @memcpy(cur1_buf[0..k1.len], k1);
        cur1 = cur1_buf[0..k1.len];
        if (key_cols == 2) {
            const k2 = last.text(1);
            if (k2.len >= cur2_buf.len) return error.UriTooLong;
            @memcpy(cur2_buf[0..k2.len], k2);
            cur2 = cur2_buf[0..k2.len];
        }

        if (result.rows.len < BUILD_PAGE_SIZE) break;

        // pacing: the builder shares turso with production reads
        turso.io.sleep(Io.Duration.fromMilliseconds(150), .awake) catch {};
    }
}

fn insertTagLocal(conn: zqlite.Conn, row: anytype) !void {
    conn.exec(
        "INSERT OR IGNORE INTO document_tags (document_uri, tag) VALUES (?, ?)",
        .{ row.text(0), row.text(1) },
    ) catch {};
}

fn insertRecommendLocal(conn: zqlite.Conn, row: anytype) !void {
    conn.exec(
        "INSERT OR REPLACE INTO recommends (uri, did, rkey, document_uri, created_at, indexed_at) VALUES (?, ?, ?, ?, ?, ?)",
        .{ row.text(0), row.text(1), row.text(2), row.text(3), row.text(4), row.text(5) },
    ) catch {};
}

fn insertSubscriptionLocal(conn: zqlite.Conn, row: anytype) !void {
    conn.exec(
        "INSERT OR REPLACE INTO subscriptions (uri, did, rkey, publication_uri, created_at, indexed_at) VALUES (?, ?, ?, ?, ?, ?)",
        .{ row.text(0), row.text(1), row.text(2), row.text(3), row.text(4), row.text(5) },
    ) catch {};
}

fn insertPopularLocal(conn: zqlite.Conn, row: anytype) !void {
    conn.exec(
        "INSERT OR REPLACE INTO popular_searches (query, count) VALUES (?, ?)",
        .{ row.text(0), row.text(1) },
    ) catch {};
}

fn insertDocumentLocal(conn: zqlite.Conn, row: anytype) !void {
    // FTS is keyed to documents.rowid so deletes are an O(1) rowid drop rather
    // than a uri-UNINDEXED full-scan (same pathology fixed turso-side in the
    // indexer; unchecked, a busy cycle held the local write lock for 300s+ and
    // wedged everything sharing it — 2026-06-10 stats/dashboard outage).
    // INSERT OR REPLACE below assigns a FRESH rowid, so the stale FTS row must
    // be dropped by its CURRENT rowid now, before the replace. New docs (the
    // majority) have no row and skip this entirely. documents_fts is external
    // content (v5), so the drop is the fts5 'delete' command carrying the OLD
    // row's indexed values — plain DELETE is an error on external content, and
    // 'delete' with wrong values silently corrupts the index.
    if (conn.row("SELECT rowid, title, content FROM documents WHERE uri = ?", .{row.text(0)}) catch null) |r| {
        defer r.deinit();
        conn.exec(
            "INSERT INTO documents_fts (documents_fts, rowid, uri, title, content) VALUES ('delete', ?, ?, ?, ?)",
            .{ r.int(0), row.text(0), r.text(1), r.text(2) },
        ) catch {};
    }

    // insert into main table
    conn.exec(
        \\INSERT OR REPLACE INTO documents
        \\(uri, did, rkey, title, content, created_at, publication_uri,
        \\ platform, source_collection, path, base_path, has_publication, indexed_at, embedded_at, cover_image, is_bridgyfed, url_dead, source_cid)
        \\VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    , .{
        row.text(0), // uri
        row.text(1), // did
        row.text(2), // rkey
        row.text(3), // title
        row.text(4), // content
        row.text(5), // created_at
        row.text(6), // publication_uri
        row.text(7), // platform
        row.text(8), // source_collection
        row.text(9), // path
        row.text(10), // base_path
        row.int(11), // has_publication
        row.text(12), // indexed_at
        row.text(13), // embedded_at
        row.text(14), // cover_image
        row.int(15), // is_bridgyfed
        row.int(16), // url_dead
        row.text(17), // source_cid
    }) catch |err| {
        return err;
    };

    // re-insert FTS keyed to the new documents.rowid (stale row already
    // dropped above, before the replace reassigned the rowid)
    const uri = row.text(0);
    conn.exec(
        "INSERT INTO documents_fts (rowid, uri, title, content) VALUES ((SELECT rowid FROM documents WHERE uri = ?), ?, ?, ?)",
        .{ uri, uri, row.text(3), row.text(4) },
    ) catch {};
}

fn insertPublicationLocal(conn: zqlite.Conn, row: anytype) !void {
    // insert into main table (no created_at - Turso publications table doesn't have it)
    conn.exec(
        \\INSERT OR REPLACE INTO publications
        \\(uri, did, rkey, name, description, base_path, platform, indexed_at, show_in_discover)
        \\VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    , .{
        row.text(0), // uri
        row.text(1), // did
        row.text(2), // rkey
        row.text(3), // name
        row.text(4), // description
        row.text(5), // base_path
        row.text(6), // platform
        row.text(7), // indexed_at
        row.int(8), // show_in_discover
    }) catch |err| {
        return err;
    };

    // update FTS
    const uri = row.text(0);
    conn.exec("DELETE FROM publications_fts WHERE uri = ?", .{uri}) catch {};
    conn.exec(
        "INSERT INTO publications_fts (uri, name, description, base_path) VALUES (?, ?, ?, ?)",
        .{ uri, row.text(3), row.text(4), row.text(5) },
    ) catch {};
}

// --- tests ---

test "insertDocumentLocal keys FTS by rowid: no orphans on update, MATCH works" {
    const conn = try zqlite.open(":memory:", zqlite.OpenFlags.Create | zqlite.OpenFlags.ReadWrite);
    defer conn.close();
    // mirrors LocalDb.createSchema v5: content last, external-content fts
    try conn.exec(
        \\CREATE TABLE documents (
        \\  uri TEXT PRIMARY KEY, did TEXT, rkey TEXT, title TEXT,
        \\  created_at TEXT, publication_uri TEXT, platform TEXT, source_collection TEXT,
        \\  path TEXT, base_path TEXT, has_publication INTEGER, indexed_at TEXT,
        \\  embedded_at TEXT, cover_image TEXT, is_bridgyfed INTEGER, url_dead INTEGER,
        \\  source_cid TEXT, content TEXT
        \\)
    , .{});
    try conn.exec("CREATE VIRTUAL TABLE documents_fts USING fts5(uri UNINDEXED, title, content, content='documents', content_rowid='rowid')", .{});

    // a fake turso row exposing the .text()/.int() accessors insertDocumentLocal reads
    const FakeRow = struct {
        uri: []const u8,
        title: []const u8,
        content: []const u8,
        source_cid: []const u8,
        fn text(self: @This(), i: usize) []const u8 {
            return switch (i) {
                0 => self.uri,
                3 => self.title,
                4 => self.content,
                17 => self.source_cid,
                else => "",
            };
        }
        fn int(_: @This(), _: usize) i64 {
            return 0;
        }
    };

    try insertDocumentLocal(conn, FakeRow{ .uri = "at://a", .title = "first", .content = "hello world", .source_cid = "bafy-first" });
    // same uri, replacement (INSERT OR REPLACE reassigns documents.rowid)
    try insertDocumentLocal(conn, FakeRow{ .uri = "at://a", .title = "second", .content = "hello world", .source_cid = "bafy-second" });

    // source identity follows the replacement into the immutable snapshot.
    {
        const r = (try conn.row("SELECT source_cid FROM documents WHERE uri = 'at://a'", .{})).?;
        defer r.deinit();
        try std.testing.expectEqualStrings("bafy-second", r.text(0));
    }

    // exactly one FTS row — the stale one was dropped, not orphaned
    {
        const r = (try conn.row("SELECT COUNT(*) FROM documents_fts", .{})).?;
        defer r.deinit();
        try std.testing.expectEqual(@as(i64, 1), r.int(0));
    }
    // it reflects the updated title
    {
        const r = (try conn.row("SELECT title FROM documents_fts", .{})).?;
        defer r.deinit();
        try std.testing.expectEqualStrings("second", r.text(0));
    }
    // FTS rowid stayed aligned to documents.rowid after the replace
    {
        const r = (try conn.row("SELECT (SELECT rowid FROM documents WHERE uri = f.uri) = f.rowid FROM documents_fts f", .{})).?;
        defer r.deinit();
        try std.testing.expectEqual(@as(i64, 1), r.int(0));
    }
    // the read path (MATCH) still finds it
    {
        const r = (try conn.row("SELECT COUNT(*) FROM documents_fts WHERE documents_fts MATCH 'hello'", .{})).?;
        defer r.deinit();
        try std.testing.expectEqual(@as(i64, 1), r.int(0));
    }
}
