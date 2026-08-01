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

const BUILD_PUB_SQL =
    "SELECT uri, did, rkey, name, description, base_path, platform, indexed_at, COALESCE(show_in_discover, 1) " ++
    "FROM publications WHERE did NOT IN (" ++ policy.banned_dids_sql ++ ")";

// watermark-pinned like documents (NULL indexed_at sorts as old → included)
const BUILD_REC_SQL =
    "SELECT uri, did, rkey, document_uri, COALESCE(created_at, ''), COALESCE(indexed_at, '') " ++
    "FROM recommends WHERE COALESCE(indexed_at, '') <= ? " ++
    "AND did NOT IN (" ++ policy.banned_dids_sql ++ ")";

pub const BUILD_REC_COUNT_SQL =
    "SELECT COUNT(*) FROM recommends WHERE COALESCE(indexed_at, '') <= ? " ++
    "AND did NOT IN (" ++ policy.banned_dids_sql ++ ")";

// watermark-pinned like recommends. did = subscriber; banned subscribers are
// excluded the same way (a banned repo's signal never lands in the snapshot).
const BUILD_SUB_SQL =
    "SELECT uri, did, rkey, publication_uri, COALESCE(created_at, ''), COALESCE(indexed_at, '') " ++
    "FROM subscriptions WHERE COALESCE(indexed_at, '') <= ? " ++
    "AND did NOT IN (" ++ policy.banned_dids_sql ++ ")";

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

    {
        var pub_result = turso.query(BUILD_PUB_SQL, &.{}) catch |err| {
            logfire.err("build: turso publications query failed: {}", .{err});
            return err;
        };
        defer pub_result.deinit();

        conn.exec("BEGIN", .{}) catch {};
        for (pub_result.rows) |row| {
            try insertPublicationLocal(conn, row);
            counts.publications += 1;
        }
        conn.exec("COMMIT", .{}) catch {};
    }

    {
        var tags_result = turso.query("SELECT document_uri, tag FROM document_tags", &.{}) catch |err| {
            logfire.err("build: turso tags query failed: {}", .{err});
            return err;
        };
        defer tags_result.deinit();

        conn.exec("BEGIN", .{}) catch {};
        for (tags_result.rows) |row| {
            conn.exec(
                "INSERT OR IGNORE INTO document_tags (document_uri, tag) VALUES (?, ?)",
                .{ row.text(0), row.text(1) },
            ) catch {};
            counts.tags += 1;
        }
        conn.exec("COMMIT", .{}) catch {};

        // tags were copied unfiltered; drop the ones whose documents the
        // policy filters excluded (local-side, cheap)
        conn.exec("DELETE FROM document_tags WHERE document_uri NOT IN (SELECT uri FROM documents)", .{}) catch {};
    }

    {
        var rec_result = turso.query(BUILD_REC_SQL, &.{watermark}) catch |err| {
            logfire.err("build: turso recommends query failed: {}", .{err});
            return err;
        };
        defer rec_result.deinit();

        conn.exec("BEGIN", .{}) catch {};
        for (rec_result.rows) |row| {
            conn.exec(
                "INSERT OR REPLACE INTO recommends (uri, did, rkey, document_uri, created_at, indexed_at) VALUES (?, ?, ?, ?, ?, ?)",
                .{ row.text(0), row.text(1), row.text(2), row.text(3), row.text(4), row.text(5) },
            ) catch {};
            counts.recommends += 1;
        }
        conn.exec("COMMIT", .{}) catch {};
    }

    {
        var sub_result = turso.query(BUILD_SUB_SQL, &.{watermark}) catch |err| {
            logfire.err("build: turso subscriptions query failed: {}", .{err});
            return err;
        };
        defer sub_result.deinit();

        conn.exec("BEGIN", .{}) catch {};
        for (sub_result.rows) |row| {
            conn.exec(
                "INSERT OR REPLACE INTO subscriptions (uri, did, rkey, publication_uri, created_at, indexed_at) VALUES (?, ?, ?, ?, ?, ?)",
                .{ row.text(0), row.text(1), row.text(2), row.text(3), row.text(4), row.text(5) },
            ) catch {};
            counts.subscriptions += 1;
        }
        conn.exec("COMMIT", .{}) catch {};

        // Ship snapshots with the materialized join key already populated so a
        // freshly-adopted replica is fast on first query (createSchema's boot
        // backfill is the safety net for older snapshots). See pubkey.joinOnStored.
        const sub_backfill_sql = comptime "UPDATE subscriptions SET publication_did = " ++ pubkey.didExpr("publication_uri") ++
            ", publication_rkey = " ++ pubkey.rkeyExpr("publication_uri") ++
            " WHERE publication_did IS NULL AND publication_uri LIKE 'at://%/%/%'";
        conn.exec(sub_backfill_sql, .{}) catch {};
    }

    {
        var popular_result = turso.query("SELECT query, count FROM popular_searches", &.{}) catch |err| {
            logfire.err("build: turso popular_searches query failed: {}", .{err});
            return err;
        };
        defer popular_result.deinit();

        conn.exec("BEGIN", .{}) catch {};
        for (popular_result.rows) |row| {
            conn.exec(
                "INSERT OR REPLACE INTO popular_searches (query, count) VALUES (?, ?)",
                .{ row.text(0), row.text(1) },
            ) catch {};
            counts.popular += 1;
        }
        conn.exec("COMMIT", .{}) catch {};
    }

    return counts;
}

fn insertDocumentLocal(conn: zqlite.Conn, row: anytype) !void {
    // FTS is keyed to documents.rowid so deletes are an O(1) rowid drop rather
    // than a uri-UNINDEXED full-scan (same pathology fixed turso-side in the
    // indexer; unchecked, a busy cycle held the local write lock for 300s+ and
    // wedged everything sharing it — 2026-06-10 stats/dashboard outage).
    // INSERT OR REPLACE below assigns a FRESH rowid, so the stale FTS row must
    // be dropped by its CURRENT rowid now, before the replace. New docs (the
    // majority) have no row and skip this entirely.
    var old_rowid: ?i64 = null;
    if (conn.row("SELECT rowid FROM documents WHERE uri = ?", .{row.text(0)}) catch null) |r| {
        old_rowid = r.int(0);
        r.deinit();
    }
    if (old_rowid) |rid| {
        conn.exec("DELETE FROM documents_fts WHERE rowid = ?", .{rid}) catch {};
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
    try conn.exec(
        \\CREATE TABLE documents (
        \\  uri TEXT PRIMARY KEY, did TEXT, rkey TEXT, title TEXT, content TEXT,
        \\  created_at TEXT, publication_uri TEXT, platform TEXT, source_collection TEXT,
        \\  path TEXT, base_path TEXT, has_publication INTEGER, indexed_at TEXT,
        \\  embedded_at TEXT, cover_image TEXT, is_bridgyfed INTEGER, url_dead INTEGER,
        \\  source_cid TEXT
        \\)
    , .{});
    try conn.exec("CREATE VIRTUAL TABLE documents_fts USING fts5(uri UNINDEXED, title, content)", .{});

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
