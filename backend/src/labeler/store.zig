//! SQLite-backed label store (zqlite).
//!
//! Same public API as labelz's store (init/insert/queryByCursor/
//! queryBySubject/latestSeq + StoredLabel), so server.zig is lifted verbatim —
//! but backed by the backend's vendored zqlite instead of a raw sqlite3 cImport
//! (the backend already links sqlite through zqlite; a second sqlite would
//! conflict). Labels live in their OWN db file, not the frozen replica: the
//! replica is wiped+replaced by snapshot adoption, and labels must survive that.
//!
//! The ws server handles queries on per-connection threads while emit() writes
//! from elsewhere; safe because zqlite's sqlite is built in serialized threading
//! mode (the default — same reason the backend's read pool shares the lib across
//! threads). Every call prepares + finalizes its own statement.

const std = @import("std");
const zqlite = @import("zqlite");
const label_mod = @import("label.zig");
const Label = label_mod.Label;

const Allocator = std.mem.Allocator;
const log = std.log.scoped(.labeler_store);

pub const StoredLabel = struct {
    seq: i64,
    label: Label,
    /// pre-encoded signed CBOR (stored as blob, avoids re-encoding)
    encoded: []const u8,
};

pub const Store = struct {
    conn: zqlite.Conn,

    pub fn init(path: [*:0]const u8) !Store {
        const flags = zqlite.OpenFlags.Create | zqlite.OpenFlags.ReadWrite;
        const conn = try zqlite.open(path, flags);
        errdefer conn.close();

        conn.execNoArgs("PRAGMA journal_mode=WAL") catch {};
        conn.execNoArgs("PRAGMA busy_timeout=5000") catch {};

        try conn.execNoArgs(
            \\CREATE TABLE IF NOT EXISTS labels (
            \\  seq INTEGER PRIMARY KEY AUTOINCREMENT,
            \\  src TEXT NOT NULL,
            \\  uri TEXT NOT NULL,
            \\  cid TEXT,
            \\  val TEXT NOT NULL,
            \\  neg INTEGER NOT NULL DEFAULT 0,
            \\  cts TEXT NOT NULL,
            \\  exp TEXT,
            \\  sig BLOB NOT NULL,
            \\  encoded BLOB NOT NULL
            \\)
        );
        try conn.execNoArgs("CREATE INDEX IF NOT EXISTS idx_labels_uri ON labels(uri)");

        return .{ .conn = conn };
    }

    pub fn deinit(self: *Store) void {
        self.conn.close();
    }

    /// insert a signed label, returns the assigned sequence number.
    pub fn insert(self: *Store, lbl: *const Label, encoded: []const u8) !i64 {
        if (lbl.sig == null) return error.UnsignedLabel;

        try self.conn.exec(
            \\INSERT INTO labels (src, uri, cid, val, neg, cts, exp, sig, encoded)
            \\VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        , .{
            lbl.src,
            lbl.uri,
            lbl.cid,
            lbl.val,
            lbl.neg,
            lbl.cts,
            lbl.exp,
            zqlite.blob(lbl.sig.?),
            zqlite.blob(encoded),
        });
        return self.conn.lastInsertedRowId();
    }

    /// get labels after a cursor (sequence number), up to limit.
    pub fn queryByCursor(self: *Store, allocator: Allocator, cursor: i64, limit: i64) ![]StoredLabel {
        var r = try self.conn.rows(
            "SELECT seq, src, uri, cid, val, neg, cts, exp, sig, encoded FROM labels WHERE seq > ? ORDER BY seq ASC LIMIT ?",
            .{ cursor, limit },
        );
        defer r.deinit();
        return collect(allocator, &r);
    }

    /// a queryLabels filter. `patterns` entries are subject URIs, optionally
    /// ending in `*` for a prefix match; a bare `*` matches every subject.
    /// An empty `patterns` also matches everything.
    pub const Filter = struct {
        patterns: []const []const u8 = &.{},
        sources: []const []const u8 = &.{},
        cursor: i64 = 0,
        limit: i64 = 50,
    };

    /// com.atproto.label.queryLabels — pattern/source filtered, seq-paginated.
    pub fn query(self: *Store, allocator: Allocator, filter: Filter) ![]StoredLabel {
        var aw: std.Io.Writer.Allocating = .init(allocator);
        defer aw.deinit();
        const w = &aw.writer;

        try w.writeAll("SELECT seq, src, uri, cid, val, neg, cts, exp, sig, encoded FROM labels WHERE seq > ?");

        const match_all = filter.patterns.len == 0 or blk: {
            for (filter.patterns) |p| if (std.mem.eql(u8, p, "*")) break :blk true;
            break :blk false;
        };

        if (!match_all) {
            try w.writeAll(" AND (");
            for (filter.patterns, 0..) |p, i| {
                if (i > 0) try w.writeAll(" OR ");
                // prefix patterns compare the leading substr rather than LIKE:
                // LIKE is ASCII case-insensitive and would need %/_ escaping.
                if (p.len > 0 and p[p.len - 1] == '*') {
                    try w.writeAll("substr(uri, 1, ?) = ?");
                } else {
                    try w.writeAll("uri = ?");
                }
            }
            try w.writeByte(')');
        }

        if (filter.sources.len > 0) {
            try w.writeAll(" AND src IN (");
            for (0..filter.sources.len) |i| {
                if (i > 0) try w.writeByte(',');
                try w.writeByte('?');
            }
            try w.writeByte(')');
        }

        try w.writeAll(" ORDER BY seq ASC LIMIT ?");

        const stmt = try self.conn.prepare(w.buffered());
        errdefer stmt.deinit();

        var idx: usize = 0;
        try stmt.bindValue(filter.cursor, idx);
        idx += 1;

        if (!match_all) {
            for (filter.patterns) |p| {
                if (p.len > 0 and p[p.len - 1] == '*') {
                    const prefix = p[0 .. p.len - 1];
                    try stmt.bindValue(@as(i64, @intCast(prefix.len)), idx);
                    idx += 1;
                    try stmt.bindValue(prefix, idx);
                } else {
                    try stmt.bindValue(p, idx);
                }
                idx += 1;
            }
        }
        for (filter.sources) |s| {
            try stmt.bindValue(s, idx);
            idx += 1;
        }
        try stmt.bindValue(filter.limit, idx);

        var r: zqlite.Rows = .{ .stmt = stmt, .err = null };
        defer r.deinit();
        return collect(allocator, &r);
    }

    /// get the latest sequence number (0 if empty).
    pub fn latestSeq(self: *Store) i64 {
        const row = self.conn.row("SELECT COALESCE(MAX(seq), 0) FROM labels", .{}) catch return 0;
        if (row) |rw| {
            defer rw.deinit();
            return rw.int(0);
        }
        return 0;
    }

    /// collect rows into owned StoredLabels. Each field is duped into
    /// `allocator`; the caller frees them (same contract server.zig expects).
    fn collect(allocator: Allocator, r: *zqlite.Rows) ![]StoredLabel {
        var results: std.ArrayList(StoredLabel) = .empty;
        errdefer {
            for (results.items) |item| freeStored(allocator, item);
            results.deinit(allocator);
        }
        while (r.next()) |row| {
            const encoded = try allocator.dupe(u8, row.blob(9));
            errdefer allocator.free(encoded);
            const sig = try allocator.dupe(u8, row.blob(8));
            errdefer allocator.free(sig);
            try results.append(allocator, .{
                .seq = row.int(0),
                .label = .{
                    .src = try allocator.dupe(u8, row.text(1)),
                    .uri = try allocator.dupe(u8, row.text(2)),
                    .cid = try dupeOpt(allocator, row.nullableText(3)),
                    .val = try allocator.dupe(u8, row.text(4)),
                    .neg = row.boolean(5),
                    .cts = try allocator.dupe(u8, row.text(6)),
                    .exp = try dupeOpt(allocator, row.nullableText(7)),
                    .sig = sig,
                },
                .encoded = encoded,
            });
        }
        return results.toOwnedSlice(allocator);
    }
};

fn dupeOpt(allocator: Allocator, v: ?[]const u8) !?[]const u8 {
    return if (v) |s| try allocator.dupe(u8, s) else null;
}

/// free a StoredLabel's owned fields (mirrors what server.zig frees).
pub fn freeStored(allocator: Allocator, item: StoredLabel) void {
    allocator.free(item.label.src);
    allocator.free(item.label.uri);
    allocator.free(item.label.val);
    allocator.free(item.label.cts);
    allocator.free(item.encoded);
    if (item.label.sig) |s| allocator.free(s);
    if (item.label.cid) |ci| allocator.free(ci);
    if (item.label.exp) |e| allocator.free(e);
}

// === tests ===

test "store insert and query by cursor" {
    var store = try Store.init(":memory:");
    defer store.deinit();
    const allocator = std.testing.allocator;

    var label1 = Label{
        .src = "did:plc:labeler",
        .uri = "did:plc:user1",
        .val = "bulk-mirror",
        .cts = "2024-01-01T00:00:00.000Z",
        .sig = &(.{0xaa} ** 64),
    };
    const seq1 = try store.insert(&label1, "encoded1");

    var label2 = Label{
        .src = "did:plc:labeler",
        .uri = "did:plc:user2",
        .val = "bulk-mirror",
        .cts = "2024-01-01T00:01:00.000Z",
        .sig = &(.{0xbb} ** 64),
    };
    const seq2 = try store.insert(&label2, "encoded2");

    try std.testing.expect(seq2 > seq1);
    try std.testing.expectEqual(seq2, store.latestSeq());

    const all = try store.queryByCursor(allocator, 0, 100);
    defer {
        for (all) |item| freeStored(allocator, item);
        allocator.free(all);
    }
    try std.testing.expectEqual(@as(usize, 2), all.len);

    const after = try store.queryByCursor(allocator, seq1, 100);
    defer {
        for (after) |item| freeStored(allocator, item);
        allocator.free(after);
    }
    try std.testing.expectEqual(@as(usize, 1), after.len);
    try std.testing.expectEqualStrings("did:plc:user2", after[0].label.uri);
}

fn seedThree(store: *Store) !void {
    var l1 = Label{
        .src = "did:plc:labeler",
        .uri = "did:plc:target",
        .val = "bulk-mirror",
        .cts = "2024-01-01T00:00:00.000Z",
        .sig = &(.{0xaa} ** 64),
    };
    _ = try store.insert(&l1, "e1");

    var l2 = Label{
        .src = "did:plc:labeler",
        .uri = "did:plc:other",
        .val = "bulk-mirror",
        .cts = "2024-01-01T00:00:01.000Z",
        .sig = &(.{0xbb} ** 64),
    };
    _ = try store.insert(&l2, "e2");

    var l3 = Label{
        .src = "did:plc:elsewhere",
        .uri = "at://did:plc:target/app.bsky.feed.post/abc",
        .val = "bulk-mirror",
        .cts = "2024-01-01T00:00:02.000Z",
        .sig = &(.{0xcc} ** 64),
    };
    _ = try store.insert(&l3, "e3");
}

fn queryUris(store: *Store, allocator: Allocator, filter: Store.Filter) ![]const []const u8 {
    const results = try store.query(allocator, filter);
    defer {
        for (results) |item| freeStored(allocator, item);
        allocator.free(results);
    }
    var uris: std.ArrayList([]const u8) = .empty;
    errdefer uris.deinit(allocator);
    for (results) |item| try uris.append(allocator, try allocator.dupe(u8, item.label.uri));
    return uris.toOwnedSlice(allocator);
}

fn freeUris(allocator: Allocator, uris: []const []const u8) void {
    for (uris) |u| allocator.free(u);
    allocator.free(uris);
}

test "store query by exact subject" {
    var store = try Store.init(":memory:");
    defer store.deinit();
    const allocator = std.testing.allocator;
    try seedThree(&store);

    const uris = try queryUris(&store, allocator, .{ .patterns = &.{"did:plc:target"} });
    defer freeUris(allocator, uris);
    try std.testing.expectEqual(@as(usize, 1), uris.len);
    try std.testing.expectEqualStrings("did:plc:target", uris[0]);
}

// regression: `uriPatterns=*` matched the literal string "*" and returned []
test "store query wildcard matches every subject" {
    var store = try Store.init(":memory:");
    defer store.deinit();
    const allocator = std.testing.allocator;
    try seedThree(&store);

    const star = try queryUris(&store, allocator, .{ .patterns = &.{"*"} });
    defer freeUris(allocator, star);
    try std.testing.expectEqual(@as(usize, 3), star.len);

    // a `*` anywhere in the list widens the whole query
    const mixed = try queryUris(&store, allocator, .{ .patterns = &.{ "did:plc:nobody", "*" } });
    defer freeUris(allocator, mixed);
    try std.testing.expectEqual(@as(usize, 3), mixed.len);
}

test "store query prefix pattern" {
    var store = try Store.init(":memory:");
    defer store.deinit();
    const allocator = std.testing.allocator;
    try seedThree(&store);

    const uris = try queryUris(&store, allocator, .{ .patterns = &.{"at://did:plc:target/*"} });
    defer freeUris(allocator, uris);
    try std.testing.expectEqual(@as(usize, 1), uris.len);
    try std.testing.expectEqualStrings("at://did:plc:target/app.bsky.feed.post/abc", uris[0]);

    // a prefix that matches nothing stays empty (no accidental full scan)
    const none = try queryUris(&store, allocator, .{ .patterns = &.{"at://did:plc:nobody/*"} });
    defer freeUris(allocator, none);
    try std.testing.expectEqual(@as(usize, 0), none.len);
}

test "store query multiple patterns, sources, limit and cursor" {
    var store = try Store.init(":memory:");
    defer store.deinit();
    const allocator = std.testing.allocator;
    try seedThree(&store);

    const both = try queryUris(&store, allocator, .{
        .patterns = &.{ "did:plc:target", "did:plc:other" },
    });
    defer freeUris(allocator, both);
    try std.testing.expectEqual(@as(usize, 2), both.len);

    const from_labeler = try queryUris(&store, allocator, .{
        .patterns = &.{"*"},
        .sources = &.{"did:plc:elsewhere"},
    });
    defer freeUris(allocator, from_labeler);
    try std.testing.expectEqual(@as(usize, 1), from_labeler.len);
    try std.testing.expectEqualStrings("at://did:plc:target/app.bsky.feed.post/abc", from_labeler[0]);

    const page1 = try store.query(allocator, .{ .patterns = &.{"*"}, .limit = 2 });
    defer {
        for (page1) |item| freeStored(allocator, item);
        allocator.free(page1);
    }
    try std.testing.expectEqual(@as(usize, 2), page1.len);

    const page2 = try queryUris(&store, allocator, .{
        .patterns = &.{"*"},
        .limit = 2,
        .cursor = page1[page1.len - 1].seq,
    });
    defer freeUris(allocator, page2);
    try std.testing.expectEqual(@as(usize, 1), page2.len);
    try std.testing.expectEqualStrings("at://did:plc:target/app.bsky.feed.post/abc", page2[0]);
}

test "store empty returns zero seq" {
    var store = try Store.init(":memory:");
    defer store.deinit();
    try std.testing.expectEqual(@as(i64, 0), store.latestSeq());
}
