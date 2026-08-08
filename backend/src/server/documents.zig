//! GET /document — full extracted text for documents by AT-URI.
//!
//! Search responses carry snippets only; this endpoint returns the complete
//! extracted `content` the indexer stored, so agents can read an article
//! without re-fetching and re-flattening the record from the author's PDS.
//! Served entirely from the local replica: no turso, no network.

const std = @import("std");
const json = std.json;
const Allocator = std.mem.Allocator;

const db = @import("../db.zig");
const policy = @import("../policy.zig");
const classifier = @import("../ingest/classifier.zig");
const search = @import("search.zig");
const visibility = @import("../visibility.zig");

pub const MAX_URIS = 25;

const DOC_SQL =
    \\SELECT d.uri, d.did, d.rkey, d.title, COALESCE(d.created_at, ''),
    \\  d.platform, COALESCE(NULLIF(d.base_path, ''), p.base_path, ''),
    \\  COALESCE(d.path, ''), d.has_publication, COALESCE(p.name, ''),
    \\  COALESCE(d.cover_image, ''), d.content,
    \\  COALESCE(d.publication_uri, '')
    \\FROM documents d LEFT JOIN publications p ON d.publication_uri = p.uri
    \\WHERE d.uri = ?
    \\AND (d.is_bridgyfed IS NULL OR d.is_bridgyfed = 0)
    \\AND (d.url_dead IS NULL OR d.url_dead = 0)
;

/// Same visibility rule as search results: labeled (bulk-generated) authors
/// are excluded unless explicitly kept — a direct fetch must not resurface
/// what search hides.
fn visible(did: []const u8) bool {
    if (policy.isBanned(did)) return false;
    return !classifier.isLabeledDid(did) or policy.isKept(did);
}

/// Render the response body for a list of AT-URIs. Found documents land in
/// `documents` (in request order); anything unknown, policy-excluded, or
/// malformed lands in `missing`.
pub fn fetch(alloc: Allocator, uris: []const []const u8, include_undiscoverable: bool) ![]const u8 {
    const local = db.getLocalDb() orelse return error.LocalNotReady;

    var output: std.Io.Writer.Allocating = .init(alloc);
    errdefer output.deinit();
    var jw: json.Stringify = .{ .writer = &output.writer };

    var missing: std.ArrayList([]const u8) = .empty;

    try jw.beginObject();
    try jw.objectField("documents");
    try jw.beginArray();

    for (uris) |uri| {
        var rows = local.query(DOC_SQL, .{uri}) catch {
            try missing.append(alloc, uri);
            continue;
        };
        defer rows.deinit();

        const row = rows.next() orelse {
            try missing.append(alloc, uri);
            continue;
        };
        const did = row.text(1);
        if (!visible(did)) {
            try missing.append(alloc, uri);
            continue;
        }

        const rkey = row.text(2);
        const platform = row.text(5);
        const base_path = row.text(6);
        const path = row.text(7);

        // Same visibility rule as search: a publication that opted out of
        // discovery must not have its full text handed out by uri either.
        // This endpoint is how an agent reads what search found, so leaving it
        // open would make the search filter cosmetic — and it leaks the whole
        // article, not just a title.
        if (!include_undiscoverable and visibility.isUndiscoverableDoc("", did, base_path)) {
            try missing.append(alloc, uri);
            continue;
        }
        const doc_type: []const u8 = if (row.int(8) != 0) "article" else "looseleaf";

        try jw.beginObject();
        try jw.objectField("type");
        try jw.write(doc_type);
        try jw.objectField("uri");
        try jw.write(row.text(0));
        try jw.objectField("did");
        try jw.write(did);
        try jw.objectField("rkey");
        try jw.write(rkey);
        try jw.objectField("title");
        try jw.write(row.text(3));
        try jw.objectField("createdAt");
        try jw.write(row.text(4));
        try jw.objectField("platform");
        try jw.write(platform);
        try jw.objectField("basePath");
        try jw.write(base_path);
        try jw.objectField("path");
        try jw.write(path);
        try jw.objectField("publicationName");
        try jw.write(row.text(9));
        try jw.objectField("publicationUri");
        try jw.write(row.text(12));
        try jw.objectField("coverImage");
        try jw.write(row.text(10));
        try jw.objectField("url");
        try jw.write(search.buildDocUrl(alloc, doc_type, platform, base_path, path, rkey, did));

        try jw.objectField("tags");
        try jw.beginArray();
        var tag_rows = local.query("SELECT tag FROM document_tags WHERE document_uri = ?", .{uri}) catch null;
        if (tag_rows) |*tr| {
            defer tr.deinit();
            while (tr.next()) |tag_row| try jw.write(tag_row.text(0));
        }
        try jw.endArray();

        try jw.objectField("content");
        try jw.write(row.text(11));
        try jw.endObject();
    }

    try jw.endArray();

    try jw.objectField("missing");
    try jw.beginArray();
    for (missing.items) |uri| try jw.write(uri);
    try jw.endArray();
    try jw.endObject();

    return output.toOwnedSlice();
}

/// Split a comma-separated `uri` query-param value into trimmed AT-URIs.
/// Empty segments are dropped; returns error.TooMany over MAX_URIS.
pub fn splitUris(alloc: Allocator, raw: []const u8) ![]const []const u8 {
    var list: std.ArrayList([]const u8) = .empty;
    var it = std.mem.splitScalar(u8, raw, ',');
    while (it.next()) |part| {
        const trimmed = std.mem.trim(u8, part, " ");
        if (trimmed.len == 0) continue;
        if (list.items.len >= MAX_URIS) return error.TooMany;
        try list.append(alloc, trimmed);
    }
    return list.items;
}

test "splitUris trims, drops empties, caps at MAX_URIS" {
    const t = std.testing;
    var arena = std.heap.ArenaAllocator.init(t.allocator);
    defer arena.deinit();
    const alloc = arena.allocator();

    const uris = try splitUris(alloc, "at://a/x/1, at://b/x/2,,at://c/x/3");
    try t.expectEqual(@as(usize, 3), uris.len);
    try t.expectEqualStrings("at://b/x/2", uris[1]);

    var big: std.ArrayList(u8) = .empty;
    for (0..MAX_URIS + 1) |i| {
        if (i > 0) try big.append(alloc, ',');
        try big.appendSlice(alloc, "at://d/x/r");
    }
    try t.expectError(error.TooMany, splitUris(alloc, big.items));
}
