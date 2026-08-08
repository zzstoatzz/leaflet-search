const std = @import("std");
const Io = std.Io;
const logfire = @import("logfire");
const policy = @import("../policy.zig");
const db = @import("../db.zig");
const pubkey = @import("../server/pubkey.zig");

fn isHttpUrl(s: []const u8) bool {
    return std.mem.startsWith(u8, s, "https://") or std.mem.startsWith(u8, s, "http://");
}

/// If `url` is an HTTP(S) URL pointing at a site ROOT (no path after the host,
/// or just "/"), return its host. A standard.site document stores its canonical
/// home in the `site` field; a bare root there is the publication's own domain
/// (e.g. "https://blog.mainasara.dev" → "blog.mainasara.dev") and should win over
/// a same-author publication guessed by DID. A `site` carrying a path (e.g.
/// leaflet's "https://leaflet.pub/p/<did>") is a deep link, not a base host, so
/// this returns null and the DID lookup finds the real subdomain pub instead.
fn httpSiteRootHost(url: []const u8) ?[]const u8 {
    const rest = if (std.mem.startsWith(u8, url, "https://"))
        url["https://".len..]
    else if (std.mem.startsWith(u8, url, "http://"))
        url["http://".len..]
    else
        return null;
    const slash = std.mem.indexOfScalar(u8, rest, '/');
    const host = if (slash) |i| rest[0..i] else rest;
    if (host.len == 0) return null;
    // only a root: nothing after the host, or just a trailing "/"
    if (slash) |i| {
        if (rest.len - i > 1) return null;
    }
    return host;
}

/// Hash title+content for cross-platform dedup.
/// Returns a 16-char hex string (wyhash of "title\x00content").
fn computeContentHash(title: []const u8, content: []const u8) [16]u8 {
    var hasher = std.hash.Wyhash.init(0);
    hasher.update(title);
    hasher.update("\x00");
    hasher.update(content);
    const hash = hasher.final();
    return std.fmt.bytesToHex(std.mem.asBytes(&hash), .lower);
}

pub fn insertDocument(
    uri: []const u8,
    did: []const u8,
    rkey: []const u8,
    title: []const u8,
    content: []const u8,
    created_at: ?[]const u8,
    publication_uri: ?[]const u8,
    tags: []const []const u8,
    platform: []const u8,
    source_collection: []const u8,
    path: ?[]const u8,
    content_type: ?[]const u8,
    cover_image: ?[]const u8,
    source_cid: ?[]const u8,
) !void {
    const c = db.getClient() orelse return error.NotInitialized;

    // banned bulk-archive repos: second gate behind the ingester's ban —
    // nothing reinserts these, not even replays or backfills (policy.zig).
    if (policy.isBanned(did)) {
        logfire.span("ingest.dropped", .{ .reason = "banned_did", .uri = uri }).end();
        return;
    }

    const content_hash: [16]u8 = computeContentHash(title, content);
    const pub_uri = publication_uri orelse "";
    const has_pub: []const u8 = if (pub_uri.len > 0) "1" else "0";

    // all point-lookup reads go over the wire as ONE pipeline request; the
    // conditional ones simply return no rows when their premise doesn't hold.
    // (was: up to 5 serial round trips per document — the ingest worker's
    // throughput ceiling under archive-backfill bursts.)
    const platform_pattern: []const u8 = if (std.mem.eql(u8, platform, "greengale"))
        "%greengale.app%"
    else if (std.mem.eql(u8, platform, "pckt"))
        "%pckt.blog%"
    else if (std.mem.eql(u8, platform, "offprint"))
        "%offprint.app%"
    else if (std.mem.eql(u8, platform, "leaflet"))
        "%leaflet.pub%"
    else
        "%";

    var reads = try c.queryBatch(&.{
        .{ .sql = "SELECT uri FROM documents WHERE did = ? AND rkey = ?", .args = &.{ did, rkey } },
        .{ .sql = "SELECT uri, COALESCE(source_collection, '') FROM documents WHERE did = ? AND content_hash = ?", .args = &.{ did, &content_hash } },
        .{ .sql = "SELECT base_path, platform FROM publications WHERE uri = ?", .args = &.{pub_uri} },
        .{ .sql = "SELECT base_path FROM publications WHERE did = ? AND base_path LIKE ? ORDER BY LENGTH(base_path) DESC LIMIT 1", .args = &.{ did, platform_pattern } },
        .{ .sql = "SELECT base_path FROM publications WHERE did = ? ORDER BY LENGTH(base_path) DESC LIMIT 1", .args = &.{did} },
    });
    defer reads.deinit();

    // dedupe: if (did, rkey) exists with different uri, clean up old record first
    // this handles cross-collection duplicates (e.g., pub.leaflet.document + site.standard.document)
    var doc_exists = false;
    var old_uri_buf: [512]u8 = undefined;
    var stale_uri: []const u8 = "";
    if (reads.getFirst(0)) |row| {
        const old_uri = row.text(0);
        if (!std.mem.eql(u8, old_uri, uri)) {
            if (old_uri.len <= old_uri_buf.len) {
                @memcpy(old_uri_buf[0..old_uri.len], old_uri);
                stale_uri = old_uri_buf[0..old_uri.len];
            }
        } else {
            doc_exists = true;
        }
    }

    // cross-platform content dedup: the same essay cross-posted to leaflet AND
    // site.standard should occupy one row, not two.
    //
    // Scoped to a DIFFERENT collection, which is what "cross-platform" means.
    // Unscoped, it also swallowed renames: publishing tools that derive rkeys
    // from paths implement a rename as create(new_rkey) + delete(old_rkey),
    // and with an unchanged body the create matched the still-present old row
    // by content hash and was dropped — then the delete removed the old one.
    // Both gone, no error anywhere. Only pure renames were affected; an edit
    // changes the hash and misses the dupe check entirely, which is why 7 of 7
    // rename-only documents vanished while 13 of 13 edited ones survived.
    var content_unchanged = false;
    if (reads.getFirst(1)) |row| {
        const existing_uri = row.text(0);
        switch (contentHashVerdict(existing_uri, row.text(1), uri, source_collection)) {
            .same_document => content_unchanged = true,
            .cross_platform_dupe => {
                logfire.debug("indexer: skipping cross-platform dupe for {s} (existing: {s})", .{ uri, existing_uri });
                logfire.span("ingest.dropped", .{ .reason = "content_hash_dupe", .uri = uri, .existing_uri = existing_uri }).end();
                return;
            },
            .rename => {
                // The old row is removed by the delete that follows on the
                // firehose; a transient duplicate is harmless because search
                // dedupes by (did, title) anyway.
                logfire.info("indexer: same-collection rename {s} -> {s}", .{ existing_uri, uri });
                logfire.counter("ingest.rename_indexed", 1);
            },
        }
    }

    // look up base_path from publication (or fallback to DID lookup)
    // use a stack buffer because row.text() returns a slice into result memory
    // which gets freed by reads.deinit()
    var base_path_buf: [256]u8 = undefined;
    var base_path: []const u8 = "";
    // pckt blogs on custom domains carry no `pckt.blog` in their host, so we
    // can't recognize them from base_path. The blog.pckt.publication sidecar
    // stamps platform='pckt' on the publication row; inherit it here.
    var pub_is_pckt = false;

    if (pub_uri.len > 0) {
        if (reads.getFirst(2)) |row| {
            const val = row.text(0);
            if (val.len > 0 and val.len <= base_path_buf.len) {
                @memcpy(base_path_buf[0..val.len], val);
                base_path = base_path_buf[0..val.len];
            }
            pub_is_pckt = std.mem.eql(u8, row.text(1), "pckt");
        }
    }
    // prefer the document's own `site` root host (read into publication_uri) over
    // a DID-guessed publication. authors can run BOTH a known-platform publication
    // and a standard.site custom-domain blog; without this, the DID fallback below
    // glues the custom-domain doc onto the unrelated platform pub and emits a dead
    // link (e.g. neutrino2211.leaflet.pub/... 404 vs blog.mainasara.dev/... 200).
    if (base_path.len == 0 and isHttpUrl(pub_uri)) {
        if (httpSiteRootHost(pub_uri)) |host| {
            if (host.len <= base_path_buf.len) {
                @memcpy(base_path_buf[0..host.len], host);
                base_path = base_path_buf[0..host.len];
            }
        }
    }
    // fallback: find publication by DID, preferring platform-specific matches,
    // then any publication (batch indices 3 and 4)
    if (base_path.len == 0) {
        for ([_]usize{ 3, 4 }) |idx| {
            if (reads.getFirst(idx)) |row| {
                const val = row.text(0);
                if (val.len > 0 and val.len <= base_path_buf.len) {
                    @memcpy(base_path_buf[0..val.len], val);
                    base_path = base_path_buf[0..val.len];
                    break;
                }
            }
        }
    }

    // fallback: if publication_uri is an HTTP(S) URL, use its host as base_path
    // standard.site documents store the origin URL in the "site" field, which
    // our extractor reads into publication_uri. Strip the scheme to match
    // base_path convention (frontend prepends "https://").
    if (base_path.len == 0 and pub_uri.len > 0) {
        var host = if (std.mem.startsWith(u8, pub_uri, "https://"))
            pub_uri["https://".len..]
        else if (std.mem.startsWith(u8, pub_uri, "http://"))
            pub_uri["http://".len..]
        else
            @as([]const u8, "");
        // strip trailing slash to avoid double-slash when combined with path
        if (host.len > 1 and host[host.len - 1] == '/')
            host = host[0 .. host.len - 1];
        if (host.len > 0 and host.len <= base_path_buf.len) {
            @memcpy(base_path_buf[0..host.len], host);
            base_path = base_path_buf[0..host.len];
        }
    }

    // normalize: strip trailing slash to avoid double-slash in URLs
    if (base_path.len > 1 and base_path[base_path.len - 1] == '/')
        base_path = base_path[0 .. base_path.len - 1];

    // skip .test domains (dev/staging data)
    if (std.mem.endsWith(u8, base_path, ".test")) {
        logfire.span("ingest.dropped", .{ .reason = "test_domain", .uri = uri, .base_path = base_path }).end();
        return;
    }

    // detect platform from basePath if platform is unknown/other
    // this handles site.standard.* documents where collection doesn't indicate platform
    var actual_platform = platform;
    if (std.mem.eql(u8, platform, "unknown") or std.mem.eql(u8, platform, "other")) {
        if (pub_is_pckt) {
            actual_platform = "pckt";
        } else if (std.mem.indexOf(u8, base_path, "leaflet.pub") != null) {
            actual_platform = "leaflet";
        } else if (std.mem.indexOf(u8, base_path, "pckt.blog") != null) {
            actual_platform = "pckt";
        } else if (std.mem.indexOf(u8, base_path, "offprint.app") != null) {
            actual_platform = "offprint";
        } else if (std.mem.indexOf(u8, base_path, "greengale.app") != null) {
            actual_platform = "greengale";
        } else if (std.mem.indexOf(u8, base_path, "lemma.pub") != null) {
            actual_platform = "lemma";
        } else if (content_type) |ct| {
            // fallback: detect platform from content.$type for custom domains
            // e.g., "pub.leaflet.content" indicates leaflet even with custom domain
            if (std.mem.startsWith(u8, ct, "pub.leaflet.")) {
                actual_platform = "leaflet";
            } else if (std.mem.startsWith(u8, ct, "pub.lemma.")) {
                actual_platform = "lemma";
            }
        }
    }

    // bridgy fed is classified authoritatively by the reconciler, which resolves
    // the DID's PDS and marks docs hosted on brid.gy. We can't cheaply resolve the
    // PDS here without blocking the firehose worker, so default to "0" at ingest.
    // (The old "platform==other && HTTP site field ⇒ bridgy fed" heuristic was
    // wrong — legit standard.site custom-domain blogs also put an HTTP URL in the
    // `site` field, so it dropped real content like blog.mainasara.dev.)
    const is_bridgyfed: []const u8 = "0";

    // all writes ship as ONE pipeline request (was: up to 8 serial round trips).
    // per-statement semantics match the old code's `catch {}` tolerance; a
    // failed HTTP request (turso down) still propagates.
    //
    // upsert uses ON CONFLICT to preserve embedded_at (INSERT OR REPLACE would
    // nuke it). indexed_at means "content last changed", not "record last
    // seen": platforms mass re-put whole archives over the firehose, and
    // re-stamping unchanged docs churns the below-watermark set the snapshot
    // builder's count gate assumes immutable (2026-07-21 crash-loop).
    // Metadata-only updates still apply; the snapshot copies full rows, so
    // they propagate regardless.
    var batch = DocWriteBatch{};
    defer batch.deinit(c.allocator);
    const write_stmts = try batch.build(c.allocator, .{
        .uri = uri,
        .did = did,
        .rkey = rkey,
        .title = title,
        .content = content,
        .created_at = created_at orelse "",
        .pub_uri = pub_uri,
        .platform = actual_platform,
        .source_collection = source_collection,
        .path = path orelse "",
        .base_path = base_path,
        .has_pub = has_pub,
        .content_hash = &content_hash,
        .cover_image = cover_image orelse "",
        .is_bridgyfed = is_bridgyfed,
        .content_type = content_type orelse "",
        .source_cid = source_cid orelse "",
        .stale_uri = stale_uri,
        .doc_exists = doc_exists,
        .content_unchanged = content_unchanged,
        .tags = tags,
    });
    var writes = try c.queryBatch(write_stmts);
    writes.deinit();
}

pub const DocWriteParams = struct {
    uri: []const u8,
    did: []const u8,
    rkey: []const u8,
    title: []const u8,
    content: []const u8,
    created_at: []const u8,
    pub_uri: []const u8,
    platform: []const u8,
    source_collection: []const u8,
    path: []const u8,
    base_path: []const u8,
    has_pub: []const u8,
    content_hash: []const u8,
    cover_image: []const u8,
    is_bridgyfed: []const u8,
    content_type: []const u8,
    source_cid: []const u8,
    /// prior uri under the same (did, rkey), when it differs from `uri`
    stale_uri: []const u8,
    doc_exists: bool,
    content_unchanged: bool,
    tags: []const []const u8,
};

/// What a content-hash match against an existing row means.
///
/// Unscoped, this check dropped renames: tools that derive rkeys from paths
/// implement a rename as create(new_rkey) + delete(old_rkey), so with an
/// unchanged body the create matched the still-present old row and was
/// discarded — then the delete removed the old one. Both gone, silently. Only
/// pure renames were hit; an edit changes the hash and never reaches here,
/// which is why 7 of 7 rename-only documents vanished and 13 of 13 edited ones
/// survived the same commit.
pub const HashVerdict = enum {
    /// same uri — an unchanged re-put
    same_document,
    /// same body under a different collection: the essay is cross-posted
    cross_platform_dupe,
    /// same body, same collection, different rkey: a rename, and it must index
    rename,
};

pub fn contentHashVerdict(
    existing_uri: []const u8,
    existing_collection: []const u8,
    uri: []const u8,
    collection: []const u8,
) HashVerdict {
    if (std.mem.eql(u8, existing_uri, uri)) return .same_document;
    if (std.mem.eql(u8, existing_collection, collection)) return .rename;
    return .cross_platform_dupe;
}

/// Builds the single-pipeline write batch for one document. Owns the arg
/// storage the returned statements point into — must stay pinned (not moved)
/// until the batch has been executed.
pub const DocWriteBatch = struct {
    stmts: std.ArrayList(db.Client.Statement) = .empty,
    tag_args: std.ArrayList([2][]const u8) = .empty,
    stale_args: [1][]const u8 = undefined,
    uri_args: [1][]const u8 = undefined,
    fts_args: [4][]const u8 = undefined,
    upsert_args: [17][]const u8 = undefined,

    pub fn deinit(self: *DocWriteBatch, allocator: std.mem.Allocator) void {
        self.stmts.deinit(allocator);
        self.tag_args.deinit(allocator);
    }

    pub fn build(self: *DocWriteBatch, allocator: std.mem.Allocator, p: DocWriteParams) ![]const db.Client.Statement {
        self.stale_args = .{p.stale_uri};
        self.uri_args = .{p.uri};
        self.fts_args = .{ p.uri, p.uri, p.title, p.content };
        self.upsert_args = .{ p.uri, p.did, p.rkey, p.title, p.content, p.created_at, p.pub_uri, p.platform, p.source_collection, p.path, p.base_path, p.has_pub, p.content_hash, p.cover_image, p.is_bridgyfed, p.content_type, p.source_cid };

        if (p.stale_uri.len > 0) {
            // FTS is keyed by documents.rowid: drop the FTS row BEFORE the
            // documents row (the rowid lookup needs it to still exist).
            try self.stmts.append(allocator, .{ .sql = "DELETE FROM documents_fts WHERE rowid = (SELECT rowid FROM documents WHERE uri = ?)", .args = &self.stale_args });
            try self.stmts.append(allocator, .{ .sql = "DELETE FROM document_tags WHERE document_uri = ?", .args = &self.stale_args });
            try self.stmts.append(allocator, .{ .sql = "DELETE FROM documents WHERE uri = ?", .args = &self.stale_args });
        }

        try self.stmts.append(allocator, .{ .sql = DOC_UPSERT_SQL, .args = &self.upsert_args });

        // FTS holds exactly title+content — what content_hash covers — so an
        // unchanged re-put has nothing to rewrite there. `uri` is UNINDEXED in
        // documents_fts, so deletes go through documents.rowid (a PK seek),
        // and only when this uri already has a row to replace.
        if (!p.content_unchanged) {
            if (p.doc_exists) {
                try self.stmts.append(allocator, .{ .sql = "DELETE FROM documents_fts WHERE rowid = (SELECT rowid FROM documents WHERE uri = ?)", .args = &self.uri_args });
            }
            try self.stmts.append(allocator, .{ .sql = "INSERT INTO documents_fts (rowid, uri, title, content) VALUES ((SELECT rowid FROM documents WHERE uri = ?), ?, ?, ?)", .args = &self.fts_args });
        }

        // per-tag arg pairs live in tag_args, reserved up front so appended
        // pointers never move.
        try self.stmts.append(allocator, .{ .sql = "DELETE FROM document_tags WHERE document_uri = ?", .args = &self.uri_args });
        try self.tag_args.ensureTotalCapacityPrecise(allocator, p.tags.len);
        for (p.tags) |tag| {
            self.tag_args.appendAssumeCapacity(.{ p.uri, tag });
            try self.stmts.append(allocator, .{
                .sql = "INSERT OR IGNORE INTO document_tags (document_uri, tag) VALUES (?, ?)",
                .args = &self.tag_args.items[self.tag_args.items.len - 1],
            });
        }

        return self.stmts.items;
    }
};

pub const DOC_UPSERT_SQL =
    \\INSERT INTO documents (uri, did, rkey, title, content, created_at, publication_uri, platform, source_collection, path, base_path, has_publication, content_hash, cover_image, indexed_at, is_bridgyfed, content_type, source_cid)
    \\VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, strftime('%Y-%m-%dT%H:%M:%S', 'now'), ?, ?, ?)
    \\ON CONFLICT(uri) DO UPDATE SET
    \\  did = excluded.did,
    \\  rkey = excluded.rkey,
    \\  title = excluded.title,
    \\  content = excluded.content,
    \\  created_at = excluded.created_at,
    \\  publication_uri = excluded.publication_uri,
    \\  platform = excluded.platform,
    \\  source_collection = excluded.source_collection,
    \\  path = excluded.path,
    \\  base_path = excluded.base_path,
    \\  has_publication = excluded.has_publication,
    \\  content_hash = excluded.content_hash,
    \\  cover_image = excluded.cover_image,
    \\  indexed_at = CASE WHEN documents.content_hash = excluded.content_hash
    \\    THEN documents.indexed_at
    \\    ELSE strftime('%Y-%m-%dT%H:%M:%S', 'now') END,
    \\  is_bridgyfed = excluded.is_bridgyfed,
    \\  content_type = excluded.content_type,
    \\  source_cid = excluded.source_cid,
    \\  embedded_at = documents.embedded_at
;

pub fn insertPublication(
    uri: []const u8,
    did: []const u8,
    rkey: []const u8,
    name: []const u8,
    description: ?[]const u8,
    base_path: ?[]const u8,
    show_in_discover: bool,
) !void {
    const c = db.getClient() orelse return error.NotInitialized;

    // banned bulk-archive repos: second gate behind the ingester's ban —
    // nothing reinserts these, not even replays or backfills (policy.zig).
    if (policy.isBanned(did)) {
        logfire.span("ingest.dropped", .{ .reason = "banned_did", .uri = uri }).end();
        return;
    }

    // dedupe: if (did, rkey) exists with different uri, clean up old record first
    if (c.query("SELECT uri FROM publications WHERE did = ? AND rkey = ?", &.{ did, rkey })) |result_val| {
        var result = result_val;
        defer result.deinit();
        if (result.first()) |row| {
            const old_uri = row.text(0);
            if (!std.mem.eql(u8, old_uri, uri)) {
                c.exec("DELETE FROM publications_fts WHERE uri = ?", &.{old_uri}) catch {};
                c.exec("DELETE FROM publications WHERE uri = ?", &.{old_uri}) catch {};
            }
        }
    } else |_| {}

    try c.exec(
        \\INSERT INTO publications (uri, did, rkey, name, description, base_path, show_in_discover, indexed_at)
        \\VALUES (?, ?, ?, ?, ?, ?, ?, strftime('%Y-%m-%dT%H:%M:%S', 'now'))
        \\ON CONFLICT(uri) DO UPDATE SET
        \\  did = excluded.did,
        \\  rkey = excluded.rkey,
        \\  name = excluded.name,
        \\  description = excluded.description,
        \\  base_path = excluded.base_path,
        \\  show_in_discover = excluded.show_in_discover,
        \\  indexed_at = strftime('%Y-%m-%dT%H:%M:%S', 'now')
    ,
        &.{ uri, did, rkey, name, description orelse "", base_path orelse "", if (show_in_discover) "1" else "0" },
    );

    // backfill: update documents whose base_path is empty or stale (differs from publication)
    if (base_path) |bp| {
        if (bp.len > 0) {
            c.exec(
                \\UPDATE documents SET
                \\  base_path = ?,
                \\  indexed_at = strftime('%Y-%m-%dT%H:%M:%S', 'now')
                \\WHERE publication_uri = ?
                \\  AND (base_path IS NULL OR base_path = '' OR base_path != ?)
            , &.{ bp, uri, bp }) catch |err| {
                logfire.warn("indexer: base_path backfill failed for pub {s}: {}", .{ uri, err });
            };
        }
    }

    // update FTS index (includes base_path for subdomain search)
    c.exec("DELETE FROM publications_fts WHERE uri = ?", &.{uri}) catch {};
    c.exec(
        "INSERT INTO publications_fts (uri, name, description, base_path) VALUES (?, ?, ?, ?)",
        &.{ uri, name, description orelse "", base_path orelse "" },
    ) catch {};
}

/// Tag a publication (and its already-indexed docs) as platform pckt, from the
/// blog.pckt.publication sidecar. Bumps indexed_at so the frozen replica syncs.
/// Idempotent; the `<> 'pckt'` guard keeps replays from churning indexed_at.
pub fn markPublicationPckt(publication_uri: []const u8) !void {
    const c = db.getClient() orelse return error.NotInitialized;
    c.exec(
        "UPDATE publications SET platform = 'pckt' WHERE uri = ? AND platform <> 'pckt'",
        &.{publication_uri},
    ) catch |err| {
        logfire.warn("indexer: pckt publication tag failed for {s}: {}", .{ publication_uri, err });
    };
    c.exec(
        \\UPDATE documents SET
        \\  platform = 'pckt',
        \\  indexed_at = strftime('%Y-%m-%dT%H:%M:%S', 'now')
        \\WHERE publication_uri = ? AND platform <> 'pckt'
    , &.{publication_uri}) catch |err| {
        logfire.warn("indexer: pckt document tag failed for {s}: {}", .{ publication_uri, err });
    };
}

fn currentTimestamp(io: Io) i64 {
    return @intCast(@divFloor(Io.Timestamp.now(io, .real).nanoseconds, std.time.ns_per_s));
}

pub fn deleteDocument(uri: []const u8) void {
    const c = db.getClient() orelse return;

    // record tombstone
    var ts_buf: [20]u8 = undefined;
    const ts = std.fmt.bufPrint(&ts_buf, "{d}", .{currentTimestamp(c.io)}) catch "0";
    c.exec(
        "INSERT OR REPLACE INTO tombstones (uri, record_type, deleted_at) VALUES (?, 'document', ?)",
        &.{ uri, ts },
    ) catch {};
    // delete record. FTS is keyed by documents.rowid, so drop the FTS row
    // BEFORE the documents row (the rowid lookup needs it to still exist).
    c.exec("DELETE FROM documents_fts WHERE rowid = (SELECT rowid FROM documents WHERE uri = ?)", &.{uri}) catch {};
    c.exec("DELETE FROM documents WHERE uri = ?", &.{uri}) catch {};
    c.exec("DELETE FROM document_tags WHERE document_uri = ?", &.{uri}) catch {};
}

pub fn insertRecommend(
    uri: []const u8,
    did: []const u8,
    rkey: []const u8,
    document_uri: []const u8,
    created_at: ?[]const u8,
) !void {
    const c = db.getClient() orelse return error.NotInitialized;

    try c.exec(
        \\INSERT INTO recommends (uri, did, rkey, document_uri, created_at, indexed_at)
        \\VALUES (?, ?, ?, ?, ?, strftime('%Y-%m-%dT%H:%M:%S', 'now'))
        \\ON CONFLICT(uri) DO UPDATE SET
        \\  did = excluded.did,
        \\  rkey = excluded.rkey,
        \\  document_uri = excluded.document_uri,
        \\  created_at = excluded.created_at,
        \\  indexed_at = strftime('%Y-%m-%dT%H:%M:%S', 'now')
    ,
        &.{ uri, did, rkey, document_uri, created_at orelse "" },
    );
}

pub fn deleteRecommend(uri: []const u8) void {
    const c = db.getClient() orelse return;

    // record tombstone
    var ts_buf: [20]u8 = undefined;
    const ts = std.fmt.bufPrint(&ts_buf, "{d}", .{currentTimestamp(c.io)}) catch "0";
    c.exec(
        "INSERT OR REPLACE INTO tombstones (uri, record_type, deleted_at) VALUES (?, 'recommend', ?)",
        &.{ uri, ts },
    ) catch {};
    c.exec("DELETE FROM recommends WHERE uri = ?", &.{uri}) catch {};
}

pub fn insertSubscription(
    uri: []const u8,
    did: []const u8,
    rkey: []const u8,
    publication_uri: []const u8,
    created_at: ?[]const u8,
) !void {
    const c = db.getClient() orelse return error.NotInitialized;

    // materialized join key (see pubkey.joinOnStored); parsed once at write time
    // so the publication join is a sargable indexed equijoin.
    const parsed = pubkey.parse(publication_uri);
    const pub_did = if (parsed) |p| p.did else "";
    const pub_rkey = if (parsed) |p| p.rkey else "";

    try c.exec(
        \\INSERT INTO subscriptions (uri, did, rkey, publication_uri, publication_did, publication_rkey, created_at, indexed_at)
        \\VALUES (?, ?, ?, ?, ?, ?, ?, strftime('%Y-%m-%dT%H:%M:%S', 'now'))
        \\ON CONFLICT(uri) DO UPDATE SET
        \\  did = excluded.did,
        \\  rkey = excluded.rkey,
        \\  publication_uri = excluded.publication_uri,
        \\  publication_did = excluded.publication_did,
        \\  publication_rkey = excluded.publication_rkey,
        \\  created_at = excluded.created_at,
        \\  indexed_at = strftime('%Y-%m-%dT%H:%M:%S', 'now')
    ,
        &.{ uri, did, rkey, publication_uri, pub_did, pub_rkey, created_at orelse "" },
    );
}

pub fn deleteSubscription(uri: []const u8) void {
    const c = db.getClient() orelse return;

    // record tombstone
    var ts_buf: [20]u8 = undefined;
    const ts = std.fmt.bufPrint(&ts_buf, "{d}", .{currentTimestamp(c.io)}) catch "0";
    c.exec(
        "INSERT OR REPLACE INTO tombstones (uri, record_type, deleted_at) VALUES (?, 'subscription', ?)",
        &.{ uri, ts },
    ) catch {};
    c.exec("DELETE FROM subscriptions WHERE uri = ?", &.{uri}) catch {};
}

pub fn deletePublication(uri: []const u8) void {
    const c = db.getClient() orelse return;

    // record tombstone
    var ts_buf: [20]u8 = undefined;
    const ts = std.fmt.bufPrint(&ts_buf, "{d}", .{currentTimestamp(c.io)}) catch "0";
    c.exec(
        "INSERT OR REPLACE INTO tombstones (uri, record_type, deleted_at) VALUES (?, 'publication', ?)",
        &.{ uri, ts },
    ) catch {};
    // delete record
    c.exec("DELETE FROM publications WHERE uri = ?", &.{uri}) catch {};
    c.exec("DELETE FROM publications_fts WHERE uri = ?", &.{uri}) catch {};
}

test "httpSiteRootHost: root URL → host, deep link → null" {
    const t = std.testing;
    // bare root: the publication's own domain → use it
    try t.expectEqualStrings("blog.mainasara.dev", httpSiteRootHost("https://blog.mainasara.dev").?);
    try t.expectEqualStrings("blog.mainasara.dev", httpSiteRootHost("https://blog.mainasara.dev/").?);
    try t.expectEqualStrings("piffey.net", httpSiteRootHost("https://piffey.net").?);
    // deep link (leaflet's generic site field) → not a base host
    try t.expect(httpSiteRootHost("https://leaflet.pub/p/did:plc:abc") == null);
    try t.expect(httpSiteRootHost("https://example.com/posts/x") == null);
    // not http
    try t.expect(httpSiteRootHost("at://did:plc:abc/foo") == null);
}

test "DocWriteBatch: stale-uri cleanup, FTS, and tags replay correctly against sqlite" {
    const t = std.testing;
    const zqlite = @import("zqlite");
    var conn = try zqlite.open(":memory:", zqlite.OpenFlags.Create | zqlite.OpenFlags.ReadWrite);
    defer conn.close();
    try conn.exec(
        \\CREATE TABLE documents (
        \\  uri TEXT PRIMARY KEY, did TEXT, rkey TEXT, title TEXT, content TEXT,
        \\  created_at TEXT, publication_uri TEXT, platform TEXT, source_collection TEXT,
        \\  path TEXT, base_path TEXT, has_publication INTEGER, content_hash TEXT,
        \\  cover_image TEXT, indexed_at TEXT, is_bridgyfed INTEGER, content_type TEXT,
        \\  source_cid TEXT, embedded_at TEXT
        \\)
    , .{});
    try conn.exec("CREATE VIRTUAL TABLE documents_fts USING fts5(uri UNINDEXED, title, content)", .{});
    try conn.exec("CREATE TABLE document_tags (document_uri TEXT, tag TEXT, PRIMARY KEY (document_uri, tag))", .{});

    // seed the OLD record under the same (did, rkey): a cross-collection re-put
    try conn.exec("INSERT INTO documents (uri, did, rkey, title, content) VALUES ('at://x/old/1', 'did:plc:x', '1', 'old title', 'old body')", .{});
    try conn.exec("INSERT INTO documents_fts (rowid, uri, title, content) SELECT rowid, uri, title, content FROM documents", .{});
    try conn.exec("INSERT INTO document_tags (document_uri, tag) VALUES ('at://x/old/1', 'stale-tag')", .{});

    var batch = DocWriteBatch{};
    defer batch.deinit(t.allocator);
    const stmts = try batch.build(t.allocator, .{
        .uri = "at://x/new/1",
        .did = "did:plc:x",
        .rkey = "1",
        .title = "new title",
        .content = "new body",
        .created_at = "2026-01-01",
        .pub_uri = "",
        .platform = "other",
        .source_collection = "site.standard.document",
        .path = "",
        .base_path = "",
        .has_pub = "0",
        .content_hash = "hash-new",
        .cover_image = "",
        .is_bridgyfed = "0",
        .content_type = "",
        .source_cid = "",
        .stale_uri = "at://x/old/1",
        .doc_exists = false,
        .content_unchanged = false,
        .tags = &.{ "zig", "search" },
    });

    // replay the batch in order, exactly as turso's pipeline would.
    // if the FTS delete were ordered AFTER the documents delete, its
    // rowid subquery would find nothing and the stale FTS row would
    // survive — the assertions below catch that reordering.
    for (stmts) |stmt| {
        const prepared = try conn.prepare(stmt.sql);
        defer prepared.deinit();
        for (stmt.args, 0..) |arg, i| try prepared.bindValue(arg, i);
        try prepared.stepToCompletion();
    }

    {
        const row = (try conn.row("SELECT COUNT(*) FROM documents", .{})).?;
        defer row.deinit();
        try t.expectEqual(@as(i64, 1), row.int(0));
    }
    {
        const row = (try conn.row("SELECT uri, title FROM documents", .{})).?;
        defer row.deinit();
        try t.expectEqualStrings("at://x/new/1", row.text(0));
        try t.expectEqualStrings("new title", row.text(1));
    }
    { // stale FTS row purged, new one searchable
        const stale = (try conn.row("SELECT COUNT(*) FROM documents_fts WHERE documents_fts MATCH 'old'", .{})).?;
        defer stale.deinit();
        try t.expectEqual(@as(i64, 0), stale.int(0));
        const fresh = (try conn.row("SELECT uri FROM documents_fts WHERE documents_fts MATCH 'new'", .{})).?;
        defer fresh.deinit();
        try t.expectEqualStrings("at://x/new/1", fresh.text(0));
    }
    { // stale tag purged, new tags in place
        const row = (try conn.row("SELECT COUNT(*), GROUP_CONCAT(tag) FROM document_tags WHERE document_uri = 'at://x/new/1'", .{})).?;
        defer row.deinit();
        try t.expectEqual(@as(i64, 2), row.int(0));
        const none = (try conn.row("SELECT COUNT(*) FROM document_tags WHERE document_uri = 'at://x/old/1'", .{})).?;
        defer none.deinit();
        try t.expectEqual(@as(i64, 0), none.int(0));
    }
}

test "DocWriteBatch: unchanged content skips FTS rewrite; changed existing doc replaces FTS in place" {
    const t = std.testing;
    // unchanged: no FTS statements at all
    var unchanged = DocWriteBatch{};
    defer unchanged.deinit(t.allocator);
    const p_base = DocWriteParams{
        .uri = "at://x/doc/1",
        .did = "did:plc:x",
        .rkey = "1",
        .title = "t",
        .content = "c",
        .created_at = "",
        .pub_uri = "",
        .platform = "other",
        .source_collection = "site.standard.document",
        .path = "",
        .base_path = "",
        .has_pub = "0",
        .content_hash = "h",
        .cover_image = "",
        .is_bridgyfed = "0",
        .content_type = "",
        .source_cid = "",
        .stale_uri = "",
        .doc_exists = true,
        .content_unchanged = true,
        .tags = &.{},
    };
    for (try unchanged.build(t.allocator, p_base)) |stmt| {
        try t.expect(std.mem.indexOf(u8, stmt.sql, "documents_fts") == null);
    }

    // changed content on an existing doc: FTS delete precedes FTS insert
    var changed = DocWriteBatch{};
    defer changed.deinit(t.allocator);
    var p = p_base;
    p.content_unchanged = false;
    var fts_delete_at: ?usize = null;
    var fts_insert_at: ?usize = null;
    for (try changed.build(t.allocator, p), 0..) |stmt, i| {
        if (std.mem.startsWith(u8, stmt.sql, "DELETE FROM documents_fts")) fts_delete_at = i;
        if (std.mem.startsWith(u8, stmt.sql, "INSERT INTO documents_fts")) fts_insert_at = i;
    }
    try t.expect(fts_delete_at.? < fts_insert_at.?);
}

test "DOC_UPSERT_SQL: unchanged content keeps indexed_at, changed content bumps it" {
    const t = std.testing;
    const zqlite = @import("zqlite");
    var conn = try zqlite.open(":memory:", zqlite.OpenFlags.Create | zqlite.OpenFlags.ReadWrite);
    defer conn.close();
    try conn.exec(
        \\CREATE TABLE documents (
        \\  uri TEXT PRIMARY KEY, did TEXT, rkey TEXT, title TEXT, content TEXT,
        \\  created_at TEXT, publication_uri TEXT, platform TEXT, source_collection TEXT,
        \\  path TEXT, base_path TEXT, has_publication INTEGER, content_hash TEXT,
        \\  cover_image TEXT, indexed_at TEXT, is_bridgyfed INTEGER, content_type TEXT,
        \\  source_cid TEXT, embedded_at TEXT
        \\)
    , .{});

    const upsert = struct {
        fn run(c: zqlite.Conn, hash: []const u8, cover: []const u8) !void {
            try c.exec(DOC_UPSERT_SQL, .{
                "at://did:plc:x/doc/1", "did:plc:x",  "1",   "title", "content",
                "2020-01-01",           "",           "other", "site.standard.document",
                "",                     "",           "0",   hash,    cover,
                "0",                    "",           "",
            });
        }
    }.run;

    try upsert(conn, "hash-a", "");
    try conn.exec("UPDATE documents SET indexed_at = '2020-06-06T00:00:00', embedded_at = '2020-06-07T00:00:00'", .{});

    // re-put with identical content hash: indexed_at preserved, metadata still updates
    try upsert(conn, "hash-a", "cover.png");
    {
        const row = (try conn.row("SELECT indexed_at, cover_image, embedded_at FROM documents", .{})).?;
        defer row.deinit();
        try t.expectEqualStrings("2020-06-06T00:00:00", row.text(0));
        try t.expectEqualStrings("cover.png", row.text(1));
        try t.expectEqualStrings("2020-06-07T00:00:00", row.text(2));
    }

    // real content change: indexed_at re-stamped
    try upsert(conn, "hash-b", "cover.png");
    {
        const row = (try conn.row("SELECT indexed_at FROM documents", .{})).?;
        defer row.deinit();
        try t.expect(!std.mem.eql(u8, "2020-06-06T00:00:00", row.text(0)));
    }
}

test "content-hash match: a same-collection rename must index, not drop" {
    const t = std.testing;
    const doc = "site.standard.document";

    // The regression: create(new rkey, identical body) arriving while the old
    // row is still present. Dropping it here, then applying the delete that
    // follows on the firehose, removed the document from the corpus entirely.
    try t.expectEqual(HashVerdict.rename, contentHashVerdict(
        "at://did:plc:x/site.standard.document/architecture-bounded-scans",
        doc,
        "at://did:plc:x/site.standard.document/operations-bounded-scans",
        doc,
    ));

    // an unchanged re-put of the same uri
    try t.expectEqual(HashVerdict.same_document, contentHashVerdict(
        "at://did:plc:x/site.standard.document/a",
        doc,
        "at://did:plc:x/site.standard.document/a",
        doc,
    ));

    // the case the check exists for: one essay cross-posted to two platforms
    try t.expectEqual(HashVerdict.cross_platform_dupe, contentHashVerdict(
        "at://did:plc:x/pub.leaflet.document/a",
        "pub.leaflet.document",
        "at://did:plc:x/site.standard.document/b",
        doc,
    ));

    // a legacy row with no recorded collection is not evidence of cross-posting
    try t.expectEqual(HashVerdict.cross_platform_dupe, contentHashVerdict(
        "at://did:plc:x/pub.leaflet.document/a",
        "",
        "at://did:plc:x/site.standard.document/b",
        doc,
    ));
}
