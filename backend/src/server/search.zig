const std = @import("std");
const json = std.json;
const Allocator = std.mem.Allocator;
const zql = @import("zql");
const logfire = @import("logfire");
const db = @import("../db.zig");
const tpuf = @import("../tpuf.zig");
const classifier = @import("../ingest/classifier.zig");
const policy = @import("../policy.zig");
const visibility = @import("../visibility.zig");

pub const Options = struct {
    /// Number of policy-visible, deduplicated results the caller needs from
    /// the start of the stable ranking. HTTP asks for offset + limit + 1 so it
    /// can slice the page and determine hasMore without guessing.
    max_results: usize = 21,
    show_labeled: bool = false,
    /// Publications with preferences.showInDiscover=false are indexed but
    /// excluded from every retrieval path unless the caller opts in with
    /// `include_undiscoverable=true`. The default is exclusion, and a path
    /// that cannot resolve the preference excludes rather than shows.
    include_undiscoverable: bool = false,
    /// Merge live-overlay hits into keyword/tag serving (Stage 2 flag; see
    /// OVERLAY_SERVE + the ?overlay= debug param in server.zig).
    use_overlay: bool = false,
};

pub const SearchMode = enum {
    keyword,
    semantic,
    hybrid,

    pub fn fromString(s: ?[]const u8) SearchMode {
        const str = s orelse return .keyword;
        if (std.mem.eql(u8, str, "semantic")) return .semantic;
        if (std.mem.eql(u8, str, "hybrid")) return .hybrid;
        return .keyword;
    }
};

// JSON output type for search results
const SearchResultJson = struct {
    type: []const u8,
    uri: []const u8,
    did: []const u8,
    title: []const u8,
    snippet: []const u8,
    createdAt: []const u8 = "",
    rkey: []const u8,
    basePath: []const u8,
    platform: []const u8,
    path: []const u8 = "", // URL path from record (e.g., "/001")
    source: []const u8 = "",
    coverImage: []const u8 = "",
    publicationName: []const u8 = "",
    url: []const u8 = "",
};

/// Document search result (internal)
const Doc = struct {
    uri: []const u8,
    did: []const u8,
    title: []const u8,
    snippet: []const u8,
    createdAt: []const u8,
    rkey: []const u8,
    basePath: []const u8,
    hasPublication: bool,
    platform: []const u8,
    path: []const u8,
    coverImage: []const u8,
    publicationName: []const u8,

    fn fromRow(row: db.Row) Doc {
        return .{
            .uri = row.text(docCol("uri")),
            .did = row.text(docCol("did")),
            .title = row.text(docCol("title")),
            .snippet = row.text(docCol("snippet")),
            .createdAt = row.text(docCol("created_at")),
            .rkey = row.text(docCol("rkey")),
            .basePath = row.text(docCol("base_path")),
            .hasPublication = row.int(docCol("has_publication")) != 0,
            .platform = row.text(docCol("platform")),
            .path = row.text(docCol("path")),
            .coverImage = row.text(docCol("cover_image")),
            .publicationName = row.text(docCol("publication_name")),
        };
    }

    fn fromLocalRow(row: db.LocalDb.Row) Doc {
        // local-side SQL strings (in searchLocal) share the same column
        // projection as the Turso doc queries — they have to, since Doc has
        // exactly one shape. Reusing docCol gives us the comptime-checked
        // index lookups for the local path too.
        return .{
            .uri = row.text(docCol("uri")),
            .did = row.text(docCol("did")),
            .title = row.text(docCol("title")),
            .snippet = row.text(docCol("snippet")),
            .createdAt = row.text(docCol("created_at")),
            .rkey = row.text(docCol("rkey")),
            .basePath = row.text(docCol("base_path")),
            .hasPublication = row.int(docCol("has_publication")) != 0,
            .platform = row.text(docCol("platform")),
            .path = row.text(docCol("path")),
            .coverImage = row.text(docCol("cover_image")),
            .publicationName = row.text(docCol("publication_name")),
        };
    }

    fn toJson(self: Doc, alloc: Allocator) SearchResultJson {
        const doc_type: []const u8 = if (self.hasPublication) "article" else "looseleaf";
        return .{
            .type = doc_type,
            .uri = self.uri,
            .did = self.did,
            .title = self.title,
            .snippet = self.snippet,
            .createdAt = self.createdAt,
            .rkey = self.rkey,
            .basePath = self.basePath,
            .platform = self.platform,
            .path = self.path,
            .coverImage = self.coverImage,
            .publicationName = self.publicationName,
            .url = buildDocUrl(alloc, doc_type, self.platform, self.basePath, self.path, self.rkey, self.did),
        };
    }
};

/// Build canonical URL for a document/publication from its fields.
/// Single source of truth: the frontend renders `doc.url` from this verbatim.
pub fn buildDocUrl(alloc: Allocator, doc_type: []const u8, platform: []const u8, base_path: []const u8, path: []const u8, rkey: []const u8, did: []const u8) []const u8 {
    // publication → https://{basePath}
    if (std.mem.eql(u8, doc_type, "publication") and base_path.len > 0) {
        return std.fmt.allocPrint(alloc, "https://{s}", .{base_path}) catch "";
    }
    // skip non-document-serving hosts (blento is a card portal, not a document platform)
    const usable_base = base_path.len > 0 and !std.mem.startsWith(u8, base_path, "blento.app");
    // explicit path wins → https://{basePath}[/]{path}
    // the rkey-URL form below is a leaflet.pub convention; it must NOT override an
    // author-set path. site.standard.document records embedding pub.leaflet.content
    // get tagged platform=leaflet (indexer.zig) but are served by their own path —
    // native pub.leaflet.document records never carry a path, so they fall through.
    if (usable_base and path.len > 0) {
        const sep: []const u8 = if (path[0] == '/') "" else "/";
        return std.fmt.allocPrint(alloc, "https://{s}{s}{s}", .{ base_path, sep, path }) catch "";
    }
    // leaflet + basePath + rkey → https://{basePath}/{rkey}
    if (std.mem.eql(u8, platform, "leaflet") and usable_base and rkey.len > 0) {
        return std.fmt.allocPrint(alloc, "https://{s}/{s}", .{ base_path, rkey }) catch "";
    }
    // leaflet fallback → https://leaflet.pub/p/{did}/{rkey}
    if (std.mem.eql(u8, platform, "leaflet") and did.len > 0 and rkey.len > 0) {
        return std.fmt.allocPrint(alloc, "https://leaflet.pub/p/{s}/{s}", .{ did, rkey }) catch "";
    }
    // whitewind fallback → https://whtwnd.com/{did}/{rkey}
    if (std.mem.eql(u8, platform, "whitewind") and did.len > 0 and rkey.len > 0) {
        return std.fmt.allocPrint(alloc, "https://whtwnd.com/{s}/{s}", .{ did, rkey }) catch "";
    }
    // universal fallback → AT Protocol record viewer
    if (did.len > 0 and rkey.len > 0) {
        return std.fmt.allocPrint(alloc, "https://pdsls.dev/at/{s}/site.standard.document/{s}", .{ did, rkey }) catch "";
    }
    return "";
}

const DocsByTag = zql.Query(
    \\SELECT d.uri, d.did, d.title, '' as snippet,
    \\  d.created_at, d.rkey, d.base_path, d.has_publication,
    \\  d.platform, COALESCE(d.path, '') as path,
    \\  COALESCE(d.cover_image, '') as cover_image,
    \\  COALESCE(p.name, '') as publication_name
    \\FROM documents d
    \\LEFT JOIN publications p ON d.publication_uri = p.uri
    \\JOIN document_tags dt ON d.uri = dt.document_uri
    \\WHERE dt.tag = :tag
    \\ORDER BY d.created_at DESC, d.uri LIMIT :limit
);

const DocsByFtsAndTag = zql.Query(
    \\SELECT f.uri, d.did, d.title,
    \\  snippet(documents_fts, 2, '', '', '...', 32) as snippet,
    \\  d.created_at, d.rkey, d.base_path, d.has_publication,
    \\  d.platform, COALESCE(d.path, '') as path,
    \\  COALESCE(d.cover_image, '') as cover_image,
    \\  COALESCE(p.name, '') as publication_name
    \\FROM documents_fts f
    \\JOIN documents d ON f.uri = d.uri
    \\LEFT JOIN publications p ON d.publication_uri = p.uri
    \\JOIN document_tags dt ON d.uri = dt.document_uri
    \\WHERE documents_fts MATCH :query AND dt.tag = :tag
    \\ORDER BY rank + COALESCE((julianday('now') - julianday(NULLIF(d.created_at, ''))) / 30.0, 120.0), d.uri LIMIT :limit
);

const DocsByFts = zql.Query(
    \\SELECT f.uri, d.did, d.title,
    \\  snippet(documents_fts, 2, '', '', '...', 32) as snippet,
    \\  d.created_at, d.rkey, d.base_path, d.has_publication,
    \\  d.platform, COALESCE(d.path, '') as path,
    \\  COALESCE(d.cover_image, '') as cover_image,
    \\  COALESCE(p.name, '') as publication_name
    \\FROM documents_fts f
    \\JOIN documents d ON f.uri = d.uri
    \\LEFT JOIN publications p ON d.publication_uri = p.uri
    \\WHERE documents_fts MATCH :query
    \\ORDER BY rank + COALESCE((julianday('now') - julianday(NULLIF(d.created_at, ''))) / 30.0, 120.0), d.uri LIMIT :limit
);

const DocsByFtsAndSince = zql.Query(
    \\SELECT f.uri, d.did, d.title,
    \\  snippet(documents_fts, 2, '', '', '...', 32) as snippet,
    \\  d.created_at, d.rkey, d.base_path, d.has_publication,
    \\  d.platform, COALESCE(d.path, '') as path,
    \\  COALESCE(d.cover_image, '') as cover_image,
    \\  COALESCE(p.name, '') as publication_name
    \\FROM documents_fts f
    \\JOIN documents d ON f.uri = d.uri
    \\LEFT JOIN publications p ON d.publication_uri = p.uri
    \\WHERE documents_fts MATCH :query AND d.created_at >= :since
    \\ORDER BY rank + COALESCE((julianday('now') - julianday(NULLIF(d.created_at, ''))) / 30.0, 120.0), d.uri LIMIT :limit
);

const DocsByFtsAndPlatform = zql.Query(
    \\SELECT f.uri, d.did, d.title,
    \\  snippet(documents_fts, 2, '', '', '...', 32) as snippet,
    \\  d.created_at, d.rkey, d.base_path, d.has_publication,
    \\  d.platform, COALESCE(d.path, '') as path,
    \\  COALESCE(d.cover_image, '') as cover_image,
    \\  COALESCE(p.name, '') as publication_name
    \\FROM documents_fts f
    \\JOIN documents d ON f.uri = d.uri
    \\LEFT JOIN publications p ON d.publication_uri = p.uri
    \\WHERE documents_fts MATCH :query AND d.platform = :platform
    \\ORDER BY rank + COALESCE((julianday('now') - julianday(NULLIF(d.created_at, ''))) / 30.0, 120.0), d.uri LIMIT :limit
);

const DocsByFtsAndPlatformAndSince = zql.Query(
    \\SELECT f.uri, d.did, d.title,
    \\  snippet(documents_fts, 2, '', '', '...', 32) as snippet,
    \\  d.created_at, d.rkey, d.base_path, d.has_publication,
    \\  d.platform, COALESCE(d.path, '') as path,
    \\  COALESCE(d.cover_image, '') as cover_image,
    \\  COALESCE(p.name, '') as publication_name
    \\FROM documents_fts f
    \\JOIN documents d ON f.uri = d.uri
    \\LEFT JOIN publications p ON d.publication_uri = p.uri
    \\WHERE documents_fts MATCH :query AND d.platform = :platform AND d.created_at >= :since
    \\ORDER BY rank + COALESCE((julianday('now') - julianday(NULLIF(d.created_at, ''))) / 30.0, 120.0), d.uri LIMIT :limit
);

const DocsByTagAndPlatform = zql.Query(
    \\SELECT d.uri, d.did, d.title, '' as snippet,
    \\  d.created_at, d.rkey, d.base_path, d.has_publication,
    \\  d.platform, COALESCE(d.path, '') as path,
    \\  COALESCE(d.cover_image, '') as cover_image,
    \\  COALESCE(p.name, '') as publication_name
    \\FROM documents d
    \\LEFT JOIN publications p ON d.publication_uri = p.uri
    \\JOIN document_tags dt ON d.uri = dt.document_uri
    \\WHERE dt.tag = :tag AND d.platform = :platform
    \\ORDER BY d.created_at DESC, d.uri LIMIT :limit
);

const DocsByFtsAndTagAndPlatform = zql.Query(
    \\SELECT f.uri, d.did, d.title,
    \\  snippet(documents_fts, 2, '', '', '...', 32) as snippet,
    \\  d.created_at, d.rkey, d.base_path, d.has_publication,
    \\  d.platform, COALESCE(d.path, '') as path,
    \\  COALESCE(d.cover_image, '') as cover_image,
    \\  COALESCE(p.name, '') as publication_name
    \\FROM documents_fts f
    \\JOIN documents d ON f.uri = d.uri
    \\LEFT JOIN publications p ON d.publication_uri = p.uri
    \\JOIN document_tags dt ON d.uri = dt.document_uri
    \\WHERE documents_fts MATCH :query AND dt.tag = :tag AND d.platform = :platform
    \\ORDER BY rank + COALESCE((julianday('now') - julianday(NULLIF(d.created_at, ''))) / 30.0, 120.0), d.uri LIMIT :limit
);

const DocsByPlatform = zql.Query(
    \\SELECT d.uri, d.did, d.title, '' as snippet,
    \\  d.created_at, d.rkey, d.base_path, d.has_publication,
    \\  d.platform, COALESCE(d.path, '') as path,
    \\  COALESCE(d.cover_image, '') as cover_image,
    \\  COALESCE(p.name, '') as publication_name
    \\FROM documents d
    \\LEFT JOIN publications p ON d.publication_uri = p.uri
    \\WHERE d.platform = :platform
    \\ORDER BY d.created_at DESC, d.uri LIMIT :limit
);

const DocsByAuthor = zql.Query(
    \\SELECT d.uri, d.did, d.title, '' as snippet,
    \\  d.created_at, d.rkey, d.base_path, d.has_publication,
    \\  d.platform, COALESCE(d.path, '') as path,
    \\  COALESCE(d.cover_image, '') as cover_image,
    \\  COALESCE(p.name, '') as publication_name
    \\FROM documents d
    \\LEFT JOIN publications p ON d.publication_uri = p.uri
    \\WHERE d.did = :author AND (d.is_bridgyfed IS NULL OR d.is_bridgyfed = 0) AND (d.url_dead IS NULL OR d.url_dead = 0)
    \\ORDER BY d.created_at DESC, d.uri LIMIT :limit
);

const DocsByAuthorAndPlatform = zql.Query(
    \\SELECT d.uri, d.did, d.title, '' as snippet,
    \\  d.created_at, d.rkey, d.base_path, d.has_publication,
    \\  d.platform, COALESCE(d.path, '') as path,
    \\  COALESCE(d.cover_image, '') as cover_image,
    \\  COALESCE(p.name, '') as publication_name
    \\FROM documents d
    \\LEFT JOIN publications p ON d.publication_uri = p.uri
    \\WHERE d.did = :author AND d.platform = :platform AND (d.is_bridgyfed IS NULL OR d.is_bridgyfed = 0) AND (d.url_dead IS NULL OR d.url_dead = 0)
    \\ORDER BY d.created_at DESC, d.uri LIMIT :limit
);

// Find documents by their publication's base_path (subdomain search)
// e.g., searching "gyst" finds all docs on gyst.leaflet.pub
// Uses recency decay: recent docs rank higher than old ones with same match
const DocsByPubBasePath = zql.Query(
    \\SELECT d.uri, d.did, d.title, '' as snippet,
    \\  d.created_at, d.rkey,
    \\  p.base_path,
    \\  1 as has_publication,
    \\  d.platform, COALESCE(d.path, '') as path,
    \\  COALESCE(d.cover_image, '') as cover_image,
    \\  COALESCE(p.name, '') as publication_name
    \\FROM documents d
    \\JOIN publications p ON d.publication_uri = p.uri
    \\JOIN publications_fts pf ON p.uri = pf.uri
    \\WHERE publications_fts MATCH :query
    \\ORDER BY rank + COALESCE((julianday('now') - julianday(NULLIF(d.created_at, ''))) / 30.0, 120.0), d.uri LIMIT :limit
);

const DocsByPubBasePathAndPlatform = zql.Query(
    \\SELECT d.uri, d.did, d.title, '' as snippet,
    \\  d.created_at, d.rkey,
    \\  p.base_path,
    \\  1 as has_publication,
    \\  d.platform, COALESCE(d.path, '') as path,
    \\  COALESCE(d.cover_image, '') as cover_image,
    \\  COALESCE(p.name, '') as publication_name
    \\FROM documents d
    \\JOIN publications p ON d.publication_uri = p.uri
    \\JOIN publications_fts pf ON p.uri = pf.uri
    \\WHERE publications_fts MATCH :query AND d.platform = :platform
    \\ORDER BY rank + COALESCE((julianday('now') - julianday(NULLIF(d.created_at, ''))) / 30.0, 120.0), d.uri LIMIT :limit
);

const DocsByPubBasePathAndSince = zql.Query(
    \\SELECT d.uri, d.did, d.title, '' as snippet,
    \\  d.created_at, d.rkey,
    \\  p.base_path,
    \\  1 as has_publication,
    \\  d.platform, COALESCE(d.path, '') as path,
    \\  COALESCE(d.cover_image, '') as cover_image,
    \\  COALESCE(p.name, '') as publication_name
    \\FROM documents d
    \\JOIN publications p ON d.publication_uri = p.uri
    \\JOIN publications_fts pf ON p.uri = pf.uri
    \\WHERE publications_fts MATCH :query AND d.created_at >= :since
    \\ORDER BY rank + COALESCE((julianday('now') - julianday(NULLIF(d.created_at, ''))) / 30.0, 120.0), d.uri LIMIT :limit
);

const DocsByPubBasePathAndPlatformAndSince = zql.Query(
    \\SELECT d.uri, d.did, d.title, '' as snippet,
    \\  d.created_at, d.rkey,
    \\  p.base_path,
    \\  1 as has_publication,
    \\  d.platform, COALESCE(d.path, '') as path,
    \\  COALESCE(d.cover_image, '') as cover_image,
    \\  COALESCE(p.name, '') as publication_name
    \\FROM documents d
    \\JOIN publications p ON d.publication_uri = p.uri
    \\JOIN publications_fts pf ON p.uri = pf.uri
    \\WHERE publications_fts MATCH :query AND d.platform = :platform AND d.created_at >= :since
    \\ORDER BY rank + COALESCE((julianday('now') - julianday(NULLIF(d.created_at, ''))) / 30.0, 120.0), d.uri LIMIT :limit
);

// Every doc query above shares the same column projection. Picking one as
// the canonical column-index source lets Doc.fromRow look up by name
// instead of positional index — adding/removing/reordering columns is a
// compile error rather than a silent runtime miscount. The comptime
// assertion below catches the case where one of the queries drifts from
// the rest.
const DocQueries = .{
    DocsByTag,                            DocsByFtsAndTag,
    DocsByFts,                            DocsByFtsAndSince,
    DocsByFtsAndPlatform,                 DocsByFtsAndPlatformAndSince,
    DocsByTagAndPlatform,                 DocsByFtsAndTagAndPlatform,
    DocsByPlatform,                       DocsByAuthor,
    DocsByAuthorAndPlatform,              DocsByPubBasePath,
    DocsByPubBasePathAndPlatform,         DocsByPubBasePathAndSince,
    DocsByPubBasePathAndPlatformAndSince,
};

inline fn docCol(comptime name: []const u8) comptime_int {
    @setEvalBranchQuota(20000);
    const canonical = DocsByTag.columnIndex(name);
    inline for (DocQueries) |Q| {
        if (Q.columnIndex(name) != canonical) {
            @compileError("doc query column index drift for '" ++ name ++ "'");
        }
    }
    return canonical;
}

// Publication-side equivalent. Only one Pub query (PubSearch) today, so the
// helper just defers to its columnIndex; if we add more Pub query variants
// later, mirror the docCol drift-check pattern.
inline fn pubCol(comptime name: []const u8) comptime_int {
    return PubSearch.columnIndex(name);
}

/// Publication search result (internal)
const Pub = struct {
    uri: []const u8,
    did: []const u8,
    name: []const u8,
    snippet: []const u8,
    rkey: []const u8,
    basePath: []const u8,
    platform: []const u8,

    fn fromRow(row: db.Row) Pub {
        return .{
            .uri = row.text(pubCol("uri")),
            .did = row.text(pubCol("did")),
            .name = row.text(pubCol("name")),
            .snippet = row.text(pubCol("snippet")),
            .rkey = row.text(pubCol("rkey")),
            .basePath = row.text(pubCol("base_path")),
            .platform = row.text(pubCol("platform")),
        };
    }

    fn fromLocalRow(row: db.LocalDb.Row) Pub {
        // local-side pub SQL (in searchLocal) mirrors PubSearch's projection.
        return .{
            .uri = row.text(pubCol("uri")),
            .did = row.text(pubCol("did")),
            .name = row.text(pubCol("name")),
            .snippet = row.text(pubCol("snippet")),
            .rkey = row.text(pubCol("rkey")),
            .basePath = row.text(pubCol("base_path")),
            .platform = row.text(pubCol("platform")),
        };
    }

    fn toJson(self: Pub, alloc: Allocator) SearchResultJson {
        return .{
            .type = "publication",
            .uri = self.uri,
            .did = self.did,
            .title = self.name,
            .snippet = self.snippet,
            .rkey = self.rkey,
            .basePath = self.basePath,
            .platform = self.platform,
            .url = buildDocUrl(alloc, "publication", self.platform, self.basePath, "", self.rkey, self.did),
        };
    }
};

const PubSearch = zql.Query(
    \\SELECT f.uri, p.did, p.name,
    \\  snippet(publications_fts, 2, '', '', '...', 32) as snippet,
    \\  p.rkey, p.base_path, p.platform
    \\FROM publications_fts f
    \\JOIN publications p ON f.uri = p.uri
    \\WHERE publications_fts MATCH :query
    \\ORDER BY rank + (julianday('now') - julianday(p.created_at)) / 30.0, p.uri LIMIT :limit
);

pub fn search(alloc: Allocator, query: []const u8, tag_filter: ?[]const u8, platform_filter: ?[]const u8, since_filter: ?[]const u8, author_filter: ?[]const u8, mode: SearchMode, options: Options) ![]const u8 {
    // The visibility set has not loaded yet (brief startup window before the
    // replica seed lands). Every row would fail closed and we would return an
    // empty result set that looks exactly like "nothing matched" — the one
    // outcome a caller cannot distinguish from a real answer. Say so instead.
    if (!options.include_undiscoverable and !visibility.isLoaded()) {
        logfire.warn("search: visibility policy not loaded, refusing to serve", .{});
        logfire.counter("search.visibility_not_ready", 1);
        return error.VisibilityNotReady;
    }

    switch (effectiveMode(query, mode)) {
        .hybrid => return searchHybrid(alloc, query, tag_filter, platform_filter, since_filter, author_filter, options),
        .semantic => return searchSemantic(alloc, query, platform_filter, since_filter, author_filter, options),
        .keyword => return searchKeyword(alloc, query, tag_filter, platform_filter, since_filter, author_filter, options),
    }
}

/// Browse (no query text) is mode-independent: there is nothing to embed, so
/// semantic/hybrid would return [] even when author/tag/platform filters have
/// results — a filtered browse in the semantic UI tab looked like the author
/// had nothing indexed (2026-08-11). The MCP server fixed this client-side
/// (b11f1bd); the backend owns it for every client.
fn effectiveMode(query: []const u8, mode: SearchMode) SearchMode {
    if (query.len == 0) return .keyword;
    return mode;
}

fn includeDid(did: []const u8, show_labeled: bool) bool {
    return show_labeled or !classifier.isLabeledDid(did) or policy.isKept(did);
}

/// Doc belongs to a publication that opted out of discovery
/// (site.standard.publication preferences.showInDiscover=false).
///
/// This used to be a point lookup against the local replica keyed on the
/// document uri, which fails open in exactly the cases that matter: a
/// document indexed above the snapshot watermark, or a superseded rkey that
/// only turso and turbopuffer still carry, has no replica row — and "no row"
/// returned "not hidden". That shipped opted-out documents into anonymous
/// semantic results until the next snapshot happened to close it.
///
/// It now tests membership in the complete set of opted-out publications
/// (visibility.zig), keyed on identity every retrieval path already carries.
/// The set is complete even when the document index is not, so every row —
/// ghost, fresh, or replica-missing — gets a real answer, with no I/O.
fn isUndiscoverableDoc(did: []const u8, base_path: []const u8) bool {
    return visibility.isUndiscoverableDoc("", did, base_path);
}

/// Publication itself opted out of discovery.
fn isUndiscoverablePublication(uri: []const u8) bool {
    return visibility.isUndiscoverablePub(uri);
}

fn queryCandidateLimit(max_results: usize) usize {
    // Deduplication and policy filtering happen after SQL/ANN retrieval. Fetch
    // a bounded surplus so the requested visible prefix can still be filled.
    return @min(2000, max_results *| 3 +| 20);
}

/// Whether a doc's `created_at` satisfies an active `since` lower bound.
/// No filter → always passes. Empty created_at fails when a filter is active
/// (an unknown date can't be proven in-range — previously these leaked through).
/// Lexicographic compare is valid because both are ISO-8601: `created_at` is a
/// full timestamp, `since` a date prefix, so "2026-05-29T..." >= "2026-05-29".
fn passesSince(created_at: []const u8, since_filter: ?[]const u8) bool {
    const since = since_filter orelse return true;
    if (created_at.len == 0) return false;
    return std.mem.order(u8, created_at, since) != .lt;
}

test "passesSince" {
    // no active filter: everything passes, including empty dates
    try std.testing.expect(passesSince("", null));
    try std.testing.expect(passesSince("2020-01-01T00:00:00Z", null));

    const since: ?[]const u8 = "2026-05-29";
    try std.testing.expect(passesSince("2026-06-02T18:23:34.663Z", since)); // newer
    try std.testing.expect(passesSince("2026-05-29T00:00:00Z", since)); // same day, kept
    try std.testing.expect(!passesSince("2026-05-28T23:59:59Z", since)); // older, dropped
    try std.testing.expect(!passesSince("2025-04-05T18:15:48.435Z", since)); // much older
    try std.testing.expect(!passesSince("", since)); // unknown date dropped under filter
}

/// Check if we've already seen a result from the same author with the same title.
/// Used to collapse cross-platform duplicates (same content published to multiple ATProto apps).
fn isDuplicateAuthorTitle(seen: *std.StringHashMap(void), alloc: Allocator, did: []const u8, title: []const u8) !bool {
    if (did.len == 0 or title.len == 0) return false;
    const key = try std.fmt.allocPrint(alloc, "{s}\x00{s}", .{ did, title });
    const result = try seen.getOrPut(key);
    if (result.found_existing) {
        alloc.free(key);
        return true;
    }
    return false;
}

/// Inject an author DID condition into a SQL query's WHERE clause before ORDER BY.
/// Uses the (? = '' OR col = ?) pattern: no-op when author_val is empty.
fn addAuthorCondition(alloc: Allocator, stmt: db.Client.Statement, col: []const u8, author_val: []const u8) !db.Client.Statement {
    const order_idx = std.mem.indexOf(u8, stmt.sql, "ORDER BY") orelse return stmt;
    const new_sql = try std.fmt.allocPrint(alloc, "{s}AND (? = '' OR {s} = ?) {s}", .{ stmt.sql[0..order_idx], col, stmt.sql[order_idx..] });
    const new_args = try alloc.alloc([]const u8, stmt.args.len + 2);
    // Every paginated search statement has LIMIT ? as its final bind. The
    // injected author placeholders occur before ORDER BY/LIMIT, so insert the
    // author values immediately before that final limit value as well.
    const before_limit = stmt.args.len - 1;
    @memcpy(new_args[0..before_limit], stmt.args[0..before_limit]);
    new_args[before_limit] = author_val;
    new_args[before_limit + 1] = author_val;
    new_args[before_limit + 2] = stmt.args[before_limit];
    return .{ .sql = new_sql, .args = new_args };
}

/// Inject bridgy fed exclusion into a SQL query's WHERE clause before ORDER BY.
/// Excludes documents where is_bridgyfed = 1 (bridgy fed content).
fn addBridgyFedExclusion(alloc: Allocator, stmt: db.Client.Statement) !db.Client.Statement {
    const order_idx = std.mem.indexOf(u8, stmt.sql, "ORDER BY") orelse return stmt;
    const new_sql = try std.fmt.allocPrint(alloc, "{s}AND (d.is_bridgyfed IS NULL OR d.is_bridgyfed = 0) AND (d.url_dead IS NULL OR d.url_dead = 0) {s}", .{ stmt.sql[0..order_idx], stmt.sql[order_idx..] });
    return .{ .sql = new_sql, .args = stmt.args };
}

/// Check if a URI is from bridgy fed by looking up is_bridgyfed in local SQLite.
/// Overlay first: a doc newer than the snapshot only exists there, and a
/// tombstoned doc must be excluded from semantic results too.
fn isBridgyFed(uri: []const u8) bool {
    if (db.getOverlay()) |o| {
        var orows = o.query("SELECT is_bridgyfed, deleted FROM documents_overlay WHERE uri = ?", .{uri}) catch null;
        if (orows) |*r| {
            defer r.deinit();
            if (r.next()) |row| return row.int(0) != 0 or row.int(1) != 0;
        }
    }
    const local = db.getLocalDb() orelse return false;
    var rows = local.query(
        "SELECT is_bridgyfed FROM documents WHERE uri = ?",
        .{uri},
    ) catch return false;
    defer rows.deinit();
    if (rows.next()) |row| {
        return row.int(0) != 0;
    }
    return false;
}

// --- Turso keyword-fallback guard ------------------------------------------
//
// Keyword search serves from the local replica (~0.2ms). During the brief
// window where the replica is not ready — process boot, snapshot adoption — it
// falls back to Turso. That fallback is the failure amplifier: a traffic burst
// during the unready window stampedes Turso with concurrent full-corpus FTS
// batches that contend and self-amplify to tens of seconds (the 2026-06-24
// storm: ~38 concurrent fallbacks, each ~26s; a lone fallback is ~1-2s). Two
// guards keep a momentary blip from becoming an outage:
//   1. wait briefly for the replica to become ready (the window is usually
//      sub-second) so most searches still take the fast local path, then
//   2. cap concurrent Turso fallbacks, shedding over-cap searches with a fast
//      empty result instead of piling onto a saturated Turso.

/// Max in-flight Turso keyword fallbacks. Deliberately small: the fallback
/// exists to cover a brief unready window, not sustained load, and Turso FTS
/// concurrency is itself what makes each query slow — admitting more is
/// counterproductive.
const TURSO_FALLBACK_CAP: u32 = 4;

/// How long an unready search waits for the replica before giving up on the
/// fast path. The unready window is typically sub-second, so a short wait turns
/// "stampede Turso" into "hit the local replica a beat later" for most requests.
const LOCAL_READY_WAIT_MS: u64 = 400;
const LOCAL_READY_POLL_MS: u64 = 20;

var turso_fallback_inflight: std.atomic.Value(u32) = std.atomic.Value(u32).init(0);

/// Return the replica once ready, waiting up to LOCAL_READY_WAIT_MS. Null if it
/// stays unready (or no replica object exists yet). Polls via the LocalDb's own
/// io so we needn't thread io through the search call chain.
fn waitForLocalReady() ?*db.LocalDb {
    if (db.getLocalDb()) |l| return l;
    const raw = db.getLocalDbRaw() orelse return null; // no replica object yet
    var waited: u64 = 0;
    while (waited < LOCAL_READY_WAIT_MS) {
        raw.io.sleep(std.Io.Duration.fromMilliseconds(LOCAL_READY_POLL_MS), .awake) catch {};
        if (db.getLocalDb()) |l| return l;
        waited += LOCAL_READY_POLL_MS;
    }
    return null;
}

/// Non-blocking acquire of a Turso-fallback slot. False when the cap is already
/// saturated (caller should shed). May briefly over-admit by one under race —
/// harmless at this cap.
fn tryAcquireTursoSlot() bool {
    const prev = turso_fallback_inflight.fetchAdd(1, .acq_rel);
    if (prev >= TURSO_FALLBACK_CAP) {
        _ = turso_fallback_inflight.fetchSub(1, .acq_rel);
        return false;
    }
    return true;
}

fn releaseTursoSlot() void {
    _ = turso_fallback_inflight.fetchSub(1, .acq_rel);
}

test "turso fallback cap admits up to N, sheds beyond, and frees on release" {
    turso_fallback_inflight.store(0, .release);
    defer turso_fallback_inflight.store(0, .release);

    var i: u32 = 0;
    while (i < TURSO_FALLBACK_CAP) : (i += 1) {
        try std.testing.expect(tryAcquireTursoSlot());
    }
    // cap reached → shed (and the failed acquire must not leak a slot)
    try std.testing.expect(!tryAcquireTursoSlot());
    try std.testing.expectEqual(TURSO_FALLBACK_CAP, turso_fallback_inflight.load(.acquire));
    // release one → a slot frees up again
    releaseTursoSlot();
    try std.testing.expect(tryAcquireTursoSlot());
}

/// Keyword search: FTS5 via local SQLite, with a bounded Turso fallback.
fn searchKeyword(alloc: Allocator, query: []const u8, tag_filter: ?[]const u8, platform_filter: ?[]const u8, since_filter: ?[]const u8, author_filter: ?[]const u8, options: Options) ![]const u8 {
    // try local SQLite first (faster for FTS queries). If the replica is
    // briefly not ready (boot / snapshot adoption), wait a short beat for it
    // rather than immediately stampeding turso.
    if (waitForLocalReady()) |local| {
        if (searchLocal(alloc, local, query, tag_filter, platform_filter, since_filter, author_filter, options)) |result| {
            logfire.info("search.local hit", .{});
            return result;
        } else |err| {
            logfire.warn("search.local failed, falling back to turso: {s}", .{@errorName(err)});
        }
    } else {
        logfire.warn("search.local unavailable (not ready), falling back to turso", .{});
    }

    // Bounded Turso fallback: shed over-cap searches fast so a not-ready window
    // plus a traffic burst can't stampede Turso into a multi-minute stall.
    if (!tryAcquireTursoSlot()) {
        logfire.counter("search.fallback_shed", 1);
        logfire.warn("search.turso fallback shed: cap {d} saturated", .{TURSO_FALLBACK_CAP});
        return alloc.dupe(u8, "[]");
    }
    defer releaseTursoSlot();

    // fall back to Turso
    logfire.info("search.turso fallback", .{});
    const c = db.getClient() orelse return error.NotInitialized;

    var output: std.Io.Writer.Allocating = .init(alloc);
    errdefer output.deinit();

    var jw: json.Stringify = .{ .writer = &output.writer };
    try jw.beginArray();

    const fts_query = try buildFtsQuery(alloc, query);
    const has_query = query.len > 0;
    const has_tag = tag_filter != null;
    const has_platform = platform_filter != null;
    const has_since = since_filter != null;
    const candidate_limit = queryCandidateLimit(options.max_results);
    const candidate_limit_str = try std.fmt.allocPrint(alloc, "{d}", .{candidate_limit});
    var result_count: usize = 0;

    // track seen URIs for deduplication (content match + base_path match)
    var seen_uris = std.StringHashMap(void).init(alloc);
    defer seen_uris.deinit();

    // track seen (did, title) pairs for cross-platform dedup
    var seen_authors = std.StringHashMap(void).init(alloc);
    defer seen_authors.deinit();

    // author-only browse: no FTS query needed, just fetch by DID
    if (author_filter != null and !has_query and !has_tag) {
        if (has_platform) {
            var res = c.query(DocsByAuthorAndPlatform.positional, &.{ author_filter.?, platform_filter.?, candidate_limit_str }) catch {
                try jw.endArray();
                return try output.toOwnedSlice();
            };
            defer res.deinit();
            for (res.rows) |row| {
                const doc = Doc.fromRow(row);
                if (!passesSince(doc.createdAt, since_filter)) continue;
                if (!includeDid(doc.did, options.show_labeled)) continue;
                if (!options.include_undiscoverable and isUndiscoverableDoc(doc.did, doc.basePath)) continue;
                if (try isDuplicateAuthorTitle(&seen_authors, alloc, doc.did, doc.title)) continue;
                try jw.write(doc.toJson(alloc));
                result_count += 1;
                if (result_count >= options.max_results) break;
            }
        } else {
            var res = c.query(DocsByAuthor.positional, &.{ author_filter.?, candidate_limit_str }) catch {
                try jw.endArray();
                return try output.toOwnedSlice();
            };
            defer res.deinit();
            for (res.rows) |row| {
                const doc = Doc.fromRow(row);
                if (!passesSince(doc.createdAt, since_filter)) continue;
                if (!includeDid(doc.did, options.show_labeled)) continue;
                if (!options.include_undiscoverable and isUndiscoverableDoc(doc.did, doc.basePath)) continue;
                if (try isDuplicateAuthorTitle(&seen_authors, alloc, doc.did, doc.title)) continue;
                try jw.write(doc.toJson(alloc));
                result_count += 1;
                if (result_count >= options.max_results) break;
            }
        }
        try jw.endArray();
        return try output.toOwnedSlice();
    }

    // build batch of queries to execute in single HTTP request
    var statements: [3]db.Client.Statement = undefined;
    var stmt_count: usize = 0;

    // author condition: inject into SQL so LIMIT applies after author filtering
    const author_val: []const u8 = if (author_filter) |af| af else "";

    // query 0: documents by content (always present if we have any filter)
    const doc_sql = getDocQuerySql(has_query, has_tag, has_platform, has_since);
    const doc_args = try getDocQueryArgs(alloc, fts_query, tag_filter, platform_filter, since_filter, has_query, has_tag, has_platform, has_since, candidate_limit_str);
    if (doc_sql) |sql| {
        statements[stmt_count] = try addBridgyFedExclusion(alloc, try addAuthorCondition(alloc, .{ .sql = sql, .args = doc_args }, "d.did", author_val));
        stmt_count += 1;
    }

    // query 1: documents by publication base_path (subdomain search)
    const run_basepath = has_query and !has_tag;
    if (run_basepath) {
        var base_stmt: db.Client.Statement = undefined;
        if (has_platform and has_since) {
            base_stmt = .{ .sql = DocsByPubBasePathAndPlatformAndSince.positional, .args = &.{ fts_query, platform_filter.?, since_filter.?, candidate_limit_str } };
        } else if (has_platform) {
            base_stmt = .{ .sql = DocsByPubBasePathAndPlatform.positional, .args = &.{ fts_query, platform_filter.?, candidate_limit_str } };
        } else if (has_since) {
            base_stmt = .{ .sql = DocsByPubBasePathAndSince.positional, .args = &.{ fts_query, since_filter.?, candidate_limit_str } };
        } else {
            base_stmt = .{ .sql = DocsByPubBasePath.positional, .args = &.{ fts_query, candidate_limit_str } };
        }
        statements[stmt_count] = try addBridgyFedExclusion(alloc, try addAuthorCondition(alloc, base_stmt, "d.did", author_val));
        stmt_count += 1;
    }

    // query 2: publications (only when no tag/platform filter)
    // Publications carry no post-date (the local replica omits created_at), so
    // they can't honor a date bound. Under an active date filter, suppress them
    // rather than leaking undated publication shells into a recency-bounded view.
    const run_pubs = tag_filter == null and platform_filter == null and has_query and !has_since;
    if (run_pubs) {
        statements[stmt_count] = try addAuthorCondition(alloc, .{ .sql = PubSearch.positional, .args = &.{ fts_query, candidate_limit_str } }, "p.did", author_val);
        stmt_count += 1;
    }

    if (stmt_count == 0) {
        try jw.endArray();
        return try output.toOwnedSlice();
    }

    // execute all queries in single HTTP request
    var batch = c.queryBatch(statements[0..stmt_count]) catch {
        try jw.endArray();
        return try output.toOwnedSlice();
    };
    defer batch.deinit();

    // process query 0: document content results
    var query_idx: usize = 0;
    if (doc_sql != null) {
        for (batch.get(query_idx)) |row| {
            if (result_count >= options.max_results) break;
            const doc = Doc.fromRow(row);
            if (author_filter) |af| {
                if (!std.mem.eql(u8, doc.did, af)) continue;
            }
            if (!includeDid(doc.did, options.show_labeled)) continue;
            if (!options.include_undiscoverable and isUndiscoverableDoc(doc.did, doc.basePath)) continue;
            if (try isDuplicateAuthorTitle(&seen_authors, alloc, doc.did, doc.title)) continue;
            const uri_dupe = try alloc.dupe(u8, doc.uri);
            try seen_uris.put(uri_dupe, {});
            try jw.write(doc.toJson(alloc));
            result_count += 1;
        }
        query_idx += 1;
    }

    // process query 1: base_path results (deduplicated)
    if (run_basepath) {
        for (batch.get(query_idx)) |row| {
            if (result_count >= options.max_results) break;
            const doc = Doc.fromRow(row);
            if (author_filter) |af| {
                if (!std.mem.eql(u8, doc.did, af)) continue;
            }
            if (!includeDid(doc.did, options.show_labeled)) continue;
            if (!options.include_undiscoverable and isUndiscoverableDoc(doc.did, doc.basePath)) continue;
            if (!seen_uris.contains(doc.uri) and !try isDuplicateAuthorTitle(&seen_authors, alloc, doc.did, doc.title)) {
                try jw.write(doc.toJson(alloc));
                result_count += 1;
            }
        }
        query_idx += 1;
    }

    // process query 2: publication results
    if (run_pubs) {
        for (batch.get(query_idx)) |row| {
            if (result_count >= options.max_results) break;
            const pub_result = Pub.fromRow(row);
            if (author_filter) |af| {
                if (!std.mem.eql(u8, pub_result.did, af)) continue;
            }
            if (!includeDid(pub_result.did, options.show_labeled)) continue;
            if (!options.include_undiscoverable and isUndiscoverablePublication(pub_result.uri)) continue;
            try jw.write(pub_result.toJson(alloc));
            result_count += 1;
        }
    }

    try jw.endArray();
    return try output.toOwnedSlice();
}

/// Local SQLite search (FTS queries only, no vector similarity)
/// Simplified version - just handles basic FTS query case to get started
const LOCAL_DOC_RECENCY_ORDER = "ORDER BY rank + COALESCE((julianday('now') - julianday(NULLIF(d.created_at, ''))) / 30.0, 120.0), d.uri LIMIT ?";

fn withLocalDocRecencyOrder(comptime sql: []const u8) []const u8 {
    return sql ++ "\n" ++ LOCAL_DOC_RECENCY_ORDER;
}

// Two-phase keyword search over document content. The inner pass ranks every
// FTS match (rank + recency + policy flags) and keeps the top candidate_limit
// rowids; the outer pass re-runs the MATCH but computes snippet() and reads
// full document rows only for rows in that set. One-phase sorted the full
// SELECT (snippet included) for every match, which tokenizes each matching
// document's whole body — 14s for 'atproto' on the prod replica (2026-08-11).
//
// All joins are on rowid: documents_fts is external-content over documents
// (schema v5) so fts rowids ARE documents rowids, and reading f.uri from an
// external-content table is itself a content-table probe per match. The v5
// documents table stores `content` LAST, so the inner pass's rowid probes for
// created_at/policy flags never read past a document body — that column-order
// guarantee replaced the covering index (pre-v5 snapshots wrote fts rows keyed
// to documents.rowid too, so these joins stay correct during the upgrade
// window; they're just slower until the first v5 snapshot adopts).
fn localDocsFtsSql(comptime inner_extra_cond: []const u8) []const u8 {
    return
    \\SELECT d.uri, d.did, d.title,
    \\  snippet(documents_fts, 2, '', '', '...', 32) as snippet,
    \\  d.created_at, d.rkey, d.base_path, d.has_publication,
    \\  d.platform, COALESCE(d.path, '') as path,
    \\  COALESCE(d.cover_image, '') as cover_image,
    \\  COALESCE(p.name, '') as publication_name,
    \\  rank + COALESCE((julianday('now') - julianday(NULLIF(d.created_at, ''))) / 30.0, 120.0) AS merge_score
    \\FROM documents_fts f
    \\JOIN documents d ON d.rowid = f.rowid
    \\LEFT JOIN publications p ON d.publication_uri = p.uri
    \\WHERE documents_fts MATCH ? AND f.rowid IN (
    \\  SELECT rid FROM (
    \\    SELECT f2.rowid AS rid,
    \\      f2.rank + COALESCE((julianday('now') - julianday(NULLIF(d2.created_at, ''))) / 30.0, 120.0) AS score
    \\    FROM documents_fts f2
    \\    JOIN documents d2 ON d2.rowid = f2.rowid
    \\    WHERE f2.documents_fts MATCH ?
    ++ inner_extra_cond ++ "\n" ++
        \\    AND (? = '' OR d2.did = ?)
        \\    AND (? = '' OR d2.created_at >= ?)
        \\    AND (d2.is_bridgyfed IS NULL OR d2.is_bridgyfed = 0) AND (d2.url_dead IS NULL OR d2.url_dead = 0)
        \\    ORDER BY score, f2.rowid LIMIT ?
        \\  )
        \\)
        \\ORDER BY rank + COALESCE((julianday('now') - julianday(NULLIF(d.created_at, ''))) / 30.0, 120.0), d.uri LIMIT ?
    ;
}

const LOCAL_DOCS_FTS_SQL = localDocsFtsSql("");
const LOCAL_DOCS_FTS_PLATFORM_SQL = localDocsFtsSql("\n    AND d2.platform = ?");

/// Bounded candidate pass for unfiltered searches. The two-phase query above
/// still probes the covering index once per FTS match, which for common words
/// is corpus-proportional ("what" = 24.6k of 71k docs; 7.4s in prod,
/// 2026-08-13). Phase 0 here lets FTS5 rank all matches by bm25 entirely
/// inside its own index (posting-list scan, no per-row table probes) and
/// keeps a generous top-K; only that bounded set gets covering-index probes for
/// the recency + policy re-rank. Trade-off: recency can only promote docs
/// out of the bm25 top-K, so a stale-but-fresh doc below it is missed — K is
/// 24x the final candidate set, and docs newer than the snapshot come from
/// the overlay regardless. Used only when author/since/platform are empty
/// (filters must see the full match set; filtered queries keep the old shape).
const CANDIDATE_PREFILTER_K: usize = 2000;

const LOCAL_DOCS_FTS_PREFILTER_SQL =
    \\SELECT d.uri, d.did, d.title,
    \\  snippet(documents_fts, 2, '', '', '...', 32) as snippet,
    \\  d.created_at, d.rkey, d.base_path, d.has_publication,
    \\  d.platform, COALESCE(d.path, '') as path,
    \\  COALESCE(d.cover_image, '') as cover_image,
    \\  COALESCE(p.name, '') as publication_name,
    \\  rank + COALESCE((julianday('now') - julianday(NULLIF(d.created_at, ''))) / 30.0, 120.0) AS merge_score
    \\FROM documents_fts f
    \\JOIN documents d ON d.rowid = f.rowid
    \\LEFT JOIN publications p ON d.publication_uri = p.uri
    \\WHERE documents_fts MATCH ? AND f.rowid IN (
    \\  SELECT rid FROM (
    \\    SELECT c.rid AS rid,
    \\      c.rank + COALESCE((julianday('now') - julianday(NULLIF(d2.created_at, ''))) / 30.0, 120.0) AS score
    \\    FROM (SELECT rowid AS rid, rank FROM documents_fts WHERE documents_fts MATCH ? ORDER BY rank LIMIT ?) c
    \\    JOIN documents d2 ON d2.rowid = c.rid
    \\    WHERE (d2.is_bridgyfed IS NULL OR d2.is_bridgyfed = 0) AND (d2.url_dead IS NULL OR d2.url_dead = 0)
    \\    ORDER BY score, c.rid LIMIT ?
    \\  )
    \\)
    \\ORDER BY rank + COALESCE((julianday('now') - julianday(NULLIF(d.created_at, ''))) / 30.0, 120.0), d.uri LIMIT ?
;

// Tag browse (no FTS text): fully local — document_tags and recommends both
// live in the replica. Ranked by recency with a recommendation lift: score is
// months-old minus RECOMMEND_LIFT·ln(1+recommenders), so one recommend
// outranks ~2 months of freshness and heavily-recommended docs sort among
// themselves, while the ~98% of docs with no recommends keep their
// newest-first order. recommend_counts is materialized once per boot
// (LocalDb.createSchema) — a PK-probe join here instead of aggregating all
// of recommends per request, which was the last O(corpus) request path.
const RECOMMEND_LIFT = "3.0"; // months of freshness one e-fold of recommenders is worth
const LOCAL_TAG_BROWSE_SQL =
    \\SELECT d.uri, d.did, d.title, '' as snippet,
    \\  d.created_at, d.rkey, d.base_path, d.has_publication,
    \\  d.platform, COALESCE(d.path, '') as path,
    \\  COALESCE(d.cover_image, '') as cover_image,
    \\  COALESCE(p.name, '') as publication_name,
    \\  COALESCE((julianday('now') - julianday(NULLIF(d.created_at, ''))) / 30.0, 120.0)
    \\  -
++ RECOMMEND_LIFT ++
    \\ * ln(1 + COALESCE(r.rc, 0)) AS merge_score
    \\FROM document_tags dt
    \\JOIN documents d ON d.uri = dt.document_uri
    \\LEFT JOIN publications p ON d.publication_uri = p.uri
    \\LEFT JOIN recommend_counts r ON r.document_uri = d.uri
    \\WHERE dt.tag = ? AND (? = '' OR d.platform = ?) AND (? = '' OR d.did = ?)
    \\AND (? = '' OR d.created_at >= ?)
    \\AND (d.is_bridgyfed IS NULL OR d.is_bridgyfed = 0) AND (d.url_dead IS NULL OR d.url_dead = 0)
    \\ORDER BY COALESCE((julianday('now') - julianday(NULLIF(d.created_at, ''))) / 30.0, 120.0)
    \\  -
++ RECOMMEND_LIFT ++
    \\ * ln(1 + COALESCE(r.rc, 0)), d.uri
    \\LIMIT ?
;

// FTS text within a tag: the standard BM25 + recency ranking (text relevance
// stays primary; the recommend lift is the tag-BROWSE ranking), restricted to
// the tag's documents. The tag restriction is an IN subquery, NOT a join —
// a join let SQLite drive from the tag index and probe FTS per row, which
// took ~8s on the real corpus (2026-08-04). The FTS index must drive.
const LOCAL_TAG_FTS_SQL = withLocalDocRecencyOrder(
    \\SELECT d.uri, d.did, d.title,
    \\  snippet(documents_fts, 2, '', '', '...', 32) as snippet,
    \\  d.created_at, d.rkey, d.base_path, d.has_publication,
    \\  d.platform, COALESCE(d.path, '') as path,
    \\  COALESCE(d.cover_image, '') as cover_image,
    \\  COALESCE(p.name, '') as publication_name,
    \\  rank + COALESCE((julianday('now') - julianday(NULLIF(d.created_at, ''))) / 30.0, 120.0) AS merge_score
    \\FROM documents_fts f
    \\JOIN documents d ON d.rowid = f.rowid
    \\LEFT JOIN publications p ON d.publication_uri = p.uri
    \\WHERE documents_fts MATCH ?
    \\AND d.uri IN (SELECT document_uri FROM document_tags WHERE tag = ?)
    \\AND (? = '' OR d.platform = ?) AND (? = '' OR d.did = ?)
    \\AND (? = '' OR d.created_at >= ?)
    \\AND (d.is_bridgyfed IS NULL OR d.is_bridgyfed = 0) AND (d.url_dead IS NULL OR d.url_dead = 0)
);

// ---------------------------------------------------------------------------
// Live-overlay merge (docs/scaling-plan.md, typeahead's snapshot+overlay
// design). The overlay holds docs newer than the adopted snapshot's watermark;
// serving interleaves overlay hits with snapshot hits on the SAME score
// expression (bm25 rank + recency). BM25 from two FTS tables is not one scale
// in general, but ranking is recency-dominant and overlay rows are at most one
// build-cadence old, so recency dominates exactly where it must. Every column
// index matches docCol's projection; merge_score rides as a 13th column.
const DOC_MERGE_SCORE_COL = 12;

const OVERLAY_DOCS_FTS_SQL =
    \\SELECT d.uri, d.did, d.title,
    \\  snippet(overlay_fts, 2, '', '', '...', 32) as snippet,
    \\  d.created_at, d.rkey, d.base_path, d.has_publication,
    \\  d.platform, COALESCE(d.path, '') as path,
    \\  COALESCE(d.cover_image, '') as cover_image,
    \\  COALESCE(d.publication_name, '') as publication_name,
    \\  rank + COALESCE((julianday('now') - julianday(NULLIF(d.created_at, ''))) / 30.0, 120.0) AS merge_score
    \\FROM overlay_fts f
    \\JOIN documents_overlay d ON f.uri = d.uri
    \\WHERE overlay_fts MATCH ? AND d.deleted = 0
    \\AND (? = '' OR d.platform = ?)
    \\AND (? = '' OR d.did = ?)
    \\AND (? = '' OR d.created_at >= ?)
    \\AND (d.is_bridgyfed IS NULL OR d.is_bridgyfed = 0)
    \\ORDER BY merge_score, d.uri LIMIT ?
;

// overlay docs have no recommend_counts (snapshot-side materialization);
// rc=0 keeps the same formula with a zero lift, which is exact for the
// overwhelmingly-common case (fresh docs have no recommends yet)
const OVERLAY_TAG_BROWSE_SQL =
    \\SELECT d.uri, d.did, d.title, '' as snippet,
    \\  d.created_at, d.rkey, d.base_path, d.has_publication,
    \\  d.platform, COALESCE(d.path, '') as path,
    \\  COALESCE(d.cover_image, '') as cover_image,
    \\  COALESCE(d.publication_name, '') as publication_name,
    \\  COALESCE((julianday('now') - julianday(NULLIF(d.created_at, ''))) / 30.0, 120.0) AS merge_score
    \\FROM overlay_doc_tags t
    \\JOIN documents_overlay d ON d.uri = t.uri
    \\WHERE t.tag = ? AND d.deleted = 0
    \\AND (? = '' OR d.platform = ?) AND (? = '' OR d.did = ?)
    \\AND (? = '' OR d.created_at >= ?)
    \\AND (d.is_bridgyfed IS NULL OR d.is_bridgyfed = 0)
    \\ORDER BY merge_score, d.uri LIMIT ?
;

const OVERLAY_TAG_FTS_SQL =
    \\SELECT d.uri, d.did, d.title,
    \\  snippet(overlay_fts, 2, '', '', '...', 32) as snippet,
    \\  d.created_at, d.rkey, d.base_path, d.has_publication,
    \\  d.platform, COALESCE(d.path, '') as path,
    \\  COALESCE(d.cover_image, '') as cover_image,
    \\  COALESCE(d.publication_name, '') as publication_name,
    \\  rank + COALESCE((julianday('now') - julianday(NULLIF(d.created_at, ''))) / 30.0, 120.0) AS merge_score
    \\FROM overlay_fts f
    \\JOIN documents_overlay d ON f.uri = d.uri
    \\WHERE overlay_fts MATCH ?
    \\AND d.uri IN (SELECT uri FROM overlay_doc_tags WHERE tag = ?)
    \\AND (? = '' OR d.platform = ?) AND (? = '' OR d.did = ?)
    \\AND (? = '' OR d.created_at >= ?)
    \\AND (d.is_bridgyfed IS NULL OR d.is_bridgyfed = 0)
    \\ORDER BY merge_score, d.uri LIMIT ?
;

const OVERLAY_HIT_CAP: usize = 50;

const OverlayHit = struct {
    doc: Doc,
    score: f64,
};

/// Materialized overlay contribution to one search: score-ascending hits plus
/// a suppression set of EVERY overlay uri (live rows shadow their snapshot
/// versions — the overlay copy is at least as new; tombstones suppress
/// outright). Materialized so no overlay read connection is held while the
/// snapshot rows stream.
const OverlaySlice = struct {
    hits: std.ArrayList(OverlayHit) = .empty,
    suppress: std.StringHashMap(void),
    next: usize = 0,

    fn init(alloc: Allocator) OverlaySlice {
        return .{ .suppress = std.StringHashMap(void).init(alloc) };
    }

    fn suppresses(self: *const OverlaySlice, uri: []const u8) bool {
        return self.suppress.contains(uri);
    }

    /// Emit every not-yet-emitted overlay hit ranked at or above `score`
    /// (lower = better), running the same policy/dedup gates snapshot rows go
    /// through. Pass -inf... = math.inf(f64) to drain at the end.
    fn emitUpTo(
        self: *OverlaySlice,
        score: f64,
        alloc: Allocator,
        jw: *json.Stringify,
        seen_uris: *std.StringHashMap(void),
        seen_authors: *std.StringHashMap(void),
        result_count: *usize,
        options: Options,
    ) !void {
        while (self.next < self.hits.items.len) {
            const hit = self.hits.items[self.next];
            if (hit.score > score) return;
            self.next += 1;
            if (result_count.* >= options.max_results) return;
            const doc = hit.doc;
            if (seen_uris.contains(doc.uri)) continue;
            if (!includeDid(doc.did, options.show_labeled)) continue;
            if (!options.include_undiscoverable and isUndiscoverableDoc(doc.did, doc.basePath)) continue;
            if (try isDuplicateAuthorTitle(seen_authors, alloc, doc.did, doc.title)) continue;
            try seen_uris.put(doc.uri, {});
            try jw.write(doc.toJson(alloc));
            result_count.* += 1;
        }
    }
};

fn dupeDocFromOverlayRow(alloc: Allocator, row: db.OverlayDb.Row) !Doc {
    return .{
        .uri = try alloc.dupe(u8, row.text(docCol("uri"))),
        .did = try alloc.dupe(u8, row.text(docCol("did"))),
        .title = try alloc.dupe(u8, row.text(docCol("title"))),
        .snippet = try alloc.dupe(u8, row.text(docCol("snippet"))),
        .createdAt = try alloc.dupe(u8, row.text(docCol("created_at"))),
        .rkey = try alloc.dupe(u8, row.text(docCol("rkey"))),
        .basePath = try alloc.dupe(u8, row.text(docCol("base_path"))),
        .hasPublication = row.int(docCol("has_publication")) != 0,
        .platform = try alloc.dupe(u8, row.text(docCol("platform"))),
        .path = try alloc.dupe(u8, row.text(docCol("path"))),
        .coverImage = try alloc.dupe(u8, row.text(docCol("cover_image"))),
        .publicationName = try alloc.dupe(u8, row.text(docCol("publication_name"))),
    };
}

/// Fetch the overlay's contribution for one search. Any failure returns what
/// was gathered so far — overlay trouble must degrade to snapshot-only
/// serving, never break search.
fn fetchOverlaySlice(alloc: Allocator, comptime sql: []const u8, args: anytype) OverlaySlice {
    var slice = OverlaySlice.init(alloc);
    const o = db.getOverlay() orelse return slice;

    blk: {
        var rows = o.query(sql, args) catch break :blk;
        defer rows.deinit();
        while (rows.next()) |row| {
            const doc = dupeDocFromOverlayRow(alloc, row) catch break :blk;
            slice.hits.append(alloc, .{ .doc = doc, .score = row.float(DOC_MERGE_SCORE_COL) }) catch break :blk;
            slice.suppress.put(doc.uri, {}) catch break :blk;
        }
    }
    blk: {
        var rows = o.query("SELECT uri FROM documents_overlay WHERE deleted = 1", .{}) catch break :blk;
        defer rows.deinit();
        while (rows.next()) |row| {
            const uri = alloc.dupe(u8, row.text(0)) catch break :blk;
            slice.suppress.put(uri, {}) catch break :blk;
        }
    }
    return slice;
}

fn overlayEnabled(options: Options) bool {
    return options.use_overlay and db.getOverlay() != null;
}

fn searchLocalTag(alloc: Allocator, local: *db.LocalDb, query: []const u8, tag: []const u8, platform_filter: ?[]const u8, since_filter: ?[]const u8, author_filter: ?[]const u8, options: Options) ![]const u8 {
    const platform_val: []const u8 = platform_filter orelse "";
    const author_val: []const u8 = author_filter orelse "";
    const since_val: []const u8 = since_filter orelse "";

    var output: std.Io.Writer.Allocating = .init(alloc);
    errdefer output.deinit();
    var jw: json.Stringify = .{ .writer = &output.writer };
    try jw.beginArray();

    var seen_authors = std.StringHashMap(void).init(alloc);
    defer seen_authors.deinit();

    var seen_uris = std.StringHashMap(void).init(alloc);
    defer seen_uris.deinit();

    var result_count: usize = 0;
    const candidate_limit = queryCandidateLimit(options.max_results);

    // overlay contribution first (materialized), so snapshot rows can stream
    var ov = if (!overlayEnabled(options))
        OverlaySlice.init(alloc)
    else if (query.len == 0)
        fetchOverlaySlice(alloc, OVERLAY_TAG_BROWSE_SQL, .{
            tag,       platform_val, platform_val, author_val,
            author_val, since_val,   since_val,    OVERLAY_HIT_CAP,
        })
    else blk: {
        const fts_query = try buildFtsQuery(alloc, query);
        break :blk fetchOverlaySlice(alloc, OVERLAY_TAG_FTS_SQL, .{
            fts_query, tag,        platform_val, platform_val,
            author_val, author_val, since_val,   since_val,
            OVERLAY_HIT_CAP,
        });
    };

    var rows = if (query.len == 0)
        try local.query(LOCAL_TAG_BROWSE_SQL, .{
            tag,        platform_val,    platform_val,
            author_val, author_val,      since_val,
            since_val,  candidate_limit,
        })
    else blk: {
        const fts_query = try buildFtsQuery(alloc, query);
        break :blk try local.query(LOCAL_TAG_FTS_SQL, .{
            fts_query,       tag,        platform_val, platform_val,
            author_val,      author_val, since_val,    since_val,
            candidate_limit,
        });
    };
    defer rows.deinit();
    while (rows.next()) |row| {
        if (result_count >= options.max_results) break;
        try ov.emitUpTo(row.float(DOC_MERGE_SCORE_COL), alloc, &jw, &seen_uris, &seen_authors, &result_count, options);
        if (result_count >= options.max_results) break;
        const doc = Doc.fromLocalRow(row);
        if (ov.suppresses(doc.uri)) continue; // overlay shadows or tombstones
        if (seen_uris.contains(doc.uri)) continue;
        if (!includeDid(doc.did, options.show_labeled)) continue;
        if (!options.include_undiscoverable and isUndiscoverableDoc(doc.did, doc.basePath)) continue;
        if (try isDuplicateAuthorTitle(&seen_authors, alloc, doc.did, doc.title)) continue;
        try seen_uris.put(try alloc.dupe(u8, doc.uri), {});
        try jw.write(doc.toJson(alloc));
        result_count += 1;
    }
    try ov.emitUpTo(std.math.inf(f64), alloc, &jw, &seen_uris, &seen_authors, &result_count, options);

    try jw.endArray();
    return try output.toOwnedSlice();
}

fn searchLocal(alloc: Allocator, local: *db.LocalDb, query: []const u8, tag_filter: ?[]const u8, platform_filter: ?[]const u8, since_filter: ?[]const u8, author_filter: ?[]const u8, options: Options) ![]const u8 {
    // only handle basic FTS queries for now (most common case)
    // fall back to Turso for complex filter combinations and author-only browse
    if (tag_filter) |tag| {
        return searchLocalTag(alloc, local, query, tag, platform_filter, since_filter, author_filter, options);
    }
    if (query.len == 0) {
        return error.UnsupportedQuery;
    }

    // author condition: pass DID or "" (empty = no-op via SQL "? = '' OR d.did = ?")
    const author_val: []const u8 = if (author_filter) |af| af else "";
    const since_val: []const u8 = if (since_filter) |since| since else "";

    var output: std.Io.Writer.Allocating = .init(alloc);
    errdefer output.deinit();

    var jw: json.Stringify = .{ .writer = &output.writer };
    try jw.beginArray();

    const fts_query = try buildFtsQuery(alloc, query);
    const candidate_limit = queryCandidateLimit(options.max_results);
    var result_count: usize = 0;

    // track seen URIs for deduplication
    var seen_uris = std.StringHashMap(void).init(alloc);
    defer seen_uris.deinit();

    // track seen (did, title) pairs for cross-platform dedup
    var seen_authors = std.StringHashMap(void).init(alloc);
    defer seen_authors.deinit();

    // live-overlay contribution (docs newer than the adopted snapshot),
    // materialized up front and interleaved by merge_score below
    const platform_val: []const u8 = platform_filter orelse "";
    var ov = if (overlayEnabled(options))
        fetchOverlaySlice(alloc, OVERLAY_DOCS_FTS_SQL, .{
            fts_query, platform_val, platform_val, author_val,
            author_val, since_val,   since_val,    OVERLAY_HIT_CAP,
        })
    else
        OverlaySlice.init(alloc);

    // document content search
    if (platform_filter) |platform| {
        var rows = try local.query(LOCAL_DOCS_FTS_PLATFORM_SQL, .{ fts_query, fts_query, platform, author_val, author_val, since_val, since_val, candidate_limit, candidate_limit });
        defer rows.deinit();

        while (rows.next()) |row| {
            if (result_count >= options.max_results) break;
            try ov.emitUpTo(row.float(DOC_MERGE_SCORE_COL), alloc, &jw, &seen_uris, &seen_authors, &result_count, options);
            if (result_count >= options.max_results) break;
            const doc = Doc.fromLocalRow(row);
            if (ov.suppresses(doc.uri)) continue; // overlay shadows or tombstones
            if (author_filter) |af| {
                if (!std.mem.eql(u8, doc.did, af)) continue;
            }
            if (!passesSince(doc.createdAt, since_filter)) continue;
            if (!includeDid(doc.did, options.show_labeled)) continue;
            if (!options.include_undiscoverable and isUndiscoverableDoc(doc.did, doc.basePath)) continue;
            if (try isDuplicateAuthorTitle(&seen_authors, alloc, doc.did, doc.title)) continue;
            const uri_dupe = try alloc.dupe(u8, doc.uri);
            try seen_uris.put(uri_dupe, {});
            try jw.write(doc.toJson(alloc));
            result_count += 1;
        }
        try ov.emitUpTo(std.math.inf(f64), alloc, &jw, &seen_uris, &seen_authors, &result_count, options);

        // base_path search with platform
        var bp_rows = try local.query(withLocalDocRecencyOrder(
            \\SELECT d.uri, d.did, d.title, '' as snippet,
            \\  d.created_at, d.rkey, p.base_path,
            \\  1 as has_publication, d.platform, COALESCE(d.path, '') as path,
            \\  COALESCE(d.cover_image, '') as cover_image,
            \\  COALESCE(p.name, '') as publication_name
            \\FROM documents d
            \\JOIN publications p ON d.publication_uri = p.uri
            \\JOIN publications_fts pf ON p.uri = pf.uri
            \\WHERE publications_fts MATCH ? AND d.platform = ? AND (? = '' OR d.did = ?)
            \\AND (? = '' OR d.created_at >= ?)
            \\AND (d.is_bridgyfed IS NULL OR d.is_bridgyfed = 0) AND (d.url_dead IS NULL OR d.url_dead = 0)
        ), .{ fts_query, platform, author_val, author_val, since_val, since_val, candidate_limit });
        defer bp_rows.deinit();

        while (bp_rows.next()) |row| {
            if (result_count >= options.max_results) break;
            const doc = Doc.fromLocalRow(row);
            if (ov.suppresses(doc.uri)) continue; // overlay shadows or tombstones
            if (author_filter) |af| {
                if (!std.mem.eql(u8, doc.did, af)) continue;
            }
            if (!passesSince(doc.createdAt, since_filter)) continue;
            if (!includeDid(doc.did, options.show_labeled)) continue;
            if (!options.include_undiscoverable and isUndiscoverableDoc(doc.did, doc.basePath)) continue;
            if (!seen_uris.contains(doc.uri) and !try isDuplicateAuthorTitle(&seen_authors, alloc, doc.did, doc.title)) {
                try jw.write(doc.toJson(alloc));
                result_count += 1;
            }
        }
    } else {
        // no platform filter. Unfiltered searches take the bounded candidate
        // pass (common words otherwise probe the covering index once per
        // match — corpus-proportional); author/since need the full match set.
        var rows = if (author_val.len == 0 and since_val.len == 0)
            try local.query(LOCAL_DOCS_FTS_PREFILTER_SQL, .{ fts_query, fts_query, CANDIDATE_PREFILTER_K, candidate_limit, candidate_limit })
        else
            try local.query(LOCAL_DOCS_FTS_SQL, .{ fts_query, fts_query, author_val, author_val, since_val, since_val, candidate_limit, candidate_limit });
        defer rows.deinit();

        {
            const iter_span = logfire.span("search.iterate.docs_fts", .{});
            defer iter_span.end();
            var doc_count: u32 = 0;
            while (rows.next()) |row| {
                if (result_count >= options.max_results) break;
                try ov.emitUpTo(row.float(DOC_MERGE_SCORE_COL), alloc, &jw, &seen_uris, &seen_authors, &result_count, options);
                if (result_count >= options.max_results) break;
                const doc = Doc.fromLocalRow(row);
                if (ov.suppresses(doc.uri)) continue; // overlay shadows or tombstones
                if (author_filter) |af| {
                    if (!std.mem.eql(u8, doc.did, af)) continue;
                }
                if (!passesSince(doc.createdAt, since_filter)) continue;
                if (!includeDid(doc.did, options.show_labeled)) continue;
                if (!options.include_undiscoverable and isUndiscoverableDoc(doc.did, doc.basePath)) continue;
                if (try isDuplicateAuthorTitle(&seen_authors, alloc, doc.did, doc.title)) continue;
                const uri_dupe = try alloc.dupe(u8, doc.uri);
                try seen_uris.put(uri_dupe, {});
                try jw.write(doc.toJson(alloc));
                doc_count += 1;
                result_count += 1;
            }
            try ov.emitUpTo(std.math.inf(f64), alloc, &jw, &seen_uris, &seen_authors, &result_count, options);
            logfire.info("search.iterate.docs_fts rows={d}", .{doc_count});
        }

        // base_path search
        var bp_rows = try local.query(withLocalDocRecencyOrder(
            \\SELECT d.uri, d.did, d.title, '' as snippet,
            \\  d.created_at, d.rkey, p.base_path,
            \\  1 as has_publication, d.platform, COALESCE(d.path, '') as path,
            \\  COALESCE(d.cover_image, '') as cover_image,
            \\  COALESCE(p.name, '') as publication_name
            \\FROM documents d
            \\JOIN publications p ON d.publication_uri = p.uri
            \\JOIN publications_fts pf ON p.uri = pf.uri
            \\WHERE publications_fts MATCH ? AND (? = '' OR d.did = ?)
            \\AND (? = '' OR d.created_at >= ?)
            \\AND (d.is_bridgyfed IS NULL OR d.is_bridgyfed = 0) AND (d.url_dead IS NULL OR d.url_dead = 0)
        ), .{ fts_query, author_val, author_val, since_val, since_val, candidate_limit });
        defer bp_rows.deinit();

        {
            const iter_span = logfire.span("search.iterate.base_path", .{});
            defer iter_span.end();
            var bp_count: u32 = 0;
            while (bp_rows.next()) |row| {
                if (result_count >= options.max_results) break;
                const doc = Doc.fromLocalRow(row);
                if (ov.suppresses(doc.uri)) continue; // overlay shadows or tombstones
                if (author_filter) |af| {
                    if (!std.mem.eql(u8, doc.did, af)) {
                        bp_count += 1;
                        continue;
                    }
                }
                if (!passesSince(doc.createdAt, since_filter)) {
                    bp_count += 1;
                    continue;
                }
                if (!includeDid(doc.did, options.show_labeled)) continue;
                if (!options.include_undiscoverable and isUndiscoverableDoc(doc.did, doc.basePath)) continue;
                if (!seen_uris.contains(doc.uri) and !try isDuplicateAuthorTitle(&seen_authors, alloc, doc.did, doc.title)) {
                    try jw.write(doc.toJson(alloc));
                    result_count += 1;
                }
                bp_count += 1;
            }
            logfire.info("search.iterate.base_path rows={d}", .{bp_count});
        }

        // publication search — publications have no post-date (the local
        // replica omits created_at), so skip them under an active date filter
        // rather than leak undated publication shells into a recency view.
        if (since_filter == null) {
            var pub_rows = try local.query(
                \\SELECT f.uri, p.did, p.name,
                \\  snippet(publications_fts, 2, '', '', '...', 32) as snippet,
                \\  p.rkey, p.base_path, p.platform
                \\FROM publications_fts f
                \\JOIN publications p ON f.uri = p.uri
                \\WHERE publications_fts MATCH ? AND (? = '' OR p.did = ?)
                \\ORDER BY rank, p.uri LIMIT ?
            , .{ fts_query, author_val, author_val, candidate_limit });
            defer pub_rows.deinit();

            const iter_span = logfire.span("search.iterate.pubs_fts", .{});
            defer iter_span.end();
            var pub_count: u32 = 0;
            while (pub_rows.next()) |row| {
                if (result_count >= options.max_results) break;
                const pub_result = Pub.fromLocalRow(row);
                if (author_filter) |af| {
                    if (!std.mem.eql(u8, pub_result.did, af)) {
                        pub_count += 1;
                        continue;
                    }
                }
                if (!includeDid(pub_result.did, options.show_labeled)) continue;
                if (!options.include_undiscoverable and isUndiscoverablePublication(pub_result.uri)) continue;
                try jw.write(pub_result.toJson(alloc));
                pub_count += 1;
                result_count += 1;
            }
            logfire.info("search.iterate.pubs_fts rows={d}", .{pub_count});
        }
    }

    try jw.endArray();
    return try output.toOwnedSlice();
}

test "local document queries share the Turso recency ranking" {
    const sql = withLocalDocRecencyOrder("SELECT * FROM documents d");
    try std.testing.expect(std.mem.endsWith(u8, sql, LOCAL_DOC_RECENCY_ORDER));
    try std.testing.expect(std.mem.indexOf(u8, sql, "julianday(NULLIF(d.created_at") != null);
    try std.testing.expect(std.mem.indexOf(u8, sql, "COALESCE") != null);
}

fn getDocQuerySql(has_query: bool, has_tag: bool, has_platform: bool, has_since: bool) ?[]const u8 {
    if (has_query and has_tag and has_platform) return DocsByFtsAndTagAndPlatform.positional;
    if (has_query and has_tag) return DocsByFtsAndTag.positional;
    if (has_query and has_platform and has_since) return DocsByFtsAndPlatformAndSince.positional;
    if (has_query and has_platform) return DocsByFtsAndPlatform.positional;
    if (has_query and has_since) return DocsByFtsAndSince.positional;
    if (has_query) return DocsByFts.positional;
    if (has_tag and has_platform) return DocsByTagAndPlatform.positional;
    if (has_tag) return DocsByTag.positional;
    if (has_platform) return DocsByPlatform.positional;
    return null;
}

fn getDocQueryArgs(alloc: Allocator, fts_query: []const u8, tag: ?[]const u8, platform: ?[]const u8, since: ?[]const u8, has_query: bool, has_tag: bool, has_platform: bool, has_since: bool, limit: []const u8) ![]const []const u8 {
    var args: std.ArrayList([]const u8) = .empty;
    if (has_query and has_tag and has_platform) {
        try args.appendSlice(alloc, &.{ fts_query, tag.?, platform.? });
    } else if (has_query and has_tag) {
        try args.appendSlice(alloc, &.{ fts_query, tag.? });
    } else if (has_query and has_platform and has_since) {
        try args.appendSlice(alloc, &.{ fts_query, platform.?, since.? });
    } else if (has_query and has_platform) {
        try args.appendSlice(alloc, &.{ fts_query, platform.? });
    } else if (has_query and has_since) {
        try args.appendSlice(alloc, &.{ fts_query, since.? });
    } else if (has_query) {
        try args.append(alloc, fts_query);
    } else if (has_tag and has_platform) {
        try args.appendSlice(alloc, &.{ tag.?, platform.? });
    } else if (has_tag) {
        try args.append(alloc, tag.?);
    } else if (has_platform) {
        try args.append(alloc, platform.?);
    }
    try args.append(alloc, limit);
    return try args.toOwnedSlice(alloc);
}

test "document query limit is the final bind across query variants" {
    const args = try getDocQueryArgs(std.testing.allocator, "fts", "tag", "leaflet", "2026-01-01", true, true, true, true, "81");
    defer std.testing.allocator.free(args);
    try std.testing.expectEqual(@as(usize, 4), args.len);
    try std.testing.expectEqualStrings("fts", args[0]);
    try std.testing.expectEqualStrings("tag", args[1]);
    try std.testing.expectEqualStrings("leaflet", args[2]);
    try std.testing.expectEqualStrings("81", args[3]);

    const browse = try getDocQueryArgs(std.testing.allocator, "", null, "pckt", null, false, false, true, false, "42");
    defer std.testing.allocator.free(browse);
    try std.testing.expectEqual(@as(usize, 2), browse.len);
    try std.testing.expectEqualStrings("pckt", browse[0]);
    try std.testing.expectEqualStrings("42", browse[1]);
}

/// Find documents similar to a given document via turbopuffer ANN search.
/// 1. Fetch source doc's vector from tpuf (~50ms)
/// 2. ANN nearest-neighbor query (~50ms)
/// 3. Filter out source URI, serialize results
pub fn findSimilar(alloc: Allocator, uri: []const u8, limit: usize) ![]const u8 {
    // hash URI to tpuf ID format (AT-URIs exceed tpuf's 64-byte limit)
    const hashed = tpuf.hashId(uri);

    // get source document's vector
    const vector = tpuf.getVectorById(alloc, &hashed) catch |err| {
        logfire.warn("similar: getVectorById failed for {s}: {}", .{ uri, err });
        return error.VectorNotFound;
    };
    defer alloc.free(vector);

    // ANN query (request limit+1 so we can filter out the source doc)
    const results = tpuf.query(alloc, vector, limit + 1) catch |err| {
        logfire.warn("similar: tpuf query failed: {}", .{err});
        return error.QueryFailed;
    };
    defer {
        for (results) |r| {
            alloc.free(r.id);
            alloc.free(r.uri);
            alloc.free(r.title);
            alloc.free(r.did);
            alloc.free(r.created_at);
            alloc.free(r.rkey);
            alloc.free(r.base_path);
            alloc.free(r.platform);
            alloc.free(r.path);
        }
        alloc.free(results);
    }

    // collect filtered URIs for snippet lookup
    var uri_buf: [21][]const u8 = undefined; // limit+1
    var uri_count: usize = 0;
    for (results) |r| {
        if (std.mem.eql(u8, r.uri, uri)) continue;
        if (uri_count >= limit) break;
        uri_buf[uri_count] = r.uri;
        uri_count += 1;
    }

    const extras = fetchLocalExtras(alloc, uri_buf[0..uri_count]);

    // serialize, filtering out the source URI
    var output: std.Io.Writer.Allocating = .init(alloc);
    errdefer output.deinit();

    // track seen (did, title) pairs for cross-platform dedup
    var seen_authors = std.StringHashMap(void).init(alloc);
    defer seen_authors.deinit();

    var jw: json.Stringify = .{ .writer = &output.writer };
    try jw.beginArray();
    var count: usize = 0;
    for (results) |r| {
        if (std.mem.eql(u8, r.uri, uri)) continue;
        if (count >= limit) break;
        // /similar had no visibility filter at all: an opted-out document
        // could not be reached by search but was one "related documents" hop
        // away from any discoverable neighbour.
        if (isUndiscoverableDoc(r.did, r.base_path)) continue;
        if (!includeDid(r.did, false)) continue;
        if (try isDuplicateAuthorTitle(&seen_authors, alloc, r.did, r.title)) continue;
        const doc_type: []const u8 = if (r.has_publication) "article" else "looseleaf";
        // prefer authoritative local-replica URL fields over stale tpuf attrs
        const platform = extras.platforms.get(r.uri) orelse r.platform;
        const base_path = extras.base_paths.get(r.uri) orelse r.base_path;
        const path = extras.paths.get(r.uri) orelse r.path;
        try jw.write(SearchResultJson{
            .type = doc_type,
            .uri = r.uri,
            .did = r.did,
            .title = r.title,
            .snippet = extras.snippets.get(r.uri) orelse "",
            .createdAt = r.created_at,
            .rkey = r.rkey,
            .basePath = base_path,
            .platform = platform,
            .path = path,
            .coverImage = extras.cover_images.get(r.uri) orelse "",
            .publicationName = extras.pub_names.get(r.uri) orelse "",
            .url = buildDocUrl(alloc, doc_type, platform, base_path, path, r.rkey, r.did),
        });
        count += 1;
    }
    try jw.endArray();

    return try output.toOwnedSlice();
}

/// Hybrid search: run keyword + semantic, merge with Reciprocal Rank Fusion.
/// score(doc) = 1/(k + rank_keyword) + 1/(k + rank_semantic), k=60
fn searchHybrid(alloc: Allocator, query: []const u8, tag_filter: ?[]const u8, platform_filter: ?[]const u8, since_filter: ?[]const u8, author_filter: ?[]const u8, options: Options) ![]const u8 {
    if (query.len == 0) return try alloc.dupe(u8, "[]");

    const span = logfire.span("search.hybrid", .{});
    defer span.end();

    // Fusion must use the same source depth on every page. If source depth
    // grew with offset, a result found by both sources at rank 30 could enter
    // ahead of a first-page single-source result and shift page boundaries.
    const fusion_options: Options = .{
        .max_results = 200,
        .show_labeled = options.show_labeled,
        .include_undiscoverable = options.include_undiscoverable,
    };

    // 1. keyword search (~10ms via local SQLite)
    const kw_json = searchKeyword(alloc, query, tag_filter, platform_filter, since_filter, author_filter, fusion_options) catch |err| blk: {
        logfire.warn("search.hybrid: keyword failed: {}", .{err});
        break :blk try alloc.dupe(u8, "[]");
    };

    // 2. semantic search (~550ms via voyage + tpuf)
    const sem_json = searchSemantic(alloc, query, platform_filter, since_filter, author_filter, fusion_options) catch |err| blk: {
        logfire.warn("search.hybrid: semantic failed: {}", .{err});
        break :blk try alloc.dupe(u8, "[]");
    };

    // check if semantic returned an error object (starts with '{')
    const sem_is_error = sem_json.len > 0 and sem_json[0] == '{';

    // 3. parse both into json.Value arrays
    const kw_parsed = json.parseFromSlice(json.Value, alloc, kw_json, .{}) catch {
        // if keyword parse fails, just return semantic (or empty)
        if (sem_is_error) return try alloc.dupe(u8, "[]");
        return sem_json;
    };
    defer kw_parsed.deinit();

    const kw_items = switch (kw_parsed.value) {
        .array => |arr| arr.items,
        else => &[_]json.Value{},
    };

    var sem_items: []const json.Value = &.{};
    var sem_parsed_opt: ?json.Parsed(json.Value) = null;
    defer if (sem_parsed_opt) |*p| p.deinit();

    if (!sem_is_error) {
        if (json.parseFromSlice(json.Value, alloc, sem_json, .{}) catch null) |parsed| {
            sem_parsed_opt = parsed;
            sem_items = switch (parsed.value) {
                .array => |arr| arr.items,
                else => &[_]json.Value{},
            };
        }
    }

    // if one side is empty, return the other with source annotation
    if (kw_items.len == 0 and sem_items.len == 0) {
        return try alloc.dupe(u8, "[]");
    }

    // 4. build RRF score map
    const RRF_K: f64 = 60.0;

    // source bits: 1=keyword, 2=semantic
    var scores = std.StringHashMap(f64).init(alloc);
    defer scores.deinit();
    var source_bits = std.StringHashMap(u8).init(alloc);
    defer source_bits.deinit();

    // map URI -> json object from keyword results (preferred for snippets)
    var kw_objects = std.StringHashMap(json.ObjectMap).init(alloc);
    defer kw_objects.deinit();
    var sem_objects = std.StringHashMap(json.ObjectMap).init(alloc);
    defer sem_objects.deinit();

    for (kw_items, 0..) |item, i| {
        const obj = switch (item) {
            .object => |o| o,
            else => continue,
        };
        const uri = jsonStr(obj, "uri");
        if (uri.len == 0) continue;

        const rank: f64 = @floatFromInt(i + 1);
        const rrf_score = 1.0 / (RRF_K + rank);

        const prev = scores.get(uri) orelse 0.0;
        try scores.put(uri, prev + rrf_score);

        const prev_bits = source_bits.get(uri) orelse 0;
        try source_bits.put(uri, prev_bits | 0b01);

        if (!kw_objects.contains(uri)) {
            try kw_objects.put(uri, obj);
        }
    }

    for (sem_items, 0..) |item, i| {
        const obj = switch (item) {
            .object => |o| o,
            else => continue,
        };
        const uri = jsonStr(obj, "uri");
        if (uri.len == 0) continue;

        const rank: f64 = @floatFromInt(i + 1);
        const rrf_score = 1.0 / (RRF_K + rank);

        const prev = scores.get(uri) orelse 0.0;
        try scores.put(uri, prev + rrf_score);

        const prev_bits = source_bits.get(uri) orelse 0;
        try source_bits.put(uri, prev_bits | 0b10);

        if (!sem_objects.contains(uri)) {
            try sem_objects.put(uri, obj);
        }
    }

    // 5. collect and sort by RRF score
    const ScoredUri = struct {
        uri: []const u8,
        score: f64,
    };

    var scored: std.ArrayList(ScoredUri) = .empty;
    defer scored.deinit(alloc);

    var it = scores.iterator();
    while (it.next()) |entry| {
        try scored.append(alloc, .{ .uri = entry.key_ptr.*, .score = entry.value_ptr.* });
    }

    std.mem.sort(ScoredUri, scored.items, {}, struct {
        fn lessThan(_: void, a: ScoredUri, b: ScoredUri) bool {
            if (a.score != b.score) return a.score > b.score; // descending
            // Hash-map iteration order is not stable. A URI tie-break prevents
            // equal-score results from jumping between offset pages.
            return std.mem.lessThan(u8, a.uri, b.uri);
        }
    }.lessThan);

    // 6. fetch content previews for semantic-only results (they have no FTS snippet)
    const candidate_count = @min(scored.items.len, options.max_results *| 2);
    var sem_uris: std.ArrayList([]const u8) = .empty;
    defer sem_uris.deinit(alloc);
    for (scored.items[0..candidate_count]) |entry| {
        const bits = source_bits.get(entry.uri) orelse 0;
        if (bits == 0b10) { // semantic-only
            const obj = sem_objects.get(entry.uri) orelse continue;
            const existing_snippet = jsonStr(obj, "snippet");
            if (existing_snippet.len == 0) {
                try sem_uris.append(alloc, entry.uri);
            }
        }
    }
    const hybrid_extras = fetchLocalExtras(alloc, sem_uris.items);

    // 7. serialize top 20 with source annotation
    var output: std.Io.Writer.Allocating = .init(alloc);
    errdefer output.deinit();

    var jw: json.Stringify = .{ .writer = &output.writer };
    try jw.beginArray();

    // track seen (did, title) pairs for cross-platform dedup
    var seen_authors = std.StringHashMap(void).init(alloc);
    defer seen_authors.deinit();

    var emitted: usize = 0;
    for (scored.items) |entry| {
        if (emitted >= options.max_results) break;
        const bits = source_bits.get(entry.uri) orelse 0;
        // prefer keyword version (has FTS snippet)
        const obj = kw_objects.get(entry.uri) orelse sem_objects.get(entry.uri) orelse continue;

        // cross-platform dedup: skip if same author+title already emitted
        if (!includeDid(jsonStr(obj, "did"), options.show_labeled)) continue;
        if (!options.include_undiscoverable and isUndiscoverableDoc(jsonStr(obj, "did"), jsonStr(obj, "basePath"))) continue;
        if (try isDuplicateAuthorTitle(&seen_authors, alloc, jsonStr(obj, "did"), jsonStr(obj, "title"))) continue;

        const source_label: []const u8 = switch (bits) {
            0b01 => "keyword",
            0b10 => "semantic",
            0b11 => "keyword+semantic",
            else => "",
        };

        // for semantic-only results with empty snippet, use fetched preview
        const snippet = blk: {
            const existing = jsonStr(obj, "snippet");
            if (existing.len > 0) break :blk existing;
            if (bits == 0b10) {
                break :blk hybrid_extras.snippets.get(entry.uri) orelse "";
            }
            break :blk existing;
        };

        try jw.beginObject();
        // write all standard fields from the source object
        inline for (.{ "type", "uri", "did", "title" }) |field| {
            try jw.objectField(field);
            try jw.write(jsonStr(obj, field));
        }
        try jw.objectField("snippet");
        try jw.write(snippet);
        inline for (.{ "rkey", "basePath", "platform", "path" }) |field| {
            try jw.objectField(field);
            try jw.write(jsonStr(obj, field));
        }
        // for semantic-only results, cover image may need local DB fallback
        const cover = blk: {
            const existing = jsonStr(obj, "coverImage");
            if (existing.len > 0) break :blk existing;
            if (bits & 0b10 != 0) break :blk hybrid_extras.cover_images.get(entry.uri) orelse "";
            break :blk existing;
        };
        try jw.objectField("coverImage");
        try jw.write(cover);
        // for semantic-only results, pub name may need local DB fallback
        const pub_name = blk: {
            const existing = jsonStr(obj, "publicationName");
            if (existing.len > 0) break :blk existing;
            if (bits & 0b10 != 0) break :blk hybrid_extras.pub_names.get(entry.uri) orelse "";
            break :blk existing;
        };
        try jw.objectField("publicationName");
        try jw.write(pub_name);
        try jw.objectField("url");
        try jw.write(buildDocUrl(alloc, jsonStr(obj, "type"), jsonStr(obj, "platform"), jsonStr(obj, "basePath"), jsonStr(obj, "path"), jsonStr(obj, "rkey"), jsonStr(obj, "did")));
        try jw.objectField("createdAt");
        try jw.write(jsonStr(obj, "createdAt"));
        try jw.objectField("source");
        try jw.write(source_label);
        try jw.objectField("score");
        try jw.write(entry.score);
        try jw.endObject();
        emitted += 1;
    }

    try jw.endArray();
    return try output.toOwnedSlice();
}

/// Semantic search: embed query via Voyage, ANN search via turbopuffer.
fn searchSemantic(alloc: Allocator, query: []const u8, platform_filter: ?[]const u8, since_filter: ?[]const u8, author_filter: ?[]const u8, options: Options) ![]const u8 {
    if (query.len == 0) return try alloc.dupe(u8, "[]");

    if (!tpuf.isSemanticEnabled()) {
        return try alloc.dupe(u8, "{\"error\":\"semantic search not available\"}");
    }

    const span = logfire.span("search.semantic", .{});
    defer span.end();

    // embed query (input_type="query" for asymmetric search)
    const vector = tpuf.embedQuery(alloc, query) catch |err| {
        logfire.warn("search.semantic: embed failed: {}", .{err});
        return try alloc.dupe(u8, "{\"error\":\"embedding failed\"}");
    };
    defer alloc.free(vector);

    // ANN query — over-fetch to allow filtering
    const results = tpuf.query(alloc, vector, queryCandidateLimit(options.max_results)) catch |err| {
        logfire.warn("search.semantic: tpuf query failed: {}", .{err});
        return try alloc.dupe(u8, "{\"error\":\"vector search failed\"}");
    };
    defer {
        for (results) |r| {
            alloc.free(r.id);
            alloc.free(r.uri);
            alloc.free(r.title);
            alloc.free(r.did);
            alloc.free(r.created_at);
            alloc.free(r.rkey);
            alloc.free(r.base_path);
            alloc.free(r.platform);
            alloc.free(r.path);
        }
        alloc.free(results);
    }

    // first pass: filter and collect URIs for snippet lookup
    var filtered_indices: std.ArrayList(usize) = .empty;
    defer filtered_indices.deinit(alloc);
    var seen: std.ArrayList([]const u8) = .empty;
    defer seen.deinit(alloc);

    // track seen (did, title) pairs for cross-platform dedup
    var seen_authors = std.StringHashMap(void).init(alloc);
    defer seen_authors.deinit();

    for (results, 0..) |r, idx| {
        if (filtered_indices.items.len >= options.max_results) break;
        if (r.title.len == 0) continue;
        if (isBridgyFed(r.uri)) continue;
        if (!includeDid(r.did, options.show_labeled)) continue;
        if (!options.include_undiscoverable and isUndiscoverableDoc(r.did, r.base_path)) continue;
        if (platform_filter) |pf| {
            if (!std.mem.eql(u8, r.platform, pf)) continue;
        }
        if (author_filter) |af| {
            if (!std.mem.eql(u8, r.did, af)) continue;
        }
        // date filter: tpuf has no since predicate, so apply it here (the
        // keyword path filters in SQL / via passesSince — semantic must match).
        if (!passesSince(r.created_at, since_filter)) continue;
        var is_dup = false;
        for (seen.items) |s| {
            if (std.mem.eql(u8, s, r.uri)) {
                is_dup = true;
                break;
            }
        }
        if (is_dup) continue;
        if (try isDuplicateAuthorTitle(&seen_authors, alloc, r.did, r.title)) continue;
        try seen.append(alloc, r.uri);
        try filtered_indices.append(alloc, idx);
    }

    // fetch content previews + cover images from local SQLite
    const extras = fetchLocalExtras(alloc, seen.items);

    // serialize results
    var output: std.Io.Writer.Allocating = .init(alloc);
    errdefer output.deinit();

    var jw: json.Stringify = .{ .writer = &output.writer };
    try jw.beginArray();
    for (filtered_indices.items) |idx| {
        const r = results[idx];
        const doc_type: []const u8 = if (r.has_publication) "article" else "looseleaf";
        // prefer the local replica's URL fields over tpuf's stored attributes,
        // which can be stale (written at embed time). Falls back to tpuf values
        // when the doc isn't in the local replica yet.
        const platform = extras.platforms.get(r.uri) orelse r.platform;
        const base_path = extras.base_paths.get(r.uri) orelse r.base_path;
        const path = extras.paths.get(r.uri) orelse r.path;
        try jw.write(SearchResultJson{
            .type = doc_type,
            .uri = r.uri,
            .did = r.did,
            .title = r.title,
            .snippet = extras.snippets.get(r.uri) orelse "",
            .createdAt = r.created_at,
            .rkey = r.rkey,
            .basePath = base_path,
            .platform = platform,
            .path = path,
            .coverImage = extras.cover_images.get(r.uri) orelse "",
            .publicationName = extras.pub_names.get(r.uri) orelse "",
            .url = buildDocUrl(alloc, doc_type, platform, base_path, path, r.rkey, r.did),
        });
    }
    try jw.endArray();

    return try output.toOwnedSlice();
}

// --- local DB helpers (for semantic/similar results) ---

/// Extra fields fetched from local SQLite for semantic/similar results.
const LocalExtras = struct {
    snippets: std.StringHashMap([]const u8),
    cover_images: std.StringHashMap([]const u8),
    pub_names: std.StringHashMap([]const u8),
    // authoritative URL-determining fields from the local replica. tpuf stores
    // these as attributes at embed time, which go stale when a doc's platform/
    // path changes without re-embedding — so prefer these for buildDocUrl.
    platforms: std.StringHashMap([]const u8),
    base_paths: std.StringHashMap([]const u8),
    paths: std.StringHashMap([]const u8),
};

/// Fetch content previews, cover images, and publication names from local SQLite for a list of URIs.
/// Gracefully returns empty maps if local db is unavailable.
fn fetchLocalExtras(alloc: Allocator, uris: []const []const u8) LocalExtras {
    var snippets = std.StringHashMap([]const u8).init(alloc);
    var cover_images = std.StringHashMap([]const u8).init(alloc);
    var pub_names = std.StringHashMap([]const u8).init(alloc);
    var platforms = std.StringHashMap([]const u8).init(alloc);
    var base_paths = std.StringHashMap([]const u8).init(alloc);
    var paths = std.StringHashMap([]const u8).init(alloc);
    const empty: LocalExtras = .{ .snippets = snippets, .cover_images = cover_images, .pub_names = pub_names, .platforms = platforms, .base_paths = base_paths, .paths = paths };
    const local = db.getLocalDb() orelse return empty;
    for (uris) |uri| {
        // overlay first: docs newer than the snapshot hydrate from the overlay
        // (otherwise semantic mode shows stale/empty fields for fresh docs)
        if (db.getOverlay()) |o| {
            var orows = o.query(
                "SELECT substr(content, 1, 200), COALESCE(cover_image, ''), COALESCE(publication_name, ''), platform, COALESCE(base_path, ''), COALESCE(path, '') FROM documents_overlay WHERE uri = ? AND deleted = 0",
                .{uri},
            ) catch null;
            if (orows) |*r| {
                defer r.deinit();
                if (r.next()) |row| {
                    const preview = row.text(0);
                    if (preview.len > 0) {
                        if (alloc.dupe(u8, preview)) |d| snippets.put(uri, d) catch {} else |_| {}
                    }
                    const cover = row.text(1);
                    if (cover.len > 0) {
                        if (alloc.dupe(u8, cover)) |d| cover_images.put(uri, d) catch {} else |_| {}
                    }
                    const pub_name = row.text(2);
                    if (pub_name.len > 0) {
                        if (alloc.dupe(u8, pub_name)) |d| pub_names.put(uri, d) catch {} else |_| {}
                    }
                    if (alloc.dupe(u8, row.text(3))) |p| platforms.put(uri, p) catch {} else |_| {}
                    if (alloc.dupe(u8, row.text(4))) |b| base_paths.put(uri, b) catch {} else |_| {}
                    if (alloc.dupe(u8, row.text(5))) |pa| paths.put(uri, pa) catch {} else |_| {}
                    continue;
                }
            }
        }
        var rows = local.query(
            "SELECT substr(content, 1, 200), COALESCE(cover_image, ''), COALESCE((SELECT name FROM publications WHERE uri = documents.publication_uri), ''), platform, COALESCE(base_path, ''), COALESCE(path, '') FROM documents WHERE uri = ?",
            .{uri},
        ) catch continue;
        defer rows.deinit();
        if (rows.next()) |row| {
            const preview = row.text(0);
            if (preview.len > 0) {
                const duped = alloc.dupe(u8, preview) catch continue;
                snippets.put(uri, duped) catch continue;
            }
            const cover = row.text(1);
            if (cover.len > 0) {
                const duped = alloc.dupe(u8, cover) catch continue;
                cover_images.put(uri, duped) catch continue;
            }
            const pub_name = row.text(2);
            if (pub_name.len > 0) {
                const duped = alloc.dupe(u8, pub_name) catch continue;
                pub_names.put(uri, duped) catch continue;
            }
            // authoritative URL fields — platform always present; dupe so they
            // outlive the row (rows.deinit frees the backing memory).
            if (alloc.dupe(u8, row.text(3))) |p| platforms.put(uri, p) catch {} else |_| {}
            if (alloc.dupe(u8, row.text(4))) |b| base_paths.put(uri, b) catch {} else |_| {}
            if (alloc.dupe(u8, row.text(5))) |pa| paths.put(uri, pa) catch {} else |_| {}
        }
    }
    return .{ .snippets = snippets, .cover_images = cover_images, .pub_names = pub_names, .platforms = platforms, .base_paths = base_paths, .paths = paths };
}

// --- JSON helpers (for hybrid search parsing) ---

fn jsonStr(obj: json.ObjectMap, key: []const u8) []const u8 {
    const val = obj.get(key) orelse return "";
    return switch (val) {
        .string => |s| s,
        else => "",
    };
}

/// Build FTS5 query from user input, following the Google convention for
/// quotes (blog.google/products/search/how-were-improving-search-results-
/// when-you-use-quotes/): a quoted term is REQUIRED and matched exactly.
/// - bare words are OR'd together, prefix `*` on last word (recall-first)
/// - user-quoted terms (`"..."`) are required: AND'd with each other, and
///   with the bare-word group if one exists. Quoting a single word also
///   disables prefix expansion (Google: quoting disables variant matching).
/// - punctuation inside quotes tokenizes as spaces (unicode61), matching
///   Google's documented `"don't"` ≈ "don t" behavior for free
/// - divergence from Google: bare words alongside quotes stay required as
///   a group (`"a" AND (b OR c*)`) — FTS5 MATCH has no "optional" operator,
///   so Google's droppable loose terms aren't expressible
/// - unclosed quotes are treated as phrases with synthetic closing quote
/// - AND binds tighter than OR in FTS5, so the bare group is parenthesized
/// Separators match FTS5 unicode61 tokenizer: any non-alphanumeric character
pub fn buildFtsQuery(alloc: Allocator, query: []const u8) ![]const u8 {
    if (query.len == 0) return "";

    // normalize: trim whitespace
    var start: usize = 0;
    var end: usize = query.len;
    while (start < end and query[start] == ' ') start += 1;
    while (end > start and query[end - 1] == ' ') end -= 1;
    if (start >= end) return "";

    const trimmed = query[start..end];

    // tokenize. `escaped_word` is a bare word that must be emitted quoted
    // because it collides with an FTS5 operator (literal "OR") — it belongs
    // to the bare-word group, NOT the required-phrase group.
    const TokenKind = enum { word, escaped_word, phrase };
    const Token = struct { kind: TokenKind, text: []const u8 };

    var tokens: std.ArrayList(Token) = .empty;
    defer tokens.deinit(alloc);

    var i: usize = 0;
    while (i < trimmed.len) {
        if (trimmed[i] == '"') {
            // quoted phrase: scan to closing quote or end
            i += 1; // skip opening quote
            const inner_start = i;
            while (i < trimmed.len and trimmed[i] != '"') : (i += 1) {}
            const inner_end = i;
            if (i < trimmed.len) i += 1; // skip closing quote

            // only emit if inner text has alphanumeric content
            const inner = trimmed[inner_start..inner_end];
            for (inner) |c| {
                if (isAlnum(c)) {
                    try tokens.append(alloc, .{ .kind = .phrase, .text = inner });
                    break;
                }
            }
        } else if (isAlnum(trimmed[i])) {
            // bare word: scan alphanumeric run
            const word_start = i;
            while (i < trimmed.len and isAlnum(trimmed[i])) : (i += 1) {}
            const word = trimmed[word_start..i];
            // "OR" is an FTS5 operator — quote it so it's searched as a literal word
            const kind: TokenKind = if (std.mem.eql(u8, word, "OR")) .escaped_word else .word;
            try tokens.append(alloc, .{ .kind = kind, .text = word });
        } else {
            i += 1; // skip separator
        }
    }

    if (tokens.items.len == 0) return "";

    var out: std.ArrayList(u8) = .empty;
    errdefer out.deinit(alloc);

    var n_phrases: usize = 0;
    for (tokens.items) |t| {
        if (t.kind == .phrase) n_phrases += 1;
    }
    const n_bare = tokens.items.len - n_phrases;

    // required group: user-quoted phrases, AND-joined
    var emitted_phrases: usize = 0;
    for (tokens.items) |token| {
        if (token.kind != .phrase) continue;
        if (emitted_phrases > 0) try out.appendSlice(alloc, " AND ");
        try out.append(alloc, '"');
        try out.appendSlice(alloc, token.text);
        try out.append(alloc, '"');
        emitted_phrases += 1;
    }

    // bare-word group: OR-joined, prefix * only when the final token of the
    // whole query is a bare word (preserves type-ahead behavior)
    if (n_bare > 0) {
        if (n_phrases > 0) try out.appendSlice(alloc, " AND (");
        var emitted_bare: usize = 0;
        for (tokens.items, 0..) |token, idx| {
            if (token.kind == .phrase) continue;
            if (emitted_bare > 0) try out.appendSlice(alloc, " OR ");
            switch (token.kind) {
                .word => {
                    try out.appendSlice(alloc, token.text);
                    if (idx == tokens.items.len - 1) try out.append(alloc, '*');
                },
                .escaped_word => {
                    try out.append(alloc, '"');
                    try out.appendSlice(alloc, token.text);
                    try out.append(alloc, '"');
                },
                .phrase => unreachable,
            }
            emitted_bare += 1;
        }
        if (n_phrases > 0) try out.append(alloc, ')');
    }

    return try out.toOwnedSlice(alloc);
}

fn isAlnum(c: u8) bool {
    return (c >= 'a' and c <= 'z') or (c >= 'A' and c <= 'Z') or (c >= '0' and c <= '9');
}

// --- tests ---

test "buildFtsQuery: empty string" {
    const result = try buildFtsQuery(std.testing.allocator, "");
    try std.testing.expectEqualStrings("", result);
}

test "buildFtsQuery: whitespace only" {
    const result = try buildFtsQuery(std.testing.allocator, "   ");
    try std.testing.expectEqualStrings("", result);
}

test "buildFtsQuery: single word" {
    const result = try buildFtsQuery(std.testing.allocator, "hello");
    defer std.testing.allocator.free(result);
    try std.testing.expectEqualStrings("hello*", result);
}

test "buildFtsQuery: single word with whitespace" {
    const result = try buildFtsQuery(std.testing.allocator, "  hello  ");
    defer std.testing.allocator.free(result);
    try std.testing.expectEqualStrings("hello*", result);
}

test "buildFtsQuery: multiple words" {
    const result = try buildFtsQuery(std.testing.allocator, "cat dog");
    defer std.testing.allocator.free(result);
    try std.testing.expectEqualStrings("cat OR dog*", result);
}

test "buildFtsQuery: three words" {
    const result = try buildFtsQuery(std.testing.allocator, "one two three");
    defer std.testing.allocator.free(result);
    try std.testing.expectEqualStrings("one OR two OR three*", result);
}

test "buildFtsQuery: quoted phrase passthrough" {
    const result = try buildFtsQuery(std.testing.allocator, "\"exact phrase\"");
    defer std.testing.allocator.free(result);
    try std.testing.expectEqualStrings("\"exact phrase\"", result);
}

test "buildFtsQuery: dots as separators" {
    const result = try buildFtsQuery(std.testing.allocator, "foo.bar");
    defer std.testing.allocator.free(result);
    try std.testing.expectEqualStrings("foo OR bar*", result);
}

test "buildFtsQuery: hyphens as separators" {
    const result = try buildFtsQuery(std.testing.allocator, "crypto-casino");
    defer std.testing.allocator.free(result);
    try std.testing.expectEqualStrings("crypto OR casino*", result);
}

test "buildFtsQuery: mixed punctuation" {
    const result = try buildFtsQuery(std.testing.allocator, "don't@stop_now");
    defer std.testing.allocator.free(result);
    try std.testing.expectEqualStrings("don OR t OR stop OR now*", result);
}

test "buildFtsQuery: embedded quoted phrase" {
    const result = try buildFtsQuery(std.testing.allocator, "python \"machine learning\" tutorial");
    defer std.testing.allocator.free(result);
    try std.testing.expectEqualStrings("\"machine learning\" AND (python OR tutorial*)", result);
}

test "buildFtsQuery: quoted phrase at start" {
    const result = try buildFtsQuery(std.testing.allocator, "\"exact phrase\" python");
    defer std.testing.allocator.free(result);
    try std.testing.expectEqualStrings("\"exact phrase\" AND (python*)", result);
}

test "buildFtsQuery: quoted phrase at end" {
    const result = try buildFtsQuery(std.testing.allocator, "python \"machine learning\"");
    defer std.testing.allocator.free(result);
    try std.testing.expectEqualStrings("\"machine learning\" AND (python)", result);
}

test "buildFtsQuery: literal OR quoted to avoid FTS5 operator collision" {
    const result = try buildFtsQuery(std.testing.allocator, "bertha OR burton");
    defer std.testing.allocator.free(result);
    try std.testing.expectEqualStrings("bertha OR \"OR\" OR burton*", result);
}

test "buildFtsQuery: multiple ORs quoted" {
    const result = try buildFtsQuery(std.testing.allocator, "cat OR dog OR fish");
    defer std.testing.allocator.free(result);
    try std.testing.expectEqualStrings("cat OR \"OR\" OR dog OR \"OR\" OR fish*", result);
}

test "buildFtsQuery: OR at start quoted" {
    const result = try buildFtsQuery(std.testing.allocator, "OR cat dog");
    defer std.testing.allocator.free(result);
    try std.testing.expectEqualStrings("\"OR\" OR cat OR dog*", result);
}

test "buildFtsQuery: OR at end" {
    const result = try buildFtsQuery(std.testing.allocator, "cat dog OR");
    defer std.testing.allocator.free(result);
    try std.testing.expectEqualStrings("cat OR dog OR \"OR\"", result);
}

test "buildFtsQuery: only OR" {
    const result = try buildFtsQuery(std.testing.allocator, "OR");
    defer std.testing.allocator.free(result);
    try std.testing.expectEqualStrings("\"OR\"", result);
}

test "buildFtsQuery: unclosed quote" {
    const result = try buildFtsQuery(std.testing.allocator, "\"hello world");
    defer std.testing.allocator.free(result);
    try std.testing.expectEqualStrings("\"hello world\"", result);
}

test "buildFtsQuery: empty quotes" {
    const result = try buildFtsQuery(std.testing.allocator, "\"\"");
    try std.testing.expectEqualStrings("", result);
}

test "buildFtsQuery: empty quotes with word" {
    const result = try buildFtsQuery(std.testing.allocator, "\"\" hello");
    defer std.testing.allocator.free(result);
    try std.testing.expectEqualStrings("hello*", result);
}

test "buildFtsQuery: mixed quotes and OR" {
    const result = try buildFtsQuery(std.testing.allocator, "\"exact phrase\" OR python");
    defer std.testing.allocator.free(result);
    try std.testing.expectEqualStrings("\"exact phrase\" AND (\"OR\" OR python*)", result);
}

test "buildFtsQuery: two quoted words are AND'd (google convention — the milk/cee case)" {
    const result = try buildFtsQuery(std.testing.allocator, "\"milk\" \"cee\"");
    defer std.testing.allocator.free(result);
    try std.testing.expectEqualStrings("\"milk\" AND \"cee\"", result);
}

test "buildFtsQuery: quoted single word gets no prefix star" {
    const result = try buildFtsQuery(std.testing.allocator, "\"cee\"");
    defer std.testing.allocator.free(result);
    try std.testing.expectEqualStrings("\"cee\"", result);
}

test "buildFtsQuery: three quoted phrases all required" {
    const result = try buildFtsQuery(std.testing.allocator, "\"a b\" \"c\" \"d e\"");
    defer std.testing.allocator.free(result);
    try std.testing.expectEqualStrings("\"a b\" AND \"c\" AND \"d e\"", result);
}

test "buildDocUrl: native leaflet doc uses rkey (no path)" {
    const url = buildDocUrl(std.testing.allocator, "article", "leaflet", "leaflet.pub", "", "abc123", "did:plc:x");
    defer std.testing.allocator.free(url);
    try std.testing.expectEqualStrings("https://leaflet.pub/abc123", url);
}

test "buildDocUrl: site.standard doc tagged leaflet uses path, not rkey" {
    // regression: feliciarondo.com site.standard.document records embed pub.leaflet.content
    // so get platform=leaflet, but must link to the author-set path — not the rkey
    const url = buildDocUrl(std.testing.allocator, "article", "leaflet", "feliciarondo.com", "/rondo-of-blog/2025/The-Heart-of-Peach/", "3m5i74ey7zs2c", "did:plc:2atpw7zrdrdptzqo7jw63rzv");
    defer std.testing.allocator.free(url);
    try std.testing.expectEqualStrings("https://feliciarondo.com/rondo-of-blog/2025/The-Heart-of-Peach/", url);
}

test "buildDocUrl: path without leading slash gets separator" {
    const url = buildDocUrl(std.testing.allocator, "article", "pckt", "example.com", "posts/hello", "rk", "did:plc:x");
    defer std.testing.allocator.free(url);
    try std.testing.expectEqualStrings("https://example.com/posts/hello", url);
}

test "local tag browse: recommend-lifted ranking, correct order at corpus scale" {
    const zqlite = @import("zqlite");
    var threaded = std.Io.Threaded.init(std.testing.allocator, .{});
    defer threaded.deinit();
    const tio = threaded.io();

    var path_buf: [64]u8 = undefined;
    const zpath = std.fmt.bufPrintZ(&path_buf, "/tmp/tag-browse-{d}.db", .{std.c.getpid()}) catch unreachable;
    const cleanup = struct {
        fn rm(p: []const u8) void {
            var b: [80]u8 = undefined;
            inline for (.{ "", "-wal", "-shm" }) |sfx| {
                const z = std.fmt.bufPrintZ(&b, "{s}{s}", .{ p, sfx }) catch return;
                _ = std.c.unlink(z.ptr);
            }
        }
    };
    cleanup.rm(zpath);
    defer cleanup.rm(zpath);

    {
        const w = try zqlite.open(zpath.ptr, zqlite.OpenFlags.Create | zqlite.OpenFlags.ReadWrite);
        defer w.close();
        try w.exec(
            \\CREATE TABLE documents (
            \\  uri TEXT PRIMARY KEY, did TEXT, rkey TEXT, title TEXT,
            \\  created_at TEXT, publication_uri TEXT, platform TEXT,
            \\  path TEXT, base_path TEXT, has_publication INTEGER,
            \\  cover_image TEXT, is_bridgyfed INTEGER, url_dead INTEGER, content TEXT
            \\)
        , .{});
        try w.exec("CREATE TABLE publications (uri TEXT PRIMARY KEY, name TEXT)", .{});
        try w.exec("CREATE TABLE document_tags (document_uri TEXT, tag TEXT)", .{});
        try w.exec("CREATE INDEX idx_document_tags_tag ON document_tags(tag)", .{});
        try w.exec("CREATE TABLE recommends (uri TEXT PRIMARY KEY, did TEXT, rkey TEXT, document_uri TEXT, created_at TEXT, indexed_at TEXT)", .{});
        try w.exec("CREATE VIRTUAL TABLE documents_fts USING fts5(uri UNINDEXED, title, content, content='documents', content_rowid='rowid')", .{});
        try w.exec("CREATE INDEX idx_recommends_document_uri ON recommends(document_uri)", .{});

        // 60k docs spread over ~700 days, every 80th tagged "photography" (~750)
        try w.exec(
            \\WITH RECURSIVE seq(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM seq WHERE i < 60000)
            \\INSERT INTO documents (uri, did, rkey, title, content, created_at, platform, base_path, has_publication, path, cover_image)
            \\SELECT 'at://doc/' || i, 'did:plc:x' || (i % 500), 'r' || i, 'title ' || i, '',
            \\  datetime('now', '-' || (i % 700) || ' days'), 'other', '', 0, '', ''
            \\FROM seq
        , .{});
        try w.exec(
            \\INSERT INTO document_tags SELECT 'at://doc/' || (80 * n), 'photography'
            \\FROM (WITH RECURSIVE s(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM s WHERE n < 750) SELECT n FROM s)
        , .{});

        // corpus-scale recommends (30k rows over 15k docs): the browse query
        // must NOT aggregate these per request — recommend_counts is
        // materialized below, and this bulk keeps the perf number honest
        // about that (with 19 rows the old per-request GROUP BY was free).
        try w.exec(
            \\WITH RECURSIVE seq(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM seq WHERE i < 30000)
            \\INSERT INTO recommends
            \\SELECT 'at://rec/bulk/' || i, 'did:plc:fan' || (i % 2000), 'rb' || i,
            \\  'at://doc/' || (2 * (i % 15000) + 1), '', ''
            \\FROM seq
        , .{});

        // four probe docs with chosen ages and recommend counts:
        //   A: 100d old, 6 recs  -> 3.33 - 3ln7  = -2.51
        //   C: 200d old, 12 recs -> 6.67 - 3ln13 = -1.03
        //   D:  40d old, 1 rec   -> 1.33 - 3ln2  = -0.75
        //   B:  10d old, 0 recs  -> 0.33
        // expected order: A, C, D, ... B somewhere after (fresh untagged-by-recs
        // docs can slot between D and B, but never above A/C/D)
        inline for (.{
            .{ "A", 100, 6 }, .{ "B", 10, 0 }, .{ "C", 200, 12 }, .{ "D", 40, 1 },
        }) |probe| {
            var sql_buf: [512]u8 = undefined;
            const ins = try std.fmt.bufPrintZ(&sql_buf, "INSERT INTO documents (uri, did, rkey, title, content, created_at, platform, base_path, has_publication, path, cover_image) " ++
                "VALUES ('at://probe/{s}', 'did:plc:probe{s}', 'rk{s}', 'probe {s}', '', datetime('now', '-{d} days'), 'other', '', 0, '', '')", .{ probe[0], probe[0], probe[0], probe[0], probe[1] });
            try w.exec(ins, .{});
            try w.exec("INSERT INTO document_tags VALUES ('at://probe/" ++ probe[0] ++ "', 'photography')", .{});
            var r: usize = 0;
            while (r < probe[2]) : (r += 1) {
                var rec_buf: [256]u8 = undefined;
                const rec = try std.fmt.bufPrintZ(&rec_buf, "INSERT INTO recommends VALUES ('at://rec/{s}/{d}', 'did:plc:fan{d}', 'rr{d}', 'at://probe/{s}', '', '')", .{ probe[0], r, r, r, probe[0] });
                try w.exec(rec, .{});
            }
        }
    }

    {
        const w = try zqlite.open(zpath.ptr, zqlite.OpenFlags.ReadWrite);
        defer w.close();
        try w.exec("INSERT INTO documents_fts (rowid, uri, title, content) SELECT rowid, uri, title, content FROM documents", .{});
        // mirrors LocalDb.createSchema's per-boot materialization
        try w.exec("CREATE TABLE recommend_counts (document_uri TEXT PRIMARY KEY, rc INTEGER NOT NULL)", .{});
        try w.exec("INSERT INTO recommend_counts SELECT document_uri, COUNT(DISTINCT did) FROM recommends GROUP BY document_uri", .{});
    }

    var ldb = db.LocalDb.init(std.testing.allocator, tio);
    for (&ldb.read_pool) |*slot| slot.* = try zqlite.open(zpath.ptr, zqlite.OpenFlags.ReadOnly);
    defer for (&ldb.read_pool) |*slot| {
        if (slot.*) |c| c.close();
        slot.* = null;
    };

    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();

    // warm once, then measure
    _ = try searchLocal(arena.allocator(), &ldb, "", "photography", null, null, null, .{ .include_undiscoverable = true });
    var best_us: i64 = std.math.maxInt(i64);
    var out: []const u8 = "";
    var run: usize = 0;
    while (run < 5) : (run += 1) {
        const t0 = std.Io.Timestamp.now(tio, .awake).toMicroseconds();
        out = try searchLocal(arena.allocator(), &ldb, "", "photography", null, null, null, .{ .include_undiscoverable = true });
        const us = std.Io.Timestamp.now(tio, .awake).toMicroseconds() - t0;
        if (us < best_us) best_us = us;
    }
    std.debug.print("\nlocal tag browse over 60k docs / 754 tagged: best of 5 = {d} us\n", .{best_us});

    // recommend-lifted order: A, C, D lead in that order; B trails all three
    const ia = std.mem.indexOf(u8, out, "at://probe/A").?;
    const ic = std.mem.indexOf(u8, out, "at://probe/C").?;
    const id_ = std.mem.indexOf(u8, out, "at://probe/D").?;
    const ib = std.mem.indexOf(u8, out, "at://probe/B") orelse out.len;
    try std.testing.expect(ia < ic and ic < id_);
    try std.testing.expect(id_ < ib);
    // and the three recommended probes beat every unrecommended doc
    const first_plain = std.mem.indexOf(u8, out, "at://doc/") orelse out.len;
    try std.testing.expect(id_ < first_plain);

    // --- FTS text within the tag: BM25 + recency, restricted to tagged docs ---
    var fts_best_us: i64 = std.math.maxInt(i64);
    var fts_out: []const u8 = "";
    var fts_run: usize = 0;
    while (fts_run < 5) : (fts_run += 1) {
        const t1 = std.Io.Timestamp.now(tio, .awake).toMicroseconds();
        fts_out = try searchLocal(arena.allocator(), &ldb, "probe", "photography", null, null, null, .{ .include_undiscoverable = true });
        const us = std.Io.Timestamp.now(tio, .awake).toMicroseconds() - t1;
        if (us < fts_best_us) fts_best_us = us;
    }
    std.debug.print("local fts+tag over 60k docs: best of 5 = {d} us\n", .{fts_best_us});

    // only the four probes match 'probe'; equal BM25 -> recency decides: B, D, A, C
    try std.testing.expect(std.mem.indexOf(u8, fts_out, "at://doc/") == null);
    const fb = std.mem.indexOf(u8, fts_out, "at://probe/B").?;
    const fd = std.mem.indexOf(u8, fts_out, "at://probe/D").?;
    const fa = std.mem.indexOf(u8, fts_out, "at://probe/A").?;
    const fc = std.mem.indexOf(u8, fts_out, "at://probe/C").?;
    try std.testing.expect(fb < fd and fd < fa and fa < fc);

    // regression: the FTS index must DRIVE the tag-filtered text query. A
    // plain join let SQLite drive from the tag index and probe FTS per row —
    // ~8s on the real corpus (2026-08-04). The first SCAN in the plan must
    // be the FTS table, never document_tags.
    {
        var plan = try ldb.query("EXPLAIN QUERY PLAN " ++ LOCAL_TAG_FTS_SQL, .{
            "probe", "photography", "", "", "", "", "", "", @as(usize, 50),
        });
        defer plan.deinit();
        while (plan.next()) |prow| {
            const detail = prow.text(3);
            if (std.mem.startsWith(u8, detail, "SCAN") or std.mem.startsWith(u8, detail, "SEARCH")) {
                try std.testing.expect(std.mem.indexOf(u8, detail, "VIRTUAL TABLE") != null);
                break;
            }
        }
    }
}

test "keyword doc search: candidate pass probes documents by rowid, never a scan" {
    // Regression (2026-08-11): ranking every FTS match through the uri primary
    // key read past every matching document's body — 14s for 'atproto' in
    // prod. Since schema v5 the guarantee is column order (content is LAST) +
    // rowid PK probes: the candidate pass must resolve d2 via INTEGER PRIMARY
    // KEY seeks, snippet() must only run for the top candidates, and the
    // planner must never fall back to a scan or automatic index.
    const zqlite = @import("zqlite");
    var threaded = std.Io.Threaded.init(std.testing.allocator, .{});
    defer threaded.deinit();
    const tio = threaded.io();

    var path_buf: [64]u8 = undefined;
    const zpath = std.fmt.bufPrintZ(&path_buf, "/tmp/docs-fts-plan-{d}.db", .{std.c.getpid()}) catch unreachable;
    const cleanup = struct {
        fn rm(p: []const u8) void {
            var b: [80]u8 = undefined;
            inline for (.{ "", "-wal", "-shm" }) |sfx| {
                const z = std.fmt.bufPrintZ(&b, "{s}{s}", .{ p, sfx }) catch return;
                _ = std.c.unlink(z.ptr);
            }
        }
    };
    cleanup.rm(zpath);
    defer cleanup.rm(zpath);

    {
        const w = try zqlite.open(zpath.ptr, zqlite.OpenFlags.Create | zqlite.OpenFlags.ReadWrite);
        defer w.close();
        try w.exec(
            \\CREATE TABLE documents (
            \\  uri TEXT PRIMARY KEY, did TEXT, rkey TEXT, title TEXT,
            \\  created_at TEXT, publication_uri TEXT, platform TEXT,
            \\  path TEXT, base_path TEXT, has_publication INTEGER,
            \\  cover_image TEXT, is_bridgyfed INTEGER, url_dead INTEGER, content TEXT
            \\)
        , .{});
        try w.exec("CREATE TABLE publications (uri TEXT PRIMARY KEY, did TEXT, rkey TEXT, name TEXT, description TEXT, base_path TEXT, platform TEXT)", .{});
        try w.exec("CREATE VIRTUAL TABLE documents_fts USING fts5(uri UNINDEXED, title, content, content='documents', content_rowid='rowid')", .{});
        try w.exec("CREATE VIRTUAL TABLE publications_fts USING fts5(uri UNINDEXED, name, description, base_path)", .{});
        try w.exec(
            \\INSERT INTO documents (uri, did, rkey, title, content, created_at, platform, base_path, has_publication, path, cover_image)
            \\VALUES ('at://doc/1', 'did:plc:a', 'r1', 'atproto notes', 'a body about atproto', datetime('now', '-3 days'), 'other', '', 0, '', ''),
            \\       ('at://doc/2', 'did:plc:b', 'r2', 'unrelated', 'nothing here', datetime('now', '-1 days'), 'other', '', 0, '', '')
        , .{});
        try w.exec("INSERT INTO documents_fts (rowid, uri, title, content) SELECT rowid, uri, title, content FROM documents", .{});
    }

    var ldb = db.LocalDb.init(std.testing.allocator, tio);
    for (&ldb.read_pool) |*slot| slot.* = try zqlite.open(zpath.ptr, zqlite.OpenFlags.ReadOnly);
    defer for (&ldb.read_pool) |*slot| {
        if (slot.*) |c| c.close();
        slot.* = null;
    };
    ldb.read_conn = ldb.read_pool[0];
    ldb.setReady(true);

    inline for (.{ LOCAL_DOCS_FTS_SQL, LOCAL_DOCS_FTS_PLATFORM_SQL }, .{ false, true }) |sql, has_platform| {
        var plan = if (has_platform)
            try ldb.query("EXPLAIN QUERY PLAN " ++ sql, .{ "\"atproto\"*", "\"atproto\"*", "other", "", "", "", "", @as(usize, 83), @as(usize, 83) })
        else
            try ldb.query("EXPLAIN QUERY PLAN " ++ sql, .{ "\"atproto\"*", "\"atproto\"*", "", "", "", "", @as(usize, 83), @as(usize, 83) });
        defer plan.deinit();
        var saw_rowid_probe = false;
        while (plan.next()) |prow| {
            const detail = prow.text(3);
            if (std.mem.indexOf(u8, detail, "SEARCH d2 USING INTEGER PRIMARY KEY (rowid=?)") != null) saw_rowid_probe = true;
            // the fat documents probe must never drive the candidate pass
            try std.testing.expect(std.mem.indexOf(u8, detail, "AUTOMATIC") == null);
            try std.testing.expect(std.mem.indexOf(u8, detail, "SCAN d2") == null);
        }
        try std.testing.expect(saw_rowid_probe);
    }

    // the bounded (prefilter) variant must also resolve its re-rank via rowid
    // seeks, with the bm25 phase staying inside the FTS index
    {
        var plan = try ldb.query("EXPLAIN QUERY PLAN " ++ LOCAL_DOCS_FTS_PREFILTER_SQL, .{ "\"atproto\"*", "\"atproto\"*", @as(usize, 2000), @as(usize, 83), @as(usize, 83) });
        defer plan.deinit();
        var saw_rowid_probe = false;
        while (plan.next()) |prow| {
            const detail = prow.text(3);
            if (std.mem.indexOf(u8, detail, "SEARCH d2 USING INTEGER PRIMARY KEY (rowid=?)") != null) saw_rowid_probe = true;
            try std.testing.expect(std.mem.indexOf(u8, detail, "AUTOMATIC") == null);
            try std.testing.expect(std.mem.indexOf(u8, detail, "SCAN d2") == null);
        }
        try std.testing.expect(saw_rowid_probe);
    }

    // end-to-end: the matching doc comes back with its snippet — and the
    // unfiltered path (which routes through the prefilter SQL) agrees with it
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const out = try searchLocal(arena.allocator(), &ldb, "atproto", null, null, null, null, .{ .include_undiscoverable = true, .show_labeled = true });
    try std.testing.expect(std.mem.indexOf(u8, out, "at://doc/1") != null);
    try std.testing.expect(std.mem.indexOf(u8, out, "at://doc/2") == null);
    // filtered path (author present) still uses the full-scan SQL and agrees
    const out_author = try searchLocal(arena.allocator(), &ldb, "atproto", null, null, null, "did:plc:a", .{ .include_undiscoverable = true, .show_labeled = true });
    try std.testing.expect(std.mem.indexOf(u8, out_author, "at://doc/1") != null);
}

test "overlay merge: fresh docs appear, overlay wins on uri, tombstones suppress" {
    const zqlite = @import("zqlite");
    var threaded = std.Io.Threaded.init(std.testing.allocator, .{});
    defer threaded.deinit();
    const tio = threaded.io();

    var path_buf: [64]u8 = undefined;
    const zpath = std.fmt.bufPrintZ(&path_buf, "/tmp/overlay-merge-{d}.db", .{std.c.getpid()}) catch unreachable;
    var opath_buf: [64]u8 = undefined;
    const opath = std.fmt.bufPrintZ(&opath_buf, "/tmp/overlay-merge-ov-{d}.db", .{std.c.getpid()}) catch unreachable;
    const cleanup = struct {
        fn rm(p: []const u8) void {
            var b: [80]u8 = undefined;
            inline for (.{ "", "-wal", "-shm" }) |sfx| {
                const z = std.fmt.bufPrintZ(&b, "{s}{s}", .{ p, sfx }) catch return;
                _ = std.c.unlink(z.ptr);
            }
        }
    };
    cleanup.rm(zpath);
    cleanup.rm(opath);
    defer cleanup.rm(zpath);
    defer cleanup.rm(opath);

    // snapshot: an old atproto doc, a doc the overlay will shadow with a newer
    // version, and a doc the overlay has tombstoned
    {
        const w = try zqlite.open(zpath.ptr, zqlite.OpenFlags.Create | zqlite.OpenFlags.ReadWrite);
        defer w.close();
        try w.exec(
            \\CREATE TABLE documents (
            \\  uri TEXT PRIMARY KEY, did TEXT, rkey TEXT, title TEXT,
            \\  created_at TEXT, publication_uri TEXT, platform TEXT,
            \\  path TEXT, base_path TEXT, has_publication INTEGER,
            \\  cover_image TEXT, is_bridgyfed INTEGER, url_dead INTEGER, content TEXT
            \\)
        , .{});
        try w.exec("CREATE TABLE publications (uri TEXT PRIMARY KEY, did TEXT, rkey TEXT, name TEXT, description TEXT, base_path TEXT, platform TEXT)", .{});
        try w.exec("CREATE VIRTUAL TABLE documents_fts USING fts5(uri UNINDEXED, title, content, content='documents', content_rowid='rowid')", .{});
        try w.exec("CREATE VIRTUAL TABLE publications_fts USING fts5(uri UNINDEXED, name, description, base_path)", .{});
        try w.exec(
            \\INSERT INTO documents (uri, did, rkey, title, content, created_at, platform, base_path, has_publication, path, cover_image)
            \\VALUES ('at://doc/old', 'did:plc:a', 'r1', 'atproto archives', 'old atproto writing', datetime('now', '-30 days'), 'other', '', 0, '', ''),
            \\       ('at://doc/shadowed', 'did:plc:b', 'r2', 'atproto draft stale', 'stale snapshot body', datetime('now', '-10 days'), 'other', '', 0, '', ''),
            \\       ('at://doc/gone', 'did:plc:c', 'r3', 'atproto deleted post', 'was deleted after the build', datetime('now', '-5 days'), 'other', '', 0, '', '')
        , .{});
        try w.exec("INSERT INTO documents_fts (rowid, uri, title, content) SELECT rowid, uri, title, content FROM documents", .{});
    }

    var ldb = db.LocalDb.init(std.testing.allocator, tio);
    for (&ldb.read_pool) |*slot| slot.* = try zqlite.open(zpath.ptr, zqlite.OpenFlags.ReadOnly);
    defer for (&ldb.read_pool) |*slot| {
        if (slot.*) |c| c.close();
        slot.* = null;
    };
    ldb.read_conn = ldb.read_pool[0];
    ldb.setReady(true);

    // overlay: one brand-new doc, one newer version of at://doc/shadowed,
    // one tombstone for at://doc/gone
    var ov = db.OverlayDb.init(std.testing.allocator, tio);
    try ov.openAt(opath);
    defer ov.deinit();
    const mkdoc = struct {
        fn d(uri: []const u8, title: []const u8, content: []const u8, created: []const u8) db.OverlayDb.DocRow {
            return .{
                .uri = uri, .did = "did:plc:ov", .rkey = "rk", .title = title, .content = content,
                .created_at = created, .publication_uri = "", .platform = "other", .path = "",
                .base_path = "", .has_publication = "0", .cover_image = "", .is_bridgyfed = "0",
            };
        }
    };
    try ov.upsert(mkdoc.d("at://doc/fresh", "fresh atproto post", "just written about atproto", "2026-08-12T00:00:00"));
    try ov.upsert(mkdoc.d("at://doc/shadowed", "atproto draft REVISED", "revised overlay body", "2026-08-12T00:00:00"));
    try ov.tombstone("at://doc/gone");

    db.setOverlayForTest(&ov);
    defer db.setOverlayForTest(null);

    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();

    // overlay ON: fresh appears, shadowed serves the overlay version, gone is suppressed
    const merged = try searchLocal(arena.allocator(), &ldb, "atproto", null, null, null, null, .{
        .include_undiscoverable = true, .show_labeled = true, .use_overlay = true,
    });
    try std.testing.expect(std.mem.indexOf(u8, merged, "at://doc/fresh") != null);
    try std.testing.expect(std.mem.indexOf(u8, merged, "at://doc/old") != null);
    try std.testing.expect(std.mem.indexOf(u8, merged, "REVISED") != null);
    try std.testing.expect(std.mem.indexOf(u8, merged, "stale snapshot body") == null);
    try std.testing.expect(std.mem.indexOf(u8, merged, "at://doc/gone") == null);
    // exactly one row for the shadowed uri
    try std.testing.expectEqual(@as(usize, 1), std.mem.count(u8, merged, "at://doc/shadowed"));

    // overlay OFF: snapshot-only serving unchanged (gate works)
    const plain = try searchLocal(arena.allocator(), &ldb, "atproto", null, null, null, null, .{
        .include_undiscoverable = true, .show_labeled = true, .use_overlay = false,
    });
    try std.testing.expect(std.mem.indexOf(u8, plain, "at://doc/gone") != null);
    try std.testing.expect(std.mem.indexOf(u8, plain, "at://doc/fresh") == null);
    try std.testing.expect(std.mem.indexOf(u8, plain, "stale") != null);
}

test "overlay merge: tag browse includes fresh tagged docs" {
    const zqlite = @import("zqlite");
    var threaded = std.Io.Threaded.init(std.testing.allocator, .{});
    defer threaded.deinit();
    const tio = threaded.io();

    var path_buf: [64]u8 = undefined;
    const zpath = std.fmt.bufPrintZ(&path_buf, "/tmp/overlay-tag-{d}.db", .{std.c.getpid()}) catch unreachable;
    var opath_buf: [64]u8 = undefined;
    const opath = std.fmt.bufPrintZ(&opath_buf, "/tmp/overlay-tag-ov-{d}.db", .{std.c.getpid()}) catch unreachable;
    const cleanup = struct {
        fn rm(p: []const u8) void {
            var b: [80]u8 = undefined;
            inline for (.{ "", "-wal", "-shm" }) |sfx| {
                const z = std.fmt.bufPrintZ(&b, "{s}{s}", .{ p, sfx }) catch return;
                _ = std.c.unlink(z.ptr);
            }
        }
    };
    cleanup.rm(zpath);
    cleanup.rm(opath);
    defer cleanup.rm(zpath);
    defer cleanup.rm(opath);

    {
        const w = try zqlite.open(zpath.ptr, zqlite.OpenFlags.Create | zqlite.OpenFlags.ReadWrite);
        defer w.close();
        try w.exec(
            \\CREATE TABLE documents (
            \\  uri TEXT PRIMARY KEY, did TEXT, rkey TEXT, title TEXT,
            \\  created_at TEXT, publication_uri TEXT, platform TEXT,
            \\  path TEXT, base_path TEXT, has_publication INTEGER,
            \\  cover_image TEXT, is_bridgyfed INTEGER, url_dead INTEGER, content TEXT
            \\)
        , .{});
        try w.exec("CREATE TABLE publications (uri TEXT PRIMARY KEY, did TEXT, rkey TEXT, name TEXT, description TEXT, base_path TEXT, platform TEXT)", .{});
        try w.exec("CREATE VIRTUAL TABLE documents_fts USING fts5(uri UNINDEXED, title, content, content='documents', content_rowid='rowid')", .{});
        try w.exec("CREATE VIRTUAL TABLE publications_fts USING fts5(uri UNINDEXED, name, description, base_path)", .{});
        try w.exec("CREATE TABLE document_tags (document_uri TEXT, tag TEXT, PRIMARY KEY (document_uri, tag))", .{});
        try w.exec("CREATE TABLE recommend_counts (document_uri TEXT PRIMARY KEY, rc INTEGER)", .{});
        try w.exec("INSERT INTO documents (uri, did, rkey, title, content, created_at, platform, base_path, has_publication, path, cover_image) VALUES ('at://doc/snap', 'did:plc:a', 'r1', 'snapshot zig post', 'zig writing', datetime('now', '-20 days'), 'other', '', 0, '', '')", .{});
        try w.exec("INSERT INTO documents_fts (rowid, uri, title, content) SELECT rowid, uri, title, content FROM documents", .{});
        try w.exec("INSERT INTO document_tags (document_uri, tag) VALUES ('at://doc/snap', 'zig')", .{});
    }

    var ldb = db.LocalDb.init(std.testing.allocator, tio);
    for (&ldb.read_pool) |*slot| slot.* = try zqlite.open(zpath.ptr, zqlite.OpenFlags.ReadOnly);
    defer for (&ldb.read_pool) |*slot| {
        if (slot.*) |c| c.close();
        slot.* = null;
    };
    ldb.read_conn = ldb.read_pool[0];
    ldb.setReady(true);

    var ov = db.OverlayDb.init(std.testing.allocator, tio);
    try ov.openAt(opath);
    defer ov.deinit();
    try ov.upsert(.{
        .uri = "at://doc/freshtag", .did = "did:plc:ov", .rkey = "rk", .title = "fresh zig post",
        .content = "new zig writing", .created_at = "2026-08-12T00:00:00", .publication_uri = "",
        .platform = "other", .path = "", .base_path = "", .has_publication = "0",
        .cover_image = "", .is_bridgyfed = "0", .tags = &.{"zig"},
    });
    db.setOverlayForTest(&ov);
    defer db.setOverlayForTest(null);

    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();

    // tag browse (empty query): fresh overlay doc merges in ahead of the old one
    const browse = try searchLocal(arena.allocator(), &ldb, "", "zig", null, null, null, .{
        .include_undiscoverable = true, .show_labeled = true, .use_overlay = true,
    });
    try std.testing.expect(std.mem.indexOf(u8, browse, "at://doc/freshtag") != null);
    try std.testing.expect(std.mem.indexOf(u8, browse, "at://doc/snap") != null);
    try std.testing.expect(std.mem.indexOf(u8, browse, "at://doc/freshtag").? < std.mem.indexOf(u8, browse, "at://doc/snap").?);

    // tag + text: overlay FTS restricted to the tag
    const tagfts = try searchLocal(arena.allocator(), &ldb, "zig", "zig", null, null, null, .{
        .include_undiscoverable = true, .show_labeled = true, .use_overlay = true,
    });
    try std.testing.expect(std.mem.indexOf(u8, tagfts, "at://doc/freshtag") != null);
}

test "empty-query browse is mode-independent (semantic author browse returned [])" {
    try std.testing.expectEqual(SearchMode.keyword, effectiveMode("", .semantic));
    try std.testing.expectEqual(SearchMode.keyword, effectiveMode("", .hybrid));
    try std.testing.expectEqual(SearchMode.keyword, effectiveMode("", .keyword));
    try std.testing.expectEqual(SearchMode.semantic, effectiveMode("atproto", .semantic));
    try std.testing.expectEqual(SearchMode.hybrid, effectiveMode("atproto", .hybrid));
}

test "search refuses to serve before the visibility set loads" {
    // Regression: the showInDiscover filter used to be a per-uri lookup that
    // returned "not hidden" whenever the replica lacked the row. Now the
    // policy is a set, and an unloaded set means we genuinely cannot answer.
    // Returning an empty result array here would be indistinguishable from
    // "nothing matched" — the caller could not tell a policy gap from a real
    // miss, which is precisely how the original leak stayed invisible.
    visibility.resetForTest();
    defer visibility.resetForTest();

    const alloc = std.testing.allocator;
    try std.testing.expectError(
        error.VisibilityNotReady,
        search(alloc, "anything", null, null, null, null, .keyword, .{}),
    );
}

test "an explicit include_undiscoverable request does not need the set" {
    // The opt-in path is asking for everything, so an unloaded set is not a
    // policy gap for it — it must not be blocked by the readiness gate.
    visibility.resetForTest();
    defer visibility.resetForTest();

    const alloc = std.testing.allocator;
    const err = search(alloc, "anything", null, null, null, null, .semantic, .{ .include_undiscoverable = true });
    // semantic is disabled in unit tests (no tpuf keys), so this returns the
    // "not available" body rather than VisibilityNotReady. The point is only
    // that the readiness gate did not fire.
    if (err) |body| {
        defer alloc.free(body);
    } else |e| {
        try std.testing.expect(e != error.VisibilityNotReady);
    }
}

test "undiscoverable documents are matched by publication identity, not document uri" {
    // The ghost case: turso and turbopuffer still carry a superseded rkey that
    // the current snapshot does not. Keyed on document uri this row was
    // invisible to the filter and leaked; keyed on the publication it does not.
    visibility.resetForTest();
    defer visibility.resetForTest();
    try visibility.installForTest(&.{
        .{ .uri = "at://did:plc:x/site.standard.publication/notes", .did = "did:plc:x", .base_path = "notes.example" },
    });

    try std.testing.expect(isUndiscoverableDoc("did:plc:x", "notes.example"));
    try std.testing.expect(!isUndiscoverableDoc("did:plc:x", "public.example"));
    try std.testing.expect(isUndiscoverablePublication("at://did:plc:x/site.standard.publication/notes"));
}
