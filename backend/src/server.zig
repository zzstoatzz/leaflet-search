const std = @import("std");
const Io = std.Io;
const http = std.http;
const mem = std.mem;
const json = std.json;
const Allocator = mem.Allocator;
const logfire = @import("logfire");
const zql = @import("zql");
const zat = @import("zat");
const db = @import("db.zig");
const ingest = @import("ingest.zig");
const metrics = @import("metrics.zig");
const search = @import("server/search.zig");
const documents = @import("server/documents.zig");
const visibility = @import("visibility.zig");
const dashboard = @import("server/dashboard.zig");
const recommended = @import("server/recommended.zig");
const curators = @import("server/curators.zig");
const recommenders = @import("server/recommenders.zig");
const subscribed = @import("server/subscribed.zig");
const subscribers = @import("server/subscribers.zig");
const wrapped_ep = @import("server/wrapped.zig");
const labeler = @import("labeler.zig");
const classifier = @import("ingest/classifier.zig");
const policy = @import("policy.zig");

pub const initRecommendedCache = recommended.init;
pub const initCuratorsCache = curators.init;
pub const initSubscribedCache = subscribed.init;
pub const initDashboardCache = dashboard.initCache;
pub const initTagsCache = TagsCache.init;
pub const initPopularCache = PopularCache.init;

const server_cache = @import("server/cache.zig");

/// /tags reads local-then-turso live; both stall during sync write bursts
/// (soak 2026-06-10). Tag aggregates tolerate minutes of staleness.
const TagsSlot = enum { all };

fn refreshTags(slot: TagsSlot, alloc: Allocator) anyerror![]const u8 {
    _ = slot;
    return try getTags(alloc);
}

const TagsCache = server_cache.WindowedJsonCache(TagsSlot, .{
    .name = "tags",
    .refresh = &refreshTags,
    .interval_ms = 300_000,
});

/// /popular aggregates search_events live on Turso per request. search_events
/// is a write-path table not carried in the frozen replica, so it can't be
/// served locally — but a 7-day popular-query window tolerates minutes of
/// staleness, so refresh it out of band like the leaderboards.
const PopularSlot = enum { all };

fn refreshPopular(slot: PopularSlot, alloc: Allocator) anyerror![]const u8 {
    _ = slot;
    return try getPopular(alloc, 5);
}

const PopularCache = server_cache.WindowedJsonCache(PopularSlot, .{
    .name = "popular",
    .refresh = &refreshPopular,
    .interval_ms = 300_000,
});

const HTTP_BUF_SIZE = 65536;
const QUERY_PARAM_BUF_SIZE = 64;
const SEARCH_MAX_LIMIT: usize = 40;
const SEARCH_MAX_OFFSET: usize = 1000;
const HYBRID_MAX_WINDOW: usize = 200;

fn microTimestamp(io: Io) i64 {
    return Io.Timestamp.now(io, .real).toMicroseconds();
}

pub fn handleConnection(stream: Io.net.Stream, io: Io, accepted_at: i64) void {
    defer stream.close(io);

    const queue_us = microTimestamp(io) - accepted_at;
    if (queue_us > 100_000) { // > 100ms
        logfire.warn("http.queue slow: {d}ms", .{@divTrunc(queue_us, 1000)});
    }

    var read_buffer: [HTTP_BUF_SIZE]u8 = undefined;
    var write_buffer: [HTTP_BUF_SIZE]u8 = undefined;

    var reader = stream.reader(io, &read_buffer);
    var writer = stream.writer(io, &write_buffer);

    var server = http.Server.init(&reader.interface, &writer.interface);

    while (true) {
        const recv_start = microTimestamp(io);
        var request = server.receiveHead() catch |err| {
            if (err != error.HttpConnectionClosing and err != error.EndOfStream) {
                logfire.debug("http receive error: {}", .{err});
            }
            return;
        };
        const recv_us = microTimestamp(io) - recv_start;
        const target = request.head.target;

        const req_span = logfire.span("http.request", .{
            .target = target,
            .queue_ms = @divTrunc(queue_us, 1000),
            .receive_ms = @divTrunc(recv_us, 1000),
        });

        handleRequest(&server, &request, io) catch |err| {
            logfire.err("request error: {}", .{err});
            req_span.end();
            return;
        };
        req_span.end();

        if (!request.head.keep_alive) return;
    }
}

fn handleRequest(server: *http.Server, request: *http.Server.Request, io: Io) !void {
    _ = server;
    const target = request.head.target;

    if (request.head.method == .OPTIONS) {
        try sendCorsHeaders(request, "");
        return;
    }

    const path = if (mem.indexOf(u8, target, "?")) |qi| target[0..qi] else target;

    if (mem.startsWith(u8, path, "/search")) {
        try handleSearch(request, target, io);
    } else if (mem.eql(u8, path, "/tags")) {
        try handleTags(request, target, io);
    } else if (mem.eql(u8, path, "/stats")) {
        try handleStats(request);
    } else if (mem.eql(u8, path, "/health")) {
        try sendJson(request, "{\"status\":\"ok\"}");
    } else if (mem.eql(u8, path, "/popular")) {
        try handlePopular(request, target, io);
    } else if (mem.eql(u8, path, "/dashboard")) {
        try handleDashboard(request);
    } else if (mem.eql(u8, path, "/api/dashboard")) {
        try handleDashboardApi(request, io);
    } else if (mem.eql(u8, path, "/api/timeline")) {
        try handleTimelineApi(request, target, io);
    } else if (mem.eql(u8, path, "/api/latency")) {
        try handleLatencyApi(request, target);
    } else if (mem.eql(u8, path, "/recommended")) {
        try handleRecommended(request, target, io);
    } else if (mem.eql(u8, path, "/recommended-by-top-authors")) {
        try handleRecommendedByTopAuthors(request, target, io);
    } else if (mem.eql(u8, path, "/curators")) {
        try handleCurators(request, target, io);
    } else if (mem.eql(u8, path, "/recommenders")) {
        try handleRecommenders(request, target);
    } else if (mem.eql(u8, path, "/subscribed")) {
        try handleSubscribed(request, target, io);
    } else if (mem.eql(u8, path, "/subscribers")) {
        try handleSubscribers(request, target);
    } else if (mem.eql(u8, path, "/wrapped")) {
        try handleWrapped(request, target, io);
    } else if (mem.eql(u8, path, "/document")) {
        try handleDocument(request, target);
    } else if (mem.startsWith(u8, path, "/similar")) {
        try handleSimilar(request, target, io);
    } else if (mem.eql(u8, path, "/activity")) {
        try handleActivity(request, io);
    } else if (mem.eql(u8, path, "/admin/backfill")) {
        try handleBackfill(request, target, io);
    } else if (mem.eql(u8, path, "/admin/reconcile-document")) {
        try handleReconcileDocument(request, target, io);
    } else if (mem.eql(u8, path, "/admin/label")) {
        try handleLabel(request, target);
    } else if (mem.eql(u8, path, "/api/labeler")) {
        try handleLabelerSummary(request);
    } else if (mem.eql(u8, path, "/snapshot")) {
        try handleSnapshot(request, io);
    } else {
        try sendNotFound(request);
    }
}

fn handleSearch(request: *http.Server.Request, target: []const u8, io: Io) !void {
    const start_time = microTimestamp(io);

    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena.deinit();
    const alloc = arena.allocator();

    const query = parseQueryParam(alloc, target, "q") catch "";
    const tag_filter = parseQueryParam(alloc, target, "tag") catch null;
    const platform_filter = parseQueryParam(alloc, target, "platform") catch null;
    const since_filter = parseQueryParam(alloc, target, "since") catch null;
    const author_param = parseQueryParam(alloc, target, "author") catch null;
    const mode_str = parseQueryParam(alloc, target, "mode") catch null;
    const mode = search.SearchMode.fromString(mode_str);
    const format = parseQueryParam(alloc, target, "format") catch "v1";
    const limit_str = parseQueryParam(alloc, target, "limit") catch null;
    const offset_str = parseQueryParam(alloc, target, "offset") catch null;
    const requested_limit = if (limit_str) |s| std.fmt.parseInt(usize, s, 10) catch 20 else 20;
    const limit = @min(@max(requested_limit, 1), SEARCH_MAX_LIMIT);
    const offset = if (offset_str) |s| std.fmt.parseInt(usize, s, 10) catch 0 else 0;

    if (offset > SEARCH_MAX_OFFSET) {
        try sendJson(request, "{\"error\":\"offset exceeds maximum of 1000\"}");
        return;
    }

    // resolve author param: if it's a handle (not a DID), resolve via AT Protocol
    const author_filter: ?[]const u8 = if (author_param) |ap| blk: {
        if (mem.startsWith(u8, ap, "did:")) break :blk ap;
        break :blk resolveHandle(alloc, ap, io) catch null;
    } else null;

    // record per-mode latency
    const timing_endpoint: metrics.timing.Endpoint = switch (mode) {
        .keyword => .search_keyword,
        .semantic => .search_semantic,
        .hybrid => .search_hybrid,
    };
    defer metrics.timing.record(timing_endpoint, start_time);

    // span attributes are now copied internally, safe to use arena strings
    const span = logfire.span("http.search", .{
        .query = query,
        .tag = tag_filter,
        .platform = platform_filter,
        .author = author_filter,
        .mode = @tagName(mode),
    });
    defer span.end();

    if (query.len == 0 and tag_filter == null and author_filter == null) {
        try sendJson(request, "{\"error\":\"enter a search term\"}");
        return;
    }

    const labeled_pref = parseQueryParam(alloc, target, "labeled") catch null;
    const show_labeled = labeled_pref != null and mem.eql(u8, labeled_pref.?, "show");
    // `include_undiscoverable=true` opts in to publications that set
    // preferences.showInDiscover=false. Default is exclusion. The old
    // `hidden=show` spelling is still accepted so existing callers keep
    // working, but it is undocumented and should not be used.
    const undiscoverable_pref = parseQueryParam(alloc, target, "include_undiscoverable") catch null;
    const legacy_hidden_pref = parseQueryParam(alloc, target, "hidden") catch null;
    const include_undiscoverable = (undiscoverable_pref != null and mem.eql(u8, undiscoverable_pref.?, "true")) or
        (legacy_hidden_pref != null and mem.eql(u8, legacy_hidden_pref.?, "show"));

    // Scoped to one identity per request. Without this the flag is a
    // corpus-wide switch: any anonymous caller reads every opted-out
    // publication, including other people's. Rejected rather than ignored —
    // a silently-dropped flag is how the original inconsistency stayed
    // invisible for months.
    if (include_undiscoverable and !visibility.optInIsScoped(author_filter)) {
        try request.respond(
            "{\"error\":\"include_undiscoverable requires an author filter — it opts you into one author's unlisted writing, not the whole corpus\"}",
            .{
                .status = .bad_request,
                .extra_headers = &.{.{ .name = "content-type", .value = "application/json" }},
            },
        );
        return;
    }

    // Retrieve the full requested prefix plus one policy-visible row. Paging
    // is then a pure slice of a stable ranking, and that extra row is the
    // evidence for hasMore.
    const result_window = std.math.add(usize, offset, limit + 1) catch {
        try sendJson(request, "{\"error\":\"pagination window is too large\"}");
        return;
    };
    if (mode == .hybrid and offset + limit > HYBRID_MAX_WINDOW) {
        try sendJson(request, "{\"error\":\"hybrid search supports the top 200 results\"}");
        return;
    }
    const raw_results = search.search(alloc, query, tag_filter, platform_filter, since_filter, author_filter, mode, .{
        .max_results = result_window,
        .show_labeled = show_labeled,
        .include_undiscoverable = include_undiscoverable,
    }) catch |err| {
        // Startup window: the visibility set has not loaded, so we cannot tell
        // which publications opted out. An empty result set would be
        // indistinguishable from "nothing matched", so answer honestly and let
        // the caller retry.
        if (err == error.VisibilityNotReady) {
            try request.respond(
                "{\"error\":\"search is still starting up, retry shortly\"}",
                .{
                    .status = .service_unavailable,
                    .extra_headers = &.{.{ .name = "content-type", .value = "application/json" }},
                },
            );
            return;
        }
        logfire.err("search failed: {}", .{err});
        metrics.stats.recordError();
        return err;
    };
    metrics.stats.recordSearch(query);
    logfire.counter("search.requests", 1);

    try sendResults(request, alloc, raw_results, format, query, @tagName(mode), limit, offset, .{
        .show_labeled = show_labeled,
        .include_undiscoverable = include_undiscoverable,
    });
}

pub const ResultPolicy = struct {
    show_labeled: bool = false,
    include_undiscoverable: bool = false,
};

/// THE serving policy choke point. Every endpoint that returns a result array
/// runs through here, via sendResults — a retrieval path cannot publish a row
/// without passing this.
///
/// Two policies, one pass over the serialized array:
///   - label policy: rows from bulk-generated accounts are dropped unless kept
///     or `show_labeled`; survivors are annotated so the UI can badge them.
///   - visibility policy: rows belonging to a publication that set
///     preferences.showInDiscover=false are dropped unless the caller opted in.
///
/// The retrieval paths ALSO filter per row. That is deliberate and is not
/// redundancy for its own sake: the per-row checks keep the bounded candidate
/// window from being spent on rows that will be dropped here, which is what
/// keeps pagination honest. This pass is the guarantee — if a path forgets its
/// row check, or a new path is added, nothing leaks. That split already
/// existed for the label policy (includeDid + this pass); visibility now uses
/// the same one instead of trusting ~14 call sites to remember.
///
/// Result sets are ≤tens of rows, so the reparse is noise.
fn applyResultPolicy(alloc: Allocator, body: []const u8, opts: ResultPolicy) ![]const u8 {
    const root = try json.parseFromSliceLeaky(json.Value, alloc, body, .{});
    if (root != .array) return body; // error payloads etc. pass through

    var out = json.Array.init(alloc);
    for (root.array.items) |item| {
        var result = item;
        const did = zat.json.getString(result, "did") orelse "";

        if (!opts.include_undiscoverable) {
            const base_path = zat.json.getString(result, "basePath") orelse "";
            const uri = zat.json.getString(result, "uri") orelse "";
            const is_pub = std.mem.eql(u8, zat.json.getString(result, "type") orelse "", "publication");
            const undiscoverable = if (is_pub)
                visibility.isUndiscoverablePub(uri)
            else
                visibility.isUndiscoverableDoc("", did, base_path);
            if (undiscoverable) continue;
        }

        if (did.len > 0 and classifier.isLabeledDid(did)) {
            const kept = policy.isKept(did);
            if (!kept and !opts.show_labeled) continue;
            try result.object.put(alloc, "labeled", .{ .bool = true });
            try result.object.put(alloc, "kept", .{ .bool = kept });
        }
        try out.append(result);
    }
    return json.Stringify.valueAlloc(alloc, json.Value{ .array = out }, .{});
}

/// Send a result array through the policy choke point, then the v1/v2 envelope.
/// Endpoints returning results must use this rather than sendJson directly —
/// that is what makes the policy impossible to forget.
fn sendResults(
    request: *http.Server.Request,
    alloc: Allocator,
    raw: []const u8,
    format: []const u8,
    query: []const u8,
    mode: []const u8,
    limit: usize,
    offset: usize,
    opts: ResultPolicy,
) !void {
    const results = applyResultPolicy(alloc, raw, opts) catch raw;
    if (mem.eql(u8, format, "v2")) {
        const wrapped = try wrapResponse(alloc, results, query, mode, limit, offset, false);
        try sendJson(request, wrapped);
    } else {
        // Always slice the bounded candidate array. The old `limit < 40`
        // shortcut leaked every candidate when limit was exactly 40 (or larger),
        // so `limit=40` could return 80+ document/base-path/publication rows.
        const paginated = try paginateJsonArray(alloc, results, limit, offset);
        try sendJson(request, paginated);
    }
}

fn handleTags(request: *http.Server.Request, target: []const u8, io: Io) !void {
    const start_time = microTimestamp(io);
    defer metrics.timing.record(.tags, start_time);

    const span = logfire.span("http.tags", .{});
    defer span.end();

    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena.deinit();
    const alloc = arena.allocator();

    const format = parseQueryParam(alloc, target, "format") catch "v1";
    // background-refreshed snapshot; live only before the first refresh.
    const tags = if (TagsCache.snapshot(.all, alloc) catch null) |body|
        body
    else
        try getTags(alloc);

    if (mem.eql(u8, format, "v2")) {
        const wrapped = try wrapResponse(alloc, tags, "", "tags", 100, 0, true);
        try sendJson(request, wrapped);
    } else {
        try sendJson(request, tags);
    }
}

fn handlePopular(request: *http.Server.Request, target: []const u8, io: Io) !void {
    const start_time = microTimestamp(io);
    defer metrics.timing.record(.popular, start_time);

    const span = logfire.span("http.popular", .{});
    defer span.end();

    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena.deinit();
    const alloc = arena.allocator();

    const format = parseQueryParam(alloc, target, "format") catch "v1";
    // background-refreshed snapshot; before the first fill, live-query and
    // degrade to [] on a transient Turso failure (handler-only, never cached).
    const popular = if (PopularCache.snapshot(.all, alloc) catch null) |body|
        body
    else
        getPopular(alloc, 5) catch "[]";

    if (mem.eql(u8, format, "v2")) {
        const wrapped = try wrapResponse(alloc, popular, "", "popular", 100, 0, true);
        try sendJson(request, wrapped);
    } else {
        try sendJson(request, popular);
    }
}

// --- tags/popular query logic ---

const TagJson = struct { tag: []const u8, count: i64 };
const PopularJson = struct { query: []const u8, count: i64 };

// Tag row matches both the Turso and local-SQLite query shape (SELECT tag, count …).
// Using a named struct + zql.Query.fromRow means adding/removing a column
// becomes a compile error rather than a runtime miscount.
const TagsQuery = zql.Query(dashboard.TAGS_SQL);
const TagsRow = struct { tag: []const u8, count: i64 };

fn getTags(alloc: Allocator) ![]const u8 {
    // try local SQLite first (faster)
    if (db.getLocalDb()) |local| {
        if (getTagsLocal(alloc, local)) |result| {
            return result;
        } else |_| {}
    }

    // fall back to Turso
    const c = db.getClient() orelse return error.NotInitialized;

    var output: std.Io.Writer.Allocating = .init(alloc);
    errdefer output.deinit();

    var res = c.query(TagsQuery.positional, &.{}) catch {
        try output.writer.writeAll("{\"error\":\"failed to fetch tags\"}");
        return try output.toOwnedSlice();
    };
    defer res.deinit();

    var jw: json.Stringify = .{ .writer = &output.writer };
    try jw.beginArray();
    for (res.rows) |row| {
        const r = TagsQuery.fromRow(TagsRow, row);
        try jw.write(TagJson{ .tag = r.tag, .count = r.count });
    }
    try jw.endArray();
    return try output.toOwnedSlice();
}

fn getTagsLocal(alloc: Allocator, local: *db.LocalDb) ![]const u8 {
    var output: std.Io.Writer.Allocating = .init(alloc);
    errdefer output.deinit();

    var rows = try local.query(dashboard.TAGS_SQL, .{});
    defer rows.deinit();

    var jw: json.Stringify = .{ .writer = &output.writer };
    try jw.beginArray();
    while (rows.next()) |row| {
        const r = TagsQuery.fromRow(TagsRow, row);
        try jw.write(TagJson{ .tag = r.tag, .count = r.count });
    }
    try jw.endArray();
    return try output.toOwnedSlice();
}

// Window for the popular-searches aggregation. 7 days strikes a balance:
// long enough for low-traffic queries to show up, short enough that test/
// seed traffic from weeks ago doesn't dominate. Seeded historical events
// fully age out 14 days after migration (see migration 013 for the seed
// strategy).
const POPULAR_WINDOW_SECS: i64 = 7 * 24 * 60 * 60;

// Aggregate distinct events in the window. Fast: idx_search_events_at
// makes the date filter range-scan, and the post-filter set is small
// (at the current ~50 searches/day rate, 7d ≈ 350 events to group).
const PopularQuery = zql.Query(
    \\SELECT query, COUNT(*) AS n
    \\FROM search_events
    \\WHERE at >= strftime('%s', 'now') - ?
    \\GROUP BY query
    \\ORDER BY n DESC, query
    \\LIMIT ?
);
const PopularRow = struct { query: []const u8, n: i64 };

fn getPopular(alloc: Allocator, limit: usize) ![]const u8 {
    const c = db.getClient() orelse return error.NotInitialized;

    var output: std.Io.Writer.Allocating = .init(alloc);
    errdefer output.deinit();

    var lim_buf: [8]u8 = undefined;
    const limit_str = std.fmt.bufPrint(&lim_buf, "{d}", .{limit}) catch "5";
    var win_buf: [16]u8 = undefined;
    const window_str = std.fmt.bufPrint(&win_buf, "{d}", .{POPULAR_WINDOW_SECS}) catch "604800";

    // Propagate query failures: the cache refresh path (refreshPopular) must
    // see the error so WindowedJsonCache keeps the previous good body rather
    // than poisoning it with [] for a full interval. Cold-start degradation to
    // [] is handled at the handler call site, before the first cache fill.
    var res = try c.query(PopularQuery.positional, &.{ window_str, limit_str });
    defer res.deinit();

    var jw: json.Stringify = .{ .writer = &output.writer };
    try jw.beginArray();
    for (res.rows) |row| {
        const r = PopularQuery.fromRow(PopularRow, row);
        try jw.write(PopularJson{ .query = r.query, .count = r.n });
    }
    try jw.endArray();
    return try output.toOwnedSlice();
}

/// Thin wrapper around `server/recommended.zig`. Parses query params,
/// pulls a cache snapshot (or live-queries for author-filtered views,
/// which can't reasonably be cached). Slices for pagination, returns JSON.
fn handleRecommended(request: *http.Server.Request, target: []const u8, io: Io) !void {
    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena.deinit();
    const alloc = arena.allocator();

    const start_time = microTimestamp(io);
    defer metrics.timing.record(.recommended, start_time);

    const span = logfire.span("http.recommended", .{});
    defer span.end();

    const limit_str = parseQueryParam(alloc, target, "limit") catch null;
    const offset_str = parseQueryParam(alloc, target, "offset") catch null;
    const since_str = parseQueryParam(alloc, target, "since") catch null;
    const sort_str = parseQueryParam(alloc, target, "sort") catch null;
    const author_param = parseQueryParam(alloc, target, "author") catch null;
    const curator_param = parseQueryParam(alloc, target, "curator") catch null;
    const limit: usize = if (limit_str) |s| std.fmt.parseInt(usize, s, 10) catch 20 else 20;
    const offset: usize = if (offset_str) |s| std.fmt.parseInt(usize, s, 10) catch 0 else 0;
    const window = recommended.Window.fromString(since_str);
    const sort = recommended.Sort.fromString(sort_str);
    span.setAttribute("window", window.slug());
    span.setAttribute("sort", sort.slug());

    // resolve author / curator → DID (accept either form, matches search.zig's pattern)
    const resolveActor = struct {
        fn run(alloc_: std.mem.Allocator, ap_: []const u8, io_: Io) ?[]const u8 {
            if (mem.startsWith(u8, ap_, "did:")) return ap_;
            return resolveHandle(alloc_, ap_, io_) catch null;
        }
    }.run;
    const author_did: ?[]const u8 = if (author_param) |ap| resolveActor(alloc, ap, io) else null;
    const curator_did: ?[]const u8 = if (curator_param) |cp| resolveActor(alloc, cp, io) else null;
    if (author_did) |d| span.setAttribute("author", d);
    if (curator_did) |d| span.setAttribute("curator", d);

    // curator + author both set: curator wins (more specific intent — "what
    // has X recommended" is narrower than "what has Y written"). Avoids
    // surprising empty results from intersecting two filters.
    const filter: recommended.Filter = .{
        .author_did = if (curator_did != null) null else author_did,
        .curator_did = curator_did,
    };

    var body: []u8 = undefined;
    if (filter.author_did != null or filter.curator_did != null) {
        // filtered queries bypass the cache — narrow scope means sub-100ms
        // Turso turnaround, and per-(filter, window, sort) cache slots
        // would explode the working set.
        span.setAttribute("cache", "bypass");
        body = try alloc.dupe(u8, try recommended.fetch(alloc, window, sort, filter));
    } else {
        var snapshot = try recommended.snapshot(sort, window, alloc);
        if (snapshot != null) {
            span.setAttribute("cache", "hit");
        } else {
            // cold fallback — refresh thread hasn't populated this slot yet.
            span.setAttribute("cache", "cold");
            snapshot = try alloc.dupe(u8, try recommended.fetch(alloc, window, sort, .{}));
        }
        body = snapshot.?;
    }

    const sliced = try recommended.sliceJson(alloc, body, limit, offset);
    try sendJson(request, sliced);
}

/// /recommended-by-top-authors — what have the network's top-N writers
/// (by all-time recommends received) themselves recommended in `since=`?
/// A transitive-taste signal distinct from raw popularity. Tunable resolution
/// via `pool=` (how many authors form the taste-pool — small = sharp focal,
/// large = broader consensus) and `since=` (day/week/month/year/all).
fn handleRecommendedByTopAuthors(request: *http.Server.Request, target: []const u8, io: Io) !void {
    const start_time = microTimestamp(io);
    defer metrics.timing.record(.recommended_top_authors, start_time);

    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena.deinit();
    const alloc = arena.allocator();

    const span = logfire.span("http.recommended_by_top_authors", .{});
    defer span.end();

    const limit_str = parseQueryParam(alloc, target, "limit") catch null;
    const offset_str = parseQueryParam(alloc, target, "offset") catch null;
    const since_str = parseQueryParam(alloc, target, "since") catch null;
    const pool_str = parseQueryParam(alloc, target, "pool") catch null;
    const limit: usize = if (limit_str) |s| std.fmt.parseInt(usize, s, 10) catch 10 else 10;
    const offset: usize = if (offset_str) |s| std.fmt.parseInt(usize, s, 10) catch 0 else 0;
    const window = recommended.Window.fromString(since_str);
    // Clamp pool to a sane range. <1 is meaningless; >500 is wider than the
    // active recommender set and just slows things down for no signal change.
    const pool_raw: i64 = if (pool_str) |s| std.fmt.parseInt(i64, s, 10) catch 10 else 10;
    const pool: i64 = @max(1, @min(500, pool_raw));
    span.setAttribute("window", window.slug());
    span.setAttribute("pool", pool);

    const body = try recommended.fetchTopAuthorCascade(alloc, window, pool);
    const sliced = try recommended.sliceJson(alloc, body, limit, offset);
    try sendJson(request, sliced);
}

/// /curators — leaderboard of recommenders (DIDs), windowed by `since=`.
/// Same cache + slice + cold-fallback shape as /recommended, minus the
/// sort + author dimensions (curators has one natural metric).
fn handleCurators(request: *http.Server.Request, target: []const u8, io: Io) !void {
    const start_time = microTimestamp(io);
    defer metrics.timing.record(.curators, start_time);

    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena.deinit();
    const alloc = arena.allocator();

    const span = logfire.span("http.curators", .{});
    defer span.end();

    const limit_str = parseQueryParam(alloc, target, "limit") catch null;
    const offset_str = parseQueryParam(alloc, target, "offset") catch null;
    const since_str = parseQueryParam(alloc, target, "since") catch null;
    const limit: usize = if (limit_str) |s| std.fmt.parseInt(usize, s, 10) catch 20 else 20;
    const offset: usize = if (offset_str) |s| std.fmt.parseInt(usize, s, 10) catch 0 else 0;
    const window = recommended.Window.fromString(since_str);
    span.setAttribute("window", window.slug());

    var snapshot = try curators.Cache.snapshot(window, alloc);
    if (snapshot != null) {
        span.setAttribute("cache", "hit");
    } else {
        span.setAttribute("cache", "cold");
        snapshot = try alloc.dupe(u8, try curators.fetch(alloc, window));
    }

    const sliced = try curators.sliceJson(alloc, snapshot.?, limit, offset);
    try sendJson(request, sliced);
}

/// /recommenders?document=<at-uri> — the recommender DIDs behind one doc's
/// count. Opens up the COUNT(DISTINCT did) aggregate so the UI can show who,
/// not just how many. Recency-ordered, deduped by did. No cache (per-document
/// keyspace is unbounded; the lookup is an indexed point query).
fn handleRecommenders(request: *http.Server.Request, target: []const u8) !void {
    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena.deinit();
    const alloc = arena.allocator();

    const span = logfire.span("http.recommenders", .{});
    defer span.end();

    const document = parseQueryParam(alloc, target, "document") catch null;
    if (document == null or document.?.len == 0) {
        try sendJson(request, "{\"error\":\"missing document param\"}");
        return;
    }
    span.setAttribute("document", document.?);

    const body = try recommenders.fetch(alloc, document.?);
    try sendJson(request, body);
}

/// /subscribed — subscription leaderboards. `view=publications|people`,
/// windowed by `since=`. Same cache + slice + cold-fallback shape as
/// /recommended. No author/curator filters — the two views ARE the two axes.
fn handleSubscribed(request: *http.Server.Request, target: []const u8, io: Io) !void {
    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena.deinit();
    const alloc = arena.allocator();

    const start_time = microTimestamp(io);
    defer metrics.timing.record(.subscribed, start_time);

    const span = logfire.span("http.subscribed", .{});
    defer span.end();

    const limit_str = parseQueryParam(alloc, target, "limit") catch null;
    const offset_str = parseQueryParam(alloc, target, "offset") catch null;
    const since_str = parseQueryParam(alloc, target, "since") catch null;
    const view_str = parseQueryParam(alloc, target, "view") catch null;
    const limit: usize = if (limit_str) |s| std.fmt.parseInt(usize, s, 10) catch 20 else 20;
    const offset: usize = if (offset_str) |s| std.fmt.parseInt(usize, s, 10) catch 0 else 0;
    const window = subscribed.Window.fromString(since_str);
    const view = subscribed.View.fromString(view_str);
    span.setAttribute("window", window.slug());
    span.setAttribute("view", view.slug());

    var snapshot = try subscribed.snapshot(view, window, alloc);
    if (snapshot != null) {
        span.setAttribute("cache", "hit");
    } else {
        span.setAttribute("cache", "cold");
        snapshot = try alloc.dupe(u8, try subscribed.fetch(alloc, view, window));
    }

    const sliced = try subscribed.sliceJson(alloc, snapshot.?, limit, offset);
    try sendJson(request, sliced);
}

/// /subscribers — the subscriber DIDs behind one publication's (or one
/// owner's) count. `?publication=<at-uri>` or `?did=<owner-did>`. Opens up the
/// COUNT(DISTINCT did) aggregate so the UI can show who, not just how many —
/// this is the "who's subscribed to me" surface. No cache (per-scope keyspace
/// is unbounded; the lookup is an indexed point query).
fn handleSubscribers(request: *http.Server.Request, target: []const u8) !void {
    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena.deinit();
    const alloc = arena.allocator();

    const span = logfire.span("http.subscribers", .{});
    defer span.end();

    const publication = parseQueryParam(alloc, target, "publication") catch null;
    const owner = parseQueryParam(alloc, target, "did") catch null;

    const scope: subscribers.Scope = if (publication != null and publication.?.len > 0)
        .{ .publication = publication.? }
    else if (owner != null and owner.?.len > 0)
        .{ .owner = owner.? }
    else {
        try sendJson(request, "{\"error\":\"missing publication or did param\"}");
        return;
    };
    switch (scope) {
        .publication => |v| span.setAttribute("publication", v),
        .owner => |v| span.setAttribute("owner", v),
    }

    const body = try subscribers.fetch(alloc, scope);
    try sendJson(request, body);
}

/// /wrapped?did=<did> or ?handle=<handle> — one identity's standing across the
/// standard.site graph (publisher / curator / reader lenses). Local-replica
/// only; resolves a handle to a DID first. No cache (per-DID keyspace).
fn handleWrapped(request: *http.Server.Request, target: []const u8, io: Io) !void {
    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena.deinit();
    const alloc = arena.allocator();

    const span = logfire.span("http.wrapped", .{});
    defer span.end();

    const did_param = parseQueryParam(alloc, target, "did") catch null;
    const handle_param = parseQueryParam(alloc, target, "handle") catch null;

    const did: ?[]const u8 = if (did_param != null and did_param.?.len > 0) blk: {
        break :blk did_param;
    } else if (handle_param != null and handle_param.?.len > 0) blk: {
        if (mem.startsWith(u8, handle_param.?, "did:")) break :blk handle_param;
        break :blk resolveHandle(alloc, handle_param.?, io) catch null;
    } else null;

    if (did == null or did.?.len == 0) {
        try sendJson(request, "{\"error\":\"missing or unresolvable did/handle\"}");
        return;
    }
    span.setAttribute("did", did.?);

    const body = try wrapped_ep.fetch(alloc, did.?);
    try sendJson(request, body);
}

fn parseQueryParam(alloc: std.mem.Allocator, target: []const u8, param: []const u8) ![]const u8 {
    // look for ?param= or &param=
    const patterns = [_][]const u8{ "?", "&" };
    for (patterns) |prefix| {
        var search_buf: [QUERY_PARAM_BUF_SIZE]u8 = undefined;
        const search_str = std.fmt.bufPrint(&search_buf, "{s}{s}=", .{ prefix, param }) catch continue;
        if (mem.indexOf(u8, target, search_str)) |idx| {
            const encoded = target[idx + search_str.len ..];
            const end = mem.indexOf(u8, encoded, "&") orelse encoded.len;
            const query_encoded = encoded[0..end];
            const buf = try alloc.dupe(u8, query_encoded);
            // decode + as space (form-urlencoded), then percent-decode
            for (buf) |*c| {
                if (c.* == '+') c.* = ' ';
            }
            return std.Uri.percentDecodeInPlace(buf);
        }
    }
    return error.NotFound;
}

fn handleStats(request: *http.Server.Request) !void {
    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena.deinit();
    const alloc = arena.allocator();

    const db_stats = metrics.stats.getStats();
    const all_timing = metrics.timing.getAllStats();

    var output: std.Io.Writer.Allocating = .init(alloc);
    errdefer output.deinit();

    var jw: json.Stringify = .{ .writer = &output.writer };
    try jw.beginObject();

    // db stats
    try jw.objectField("documents");
    try jw.write(db_stats.documents);
    try jw.objectField("publications");
    try jw.write(db_stats.publications);
    try jw.objectField("embeddings");
    try jw.write(db_stats.embeddings);
    try jw.objectField("searches");
    try jw.write(db_stats.searches);
    try jw.objectField("errors");
    try jw.write(db_stats.errors);
    try jw.objectField("started_at");
    try jw.write(db_stats.started_at);
    try jw.objectField("cache_hits");
    try jw.write(db_stats.cache_hits);
    try jw.objectField("cache_misses");
    try jw.write(db_stats.cache_misses);

    // timing stats per endpoint
    try jw.objectField("timing");
    try jw.beginObject();
    inline for (@typeInfo(metrics.timing.Endpoint).@"enum".fields, 0..) |field, i| {
        const t = all_timing[i];
        try jw.objectField(field.name);
        try jw.beginObject();
        try jw.objectField("count");
        try jw.write(t.count);
        try jw.objectField("avg_ms");
        try jw.write(t.avg_ms);
        try jw.objectField("p50_ms");
        try jw.write(t.p50_ms);
        try jw.objectField("p95_ms");
        try jw.write(t.p95_ms);
        try jw.objectField("p99_ms");
        try jw.write(t.p99_ms);
        try jw.objectField("max_ms");
        try jw.write(t.max_ms);
        try jw.endObject();
    }
    try jw.endObject();

    try jw.endObject();

    try sendJson(request, try output.toOwnedSlice());
}

/// Only one backfill may run at a time: each one writes to turso, and N
/// concurrent admin backfills saturating turso is a self-inflicted outage
/// (the 2026-06-10 purge lesson). Excess requests get 409.
var backfill_busy = std.atomic.Value(bool).init(false);

fn backfillWorker(io: Io, did: []u8, collection: ?[]u8) void {
    defer backfill_busy.store(false, .release);
    defer std.heap.page_allocator.free(did);
    defer if (collection) |c| std.heap.page_allocator.free(c);

    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena.deinit();
    _ = ingest.ingester.backfillRepo(arena.allocator(), io, did, collection) catch |err| {
        logfire.warn("backfill: {s} failed: {s}", .{ did, @errorName(err) });
    };
}

/// On-demand backfill of a single repo, bypassing the ingester's serial resync queue.
/// `POST /admin/backfill?did=<did>[&collection=<nsid>]`. Pulls every record of
/// our collections straight from the author's PDS through the normal
/// extract+index path. Guarded by BACKFILL_TOKEN (?token=) when that env is set.
/// Responds 202 and runs in the background (fly's proxy drops long-held
/// connections; big repos take minutes). `&sync=1` keeps the old blocking
/// behavior and returns counts. Completion signal either way is the logfire
/// line `backfill: <did> done`.
// Emit (or negate) a labeler account-label. Gated by BACKFILL_TOKEN (same admin
// secret as /admin/backfill). To retract a label, pass neg=1 with the same
// did+val — per the atproto spec, consumers stop hydrating the original.
//   GET /admin/label?token=…&did=did:plc:…&val=bulk-mirror&neg=0
fn handleLabel(request: *http.Server.Request, target: []const u8) !void {
    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena.deinit();
    const alloc = arena.allocator();

    const json_hdr: []const http.Header = &.{.{ .name = "content-type", .value = "application/json" }};

    if (std.c.getenv("BACKFILL_TOKEN")) |tok_c| {
        const provided = parseQueryParam(alloc, target, "token") catch "";
        if (!mem.eql(u8, provided, std.mem.span(tok_c))) {
            try request.respond("{\"error\":\"unauthorized\"}", .{ .status = .unauthorized, .extra_headers = json_hdr });
            return;
        }
    }

    const did = parseQueryParam(alloc, target, "did") catch {
        try request.respond("{\"error\":\"missing did param\"}", .{ .status = .bad_request, .extra_headers = json_hdr });
        return;
    };
    const val = parseQueryParam(alloc, target, "val") catch labeler.LABEL_BULK_GENERATED;
    const neg = blk: {
        const v = parseQueryParam(alloc, target, "neg") catch break :blk false;
        break :blk mem.eql(u8, v, "1") or mem.eql(u8, v, "true");
    };

    const seq = labeler.emit(did, val, neg) catch |err| {
        const msg = if (err == error.NotConfigured)
            "{\"error\":\"labeler not configured (LABELER_DID/LABELER_SECRET_KEY unset)\"}"
        else
            "{\"error\":\"emit failed\"}";
        try request.respond(msg, .{ .status = .internal_server_error, .extra_headers = json_hdr });
        return;
    };

    // a negation is the operator overruling the model — keep the classifier's
    // book in sync so /labels reflects the retraction and the DID never re-flags.
    if (neg and mem.eql(u8, val, labeler.LABEL_BULK_GENERATED)) classifier.markNegated(did);

    const body = try std.fmt.allocPrint(alloc, "{{\"ok\":true,\"seq\":{d},\"did\":\"{s}\",\"val\":\"{s}\",\"neg\":{}}}", .{ seq, did, val, neg });
    try request.respond(body, .{ .extra_headers = json_hdr });
}

// Read-only labeler summary for the /labels heads-up page (counts by state +
// every decided author with score + title patterns). Public — the data is the
// labels we already publish.
fn handleLabelerSummary(request: *http.Server.Request) !void {
    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena.deinit();
    const body = classifier.writeSummaryJson(arena.allocator()) catch {
        try sendJson(request, "{\"counts\":{},\"authors\":[]}");
        return;
    };
    try sendJson(request, body);
}

fn handleBackfill(request: *http.Server.Request, target: []const u8, io: Io) !void {
    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena.deinit();
    const alloc = arena.allocator();

    if (std.c.getenv("BACKFILL_TOKEN")) |tok_c| {
        const expected = std.mem.span(tok_c);
        const provided = parseQueryParam(alloc, target, "token") catch "";
        if (!mem.eql(u8, provided, expected)) {
            try request.respond("{\"error\":\"unauthorized\"}", .{
                .status = .unauthorized,
                .extra_headers = &.{.{ .name = "content-type", .value = "application/json" }},
            });
            return;
        }
    }

    const did = parseQueryParam(alloc, target, "did") catch {
        try request.respond("{\"error\":\"missing did param\"}", .{
            .status = .bad_request,
            .extra_headers = &.{.{ .name = "content-type", .value = "application/json" }},
        });
        return;
    };
    const collection: ?[]const u8 = parseQueryParam(alloc, target, "collection") catch null;

    const sync_mode = blk: {
        const v = parseQueryParam(alloc, target, "sync") catch break :blk false;
        break :blk mem.eql(u8, v, "1");
    };

    if (!sync_mode) {
        if (backfill_busy.cmpxchgStrong(false, true, .acq_rel, .acquire) != null) {
            try request.respond("{\"error\":\"a backfill is already running\"}", .{
                .status = .conflict,
                .extra_headers = &.{.{ .name = "content-type", .value = "application/json" }},
            });
            return;
        }

        const did_owned = std.heap.page_allocator.dupe(u8, did) catch {
            backfill_busy.store(false, .release);
            return error.OutOfMemory;
        };
        const coll_owned: ?[]u8 = if (collection) |c|
            std.heap.page_allocator.dupe(u8, c) catch {
                std.heap.page_allocator.free(did_owned);
                backfill_busy.store(false, .release);
                return error.OutOfMemory;
            }
        else
            null;

        const thread = std.Thread.spawn(.{}, backfillWorker, .{ io, did_owned, coll_owned }) catch {
            std.heap.page_allocator.free(did_owned);
            if (coll_owned) |c| std.heap.page_allocator.free(c);
            backfill_busy.store(false, .release);
            try request.respond("{\"error\":\"failed to start backfill\"}", .{
                .status = .internal_server_error,
                .extra_headers = &.{.{ .name = "content-type", .value = "application/json" }},
            });
            return;
        };
        thread.detach();

        const body = try std.fmt.allocPrint(alloc, "{{\"did\":\"{s}\",\"status\":\"accepted\"}}", .{did});
        try request.respond(body, .{
            .status = .accepted,
            .extra_headers = &.{.{ .name = "content-type", .value = "application/json" }},
        });
        return;
    }

    const counts = ingest.ingester.backfillRepo(alloc, io, did, collection) catch |err| {
        const body = try std.fmt.allocPrint(alloc, "{{\"error\":\"backfill failed: {s}\"}}", .{@errorName(err)});
        try request.respond(body, .{
            .status = .internal_server_error,
            .extra_headers = &.{.{ .name = "content-type", .value = "application/json" }},
        });
        return;
    };

    const body = try std.fmt.allocPrint(
        alloc,
        "{{\"did\":\"{s}\",\"documents\":{d},\"publications\":{d},\"recommends\":{d},\"subscriptions\":{d},\"skipped\":{d}}}",
        .{ did, counts.documents, counts.publications, counts.recommends, counts.subscriptions, counts.skipped },
    );
    try sendJson(request, body);
}

/// Apply one pre-audited site.standard.document ledger item. This endpoint is
/// intentionally synchronous and item-scoped: the operator controls pacing,
/// while the backend re-fetches the source and enforces the expected CID at
/// the last possible moment before an upsert. Deletes require a fresh,
/// definitive source 400/404.
fn handleReconcileDocument(request: *http.Server.Request, target: []const u8, io: Io) !void {
    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena.deinit();
    const alloc = arena.allocator();
    const json_hdr: []const http.Header = &.{.{ .name = "content-type", .value = "application/json" }};

    const tok_c = std.c.getenv("BACKFILL_TOKEN") orelse {
        try request.respond("{\"error\":\"reconciliation endpoint disabled\"}", .{
            .status = .service_unavailable,
            .extra_headers = json_hdr,
        });
        return;
    };
    const provided = parseQueryParam(alloc, target, "token") catch "";
    if (!mem.eql(u8, provided, std.mem.span(tok_c))) {
        try request.respond("{\"error\":\"unauthorized\"}", .{ .status = .unauthorized, .extra_headers = json_hdr });
        return;
    }

    const did = parseQueryParam(alloc, target, "did") catch {
        try request.respond("{\"error\":\"missing did\"}", .{ .status = .bad_request, .extra_headers = json_hdr });
        return;
    };
    const collection = parseQueryParam(alloc, target, "collection") catch {
        try request.respond("{\"error\":\"missing collection\"}", .{ .status = .bad_request, .extra_headers = json_hdr });
        return;
    };
    const rkey = parseQueryParam(alloc, target, "rkey") catch {
        try request.respond("{\"error\":\"missing rkey\"}", .{ .status = .bad_request, .extra_headers = json_hdr });
        return;
    };
    const pds = parseQueryParam(alloc, target, "pds") catch {
        try request.respond("{\"error\":\"missing pds\"}", .{ .status = .bad_request, .extra_headers = json_hdr });
        return;
    };
    const expected_cid: ?[]const u8 = parseQueryParam(alloc, target, "expected_cid") catch null;
    const observe_classifier = blk: {
        const value = parseQueryParam(alloc, target, "observe_classifier") catch break :blk false;
        break :blk mem.eql(u8, value, "1") or mem.eql(u8, value, "true");
    };
    const action_text = parseQueryParam(alloc, target, "action") catch "";
    const action: ingest.ingester.TargetedAction = if (mem.eql(u8, action_text, "upsert"))
        .upsert
    else if (mem.eql(u8, action_text, "delete"))
        .delete
    else {
        try request.respond("{\"error\":\"action must be upsert or delete\"}", .{ .status = .bad_request, .extra_headers = json_hdr });
        return;
    };

    const result = ingest.ingester.applyDocumentReconciliation(
        alloc, io, did, collection, rkey, pds, expected_cid, action, observe_classifier,
    ) catch |err| {
        const body = try std.fmt.allocPrint(alloc, "{{\"error\":\"{s}\"}}", .{@errorName(err)});
        try request.respond(body, .{ .status = .service_unavailable, .extra_headers = json_hdr });
        return;
    };
    const cid = result.source_cid orelse "";
    const body = try std.fmt.allocPrint(
        alloc,
        "{{\"outcome\":\"{s}\",\"source_cid\":\"{s}\"}}",
        .{ @tagName(result.outcome), cid },
    );
    try request.respond(body, .{ .extra_headers = json_hdr });
}

/// Serve the live replica's manifest sidecar (build id, sha256, watermark,
/// counts). This is the watchdog's snapshot-age signal and a human's
/// "what is prod actually serving" answer. 404 until the first adoption.
fn handleSnapshot(request: *http.Server.Request, io: Io) !void {
    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena.deinit();
    const alloc = arena.allocator();

    const live = if (std.c.getenv("LOCAL_DB_PATH")) |p| std.mem.span(p) else "/data/local.db";
    const sidecar = try std.fmt.allocPrint(alloc, "{s}.manifest.json", .{live});

    const file = Io.Dir.openFileAbsolute(io, sidecar, .{}) catch {
        try request.respond("{\"error\":\"no snapshot manifest (pre-adoption replica)\"}", .{
            .status = .not_found,
            .extra_headers = &.{.{ .name = "content-type", .value = "application/json" }},
        });
        return;
    };
    defer file.close(io);

    var buf: [16 * 1024]u8 = undefined;
    const n = file.readStreaming(io, &.{&buf}) catch |err| switch (err) {
        error.EndOfStream => 0,
        else => return err,
    };
    try sendJson(request, try alloc.dupe(u8, buf[0..n]));
}

fn sendJson(request: *http.Server.Request, body: []const u8) !void {
    try request.respond(body, .{
        .status = .ok,
        .extra_headers = &.{
            .{ .name = "content-type", .value = "application/json" },
            .{ .name = "access-control-allow-origin", .value = "*" },
            .{ .name = "access-control-allow-methods", .value = "GET, OPTIONS" },
            .{ .name = "access-control-allow-headers", .value = "content-type" },
        },
    });
}

fn sendCorsHeaders(request: *http.Server.Request, body: []const u8) !void {
    // public read-only API — wildcard origin, no credentials needed.
    try request.respond(body, .{
        .status = .no_content,
        .extra_headers = &.{
            .{ .name = "access-control-allow-origin", .value = "*" },
            .{ .name = "access-control-allow-methods", .value = "GET, OPTIONS" },
            .{ .name = "access-control-allow-headers", .value = "content-type" },
        },
    });
}

fn sendNotFound(request: *http.Server.Request) !void {
    try request.respond("{\"error\":\"not found\"}", .{
        .status = .not_found,
        .extra_headers = &.{
            .{ .name = "content-type", .value = "application/json" },
            .{ .name = "access-control-allow-origin", .value = "*" },
        },
    });
}

fn handleDashboardApi(request: *http.Server.Request, io: Io) !void {
    const start_time = microTimestamp(io);
    defer metrics.timing.record(.dashboard, start_time);

    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena.deinit();
    const alloc = arena.allocator();

    // background-refreshed snapshot — the page never waits on a live turso
    // query. Falls through to a live fetch only before the first refresh.
    if (dashboard.ApiCache.snapshot(.main, alloc) catch null) |body| {
        try sendJson(request, body);
        return;
    }

    const data = dashboard.fetch(alloc) catch {
        try sendJson(request, "{\"error\":\"failed to fetch dashboard data\"}");
        return;
    };

    const json_response = dashboard.toJson(alloc, data) catch {
        try sendJson(request, "{\"error\":\"failed to serialize dashboard data\"}");
        return;
    };

    try sendJson(request, json_response);
}

fn handleTimelineApi(request: *http.Server.Request, target: []const u8, io: Io) !void {
    const start_time = microTimestamp(io);
    defer metrics.timing.record(.timeline, start_time);

    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena.deinit();
    const alloc = arena.allocator();

    const range_str = parseQueryParam(alloc, target, "range") catch "30d";
    const range = dashboard.TimelineRange.fromString(range_str);

    const field_str = parseQueryParam(alloc, target, "field") catch "indexed";
    const field = dashboard.TimelineField.fromString(field_str);

    // background-refreshed snapshot; live fetch only before the first refresh.
    if (dashboard.TimelineCache.snapshot(dashboard.TimelineSlot.from(range, field), alloc) catch null) |body| {
        try sendJson(request, body);
        return;
    }

    const json_response = dashboard.fetchTimeline(alloc, range, field) catch {
        try sendJson(request, "{\"error\":\"failed to fetch timeline\"}");
        return;
    };

    try sendJson(request, json_response);
}

fn handleLatencyApi(request: *http.Server.Request, target: []const u8) !void {
    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena.deinit();
    const alloc = arena.allocator();

    const range_str = parseQueryParam(alloc, target, "range") catch "24h";
    const range = metrics.timing.LatencyRange.fromString(range_str);

    const json_response = dashboard.fetchLatency(alloc, range) catch {
        try sendJson(request, "{\"error\":\"failed to fetch latency\"}");
        return;
    };

    try sendJson(request, json_response);
}

fn getDashboardUrl() []const u8 {
    return if (std.c.getenv("DASHBOARD_URL")) |p| std.mem.span(p) else "https://pub-search.waow.tech/stats";
}

fn handleDashboard(request: *http.Server.Request) !void {
    const dashboard_url = getDashboardUrl();
    try request.respond("", .{
        .status = .moved_permanently,
        .extra_headers = &.{
            .{ .name = "location", .value = dashboard_url },
        },
    });
}

fn handleDocument(request: *http.Server.Request, target: []const u8) !void {
    const json_hdr: []const http.Header = &.{.{ .name = "content-type", .value = "application/json" }};
    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena.deinit();
    const alloc = arena.allocator();

    const raw = parseQueryParam(alloc, target, "uri") catch {
        try request.respond("{\"error\":\"missing uri parameter (comma-separated AT-URIs)\"}", .{ .status = .bad_request, .extra_headers = json_hdr });
        return;
    };
    const uris = documents.splitUris(alloc, raw) catch {
        try request.respond("{\"error\":\"too many uris (max 25)\"}", .{ .status = .bad_request, .extra_headers = json_hdr });
        return;
    };
    if (uris.len == 0) {
        try request.respond("{\"error\":\"missing uri parameter (comma-separated AT-URIs)\"}", .{ .status = .bad_request, .extra_headers = json_hdr });
        return;
    }

    const span = logfire.span("http.document", .{ .count = uris.len });
    defer span.end();

    const doc_undiscoverable_pref = parseQueryParam(alloc, target, "include_undiscoverable") catch null;
    const doc_include_undiscoverable = doc_undiscoverable_pref != null and
        mem.eql(u8, doc_undiscoverable_pref.?, "true");

    // One identity per request, same rule as /search. A batch of 25 arbitrary
    // uris with the flag set would be a small enumeration of other people's
    // opted-out documents.
    if (doc_include_undiscoverable and !visibility.urisShareOneDid(uris)) {
        try request.respond(
            "{\"error\":\"include_undiscoverable requires every uri to belong to the same repo\"}",
            .{
                .status = .bad_request,
                .extra_headers = json_hdr,
            },
        );
        return;
    }

    // Same startup gate as search: without the visibility set we cannot tell
    // which publications opted out, and reporting every uri as "missing" would
    // read as "these do not exist".
    if (!doc_include_undiscoverable and !visibility.isLoaded()) {
        try request.respond(
            "{\"error\":\"document fetch is still starting up, retry shortly\"}",
            .{ .status = .service_unavailable, .extra_headers = json_hdr },
        );
        return;
    }

    const body = documents.fetch(alloc, uris, doc_include_undiscoverable) catch {
        try request.respond("{\"error\":\"replica not ready, retry shortly\"}", .{ .status = .service_unavailable, .extra_headers = json_hdr });
        return;
    };
    try sendJson(request, body);
}

fn handleSimilar(request: *http.Server.Request, target: []const u8, io: Io) !void {
    const start_time = microTimestamp(io);
    defer metrics.timing.record(.similar, start_time);

    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena.deinit();
    const alloc = arena.allocator();

    const uri = parseQueryParam(alloc, target, "uri") catch {
        try sendJson(request, "{\"error\":\"missing uri parameter\"}");
        return;
    };

    const format = parseQueryParam(alloc, target, "format") catch "v1";

    // No opt-in here. /similar returns neighbours from any author, so the flag
    // could not be scoped to one identity — it would be exactly the
    // corpus-wide switch we are removing from /search. Rejected loudly rather
    // than ignored.
    if (parseQueryParam(alloc, target, "include_undiscoverable")) |pref| {
        if (mem.eql(u8, pref, "true")) {
            try request.respond(
                "{\"error\":\"include_undiscoverable is not supported on /similar — neighbours come from every author, so the opt-in cannot be scoped to one identity. Use /search?author=\"}",
                .{
                    .status = .bad_request,
                    .extra_headers = &.{.{ .name = "content-type", .value = "application/json" }},
                },
            );
            return;
        }
    } else |_| {}
    const similar_include_undiscoverable = false;

    if (!similar_include_undiscoverable and !visibility.isLoaded()) {
        try request.respond(
            "{\"error\":\"similar is still starting up, retry shortly\"}",
            .{
                .status = .service_unavailable,
                .extra_headers = &.{.{ .name = "content-type", .value = "application/json" }},
            },
        );
        return;
    }

    // span attributes are copied internally, safe to use arena strings
    const span = logfire.span("http.similar", .{ .uri = uri });
    defer span.end();

    const results = search.findSimilar(alloc, uri, 5) catch {
        if (mem.eql(u8, format, "v2")) {
            try sendJson(request, "{\"results\":[],\"total\":0,\"hasMore\":false}");
        } else {
            try sendJson(request, "[]");
        }
        return;
    };

    // /similar used to send results straight out, bypassing policy entirely:
    // an opted-out (or labeled) document was one "related documents" hop away
    // from any discoverable neighbour. It goes through the choke point now.
    try sendResults(request, alloc, results, format, "", "similar", 20, 0, .{
        .include_undiscoverable = similar_include_undiscoverable,
    });
}

/// Wrap a result prefix in the v2 page envelope. Search prefixes pass
/// total_known=false rather than presenting the prefix length as a corpus
/// count; bounded endpoints such as tags can opt into their exact total.
fn wrapResponse(alloc: Allocator, array_json: []const u8, query: []const u8, mode: []const u8, limit: usize, offset: usize, total_known: bool) ![]const u8 {
    // parse the array to count items and apply pagination
    const parsed = json.parseFromSlice(json.Value, alloc, array_json, .{}) catch {
        return array_json; // fallback to raw if parse fails
    };
    defer parsed.deinit();

    const items = switch (parsed.value) {
        .array => |arr| arr.items,
        else => return array_json,
    };

    const total = items.len;
    const start = @min(offset, total);
    const end = @min(start + limit, total);
    const has_more = end < total;

    var output: std.Io.Writer.Allocating = .init(alloc);
    errdefer output.deinit();

    var jw: json.Stringify = .{ .writer = &output.writer };
    try jw.beginObject();

    try jw.objectField("results");
    try jw.beginArray();
    for (items[start..end]) |item| {
        try jw.write(item);
    }
    try jw.endArray();

    try jw.objectField("total");
    if (total_known) {
        try jw.write(total);
    } else {
        try jw.write(null);
    }

    try jw.objectField("hasMore");
    try jw.write(has_more);

    try jw.objectField("nextOffset");
    if (has_more) {
        try jw.write(end);
    } else {
        try jw.write(null);
    }

    if (query.len > 0) {
        try jw.objectField("query");
        try jw.write(query);
    }

    if (mode.len > 0) {
        try jw.objectField("mode");
        try jw.write(mode);
    }

    try jw.endObject();
    return try output.toOwnedSlice();
}

/// Apply pagination to a JSON array (for v1 format with limit/offset)
fn paginateJsonArray(alloc: Allocator, array_json: []const u8, limit: usize, offset: usize) ![]const u8 {
    const parsed = json.parseFromSlice(json.Value, alloc, array_json, .{}) catch {
        return array_json;
    };
    defer parsed.deinit();

    const items = switch (parsed.value) {
        .array => |arr| arr.items,
        else => return array_json,
    };

    const total = items.len;
    const start = @min(offset, total);
    const end = @min(start + limit, total);

    var output: std.Io.Writer.Allocating = .init(alloc);
    errdefer output.deinit();

    var jw: json.Stringify = .{ .writer = &output.writer };
    try jw.beginArray();
    for (items[start..end]) |item| {
        try jw.write(item);
    }
    try jw.endArray();
    return try output.toOwnedSlice();
}

test "wrapResponse uses an extra result for truthful page metadata" {
    const page = try wrapResponse(std.testing.allocator, "[0,1,2,3,4,5]", "q", "keyword", 2, 2, false);
    defer std.testing.allocator.free(page);

    const parsed = try json.parseFromSlice(json.Value, std.testing.allocator, page, .{});
    defer parsed.deinit();
    const obj = parsed.value.object;
    try std.testing.expectEqual(@as(usize, 2), obj.get("results").?.array.items.len);
    try std.testing.expect(obj.get("total").? == .null);
    try std.testing.expect(obj.get("hasMore").?.bool);
    try std.testing.expectEqual(@as(i64, 4), obj.get("nextOffset").?.integer);
}

test "wrapResponse reports total for known complete arrays" {
    const page = try wrapResponse(std.testing.allocator, "[0,1,2,3]", "", "tags", 2, 2, true);
    defer std.testing.allocator.free(page);

    const parsed = try json.parseFromSlice(json.Value, std.testing.allocator, page, .{});
    defer parsed.deinit();
    const obj = parsed.value.object;
    try std.testing.expectEqual(@as(i64, 4), obj.get("total").?.integer);
    try std.testing.expect(!obj.get("hasMore").?.bool);
    try std.testing.expect(obj.get("nextOffset").? == .null);
}

test "paginateJsonArray honors limit equal to the search candidate cap" {
    const input =
        \\[0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40]
    ;
    const page = try paginateJsonArray(std.testing.allocator, input, 40, 0);
    defer std.testing.allocator.free(page);

    const parsed = try json.parseFromSlice(json.Value, std.testing.allocator, page, .{});
    defer parsed.deinit();
    try std.testing.expectEqual(@as(usize, 40), parsed.value.array.items.len);
    try std.testing.expectEqual(@as(i64, 39), parsed.value.array.items[39].integer);
}

test "paginateJsonArray applies offset with an oversized limit" {
    const input =
        \\[{"id":1},{"id":2},{"id":3},{"id":4},{"id":5}]
    ;
    const page = try paginateJsonArray(std.testing.allocator, input, 40, 3);
    defer std.testing.allocator.free(page);

    const parsed = try json.parseFromSlice(json.Value, std.testing.allocator, page, .{});
    defer parsed.deinit();
    try std.testing.expectEqual(@as(usize, 2), parsed.value.array.items.len);
    try std.testing.expectEqual(@as(i64, 4), parsed.value.array.items[0].object.get("id").?.integer);
}

/// Resolve an AT Protocol handle to a DID via zat's HandleResolver.
/// Tries HTTP .well-known first, falls back to DNS-over-HTTPS.
fn resolveHandle(alloc: std.mem.Allocator, handle: []const u8, io: Io) ![]const u8 {
    const parsed = zat.Handle.parse(handle) orelse {
        logfire.warn("resolveHandle: invalid handle: {s}", .{handle});
        return error.InvalidHandle;
    };

    var resolver = zat.HandleResolver.init(io, alloc);
    defer resolver.deinit();

    if (resolver.resolve(parsed)) |did| {
        return did;
    } else |err| {
        // zat's DoH parser rejects Cloudflare's `Comment` field (present on
        // DNSSEC handles like dholms.at), surfacing as InvalidDnsResponse and
        // silently dropping the author filter. Fall back to a lenient DoH
        // lookup here so DNS-based handles still resolve. See zat note:
        // BUG-doh-comment-field.md. Remove once a fixed zat is pinned.
        logfire.warn("resolveHandle: zat failed for {s}: {}, trying fallbacks", .{ handle, err });
        // HTTP .well-known first: covers *.bsky.social and any handle without a
        // `_atproto` DNS TXT record (zat's own HTTP path is unreliable in our
        // env — it falls through to DNS and dies on bsky.social's no-Answer
        // DoH body). Then DoH for DNS-based handles.
        if (resolveHandleWellKnown(alloc, handle, io)) |did| {
            return did;
        } else |wk_err| {
            logfire.warn("resolveHandle: well-known fallback failed for {s}: {}", .{ handle, wk_err });
        }
        return resolveHandleDoh(alloc, handle, io) catch |fb_err| {
            logfire.warn("resolveHandle: DoH fallback failed for {s}: {}", .{ handle, fb_err });
            return error.ResolveFailed;
        };
    }
}

// HTTP well-known resolution: GET https://<handle>/.well-known/atproto-did,
// body is the DID as plain text. Covers bsky.social handles (no DNS TXT).
fn resolveHandleWellKnown(alloc: Allocator, handle: []const u8, io: Io) ![]const u8 {
    var url_buf: [256]u8 = undefined;
    const url = try std.fmt.bufPrint(&url_buf, "https://{s}/.well-known/atproto-did", .{handle});

    var client: http.Client = .{ .allocator = alloc, .io = io };
    defer client.deinit();

    var response_body: std.Io.Writer.Allocating = .init(alloc);
    defer response_body.deinit();

    const res = client.fetch(.{
        .location = .{ .url = url },
        .response_writer = &response_body.writer,
    }) catch return error.WellKnownRequestFailed;
    if (res.status != .ok) return error.WellKnownRequestFailed;

    const did = mem.trim(u8, response_body.written(), &std.ascii.whitespace);
    if (!mem.startsWith(u8, did, "did:")) return error.NoDidInWellKnown;
    return try alloc.dupe(u8, did);
}

// Lenient DNS-over-HTTPS resolution of `_atproto.<handle>` TXT → did=.
// Mirrors zat's resolveDns but parses with ignore_unknown_fields so the
// optional `Comment` field Cloudflare adds for DNSSEC zones doesn't abort
// the parse (the root cause of dropped author filters for dholms.at et al).
fn resolveHandleDoh(alloc: Allocator, handle: []const u8, io: Io) ![]const u8 {
    var url_buf: [256]u8 = undefined;
    const url = try std.fmt.bufPrint(
        &url_buf,
        "https://cloudflare-dns.com/dns-query?name=_atproto.{s}&type=TXT",
        .{handle},
    );

    var client: http.Client = .{ .allocator = alloc, .io = io };
    defer client.deinit();

    var response_body: std.Io.Writer.Allocating = .init(alloc);
    defer response_body.deinit();

    const res = client.fetch(.{
        .location = .{ .url = url },
        .extra_headers = &.{.{ .name = "accept", .value = "application/dns-json" }},
        .response_writer = &response_body.writer,
    }) catch return error.DohRequestFailed;
    if (res.status != .ok) return error.DohRequestFailed;

    return didFromDohBody(alloc, response_body.written());
}

const DohAnswer = struct { data: ?[]const u8 = null };
const DohResponse = struct { Answer: ?[]DohAnswer = null };

// Parse a Cloudflare DoH JSON body and pull the did= out of the TXT answer.
// Parses leniently (ignore_unknown_fields) so the optional `Comment` field
// Cloudflare emits for DNSSEC zones doesn't abort the parse.
fn didFromDohBody(alloc: Allocator, body: []const u8) ![]const u8 {
    const parsed = json.parseFromSlice(DohResponse, alloc, body, .{
        .ignore_unknown_fields = true,
    }) catch return error.InvalidDohResponse;
    defer parsed.deinit();

    const answers = parsed.value.Answer orelse return error.NoDnsRecords;
    for (answers) |answer| {
        var data = answer.data orelse continue;
        // TXT data arrives quoted, e.g. "\"did=did:plc:...\""
        if (data.len >= 2 and data[0] == '"' and data[data.len - 1] == '"') {
            data = data[1 .. data.len - 1];
        }
        const prefix = "did=";
        if (mem.startsWith(u8, data, prefix)) {
            return try alloc.dupe(u8, data[prefix.len..]);
        }
    }
    return error.NoDidInTxt;
}

test "didFromDohBody tolerates Cloudflare Comment field (dholms.at regression)" {
    // the literal body Cloudflare returns for _atproto.dholms.at — the trailing
    // `Comment` field is what broke zat's strict parser and silently dropped the
    // author filter (2026-06-05). lenient parse must still extract the did.
    const body =
        \\{"Status":0,"TC":false,"RD":true,"RA":true,"AD":false,"CD":false,"Question":[{"name":"_atproto.dholms.at","type":16}],"Answer":[{"name":"_atproto.dholms.at","type":16,"TTL":3600,"data":"\"did=did:plc:yk4dd2qkboz2yv6tpubpc6co\""}],"Comment":["EDE(10): RRSIGs Missing for DNSKEY at., id = 1253"]}
    ;
    const did = try didFromDohBody(std.testing.allocator, body);
    defer std.testing.allocator.free(did);
    try std.testing.expectEqualStrings("did:plc:yk4dd2qkboz2yv6tpubpc6co", did);
}

test "didFromDohBody errors when no TXT answer present" {
    const body =
        \\{"Status":0,"TC":false,"RD":true,"RA":true,"AD":false,"CD":false}
    ;
    try std.testing.expectError(error.NoDnsRecords, didFromDohBody(std.testing.allocator, body));
}

fn handleActivity(request: *http.Server.Request, io: Io) !void {
    const start_time = microTimestamp(io);
    defer metrics.timing.record(.activity, start_time);

    const counts = metrics.activity.getCounts();

    // format as JSON array manually into buffer
    var buf: [512]u8 = undefined;
    var pos: usize = 0;
    buf[pos] = '[';
    pos += 1;
    for (counts, 0..) |c, i| {
        if (i > 0) {
            buf[pos] = ',';
            pos += 1;
        }
        const written = std.fmt.bufPrint(buf[pos..], "{d}", .{c}) catch return;
        pos += written.len;
    }
    buf[pos] = ']';
    pos += 1;

    try sendJson(request, buf[0..pos]);
}

test "result policy is the choke point: undiscoverable rows drop whatever produced them" {
    // The guarantee that makes the per-row checks an optimization rather than
    // the enforcement. A retrieval path that forgets its row filter — or a new
    // path added later — still cannot publish an opted-out publication's row.
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const alloc = arena.allocator();

    visibility.resetForTest();
    defer visibility.resetForTest();
    try visibility.installForTest(&.{
        .{ .uri = "at://did:plc:x/site.standard.publication/notes", .did = "did:plc:x", .base_path = "notes.example" },
    });

    const body =
        \\[{"type":"article","uri":"at://did:plc:x/site.standard.document/a","did":"did:plc:x","basePath":"notes.example","title":"hidden"},
        \\ {"type":"article","uri":"at://did:plc:y/site.standard.document/b","did":"did:plc:y","basePath":"public.example","title":"shown"},
        \\ {"type":"publication","uri":"at://did:plc:x/site.standard.publication/notes","did":"did:plc:x","basePath":"notes.example","title":"notes"}]
    ;

    const filtered = try applyResultPolicy(alloc, body, .{});
    const root = try json.parseFromSliceLeaky(json.Value, alloc, filtered, .{});
    try std.testing.expectEqual(@as(usize, 1), root.array.items.len);
    try std.testing.expectEqualStrings("shown", zat.json.getString(root.array.items[0], "title").?);

    // ...and the opt-in returns all three, including the publication row.
    const opted_in = try applyResultPolicy(alloc, body, .{ .include_undiscoverable = true });
    const all = try json.parseFromSliceLeaky(json.Value, alloc, opted_in, .{});
    try std.testing.expectEqual(@as(usize, 3), all.array.items.len);
}

test "result policy fails closed when the visibility set never loaded" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const alloc = arena.allocator();

    visibility.resetForTest();
    defer visibility.resetForTest();

    const body =
        \\[{"type":"article","uri":"at://did:plc:y/site.standard.document/b","did":"did:plc:y","basePath":"public.example","title":"shown"}]
    ;
    // Unloaded set = we cannot tell who opted out. Callers reach this only via
    // the 503 gate; if they somehow do, dropping beats publishing.
    const filtered = try applyResultPolicy(alloc, body, .{});
    const root = try json.parseFromSliceLeaky(json.Value, alloc, filtered, .{});
    try std.testing.expectEqual(@as(usize, 0), root.array.items.len);
}
