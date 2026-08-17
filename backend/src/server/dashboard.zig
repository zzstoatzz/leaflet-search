const std = @import("std");
const json = std.json;
const Allocator = std.mem.Allocator;
const zql = @import("zql");
const db = @import("../db.zig");
const logfire = @import("logfire");
const timing = @import("../metrics.zig").timing;
const stats_buffer = @import("../metrics.zig").buffer;
const cache = @import("cache.zig");

/// /api/dashboard is the heaviest live-turso endpoint on the stats page and
/// the first to flap under ingest write pressure (multi-second, sometimes
/// timing out — 2026-06-10 outage). Serve it from a background-refreshed
/// snapshot like /popular; a minute of staleness is invisible on a dashboard.
const ApiSlot = enum { main };

fn refreshApi(slot: ApiSlot, alloc: Allocator) anyerror![]const u8 {
    _ = slot;
    const data = try fetch(alloc);
    // emit a point-in-time ops snapshot so Logfire can TREND + ALERT on the
    // pipeline signals span data can't otherwise see (embedding backlog,
    // ingestion freshness). rides the cache's 60s background tick, so it's a
    // steady ~1/min series regardless of request traffic.
    emitOpsSnapshot(data);
    return try toJson(alloc, data);
}

/// One `ops.snapshot` span per refresh. Attributes are typed (int) so they're
/// queryable: `SELECT attributes->>'embed_backlog' ... WHERE span_name='ops.snapshot'`.
/// `last_indexed_at` is epoch seconds (turso-sourced); freshness age is derived
/// at query time as `now - last_indexed_at` so the emit needs no clock.
fn emitOpsSnapshot(data: Data) void {
    const backlog = if (data.documents > data.embeddings) data.documents - data.embeddings else 0;
    var overlay_rows: i64 = -1; // -1 = overlay not enabled
    var overlay_tombstones: i64 = -1;
    if (db.getOverlay()) |o| {
        const s = o.stats();
        overlay_rows = s.rows;
        overlay_tombstones = s.tombstones;
    }
    logfire.span("ops.snapshot", .{
        .documents = data.documents,
        .embeddings = data.embeddings,
        .embed_backlog = backlog,
        .publications = data.publications,
        .last_indexed_at = data.last_indexed_at,
        .overlay_rows = overlay_rows,
        .overlay_tombstones = overlay_tombstones,
    }).end();
}

pub const ApiCache = cache.WindowedJsonCache(ApiSlot, .{
    .name = "dashboard.api",
    .refresh = &refreshApi,
    .interval_ms = 60_000,
});

/// Timeline chart: same treatment, one slot per (range, field) combination.
pub const TimelineSlot = enum {
    d7_indexed,
    d30_indexed,
    d90_indexed,
    y1_indexed,
    all_time_indexed,
    d7_created,
    d30_created,
    d90_created,
    y1_created,
    all_time_created,

    pub fn from(r: TimelineRange, f: TimelineField) TimelineSlot {
        const base: u4 = switch (r) {
            .d7 => 0,
            .d30 => 1,
            .d90 => 2,
            .y1 => 3,
            .all_time => 4,
        };
        return @enumFromInt(base + @as(u4, if (f == .created) 5 else 0));
    }

    fn range(self: TimelineSlot) TimelineRange {
        return switch (@intFromEnum(self) % 5) {
            0 => .d7,
            1 => .d30,
            2 => .d90,
            3 => .y1,
            else => .all_time,
        };
    }

    fn field(self: TimelineSlot) TimelineField {
        return if (@intFromEnum(self) >= 5) .created else .indexed;
    }
};

fn refreshTimelineSlot(slot: TimelineSlot, alloc: Allocator) anyerror![]const u8 {
    return try fetchTimeline(alloc, slot.range(), slot.field());
}

pub const TimelineCache = cache.WindowedJsonCache(TimelineSlot, .{
    .name = "dashboard.timeline",
    .refresh = &refreshTimelineSlot,
    .interval_ms = 60_000,
});

pub fn initCache(io: std.Io) void {
    ApiCache.init(io);
    TimelineCache.init(io);
}

// JSON output types
const TagJson = struct { tag: []const u8, count: i64 };
const TimelineJson = struct { date: []const u8, count: i64, bridgyfed: i64 };
const PubJson = struct { name: []const u8, basePath: []const u8, count: i64 };
const PlatformJson = struct { platform: []const u8, count: i64 };

/// Time range for the indexing timeline.
///
/// Bucket sizes are chosen to keep bar counts bounded:
///   d7/d30/d90 -> daily, y1 -> weekly, all_time -> monthly.
pub const TimelineRange = enum {
    d7,
    d30,
    d90,
    y1,
    all_time,

    pub fn fromString(s: []const u8) TimelineRange {
        if (std.mem.eql(u8, s, "7d")) return .d7;
        if (std.mem.eql(u8, s, "30d")) return .d30;
        if (std.mem.eql(u8, s, "90d")) return .d90;
        if (std.mem.eql(u8, s, "1y")) return .y1;
        if (std.mem.eql(u8, s, "all")) return .all_time;
        return .d30;
    }

    pub fn bucketLabel(self: TimelineRange) []const u8 {
        return switch (self) {
            .d7, .d30, .d90 => "daily",
            .y1 => "weekly",
            .all_time => "monthly",
        };
    }

    /// Build the timeline query for this range bucketed on `col` (a bare
    /// column name, either `indexed_at` or `created_at`). Both `self` and
    /// `col` are comptime, so each (range × field) pair is a distinct
    /// comptime-known SQL string — same shape the DB layer expects.
    fn sql(comptime self: TimelineRange, comptime col: []const u8) []const u8 {
        // created_at spans decades (authored dates, incl. backfilled content),
        // so the all-time view buckets yearly; indexed_at is service-lifetime
        // only, so it stays monthly. see also bucketLabelFor.
        const created = comptime std.mem.eql(u8, col, "created_at");
        const bucket = switch (self) {
            .d7, .d30, .d90 => "DATE(" ++ col ++ ")",
            .y1 => "DATE(" ++ col ++ ", 'weekday 0', '-6 days')",
            .all_time => if (created) "strftime('%Y-01-01', " ++ col ++ ")" else "strftime('%Y-%m-01', " ++ col ++ ")",
        };
        const lower = switch (self) {
            .d7 => "AND DATE(" ++ col ++ ") >= DATE('now', '-6 days')",
            .d30 => "AND DATE(" ++ col ++ ") >= DATE('now', '-29 days')",
            .d90 => "AND DATE(" ++ col ++ ") >= DATE('now', '-89 days')",
            .y1 => "AND DATE(" ++ col ++ ") >= DATE('now', '-364 days')",
            // floor out junk pre-internet created_at dates (year 0001, 0110, 1921…)
            // that would otherwise stretch the axis back two millennia
            .all_time => if (created) "AND DATE(" ++ col ++ ") >= '2000-01-01'" else "",
        };
        return std.fmt.comptimePrint(
            \\SELECT {[bucket]s} as date, COUNT(*) as count,
            \\  SUM(CASE WHEN COALESCE(is_bridgyfed, 0) = 1 THEN 1 ELSE 0 END) as bridgyfed
            \\FROM documents
            \\WHERE {[col]s} IS NOT NULL AND {[col]s} != ''
            \\AND DATE({[col]s}) <= DATE('now')
            \\{[lower]s}
            \\GROUP BY {[bucket]s}
            \\ORDER BY date DESC
        , .{ .bucket = bucket, .col = col, .lower = lower });
    }
};

/// Which timestamp the documents-over-time chart buckets on:
///   indexed -> when we ingested it (ingestion throughput)
///   created -> when the author published it (content age)
pub const TimelineField = enum {
    indexed,
    created,

    pub fn fromString(s: []const u8) TimelineField {
        if (std.mem.eql(u8, s, "created")) return .created;
        return .indexed;
    }

    fn column(self: TimelineField) []const u8 {
        return switch (self) {
            .indexed => "indexed_at",
            .created => "created_at",
        };
    }
};

/// Bucket label for a (range, field) pair. Matches the bucketing in
/// `TimelineRange.sql`: all-time on created_at is yearly, everything else
/// follows the range's own granularity.
fn bucketLabelFor(range: TimelineRange, field: TimelineField) []const u8 {
    if (range == .all_time and field == .created) return "yearly";
    return range.bucketLabel();
}

/// All data needed to render the dashboard
pub const Data = struct {
    started_at: i64,
    searches: i64,
    publications: i64,
    documents: i64,
    embeddings: i64,
    bridgyfed_documents: i64,
    /// epoch seconds of the most recently ingested document (MAX(indexed_at)).
    /// 0 when empty. The stats page renders this as "last indexed N ago" — the
    /// cheapest is-ingestion-alive signal, which span telemetry can't show.
    last_indexed_at: i64,
    relay_url: []const u8,
    tags_json: []const u8,
    timeline_json: []const u8,
    top_pubs_json: []const u8,
    platforms_json: []const u8,
    timing_json: []const u8,
    traffic_json: []const u8,
};

fn getRelayUrl() []const u8 {
    // the event source shown on the dashboard; since the 2026-08-17 jetstream
    // cutover this is the stream instance, not a relay we dial ourselves
    return if (std.c.getenv("JETSTREAM_HOSTS")) |p| std.mem.span(p) else "https://stream.waow.tech";
}

// all dashboard queries batched into one request
const STATS_SQL =
    \\SELECT
    \\  (SELECT COUNT(*) FROM documents) as docs,
    \\  (SELECT COUNT(*) FROM publications) as pubs,
    \\  (SELECT total_searches FROM stats WHERE id = 1) as searches,
    \\  (SELECT total_errors FROM stats WHERE id = 1) as errors,
    \\  (SELECT service_started_at FROM stats WHERE id = 1) as started_at,
    \\  (SELECT COUNT(*) FROM documents WHERE embedded_at IS NOT NULL) as embeddings,
    \\  (SELECT COUNT(*) FROM documents WHERE COALESCE(is_bridgyfed, 0) = 1) as bridgyfed,
    \\  (SELECT COALESCE(CAST(strftime('%s', MAX(indexed_at)) AS INTEGER), 0) FROM documents) as last_indexed
;

const PLATFORMS_SQL =
    \\SELECT CASE WHEN COALESCE(is_bridgyfed, 0) = 1 THEN 'bridgy fed'
    \\  ELSE platform END as platform,
    \\  COUNT(*) as count
    \\FROM documents
    \\GROUP BY CASE WHEN COALESCE(is_bridgyfed, 0) = 1 THEN 'bridgy fed'
    \\  ELSE platform END
    \\ORDER BY count DESC
;

pub const TAGS_SQL =
    \\SELECT tag, COUNT(*) as count
    \\FROM document_tags
    \\GROUP BY tag
    \\ORDER BY count DESC
    \\LIMIT 100
;

// default timeline range for /api/dashboard (kept for back-compat)
const DEFAULT_TIMELINE_RANGE: TimelineRange = .d30;

const TOP_PUBS_SQL =
    \\SELECT p.name, p.base_path, COUNT(d.uri) as doc_count
    \\FROM publications p
    \\JOIN documents d ON d.publication_uri = p.uri
    \\WHERE COALESCE(d.is_bridgyfed, 0) = 0
    \\GROUP BY p.uri
    \\ORDER BY doc_count DESC
    \\LIMIT 8
;

// zql.Query wrappers + Row structs so dashboard row reads use comptime-checked
// column-name lookups instead of brittle positional indices. Adding/renaming
// a column becomes a compile error rather than a silent runtime miscount.
//
// Timeline queries are runtime-dispatched (per TimelineRange.sql()), but all
// variants share the same column projection — wrapping any one of them gives
// us the canonical column index for date/count/bridgyfed.
const StatsQuery = zql.Query(STATS_SQL);
const PlatformsQuery = zql.Query(PLATFORMS_SQL);
const TagsQuery = zql.Query(TAGS_SQL);
const TopPubsQuery = zql.Query(TOP_PUBS_SQL);
const TimelineQueryRef = zql.Query((TimelineRange.d30).sql("indexed_at"));

const StatsRow = struct {
    docs: i64,
    pubs: i64,
    searches: i64,
    errors: i64,
    started_at: i64,
    embeddings: i64,
    bridgyfed: i64,
    last_indexed: i64,
};
const PlatformsRow = struct { platform: []const u8, count: i64 };
const TagsRow = struct { tag: []const u8, count: i64 };
const TopPubsRow = struct { name: []const u8, base_path: []const u8, doc_count: i64 };
const TimelineRow = struct { date: []const u8, count: i64, bridgyfed: i64 };

pub fn fetch(alloc: Allocator) !Data {
    // try local SQLite first (fast)
    if (db.getLocalDb()) |local| {
        if (fetchLocal(alloc, local)) |result| {
            return result;
        } else |err| {
            logfire.warn("dashboard: fetchLocal failed: {s}, falling back to turso batch", .{@errorName(err)});
        }
    }

    // fall back to Turso (slow)
    const client = db.getClient() orelse return error.NotInitialized;

    // batch all 5 queries into one HTTP request
    var batch = client.queryBatch(&.{
        .{ .sql = STATS_SQL },
        .{ .sql = PLATFORMS_SQL },
        .{ .sql = TAGS_SQL },
        .{ .sql = DEFAULT_TIMELINE_RANGE.sql("indexed_at") },
        .{ .sql = TOP_PUBS_SQL },
    }) catch return error.QueryFailed;
    defer batch.deinit();

    // extract stats (query 0) — column-name lookup via StatsQuery
    const stats_row = batch.getFirst(0);
    if (stats_row == null) {
        logfire.warn("dashboard: turso batch returned no stats row", .{});
    }
    const s: StatsRow = if (stats_row) |r| StatsQuery.fromRow(StatsRow, r) else .{
        .docs = 0, .pubs = 0, .searches = 0, .errors = 0,
        .started_at = 0, .embeddings = 0, .bridgyfed = 0, .last_indexed = 0,
    };
    const started_at = s.started_at;
    const searches = s.searches;
    const publications = s.pubs;
    const documents = s.docs;
    const embeddings = s.embeddings;
    const bridgyfed_documents = s.bridgyfed;

    return .{
        .started_at = started_at,
        .searches = searches,
        .publications = publications,
        .documents = documents,
        .embeddings = embeddings,
        .bridgyfed_documents = bridgyfed_documents,
        .last_indexed_at = s.last_indexed,
        .relay_url = getRelayUrl(),
        .tags_json = try formatTagsJson(alloc, batch.get(2)),
        .timeline_json = try formatTimelineJson(alloc, batch.get(3)),
        .top_pubs_json = try formatPubsJson(alloc, batch.get(4)),
        .platforms_json = try formatPlatformsJson(alloc, batch.get(1)),
        .timing_json = try formatTimingJson(alloc),
        .traffic_json = try formatTrafficJson(alloc),
    };
}

fn fetchLocal(alloc: Allocator, local: *db.LocalDb) !Data {
    // pull cached stats from the in-memory buffer instead of querying turso.
    // total_searches / service_started_at don't live on the local replica
    // (only sync-tracked tables do), but the stats_buffer module already
    // refreshes them in the background every 5s. a synchronous turso call
    // here would hang the *entire* dashboard handler for as long as turso
    // takes to respond — and on 2026-05-07 turso had a ~95s hiccup that did
    // exactly that, leaving fly proxy to 502 every dashboard request.
    //
    // the cached values can be up to ~5s stale, which is fine for a stats
    // counter rendered on a debug page.
    const cached = stats_buffer.getCachedStats();
    const searches = if (cached) |c| c.searches else 0;
    const started_at = if (cached) |c| c.started_at else 0;
    // freshness is turso-sourced via the stats cache. The local replica is
    // snapshot-frozen (in-place sync deleted; refreshed only by adoption), so
    // its MAX(indexed_at) reports the snapshot's age — stale by however long
    // since the last adoption — and would show "last indexed" as long-ago even
    // while ingestion is healthy.
    const last_indexed_at = if (cached) |c| c.last_indexed_at else 0;

    // get document/publication/embedding counts from local (fast)
    // Local counts query is a strict subset of STATS_SQL (no searches/errors/
    // started_at, which live in the stats table and are sourced from
    // stats_buffer above). Wrapping it in its own zql.Query so the indices
    // are name-resolved at comptime.
    const LocalCountsQuery = zql.Query(
        \\SELECT
        \\  (SELECT COUNT(*) FROM documents) as docs,
        \\  (SELECT COUNT(*) FROM publications) as pubs,
        \\  (SELECT COUNT(*) FROM documents WHERE embedded_at IS NOT NULL) as embeddings,
        \\  (SELECT COUNT(*) FROM documents WHERE COALESCE(is_bridgyfed, 0) = 1) as bridgyfed
    );
    const LocalCountsRow = struct { docs: i64, pubs: i64, embeddings: i64, bridgyfed: i64 };

    var counts_rows = try local.query(LocalCountsQuery.positional, .{});
    defer counts_rows.deinit();
    const counts_row = counts_rows.next() orelse return error.NoStats;
    const counts = LocalCountsQuery.fromRow(LocalCountsRow, counts_row);
    const documents = counts.docs;
    const publications = counts.pubs;
    const embeddings = counts.embeddings;
    const bridgyfed_documents = counts.bridgyfed;

    // platforms query
    var platforms_rows = try local.query(PLATFORMS_SQL, .{});
    defer platforms_rows.deinit();
    const platforms_json = try formatPlatformsJsonLocal(alloc, &platforms_rows);

    // tags query
    var tags_rows = try local.query(TAGS_SQL, .{});
    defer tags_rows.deinit();
    const tags_json = try formatTagsJsonLocal(alloc, &tags_rows);

    // timeline query
    var timeline_rows = try local.query(DEFAULT_TIMELINE_RANGE.sql("indexed_at"), .{});
    defer timeline_rows.deinit();
    const timeline_json = try formatTimelineJsonLocal(alloc, &timeline_rows);

    // top pubs query
    var pubs_rows = try local.query(TOP_PUBS_SQL, .{});
    defer pubs_rows.deinit();
    const top_pubs_json = try formatPubsJsonLocal(alloc, &pubs_rows);

    return .{
        .started_at = started_at,
        .searches = searches,
        .publications = publications,
        .documents = documents,
        .embeddings = embeddings,
        .bridgyfed_documents = bridgyfed_documents,
        .last_indexed_at = last_indexed_at,
        .relay_url = getRelayUrl(),
        .tags_json = tags_json,
        .timeline_json = timeline_json,
        .top_pubs_json = top_pubs_json,
        .platforms_json = platforms_json,
        .timing_json = try formatTimingJson(alloc),
        .traffic_json = try formatTrafficJson(alloc),
    };
}

fn formatTagsJsonLocal(alloc: Allocator, rows: *db.LocalDb.Rows) ![]const u8 {
    var output: std.Io.Writer.Allocating = .init(alloc);
    errdefer output.deinit();
    var jw: json.Stringify = .{ .writer = &output.writer };
    try jw.beginArray();
    while (rows.next()) |row| {
        const r = TagsQuery.fromRow(TagsRow, row);
        try jw.write(TagJson{ .tag = r.tag, .count = r.count });
    }
    try jw.endArray();
    return try output.toOwnedSlice();
}

fn formatTimelineJsonLocal(alloc: Allocator, rows: *db.LocalDb.Rows) ![]const u8 {
    var output: std.Io.Writer.Allocating = .init(alloc);
    errdefer output.deinit();
    var jw: json.Stringify = .{ .writer = &output.writer };
    try jw.beginArray();
    while (rows.next()) |row| {
        const r = TimelineQueryRef.fromRow(TimelineRow, row);
        try jw.write(TimelineJson{ .date = r.date, .count = r.count, .bridgyfed = r.bridgyfed });
    }
    try jw.endArray();
    return try output.toOwnedSlice();
}

fn formatPubsJsonLocal(alloc: Allocator, rows: *db.LocalDb.Rows) ![]const u8 {
    var output: std.Io.Writer.Allocating = .init(alloc);
    errdefer output.deinit();
    var jw: json.Stringify = .{ .writer = &output.writer };
    try jw.beginArray();
    while (rows.next()) |row| {
        const r = TopPubsQuery.fromRow(TopPubsRow, row);
        try jw.write(PubJson{ .name = r.name, .basePath = r.base_path, .count = r.doc_count });
    }
    try jw.endArray();
    return try output.toOwnedSlice();
}

fn formatPlatformsJsonLocal(alloc: Allocator, rows: *db.LocalDb.Rows) ![]const u8 {
    var output: std.Io.Writer.Allocating = .init(alloc);
    errdefer output.deinit();
    var jw: json.Stringify = .{ .writer = &output.writer };
    try jw.beginArray();
    while (rows.next()) |row| {
        const r = PlatformsQuery.fromRow(PlatformsRow, row);
        try jw.write(PlatformJson{ .platform = r.platform, .count = r.count });
    }
    try jw.endArray();
    return try output.toOwnedSlice();
}

fn formatTagsJson(alloc: Allocator, rows: []const db.Row) ![]const u8 {
    var output: std.Io.Writer.Allocating = .init(alloc);
    errdefer output.deinit();
    var jw: json.Stringify = .{ .writer = &output.writer };
    try jw.beginArray();
    for (rows) |row| {
        const r = TagsQuery.fromRow(TagsRow, row);
        try jw.write(TagJson{ .tag = r.tag, .count = r.count });
    }
    try jw.endArray();
    return try output.toOwnedSlice();
}

fn formatTimelineJson(alloc: Allocator, rows: []const db.Row) ![]const u8 {
    var output: std.Io.Writer.Allocating = .init(alloc);
    errdefer output.deinit();
    var jw: json.Stringify = .{ .writer = &output.writer };
    try jw.beginArray();
    for (rows) |row| {
        const r = TimelineQueryRef.fromRow(TimelineRow, row);
        try jw.write(TimelineJson{ .date = r.date, .count = r.count, .bridgyfed = r.bridgyfed });
    }
    try jw.endArray();
    return try output.toOwnedSlice();
}

fn formatPubsJson(alloc: Allocator, rows: []const db.Row) ![]const u8 {
    var output: std.Io.Writer.Allocating = .init(alloc);
    errdefer output.deinit();
    var jw: json.Stringify = .{ .writer = &output.writer };
    try jw.beginArray();
    for (rows) |row| {
        const r = TopPubsQuery.fromRow(TopPubsRow, row);
        try jw.write(PubJson{ .name = r.name, .basePath = r.base_path, .count = r.doc_count });
    }
    try jw.endArray();
    return try output.toOwnedSlice();
}

fn formatPlatformsJson(alloc: Allocator, rows: []const db.Row) ![]const u8 {
    var output: std.Io.Writer.Allocating = .init(alloc);
    errdefer output.deinit();
    var jw: json.Stringify = .{ .writer = &output.writer };
    try jw.beginArray();
    for (rows) |row| {
        const r = PlatformsQuery.fromRow(PlatformsRow, row);
        try jw.write(PlatformJson{ .platform = r.platform, .count = r.count });
    }
    try jw.endArray();
    return try output.toOwnedSlice();
}

fn formatTimingJson(alloc: Allocator) ![]const u8 {
    const all_timing = timing.getAllStats();
    const all_series = timing.getAllTimeSeries();

    var output: std.Io.Writer.Allocating = .init(alloc);
    errdefer output.deinit();
    var jw: json.Stringify = .{ .writer = &output.writer };

    try jw.beginObject();
    inline for (@typeInfo(timing.Endpoint).@"enum".fields, 0..) |field, i| {
        const t = all_timing[i];
        const series = all_series[i];
        try jw.objectField(field.name);
        // EndpointStats + the 24h history serialize as one struct — field
        // names of EndpointStats and TimeSeriesPoint already match the wire
        // shape we want.
        try jw.write(.{
            .count = t.count,
            .avg_ms = t.avg_ms,
            .p50_ms = t.p50_ms,
            .p95_ms = t.p95_ms,
            .p99_ms = t.p99_ms,
            .max_ms = t.max_ms,
            .history = series[0..],
        });
    }
    try jw.endObject();

    return try output.toOwnedSlice();
}

fn formatTrafficJson(alloc: Allocator) ![]const u8 {
    const series = timing.getTrafficSeries();

    var output: std.Io.Writer.Allocating = .init(alloc);
    errdefer output.deinit();
    var jw: json.Stringify = .{ .writer = &output.writer };

    try jw.beginArray();
    for (series) |point| {
        try jw.beginObject();
        try jw.objectField("hour");
        try jw.write(point.hour);
        try jw.objectField("count");
        try jw.write(point.count);
        try jw.endObject();
    }
    try jw.endArray();

    return try output.toOwnedSlice();
}

/// Fetch the documents-indexed timeline at the requested range.
/// Returns a JSON object: `{"bucket":"daily|weekly|monthly","range":"30d","points":[...]}`.
pub fn fetchTimeline(alloc: Allocator, range: TimelineRange, field: TimelineField) ![]const u8 {
    var output: std.Io.Writer.Allocating = .init(alloc);
    errdefer output.deinit();
    var jw: json.Stringify = .{ .writer = &output.writer };

    try jw.beginObject();
    try jw.objectField("bucket");
    try jw.write(bucketLabelFor(range, field));
    try jw.objectField("range");
    try jw.write(@tagName(range));
    try jw.objectField("field");
    try jw.write(@tagName(field));
    try jw.objectField("points");

    // local + turso queries both take comptime SQL — dispatch on the (range,
    // field) tags so each branch runs a comptime-known query string
    switch (range) {
        inline else => |r| switch (field) {
            inline else => |f| try writeTimelinePoints(&jw, comptime r.sql(f.column())),
        },
    }

    try jw.endObject();
    return try output.toOwnedSlice();
}

/// Fetch per-endpoint latency history at the requested range (24h / 7d / 30d).
/// Returns JSON: `{"range":"7d","hours":168,"endpoints":{"search_keyword":[...], …}}`.
/// The hourly buckets live in-memory for `HOURS_TO_KEEP` (30d) regardless, so
/// this endpoint just slices the existing window — no new storage cost.
pub fn fetchLatency(alloc: Allocator, range: timing.LatencyRange) ![]const u8 {
    const hours = range.hours();

    // shared scratch buffer reused across endpoints (we serialize one at a time)
    const points = try alloc.alloc(timing.TimeSeriesPoint, hours);
    defer alloc.free(points);

    var output: std.Io.Writer.Allocating = .init(alloc);
    errdefer output.deinit();
    var jw: json.Stringify = .{ .writer = &output.writer };

    try jw.beginObject();
    try jw.objectField("range");
    try jw.write(@tagName(range));
    try jw.objectField("hours");
    try jw.write(@as(i64, @intCast(hours)));
    try jw.objectField("endpoints");
    try jw.beginObject();

    inline for (@typeInfo(timing.Endpoint).@"enum".fields) |field| {
        const ep: timing.Endpoint = @enumFromInt(field.value);
        timing.writeTimeSeries(ep, points);
        try jw.objectField(field.name);
        // TimeSeriesPoint's field names match the wire shape we want, so the
        // serializer walks them directly — no per-field boilerplate needed.
        try jw.write(points);
    }

    try jw.endObject();
    try jw.endObject();
    return try output.toOwnedSlice();
}

fn writeTimelinePoints(jw: *json.Stringify, comptime sql_query: []const u8) !void {
    // try local SQLite first
    if (db.getLocalDb()) |local| {
        if (local.query(sql_query, .{})) |rows_const| {
            var rows = rows_const;
            defer rows.deinit();
            try jw.beginArray();
            while (rows.next()) |row| {
                const r = TimelineQueryRef.fromRow(TimelineRow, row);
                try jw.write(TimelineJson{ .date = r.date, .count = r.count, .bridgyfed = r.bridgyfed });
            }
            try jw.endArray();
            return;
        } else |err| {
            logfire.warn("dashboard: timeline local query failed: {s}, falling back to turso", .{@errorName(err)});
        }
    }

    // fall back to Turso
    const client = db.getClient() orelse return error.NotInitialized;
    var res = client.query(sql_query, &.{}) catch return error.QueryFailed;
    defer res.deinit();

    try jw.beginArray();
    for (res.rows) |row| {
        const r = TimelineQueryRef.fromRow(TimelineRow, row);
                try jw.write(TimelineJson{ .date = r.date, .count = r.count, .bridgyfed = r.bridgyfed });
    }
    try jw.endArray();
}

/// Generate dashboard data as JSON for API endpoint
pub fn toJson(alloc: Allocator, data: Data) ![]const u8 {
    var output: std.Io.Writer.Allocating = .init(alloc);
    errdefer output.deinit();

    var jw: json.Stringify = .{ .writer = &output.writer };
    try jw.beginObject();

    try jw.objectField("startedAt");
    try jw.write(data.started_at);

    try jw.objectField("searches");
    try jw.write(data.searches);

    try jw.objectField("publications");
    try jw.write(data.publications);

    try jw.objectField("documents");
    try jw.write(data.documents);

    try jw.objectField("embeddings");
    try jw.write(data.embeddings);

    try jw.objectField("bridgyfedDocuments");
    try jw.write(data.bridgyfed_documents);

    try jw.objectField("lastIndexedAt");
    try jw.write(data.last_indexed_at);

    try jw.objectField("relayUrl");
    try jw.write(data.relay_url);

    try jw.objectField("platforms");
    try jw.beginWriteRaw();
    try jw.writer.writeAll(data.platforms_json);
    jw.endWriteRaw();

    // use beginWriteRaw/endWriteRaw for pre-formatted JSON arrays
    try jw.objectField("tags");
    try jw.beginWriteRaw();
    try jw.writer.writeAll(data.tags_json);
    jw.endWriteRaw();

    try jw.objectField("timeline");
    try jw.beginWriteRaw();
    try jw.writer.writeAll(data.timeline_json);
    jw.endWriteRaw();

    try jw.objectField("topPubs");
    try jw.beginWriteRaw();
    try jw.writer.writeAll(data.top_pubs_json);
    jw.endWriteRaw();

    try jw.objectField("timing");
    try jw.beginWriteRaw();
    try jw.writer.writeAll(data.timing_json);
    jw.endWriteRaw();

    try jw.objectField("trafficHistory");
    try jw.beginWriteRaw();
    try jw.writer.writeAll(data.traffic_json);
    jw.endWriteRaw();

    try jw.endObject();
    return try output.toOwnedSlice();
}

test "timeline sql buckets on the requested field column" {
    // every (range × field) pair must compile and reference the right column
    inline for (.{ TimelineRange.d7, .d30, .d90, .y1, .all_time }) |range| {
        inline for (.{ TimelineField.indexed, TimelineField.created }) |field| {
            const q = comptime range.sql(field.column());
            try std.testing.expect(std.mem.indexOf(u8, q, field.column()) != null);
            // the opposite column must never leak in
            const other = if (field == .indexed) "created_at" else "indexed_at";
            try std.testing.expect(std.mem.indexOf(u8, q, other) == null);
        }
    }
}

test "all-time created_at buckets yearly with a pre-2000 junk floor" {
    const q = comptime TimelineRange.all_time.sql(TimelineField.created.column());
    try std.testing.expect(std.mem.indexOf(u8, q, "strftime('%Y-01-01'") != null);
    try std.testing.expect(std.mem.indexOf(u8, q, ">= '2000-01-01'") != null);
    try std.testing.expectEqualStrings("yearly", bucketLabelFor(.all_time, .created));

    // indexed_at all-time stays monthly with no floor
    const qi = comptime TimelineRange.all_time.sql(TimelineField.indexed.column());
    try std.testing.expect(std.mem.indexOf(u8, qi, "strftime('%Y-%m-01'") != null);
    try std.testing.expect(std.mem.indexOf(u8, qi, "2000-01-01") == null);
    try std.testing.expectEqualStrings("monthly", bucketLabelFor(.all_time, .indexed));
}
