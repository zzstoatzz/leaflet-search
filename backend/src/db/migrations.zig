//! Migration list for the Turso schema, run via `zug.sqlite.run`.
//!
//! ## design
//!
//! Migration `001_initial_schema` is a *snapshot* of the current schema
//! (every CREATE TABLE / VIRTUAL TABLE / INDEX with all columns currently in
//! production). New deployments running the migration list from scratch get
//! the modern schema in one shot. Production turso, which is already in this
//! state, gets bootstrapped — `schema.zig:bootstrapIfNeeded` seeds
//! `zug_migrations` with the **baseline** migrations (the first
//! `BOOTSTRAP_BASELINE_COUNT` entries) marked already-applied so zug runs zero
//! of them on the existing DB.
//!
//! **Migrations appended after zug adoption are NOT part of the baseline.**
//! Future schema/data changes get new entries (011, 012, …); zug runs them
//! against turso normally. `BOOTSTRAP_BASELINE_COUNT` stays frozen at the
//! count that existed when zug was adopted, so a restored pre-zug backup
//! gets the baseline pre-applied but every later migration runs for real.
//!
//! Schema additions in the future should be appended as new ALTER TABLE
//! migrations — never edit migration 001 retroactively (zug's checksum check
//! would catch the change and refuse to run).
//!
//! ## transactions
//!
//! All migrations are `transactional: false` because the Turso HTTP client
//! closes the connection at the end of each pipeline request, so BEGIN /
//! COMMIT can't span multiple `conn.exec` calls. Partial failure is
//! recoverable via zug's dirty flag — the migration row stays `dirty=1` and
//! blocks subsequent runs until repaired.

const zug = @import("zug");

/// Number of leading migrations that bootstrap pre-marks as already-applied
/// when adopting zug on top of an existing turso DB.
///
/// **Critical: this number is FROZEN at the count of migrations that existed
/// at the moment of adoption.** Migrations appended after zug adoption (011,
/// 012, …) must NOT be folded into the baseline — they need to actually run
/// against turso. If a fresh-but-pre-zug DB ever bootstraps later (e.g. a
/// restored backup), only these baseline entries should be marked applied;
/// everything past this index runs through `zug.sqlite.run` normally.
pub const BOOTSTRAP_BASELINE_COUNT: usize = 10;

pub const migrations = [_]zug.Migration{
    .{
        .id = "001_initial_schema",
        .name = "create all tables, virtual tables, and indexes",
        .sql =
        \\CREATE TABLE IF NOT EXISTS documents (
        \\  uri TEXT PRIMARY KEY,
        \\  did TEXT NOT NULL,
        \\  rkey TEXT NOT NULL,
        \\  title TEXT NOT NULL,
        \\  content TEXT NOT NULL,
        \\  created_at TEXT,
        \\  publication_uri TEXT,
        \\  platform TEXT DEFAULT 'leaflet',
        \\  source_collection TEXT DEFAULT 'pub.leaflet.document',
        \\  embedded_at TEXT,
        \\  content_hash TEXT,
        \\  path TEXT,
        \\  base_path TEXT DEFAULT '',
        \\  has_publication INTEGER DEFAULT 0,
        \\  cover_image TEXT,
        \\  verified_at TEXT,
        \\  indexed_at TEXT,
        \\  is_bridgyfed INTEGER DEFAULT 0
        \\);
        \\CREATE VIRTUAL TABLE IF NOT EXISTS documents_fts USING fts5(
        \\  uri UNINDEXED,
        \\  title,
        \\  content
        \\);
        \\CREATE TABLE IF NOT EXISTS publications (
        \\  uri TEXT PRIMARY KEY,
        \\  did TEXT NOT NULL,
        \\  rkey TEXT NOT NULL,
        \\  name TEXT NOT NULL,
        \\  description TEXT,
        \\  base_path TEXT,
        \\  platform TEXT DEFAULT 'leaflet',
        \\  source_collection TEXT DEFAULT 'pub.leaflet.publication',
        \\  indexed_at TEXT
        \\);
        \\CREATE VIRTUAL TABLE IF NOT EXISTS publications_fts USING fts5(
        \\  uri UNINDEXED,
        \\  name,
        \\  description,
        \\  base_path
        \\);
        \\CREATE TABLE IF NOT EXISTS document_tags (
        \\  document_uri TEXT NOT NULL,
        \\  tag TEXT NOT NULL,
        \\  PRIMARY KEY (document_uri, tag)
        \\);
        \\CREATE INDEX IF NOT EXISTS idx_document_tags_tag ON document_tags(tag);
        \\CREATE TABLE IF NOT EXISTS stats (
        \\  id INTEGER PRIMARY KEY CHECK (id = 1),
        \\  total_searches INTEGER DEFAULT 0,
        \\  total_errors INTEGER DEFAULT 0,
        \\  service_started_at INTEGER,
        \\  cache_hits INTEGER DEFAULT 0,
        \\  cache_misses INTEGER DEFAULT 0
        \\);
        \\INSERT OR IGNORE INTO stats (id) VALUES (1);
        \\CREATE TABLE IF NOT EXISTS popular_searches (
        \\  query TEXT PRIMARY KEY,
        \\  count INTEGER DEFAULT 1
        \\);
        \\CREATE TABLE IF NOT EXISTS tombstones (
        \\  uri TEXT PRIMARY KEY,
        \\  record_type TEXT NOT NULL,
        \\  deleted_at INTEGER NOT NULL
        \\);
        \\CREATE UNIQUE INDEX IF NOT EXISTS idx_documents_did_rkey ON documents(did, rkey);
        \\CREATE UNIQUE INDEX IF NOT EXISTS idx_publications_did_rkey ON publications(did, rkey);
        \\CREATE INDEX IF NOT EXISTS idx_documents_did_content_hash ON documents(did, content_hash);
        ,
    },
    .{
        .id = "002_init_stats_started_at",
        .name = "stamp stats.service_started_at on first ever boot",
        .sql = "UPDATE stats SET service_started_at = strftime('%s', 'now') WHERE id = 1 AND service_started_at IS NULL",
    },
    .{
        .id = "003_backfill_default_platform",
        .name = "backfill platform / source_collection for pre-multi-platform rows",
        .sql =
        \\UPDATE documents SET platform = 'leaflet' WHERE platform IS NULL;
        \\UPDATE documents SET source_collection = 'pub.leaflet.document' WHERE source_collection IS NULL;
        \\UPDATE publications SET platform = 'leaflet' WHERE platform IS NULL;
        \\UPDATE publications SET source_collection = 'pub.leaflet.publication' WHERE source_collection IS NULL;
        ,
    },
    .{
        .id = "004_reclassify_platform_from_collection",
        .name = "fix 'unknown'/'standardsite' rows from pre-detection ingest",
        .sql =
        \\UPDATE documents SET platform = 'leaflet' WHERE platform = 'unknown' AND source_collection LIKE 'pub.leaflet.%';
        \\UPDATE documents SET platform = 'pckt' WHERE platform = 'unknown' AND source_collection LIKE 'blog.pckt.%';
        \\UPDATE documents SET platform = 'other' WHERE platform = 'standardsite';
        ,
    },
    .{
        .id = "005_reclassify_platform_from_basepath",
        .name = "detect platform from publication.base_path for site.standard.* docs",
        .sql =
        \\UPDATE documents SET platform = 'pckt'
        \\  WHERE platform IN ('other', 'unknown')
        \\  AND publication_uri IN (SELECT uri FROM publications WHERE base_path LIKE '%pckt.blog%');
        \\UPDATE documents SET platform = 'leaflet'
        \\  WHERE platform IN ('other', 'unknown')
        \\  AND publication_uri IN (SELECT uri FROM publications WHERE base_path LIKE '%leaflet.pub%');
        \\UPDATE documents SET platform = 'offprint'
        \\  WHERE platform IN ('other', 'unknown')
        \\  AND publication_uri IN (SELECT uri FROM publications WHERE base_path LIKE '%offprint.app%' OR base_path LIKE '%offprint.test%');
        \\UPDATE documents SET platform = 'greengale'
        \\  WHERE platform IN ('other', 'unknown')
        \\  AND publication_uri IN (SELECT uri FROM publications WHERE base_path LIKE '%greengale.app%');
        ,
    },
    .{
        .id = "006_backfill_base_path",
        .name = "denormalize publications.base_path onto documents.base_path",
        .sql =
        \\UPDATE documents SET base_path = COALESCE(
        \\  (SELECT p.base_path FROM publications p WHERE p.uri = documents.publication_uri),
        \\  (SELECT p.base_path FROM publications p WHERE p.did = documents.did LIMIT 1),
        \\  ''
        \\) WHERE base_path IS NULL OR base_path = '';
        ,
    },
    .{
        .id = "007_backfill_has_publication",
        .name = "denormalize documents.has_publication from publication_uri",
        .sql = "UPDATE documents SET has_publication = CASE WHEN publication_uri != '' THEN 1 ELSE 0 END WHERE has_publication = 0 AND publication_uri != ''",
    },
    .{
        .id = "008_cleanup_greengale_publications",
        .name = "remove stale greengale publication/self records and re-derive document base_path",
        // Targeted cleanup from 2026-01-22: deleted upstream record left wrong
        // basePath for greengale documents on did:plc:27ivzcszryxp6mehutodmcxo.
        .sql =
        \\DELETE FROM publications WHERE rkey = 'self'
        \\  AND base_path = 'greengale.app'
        \\  AND did = 'did:plc:27ivzcszryxp6mehutodmcxo';
        \\DELETE FROM publications_fts WHERE uri IN (
        \\  SELECT 'at://' || did || '/site.standard.publication/self'
        \\  FROM publications WHERE rkey = 'self' AND base_path = 'greengale.app'
        \\);
        \\UPDATE documents SET base_path = (
        \\  SELECT p.base_path FROM publications p
        \\    WHERE p.did = documents.did
        \\    AND p.base_path LIKE 'greengale.app/%'
        \\    ORDER BY LENGTH(p.base_path) DESC
        \\    LIMIT 1
        \\)
        \\  WHERE platform = 'greengale'
        \\  AND (base_path = 'greengale.app' OR base_path LIKE '%pckt.blog%')
        \\  AND did IN (SELECT did FROM publications WHERE base_path LIKE 'greengale.app/%');
        ,
    },
    .{
        .id = "009_backfill_documents_indexed_at",
        .name = "stamp indexed_at = created_at for pre-incremental-sync rows",
        .sql = "UPDATE documents SET indexed_at = created_at WHERE indexed_at IS NULL",
    },
    .{
        .id = "010_backfill_publications_indexed_at",
        .name = "stamp publications.indexed_at = now() for pre-incremental-sync rows",
        .sql = "UPDATE publications SET indexed_at = strftime('%Y-%m-%dT%H:%M:%S', 'now') WHERE indexed_at IS NULL",
    },
    .{
        .id = "011_add_documents_content_type",
        .name = "capture content.$type so we can identify the publisher (e.g. org.wordpress.html, at.markpub.markdown, pub.leaflet.content)",
        .sql = "ALTER TABLE documents ADD COLUMN content_type TEXT",
    },
    .{
        .id = "012_create_recommends",
        .name = "site.standard.graph.recommend table for endorsement aggregation",
        .sql =
        \\CREATE TABLE IF NOT EXISTS recommends (
        \\  uri TEXT PRIMARY KEY,
        \\  did TEXT NOT NULL,
        \\  rkey TEXT NOT NULL,
        \\  document_uri TEXT NOT NULL,
        \\  created_at TEXT,
        \\  indexed_at TEXT
        \\);
        \\CREATE UNIQUE INDEX IF NOT EXISTS idx_recommends_did_rkey ON recommends(did, rkey);
        \\CREATE INDEX IF NOT EXISTS idx_recommends_document_uri ON recommends(document_uri);
        \\CREATE INDEX IF NOT EXISTS idx_recommends_did ON recommends(did);
        ,
    },
    .{
        .id = "013_create_search_events",
        .name = "event log for time-windowed popular searches with discounted seed",
        // Per-event row replaces the monotonic popular_searches counter so
        // /popular can window by recency. Old test/seed traffic naturally
        // ages out; recent organic searches win immediately.
        //
        // Seed: each existing popular_searches row contributes MIN(count, 5)
        // events with random timestamps in the last 14 days. The heavy
        // discount turns a 689-hit "test" into 5 events spread over 2 weeks
        // (~2.5 events in a 7-day window) — a query searched 3 times today
        // already beats it. Within 14 days all seed data ages out fully.
        //
        // Length and trim filters mirror the runtime queue rules so the seed
        // stays consistent with what new events look like.
        .sql =
        \\CREATE TABLE IF NOT EXISTS search_events (
        \\  query TEXT NOT NULL,
        \\  at INTEGER NOT NULL
        \\);
        \\CREATE INDEX IF NOT EXISTS idx_search_events_at ON search_events(at);
        \\CREATE INDEX IF NOT EXISTS idx_search_events_query_at ON search_events(query, at);
        \\WITH RECURSIVE seq(n) AS (
        \\  SELECT 1 UNION ALL SELECT n + 1 FROM seq WHERE n < 5
        \\)
        \\INSERT INTO search_events (query, at)
        \\SELECT lower(trim(ps.query)),
        \\       strftime('%s', 'now') - abs(random() % (14 * 86400))
        \\FROM popular_searches ps, seq
        \\WHERE seq.n <= ps.count
        \\  AND length(trim(ps.query)) >= 2;
        ,
    },
    .{
        .id = "014_add_documents_url_dead",
        .name = "soft-hide docs whose destination URL HEADs 404",
        // The PDS-record-exists check (reconciler.checkRecord) is insufficient
        // for cases where the publisher updates their website without touching
        // the standard.site record — the record persists, the URL 404s, we
        // surface a dead link in search. Reconciler will populate this column
        // via a HEAD on the destination URL.
        //
        // Soft hide (vs delete) because the indexer doesn't consult
        // tombstones, so a delete-on-URL-404 strategy would flap every
        // resync. The row stays; search.zig WHERE clauses add `url_dead = 0`.
        .sql = "ALTER TABLE documents ADD COLUMN url_dead INTEGER DEFAULT 0",
    },
    .{
        .id = "015_index_documents_verified_at_embedded_at",
        .name = "scan-bound indexes for reconciler + embedder hot queries",
        // The reconciler picks docs via
        //   WHERE verified_at IS NULL OR verified_at < ? ORDER BY verified_at ASC LIMIT ?
        // and the embedder picks docs via
        //   WHERE embedded_at IS NULL LIMIT ?
        // Without an index on these columns, SQLite scans every row (~17.5k)
        // every cycle. `turso db inspect leaf --queries` showed these two
        // patterns accounted for ~44% of period-to-date rows-read (51.7M for
        // reconciler, 7.2M for embedder).
        //
        // With these indexes the planner can range-scan the NULL/early
        // entries directly and stop at LIMIT. Per-cycle reads drop from
        // ~17.5k to ~LIMIT (50 for reconciler, 50 for embedder).
        .sql =
        \\CREATE INDEX IF NOT EXISTS idx_documents_verified_at ON documents(verified_at);
        \\CREATE INDEX IF NOT EXISTS idx_documents_embedded_at ON documents(embedded_at);
        ,
    },
    .{
        .id = "016_index_documents_publication_uri",
        .name = "index documents.publication_uri for the base_path backfill",
        // insertPublication runs
        //   UPDATE documents SET base_path = ?, indexed_at = ..., embedded_at = NULL
        //   WHERE publication_uri = ? AND (...)
        // on every publication upsert. publication_uri was unindexed, so each
        // call full-scanned ~30k rows on remote turso — observed at avg ~3s,
        // max 48s, ~467s of turso query time over a 9h window (2026-06-16),
        // tail-latency that bled into search via shared-instance contention.
        // A plain btree turns the scan into a seek.
        .sql = "CREATE INDEX IF NOT EXISTS idx_documents_publication_uri ON documents(publication_uri)",
    },
    .{
        .id = "017_create_subscriptions",
        .name = "site.standard.graph.subscription table for publication-subscriber aggregation",
        // Sibling of `recommends` (012) one grain up: a recommend endorses one
        // document, a subscription follows a whole publication. `publication_uri`
        // is the at-uri the record points at; `did` is the subscriber. Indexed
        // both ways — by publication (leaderboard + "who's subscribed to me")
        // and by subscriber (dedupe via the unique (did, rkey)).
        .sql =
        \\CREATE TABLE IF NOT EXISTS subscriptions (
        \\  uri TEXT PRIMARY KEY,
        \\  did TEXT NOT NULL,
        \\  rkey TEXT NOT NULL,
        \\  publication_uri TEXT NOT NULL,
        \\  created_at TEXT,
        \\  indexed_at TEXT
        \\);
        \\CREATE UNIQUE INDEX IF NOT EXISTS idx_subscriptions_did_rkey ON subscriptions(did, rkey);
        \\CREATE INDEX IF NOT EXISTS idx_subscriptions_publication_uri ON subscriptions(publication_uri);
        \\CREATE INDEX IF NOT EXISTS idx_subscriptions_did ON subscriptions(did);
        ,
    },
    .{
        .id = "018_create_traffic_hourly",
        .name = "durable per-hour request metrics (was an in-memory ring buffer + brittle binary blob that reset whenever the endpoint enum changed)",
        // One row per (hour, endpoint). The backend keeps the hot path in memory
        // and upserts the current/previous hour here every 30s (off the request
        // path, same pattern as the stats table), then loads it on boot. Durable
        // across restarts AND endpoint-enum changes, and backfillable with plain
        // SQL (e.g. from logfire).
        .sql =
        \\CREATE TABLE IF NOT EXISTS traffic_hourly (
        \\  hour INTEGER NOT NULL,
        \\  endpoint TEXT NOT NULL,
        \\  count INTEGER NOT NULL DEFAULT 0,
        \\  sum_us INTEGER NOT NULL DEFAULT 0,
        \\  max_us INTEGER NOT NULL DEFAULT 0,
        \\  PRIMARY KEY (hour, endpoint)
        \\);
        ,
    },
    .{
        .id = "019_subscriptions_pubkey_columns",
        .name = "materialize (publication_did, publication_rkey) on subscriptions for a sargable publication join",
        // The publication join matched on the (did, rkey) PARSED out of
        // publication_uri per row (pubkey.joinOn), which defeated every index and
        // made a 621-publication publisher full-scan the whole subscriptions table
        // per publication. Store the parsed key once and index it so the join is
        // an indexed equijoin (pubkey.joinOnStored). The backfill uses the same
        // substr/instr decode the read-side expressions used.
        .sql =
        \\ALTER TABLE subscriptions ADD COLUMN publication_did TEXT;
        \\ALTER TABLE subscriptions ADD COLUMN publication_rkey TEXT;
        \\UPDATE subscriptions SET
        \\  publication_did = substr(publication_uri, 6, instr(substr(publication_uri, 6), '/') - 1),
        \\  publication_rkey = substr(substr(substr(publication_uri, 6), instr(substr(publication_uri, 6), '/') + 1), instr(substr(substr(publication_uri, 6), instr(substr(publication_uri, 6), '/') + 1), '/') + 1)
        \\  WHERE publication_did IS NULL AND publication_uri LIKE 'at://%/%/%';
        \\CREATE INDEX IF NOT EXISTS idx_subscriptions_pub_did_rkey ON subscriptions(publication_did, publication_rkey);
        ,
    },
    .{
        .id = "020_add_documents_source_cid",
        .name = "persist authoritative ATProto record CID for reconciliation",
        // NULL/empty means the row predates CID-aware ingestion and must be
        // baselined by a source-verified reconciliation pass. New firehose and
        // PDS-backfill writes populate it directly.
        .sql = "ALTER TABLE documents ADD COLUMN source_cid TEXT",
    },
    .{
        .id = "021_add_publications_show_in_discover",
        .name = "persist site.standard.publication preferences.showInDiscover",
        // 1 = discoverable (the lexicon default when preferences are absent).
        // 0 = the publication opted out of discovery surfaces; still indexed,
        // but hidden from search results unless the caller asks (hidden=show).
        .sql = "ALTER TABLE publications ADD COLUMN show_in_discover INTEGER NOT NULL DEFAULT 1",
    },
};

// --- tests ---

const std = @import("std");

test "migration ids are unique" {
    // zug.validateUnique runs this check at zug.sqlite.run time, but that's
    // the production startup path. catching dupes at build time is cheaper.
    for (migrations, 0..) |left, i| {
        for (migrations[i + 1 ..]) |right| {
            try std.testing.expect(!std.mem.eql(u8, left.id, right.id));
        }
    }
}

test "every migration has SQL or a callback" {
    // we don't currently use the .up callback path; if that changes this
    // assertion can relax to "has at least one of sql or up".
    for (migrations) |m| {
        try std.testing.expect(m.sql != null);
        try std.testing.expect(m.sql.?.len > 0);
    }
}

test "BOOTSTRAP_BASELINE_COUNT does not exceed migrations.len" {
    // safety: if someone shrinks the migrations array without updating the
    // constant, bootstrap would index out of bounds. tests catch this.
    try std.testing.expect(BOOTSTRAP_BASELINE_COUNT <= migrations.len);
}

test "migration ids start with three-digit prefix" {
    // discipline: prevents accidental 'fix_typo' style ids that don't sort.
    for (migrations) |m| {
        try std.testing.expect(m.id.len >= 4);
        try std.testing.expect(std.ascii.isDigit(m.id[0]));
        try std.testing.expect(std.ascii.isDigit(m.id[1]));
        try std.testing.expect(std.ascii.isDigit(m.id[2]));
        try std.testing.expectEqual(@as(u8, '_'), m.id[3]);
    }
}
