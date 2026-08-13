# overlay serving: live freshness beside the frozen snapshot

*shipped 2026-08-13. the serving stack is now: CF edge cache → snapshot + live
overlay merge (fly `app`) → background tenants (fly `worker`) → offline builder
(heavypad). this is typeahead's architecture ported to document search.*

## the problem it solved

the local replica is frozen by construction (in-place sync deleted 2026-06-26
after the persist-format outage). before the overlay, its ONLY freshness
mechanism was adopting a complete new ~800MB snapshot every 2h — which
replaced every byte of the serving file (100% page-cache eviction), saturated
the 1GB machine during download/verify, and coupled freshness to a heavyweight
operation. measured cost: first-query-after-idle 0.6–2s chronically; 5–58s
during adoption + backlog windows; a fresh document was invisible to search
for up to 2 hours.

## the design

- **`/data/overlay.db`** (`backend/src/db/OverlayDb.zig`): a small persistent
  SQLite file that survives adoptions. holds `documents_overlay` (full result
  projection per doc), `overlay_fts` (fts5, **same unicode61 tokenizer as the
  snapshot** — divergence serves wrong results with no error), tags, and
  tombstones (`deleted=1`).
- **write path**: the ingest worker projects every document upsert/delete into
  the overlay AFTER the turso batch commits (`ingest/indexer.zig`).
  at-least-once + idempotent; overlay failure logs + counts, never fails
  ingest. a lost write is healed by the next snapshot — the overlay is a
  freshness cache, never a source of truth.
- **read path** (`server/search.zig`): keyword and tag queries run the
  snapshot query and a capped overlay query (`OVERLAY_HIT_CAP`), computing the
  IDENTICAL score expression (`bm25 rank + days_old/30`) per source, then
  merge in Zig: overlay wins on uri collision, tombstones suppress snapshot
  hits, and the merged stream feeds the existing dedup/visibility gates so
  the response shape is unchanged. semantic/hybrid hydration
  (`fetchLocalExtras`, `isBridgyFed`) reads overlay-first.
  - bm25 across two FTS tables is not one scale in general; it works here
    because ranking is recency-dominant and overlay rows are at most one
    build-cadence old. verified empirically at cutover (A/B parity on 4
    surfaces; the positive case: a doc indexed post-snapshot was a miss with
    `overlay=0` and a HIT with `overlay=1`).
- **compaction**: the promote watcher, after a successful adoption, deletes
  overlay rows with `indexed_at <=` the adopted manifest's `source_watermark`
  (adopt-then-compact — the crash-safe order; a crash before compaction
  leaves shadowed-but-correct rows for the next cycle). the builder also
  stamps `sync_meta('source_watermark')` into the snapshot itself.
- **bounds**: `MAX_OVERLAY_ROWS` (default 50k) sheds oldest under backfill
  floods (amortized check every 512 upserts). overlay size is
  ingest_rate × snapshot_cadence — corpus-independent.

flags: `OVERLAY_WRITE=1` (projection), `OVERLAY_SERVE=1` (merge), both live in
`backend/fly.toml`. per-request `?overlay=0/1` overrides serving for A/B.
`/admin/overlay/status` exposes rows/tombstones/watermark + recent uris.

## what it unlocks

snapshot cadence is now decoupled from freshness: docs are searchable within
~a minute of publish regardless of when the builder last ran. the prefect
cadence (`40 */2` on `pub-search-snapshot`) can stretch to 6h/24h (Stage 4,
pending soak data) — which is what makes 10x corpus cheap: builder export is
linear but offline, adoption is rare, and no request path does
corpus-proportional work.

## the bounded candidate pass (same day)

common words made the keyword candidate pass corpus-proportional ("what"
matched 24.6k of 71k docs → one covering-index probe per match → 7–14s).
unfiltered queries now rank all matches by bm25 INSIDE the fts index
(posting-list scan, no table probes), and only the top
`CANDIDATE_PREFILTER_K` (2000) get covering-index probes for the
recency+policy re-rank. measured: candidate pass 0.229s → 0.088s warm; cold
"what" 7.4s → 0.7s. trade: recency promotes only within the bm25 top-K on
unfiltered queries (author/since/platform keep the exhaustive shape). the
results this displaced for "what" were <1-day-old docs with bm25 ≈ -1.1 —
barely-relevant mentions that recency had rescued.

## process isolation + edge (same day)

- **fly process groups** (`backend/fly.toml`, plyr.fm's pattern):
  `app` = HTTP + ingest + labeler + promote (everything touching `/data`);
  `worker` = reconciler + embedder (stateless; turso/voyage/tpuf only) on its
  own 512MB machine. background storms (PDS-timeout bursts, embedding
  batches, a review loop hot-failing on a delisted cocore model) can no
  longer share a vCPU with search — the cause of a 14s query on 2026-08-13.
  `PROCESS_ROLE` env selects the role; unset runs everything (dev unchanged).
  **the worker group must stay at exactly one machine** (single embedder
  writer against turso DiskANN).
- **CF edge cache** (`site/functions/api/[[route]].js`): the frontend calls
  same-origin `/api/*`; GET `/search` is cached at the edge 60s with 10min
  stale-while-revalidate. repeated queries answer in ~10-30ms without
  touching fly; backend blips serve stale. `/admin` is never proxied;
  `?edge=0` bypasses per-request; rollback is reverting the frontend to the
  fly URL (which still serves directly).
