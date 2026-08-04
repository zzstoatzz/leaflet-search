# scale plan: +300k docs in one week (5x corpus)

context: large publications (e.g. techdirt's ~80k archive) are starting to backfill
onto atproto as standard.site records. planning target: **300k new documents over one
week** on top of today's ~60k corpus. 300k/week ≈ 0.5 docs/s sustained, but reality is
bursts — a single publisher may put tens of thousands of records in minutes.

informed by a full audit of this repo's throughput limits and deep reads of two
sibling systems:

- [typeahead](https://tangled.sh/@zzstoatzz.io/typeahead) (10M+ actors), whose
  governing invariant we adopt for the serve side: **no request path may do work
  proportional to corpus size; ranking belongs at build time where possible.**
  its evidence also says FTS5 at 300k–1M docs is squarely inside SQLite's comfort
  zone — we harden the pipeline around FTS, we do not replace it.
- [stream](https://tangled.sh/@zat.dev/stream) (Zig Jetstream V2 reimplementation
  on zat; 16.7B events archived in its full-network experiment), whose live
  scheduler is the proven shape for our ingest side: **thin reader thread; verify
  + DID resolution in a per-DID-FIFO worker pool; per-DID (not global) bounded
  drop-oldest; durable cursor = min(inflight)−1, committed only after the data it
  covers is durable.**

## ranked bottlenecks (from code audit, 2026-08-04)

1. **backend ingest worker** — 1 worker, 256-frame drop-oldest queue, ~6–10 serial
   turso RTTs per doc (`backend/src/ingest/ingester.zig:89,264`, `ingest/indexer.zig:47-281`).
   sustains ~1–3 docs/s; any burst of thousands overflows the queue and drops
   **already-ACKed** frames. the #1 pre-influx fix.
2. **builder's unpaginated table dumps** — `document_tags`, `recommends`,
   `subscriptions`, `publications`, `popular_searches` each fetched in ONE giant
   SELECT (`db/sync.zig:124-205`). memory-unbounded single turso scans: the exact
   pattern that has wedged turso before.
3. **builder watchdog 2700s** vs builds already ~40min at 60k (`builder.zig:31`).
   doc paging + VACUUM + sha256 + R2 upload all scale ~linearly; builds start
   getting killed at 5x.
4. **tag-browse recommends aggregate per request** — full
   `GROUP BY document_uri` over all of `recommends` on every browse
   (`server/search.zig` LOCAL_TAG_BROWSE_SQL). O(corpus) on the request path.
5. **ingester sync PLC resolve** — up to 10s blocking the firehose thread per new
   author (`ingester/src/verifier.zig:46`). archive backfills = many new DIDs.
6. **embedder drain** — 1 worker, batch 20, 20 serial turso UPDATEs per batch
   (`ingest/embedder.zig`). 300k docs = 15,000 sequential batches; 429 backoff
   escalates 300s×n. multi-day drain concurrent with ingest write load.
7. **frame-size mismatch** — channel sends up to 5MB (`channel.zig:257`), backend
   consumer caps reads at 1MB (`ingest/ingester.zig:243`). large long-form docs
   error the read loop.

not at risk: doc-count gate (watermark-pinned both sides), FTS query paths with plan
tests, keyset doc export (linear + paced), ingester ring (evicts but pins cursor —
relay replay recovers).

## phase 1 — before the influx (SHIPPED 2026-08-04)

### 1a. batch the ingest write path
replace per-doc serial RTTs with batched writes: accumulate N docs (or T ms), then
one turso pipeline/transaction for dedupe checks + upserts + FTS + tags. this is the
search-mutex incident's lesson generalized: drain the queue fast, do I/O in bulk.
targets: queue drains at ≥50 docs/s; keep `drop_oldest` as backstop but it should
never fire. consider raising QUEUE_CAPACITY (256 → 4096 frames) since frames are
small; the fix is throughput, the buffer is insurance.

### 1b. fix the 1MB/5MB frame mismatch
raise the backend consumer's websocket `max_size` to match the channel's 5MB.
one-line-ish; do it first.

### 1c. paginate the builder's table dumps
keyset-paginate `document_tags`, `recommends`, `subscriptions`, `publications` the
same way docs are paged (LIMIT 500 + pacing). removes the unbounded turso scans and
the memory cliff.

### 1d. raise builder timeout + measure
bump `BUILDER_TIMEOUT_SECS` (2700 → 7200) via the prefect deployment env, and emit
per-phase timings (export / vacuum / hash / upload) in ops.snapshot telemetry so we
see which phase grows. (typeahead lesson for later, not now: turso's binary **export
endpoint** was ~60× faster than row streaming for them — the escape hatch if paging
gets slow at 300k+.)

### 1e. restructure the ingester read path on stream's live-scheduler shape
new-DID key resolution must not block readLoop for up to 10s — and stream shows the
principled fix rather than a deadline tweak (`stream/src/internal/ingest/pipeline.zig`,
`verify.zig`, docs/live-scheduler.md):

- **thin reader thread**: readLoop does frame inspection/classification only;
  verification moves to a worker pool (stream uses 32 for the whole network; a
  small pool — ~8 — fits our slice).
- **per-DID FIFO chains**: a worker owns one DID's chain until drained, so events
  for one author stay ordered while cross-DID work parallelizes. resolution
  happens *inside* the worker (each worker owns its own keep-alive `zat.DidResolver`),
  so a slow PLC blocks one worker, not the firehose.
- **per-DID bounded drop-oldest** (stream: 64/DID) instead of only the global ring:
  one hot/spammy repo during a burst can't evict everyone else's events. count
  drops per-DID. beware stream's "workers×2 is one constant" trap: the queue bound
  and the pool size are coupled — a bounded producer must not outrun its consumer.
- **LRU key cache, sized generously, never clear-on-full** (stream: 250k entries;
  clear-on-full causes a resolve stampede — exactly what a many-new-authors influx
  would trigger). signature mismatch → evict key, re-resolve once (rotation);
  `#identity` events evict.
- **typed verify outcomes** (valid / replay / invalid_signature / unverified /
  needs_resync) with counters, instead of binary pass/drop — this is what makes
  "dropped ~70% under resync lag" diagnosable, and `needs_resync` becomes a signal
  the reconciler/backfill can consume instead of silent loss.
- **cursor semantics**: durable cursor = min(inflight)−1 (monotone, seeded from the
  stored cursor so replays can't regress it), flushed ~5s, and advanced only after
  the covered data is durable downstream. today we ACK to the ingester ring before
  the backend has written turso — under this lens that's an at-most-once gap; the
  batching work in 1a should move the ACK after the batch commit.

## phase 2 — during the influx (watch + absorb)

- **watch the ingest queue**: add a gauge/log for queue depth + drop count; alert on
  any drop (drops are silent data loss of ACKed frames).
- **turso canary discipline**: the embedder drain + ingest batches are OUR load; if
  turso degrades, throttle ourselves first (feedback: bulk scans ARE the outage).
- **embedder**: let it drain; it's days, not hours, and that's fine — keyword search
  is live immediately, semantic lags. add an index-friendly backlog query
  (partial index on `embedded_at IS NULL`) so the per-minute scan doesn't grow with
  the backlog. if voyage 429s, cap the escalating backoff at ~10min instead of 1h.
- **recovery playbook**: per-repo catch-up = `/admin/backfill` (single-flight,
  listRecords paging — fine for one publisher, too slow for many). if we drop frames
  across many repos, batch-drive it via `scripts/backfill` overnight rather than
  hand-triggering. **do not scale recovery by adding concurrency**: stream measured
  backfill throughput getting *worse* going 100→200 workers and 200/400 OOM-killing
  a 15GB box — the lever is per-host accounting/parking (few workers per PDS host),
  not a bigger pool.
- **labeler**: 80k puts from one DID is the bulk-generated volume signature.
  archive backfills of composed content (techdirt et al.) must not get flagged —
  pre-exempt known publisher DIDs or gate the volume heuristic on account age /
  platform.

## phase 3 — request-path hardening (3a SHIPPED 2026-08-04; 3b audit pending)

### 3a. precompute tag-browse ranking (typeahead's core idea, applied narrowly)
tag browse currently aggregates all of `recommends` per request. move the recommend
count to build time: materialize `document_recommend_counts` (or bake a ranked
per-tag top-N) into the snapshot. browse becomes an index lookup; zero request-time
aggregation. this is the one place we adopt "ranking at build time" now.

### 3b. audit remaining O(corpus) request work
- dashboard fallback STATS_SQL: five COUNT(*) full scans against turso — cache
  harder or serve counts from snapshot meta.
- rerun `turso db inspect --queries` after the influx settles (access-pattern audit
  playbook) and kill anything corpus-proportional that appeared.

## phase 4 — promotion-gate refinements (SHIPPED 2026-08-04 — most already existed; added shrink floor + tokenizer-version rule)

- stamp tokenizer/normalizer + scoring version into snapshot meta; server refuses a
  snapshot with a mismatched version.
- verify byte_size + sha256 on adoption (we hash at build; check at adopt too).
- count floor on adoption: refuse a snapshot <50% of the previous build's doc count.
- keep the previous snapshot on the volume for one-manifest-rewrite rollback.

## explicitly NOT doing

- **not replacing FTS5.** typeahead abandoned FTS at 5.9M tiny records with a fixed
  prefix-query shape; document BM25 over bodies is what FTS5 is for, and 300k–1M is
  comfortable. our plan-test guards already cover the pathological plans.
- **not building an overlay layer yet.** the frozen-replica + 2h snapshot cadence
  keeps working at 300k; overlay (typeahead's live-freshness-on-immutable-base
  pattern) is the documented next step if 2h staleness ever hurts, not part of this.
- **no sharding, no new datastore.** disk/RAM at 5x is single-digit GB; the fly
  volume math holds.

## success criteria

- zero ingest-queue drops through the influx (gauge proves it, not absence of alarms)
- builds complete inside the (raised) watchdog throughout; snapshot adoption cadence
  holds at 2h
- keyword + tag + semantic search all pass /check-prod during and after
- turso stays healthy under our own write load (canary, not vibes)
