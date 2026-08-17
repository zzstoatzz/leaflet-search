# scaling economics

Decisions and measurements from the 2026-08-17 capacity work: what one
$6/mo fly machine actually sustains, what we changed to stretch it, and what
the next dollar should buy. Companion to `COSTS.md` (spend) and
`search-architecture.md` (mechanics).

## the measured baseline (loadgen, 2026-08-17)

Method: throwaway fly machine (`python:3.12-slim`, 256MB, ewr — same region
as the backend), distinct nonce'd queries so no cache/memo layer answers,
45s per level against the fly hostname directly. Pattern stolen from
zat.dev/stream's disposable loadgen apps; costs pennies, destroy after.

| mode | concurrency | rps | p50 | p95 | errors |
|---|---|---|---|---|---|
| keyword | 2 | **9.9** | 189ms | 339ms | 0 |
| keyword | 5 | **2.1** | 654ms | 9.2s | 0 |
| keyword | 10 | **0.7** | 11.9s | 27.1s | 0 |
| hybrid | 2 | 0.3 | 6.9s | 9.3s | 0 |
| hybrid | 5 | 0.4 | 14.5s | 23.4s | 0 |

Reading: the first level ran on **burst credits** (9.9 rps, sub-200ms). The
second level exhausted them mid-test and the machine dropped to its
**6.25%-of-a-core sustained baseline** (fly shared vCPU quota: 5ms per 80ms
period, ~500 CPU-s burst balance — fly.io/docs/machines/cpu-performance).
The arithmetic closes: 0.0625 core ÷ ~100ms CPU per keyword query ≈ 0.6 rps,
which is what concurrency 10 measured. Nothing errored — requests just
queued (latency, not failures, is how this box degrades). Recovery to normal
latency took minutes after load stopped (burst balance refills at 1−load).

**So: uncached sustained capacity of the current box ≈ 1 keyword rps / ~0.3
hybrid rps.** Everything above that must come from cache layers or different
hardware.

## what we changed instead of buying hardware (all shipped 2026-08-16/17)

1. **Per-query CPU, hybrid: ~60× reduction** (`70165ac`, `5288e75`).
   Fusion's keyword pass ran a second corpus-scan FTS MATCH plus `snippet()`
   over 620 external-content bodies. Now: single-MATCH snippet-free
   candidate SQL (`LOCAL_DOCS_FTS_PREFILTER_NOSNIP_SQL`), snippets hydrated
   post-fusion via FTS5 rowid-constrained MATCH probes (a posting-list seek,
   ~0.1ms), fusion depth 200→75 (RRF weight at rank r is 1/(60+r); the tail
   was noise). Hybrid 8.4s → 0.7s warm. Plan-guard tests lock the shapes.
2. **Edge cache 60s→10min fresh, 30min stale** (`5288e75`). The replica
   refreshes on ~2h snapshot adoption; a 60s TTL was conservatism we paid
   for in origin load.
3. **Origin memo + ETag** (`b232b1d`, `backend/src/server/memo.zig`).
   ETag = (snapshot adoption generation, 5min bucket); memo keyed per URL,
   512-entry bound, clear-all on token change. Repeat queries cost a map
   lookup; If-None-Match answers 304 with zero search work. This is the
   cross-colo complement to the per-colo edge cache (`caches.default` is
   per-datacenter — N colos would otherwise miss N times). Watch
   `search.memo_hit` / `search.etag_304` counters.
4. **Zone rate limit**: 30 req/10s per IP on `pub-search.waow.tech/api/*`
   → JSON 429 (the free plan's single rule). A storm seatbelt, not adversary
   protection — the fly hostname still serves raw (rollback path).
5. **MCP server routed through the edge** (`0b1b447`) so agent traffic
   shares all of the above.

## capacity model

Demand that reaches the origin ≈ distinct-query QPS × (1 − edge/memo hit
rate). Supply ≈ sustained cores × (1 / CPU-s per query). Today: ~0.06
sustained cores, ~0.1 CPU-s/keyword query. A thousand *casual* users are
bursty and mostly cache-served; a thousand *agents* issuing distinct
long-tail queries are sustained load and blow through this box by ~an order
of magnitude.

## price of a sustained core (fly, 2026-08 list)

| machine | $/mo | sustained cores | $/core |
|---|---|---|---|
| shared-cpu-1x 1GB | 5.92 | 0.0625 | ~$95 |
| shared-cpu-2x 1GB | 6.64 | 0.125 | ~$53 |
| performance-1x 2GB | 32.19 | 1.0 | **~$32** |

For sustained load, one performance-1x out-provides five shared-1x at equal
price. Shared machines only win for bursty traffic.

## next levers, in order

1. Watch the counters: if memo/edge hit rate is high, the current box may
   hold much longer than the uncached numbers suggest. Measure before
   spending.
2. Serve/ingest split: a serve-only process group that pulls the R2
   snapshot and owns no volume — stateless cattle, the prerequisite for
   horizontal serving. (Pattern: typeahead's search box + stream's sealed
   immutable artifacts.)
3. First hardware dollar: one performance-1x serve machine (~$32/mo) ≈ 10+
   sustained keyword rps uncached, more with caches. Re-run the loadgen to
   confirm.
4. Hybrid's remaining cost is the semantic leg (~0.5–2.6s, network wait on
   voyage+turbopuffer) — parallelizing keyword+semantic hides it entirely
   (deferred deliberately; see memory/git history for the tradeoff
   discussion).

## patterns imported from sibling projects

From `~/tangled.org/zzstoatzz.io/typeahead`: COSTS.md as a mandate with
measured-utilization tables; "no request does corpus-proportional work" as a
regression class; fail-fast read pool → 503 → edge serves stale; freshness
endpoint through the real serving path; aggressive-but-bounded cache-key
normalization; never cache a timeout.

From `~/tangled.org/zat.dev/stream`: encode-once shared responses with
single-flight (→ our memo); ETags from immutable artifacts; "the volume is
the cost"; kill the laggard, don't buffer it; bounded cardinality on every
map an attacker can grow; disposable fly loadgen apps; label every
worker-exit path.

## open items

- Loadgen showed prod latency degrades globally under one hot client —
  the zone rate limit doesn't protect the raw fly hostname. Closing that
  means fly-side per-IP limiting or making the edge the only public door.
- The site SPA + fly host both still answer `/search` unauthenticated;
  fine while public-read is the product, revisit if abuse appears.
