# costs

what this project spends and why. if the spend here is unjustified, that's a
signal to clean up — this file exists so scaling decisions run on measured
numbers, not vibes. (format stolen from typeahead's COSTS.md and stream's
"don't hardcode the number, name the query".)

## fleet (verify live: `fly machine list -a <app>`, `fly volumes list -a <app>`)

| app | machines | size | storage |
|---|---|---|---|
| leaflet-search-backend | 1× app + 1× worker | shared-cpu-1x 1GB each | 3GB volume (app) |
| leaflet-search-ingester | 1 | shared-cpu-1x 512MB | small volume |

Compute at fly list prices is roughly $5.92/mo per shared-cpu-1x·1GB and a
few $/mo for volumes — order $15–20/mo total. Verify against the dashboard
invoice, not this file. Shared org infra (Turso plan, Cloudflare, voyage,
turbopuffer, logfire) is deliberately excluded here.

## the capacity fact that matters

A fly **shared** vCPU is throttled to **6.25% of a core sustained** (5ms per
80ms period), with a burst balance capped at ~500 CPU-seconds
(fly.io/docs/machines/cpu-performance). Our interactive traffic lives on
burst credits; sustained load is what the 6.25% buys. Per sustained core:

- shared-cpu-1x 1GB: $5.92 / 0.0625 core ≈ **$95/core**
- performance-1x 2GB: $32.19 / 1.0 core ≈ **$32/core**

So under *sustained* load, performance machines are ~3× cheaper per unit of
work; shared machines only win for bursty traffic. Don't scale by adding
shared machines to serve sustained agent load.

## measure before resizing

```sh
# CPU throttle + load on the backend (fly Prometheus; note FlyV1 prefix, not Bearer)
curl -s "https://api.fly.io/prometheus/personal/api/v1/query?query=fly_instance_cpu_throttle" \
  -H "Authorization: FlyV1 $FLY_API_TOKEN"

# per-query cost + memo/etag effectiveness (logfire)
#   search.iterate.docs_fts duration, search.memo_hit, search.etag_304 counters

# sustained capacity: run scripts/ loadgen pattern — throwaway fly machine in ewr
# (see docs/scaling-economics.md for the measured baseline)
```

## levers, in order (as of 2026-08)

1. cache hit rate (free): 10min edge TTL + ETag 304s + origin memo — shipped
   2026-08-17. Watch `search.memo_hit` / `search.etag_304` before buying
   anything.
2. per-query CPU (free): hybrid fusion fixes shipped 2026-08-17 (8.4s → sub-1s
   warm). Remaining: semantic leg ~0.5–2.6s (network wait, parallelizable).
3. serve/ingest split, then horizontal serve machines: serve-only process
   group pulls the R2 snapshot, no volume — cattle. Prereq for any spend.
4. buy sustained cores only when measured demand says so: one performance-1x
   ≈ 5 shared-1x of sustained capacity at the same price.

## changelog

- 2026-08-17: hybrid 60×-per-query fix + edge TTL 60s→10min + origin
  memo/ETag + zone rate limit (30/10s/IP). Baseline fleet unchanged (~$15-20/mo).
