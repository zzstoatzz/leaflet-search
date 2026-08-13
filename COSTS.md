# COSTS.md

> **Mandate.** This file tracks the infrastructure costs attributed to **pub-search** and
> what we're doing to bring them down. It is the running record of what this project
> spends, why, and a changelog of changes that moved the number. If the spend here is
> unjustified, that's a signal to clean up — not to ignore it.

## current cost — fetch it live, never hardcode

Costs drift, so this file deliberately does **not** hardcode a dollar figure. Get the
current monthly cost for this repo from the daily snapshot (collected by
`my-prefect-server`, surfaced at https://hub.waow.tech):

```bash
curl -s https://hub.waow.tech/api/costs.json | jq '{
  as_of: .generatedAt,
  this_repo_monthly_usd: (
    [ .lineItems[] | select(.service as $s | ["leaflet-search-backend", "leaflet-search-ingester", "leaflet-search-tap"] | index($s)) ]
    | (map(.amount) | add // 0) / 100
  ),
  lines: [ .lineItems[] | select(.service as $s | ["leaflet-search-backend", "leaflet-search-ingester", "leaflet-search-tap"] | index($s))
           | {service, provider, usd: (.amount/100), estimated} ]
}'
```

Or open the costs panel at https://hub.waow.tech and group **by project**.

Services attributed to this repo: `leaflet-search-backend`, `leaflet-search-ingester`, `leaflet-search-tap`. If that list is
wrong, fix the mapping in `my-prefect-server`
(`packages/mps/src/mps/costs/projects.py`) rather than editing numbers here.

## how we might bring this down
- biggest line is usually `leaflet-search-backend` — check its utilization and right-size before anything else.
- Fly figures are **estimates** from machine inventory — reconcile against the Fly dashboard, and enable auto-stop on bursty/idle machines.

## changelog
- **2026-08-13** — app/worker process-group split added a second backend machine
  (worker: reconciler + embedder), immediately right-sized 1GB → **512MB**
  (~$3/mo; measured ~110MB in use). App stays 1GB shared-1x — its RAM is the
  page cache holding the ~800MB serving replica, i.e. load-bearing, not slack.
  CF edge proxy (`/api/*`, 60s cache) added at **$0** (Pages Functions free
  tier; volume is orders of magnitude under the 100k/day limit). Possible
  future cut: move the worker's tenants to heavypad prefect flows and delete
  the machine. ⚠️ the live-figure command above currently returns **$0 with
  zero line items** — the service mapping in `my-prefect-server`
  (`costs/projects.py`) has drifted; fix it there before trusting the number.
- **2026-06-17** — initial cost notice; 3 service(s) attributed here. Run the command above for the live figure.
