# builder off-box: completing the lambda shape

status: **in progress** (2026-07-25). companion to [scaling-plan.md](scaling-plan.md)
(this executes its steps 2-revision and 3) and to typeahead's
`docs/typeahead-index-design.md` / the "pendulum backswing" post, which this
deliberately copies. typeahead lifted its builder off fly onto heavypad via
prefect in june 2026 (`my-prefect-server/flows/typeahead_index.py`); pub-search
adopted typeahead's snapshot pipeline but kept the builder on a fly machine
with an hourly schedule — and that residue is the recurring failure.

## why the fly builder keeps paging us

freshness currently comes ONLY from snapshot cadence (no overlay), so the
builder is on an hourly leash with a wall-clock watchdog:

- build time grew 17→40min with the corpus (full 600MB content re-page from
  turso every hour); turso slow patches push past the watchdog → killed build
  → stale-freshness alert (2026-07-23, -24, -25)
- the watchdog value itself was reverted by the deploy-time reconcile script
  (2026-07-24) — machine-level tuning does not survive the pipeline
- hourly full-table scans of turso are standing load the serving path feels

the lambda framing (see notes repo `architecture/serving-from-snapshots.md`):
batch cadence is load-bearing *only because the speed layer is missing*.
typeahead's answer: builder = pure batch, off-SLA, on the home box ("a
late/failed run just leaves the prior snapshot serving"), freshness = overlay.

## target

```
heavypad (home-pool prefect worker)          fly
─────────────────────────────────           ─────────────────────────────────
pub-search-snapshot flow (daily):            backend serves search unchanged:
  clone pub-search → zig build →              promote watcher polls R2,
  BUILDER_MODE=1 CHANNEL=prod →               verifies manifest, adopts.
  gated build → rclone → R2                   (adoption is ~1s since sidecar
                                              attestation)
```

- same binary, same gates (doc-count vs turso, FTS sentinel, quick_check),
  same R2 bucket/channels, same manifest contract. the serving side cannot
  tell where a snapshot was built — that is what makes the cutover clean.
- no watchdog pressure: prefect flow timeout is generous (hours); a slow or
  failed run just leaves the previous snapshot serving. no SLA, no restart
  loop, no fly scan-storm.
- cadence: hourly-equivalent freshness is NOT preserved by this phase alone;
  see sequencing. overlay (phase 2) is what buys the relaxed cadence.

## sequencing (no downtime, verify before delete)

1. **flow lands** in my-prefect-server (`flows/pub_search_snapshot.py`,
   modeled 1:1 on `typeahead_index.py`): clone → `cd backend && zig build
   -Doptimize=ReleaseSafe` → run `pub-search` with `BUILDER_MODE=1`.
   env via secret blocks: shared `turso-url`/`turso-token` (already the leaf
   db — same blocks atlas uses) + new `leaflet-index-r2-*` blocks
   (`INDEX_R2_{ENDPOINT,BUCKET,ACCESS_KEY_ID,SECRET_ACCESS_KEY}`, bucket
   `leaflet-search-index`). `RCLONE_BWLIMIT` set like typeahead's.
2. **staging soak**: run the deployment with `BUILDER_CHANNEL=staging` from
   heavypad; verify `latest.staging.json` + manifest sha/counts against a
   fly-built prod manifest of the same hour.
3. **prod flip**: set `BUILDER_CHANNEL=prod` + `BUILDER_ALLOW_PROD=1` on the
   deployment; watch one publish → fly promote watcher adopts it (log:
   `local db: adopted pending snapshot`). fly builder machine still scheduled
   at this point — pointer-last upload makes concurrent builders safe (last
   writer wins a whole snapshot, never a torn one).
4. **decommission fly builder** (same day): destroy machine
   `snapshot-builder-hourly`; DELETE `scripts/reconcile-snapshot-builder`,
   `scripts/guard-snapshot-builder`, their workflow step + path triggers in
   `.github/workflows/deploy-backend.yml`, and the builder-machine runbooks
   in CLAUDE.md. builder *code* (builder.zig, r2.zig, BUILDER_* env handling)
   stays — it is what heavypad runs. the watchdog stays too (flow-level belt
   + binary-level suspenders), default bumped so ad-hoc runs aren't leashed.
5. **cadence**: start at every 2h (bandwidth ~14GB/day upload at 600MB —
   fine on the uplink with RCLONE_BWLIMIT; revisit vs typeahead's every-3-day
   + overlay endgame). freshness alert threshold adjusted to cadence + build
   time + margin.
6. **phase 2 — live overlay** (separate design, lands independently): ingest
   path projects creates/updates/deletes into a small overlay next to the
   snapshot (authoritative, not a cache; compacted at adoption by
   `source_watermark`). then cadence relaxes to daily and the alert measures
   something the overlay has already fixed.

## R2 credential note

the fly builder used `INDEX_R2_*` app secrets (values not readable back).
heavypad needs its own: mint a fresh R2 API token scoped to
`leaflet-search-index` (account has `CLOUDFLARE_API_TOKEN` locally) and store
as prefect secret blocks. fly serving machines keep their existing secrets —
read path is untouched.

## what "solved completely" means here

the failure class was: *builder duration/health coupled to serving freshness
through a tight schedule on infrastructure that fights long batch jobs*.
after this: builds are batch work on a 24-core box with no wall-clock
opponent, publishing is best-effort with gates, and a missed run costs
nothing until the overlay (phase 2) makes even cadence mostly cosmetic.
