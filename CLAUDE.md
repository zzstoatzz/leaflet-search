# pub-search notes

## deployment
- **backend**: push to `main` touching `backend/**` → auto-deploys via GitHub Actions
- **frontend**: manual deploy via `site/deploy.sh` — regenerates the workbox service worker (precache manifest embeds content hashes) then runs wrangler with `--branch=main`. Don't call wrangler directly or returning visitors get stale precached assets.
  - ⚠️ `deploy.sh` ships whatever `site/atlas.json` is on disk (gitignored, likely stale) — **always pull the live one first**: `curl -sfo site/atlas.json https://pub-search.waow.tech/atlas.json`. Otherwise you regress prod atlas data until the next 6h `leaflet-atlas` prefect rebuild.
- **ingester**: manual deploy via `ingester/scripts/deploy.sh` (stages the repo-root `banned-dids.txt` into the build context first, then `fly deploy --app leaflet-search-ingester`). Don't call `fly deploy` directly — the build embeds `../banned-dids.txt` which isn't in `ingester/`'s context without staging.
- `--app` does NOT protect against deploying from the wrong directory — it only renames the target; the config (ports, env, mounts) still comes from that directory's `fly.toml`. Always `cd` into the app dir first. (2026-06-10: root-dir deploy with `--app leaflet-search-ingester` was stopped only by a volume-name mismatch.)

## remotes
- `origin`: tangled.sh:zzstoatzz.io/pub-search
- `github`: github.com/zzstoatzz/pub-search (CI runs here)
- push to both: `git push origin main && git push github main`

## architecture
- **backend** (Zig): HTTP API, FTS5 search, vector similarity; same binary runs as the snapshot builder under `BUILDER_MODE=1`
- **ingester** (Zig): our own firehose consumer — verifies every commit (signature + MST diff via zat), drops bridgy/non-canonical repos, re-emits over a `/channel` websocket the backend consumes (`backend/src/ingest/ingester.zig`)
- **site**: static frontend on Cloudflare Pages
- **db**: Turso (source of truth) + local SQLite read replica (FTS queries; FROZEN by construction — in-place sync deleted 2026-06-26 — refreshed only by snapshot adoption, see docs/scaling-plan.md)
- **R2**: `leaflet-search-index` bucket for builder snapshots (`INDEX_R2_*` secrets on the backend app)

## platforms
- leaflet, pckt, offprint, greengale, whitewind, lemma: known platforms
- leaflet/pckt/offprint/greengale/lemma detected via basePath; whitewind via `com.whtwnd.*` collection
- other: site.standard.* documents not from a known platform

## search ranking
- hybrid BM25 + recency: `ORDER BY rank + (days_old / 30)`
- OR between terms for recall, prefix on last word
- unicode61 tokenizer (non-alphanumeric = separator)

## snapshot builder (replica freshness)
- runs OFF fly since 2026-07-25: prefect deployment `pub-search-snapshot` on heavypad (`my-prefect-server/flows/pub_search_snapshot.py`), every 2h — see `docs/builder-offbox-plan.md`
- trigger a build now: `prefect deployment run 'pub-search-snapshot/pub-search-snapshot' --watch` (against prefect-server.waow.tech, tailnet)
- channels: `staging` (default) → `staging/builds/…` + `latest.staging.json`; `prod` requires `BUILDER_ALLOW_PROD=1` and writes `builds/…` + `latest.json` (pointer uploaded LAST)
- gates before publish: doc-count tolerance vs turso, FTS sentinel, quick_check; banned DIDs + bridgy rows excluded at build time (`policy.zig`)
- completion signal: `builder: published <id> to <channel> channel` in the flow-run logs; fly promote watcher adopts within its 5-min poll

## zig dependencies
- update a dependency hash: `zig fetch --save <url>` (fetches and updates build.zig.zon automatically)

## schema migrations
- run via [zug](https://tangled.sh/@zzstoatzz.io/zug) — see `docs/migrations.md`
- list lives in `backend/src/db/migrations.zig`
- to add: append a new entry with the next 3-digit prefix; **never edit existing migrations** (zug checksums them)
- `BOOTSTRAP_BASELINE_COUNT` is FROZEN at 10 — don't change it when adding new migrations
- repair a dirty migration: fix the underlying issue, then `UPDATE zug_migrations SET dirty=0 WHERE id='...'` and redeploy

## MCP server
- hosted: `claude mcp add-json pub-search '{"type": "http", "url": "https://pub-search-by-zzstoatzz.fastmcp.app/mcp"}'`
- local dev: `cd pub-search-mcp/server && uv run pytest` for tests
- the installable project lives in `pub-search-mcp/server/` — nested intentionally to work around a horizon (fastmcp.app's builder) bug where single-segment pyproject paths render as bare-name PyPI lookups instead of path installs (see prefecthq/horizon#3814). Remove the `server/` nesting once that PR lands.
- deployed on fastmcp.app

## common tasks
- check indexing: `curl -s https://leaflet-search-backend.fly.dev/api/dashboard | jq`
