# pub search

by [@zzstoatzz.io](https://bsky.app/profile/zzstoatzz.io)

search ATProto publishing platforms ([leaflet](https://leaflet.pub), [pckt](https://pckt.blog), [offprint](https://offprint.app), [greengale](https://greengale.app), and others using [standard.site](https://standard.site)).

**live:** [pub-search.waow.tech](https://pub-search.waow.tech)

> formerly "leaflet-search" - generalized to support multiple publishing platforms

## how it works

1. **ingester** (`ingester/`, our own firehose consumer) subscribes to an ATProto relay, filters to publishing collections (`pub.leaflet.*`, `site.standard.*`, `com.whtwnd.*`), cryptographically verifies each commit (signature + MST diff), and re-emits verified records over a websocket channel
2. **backend** consumes the channel, extracts content per platform, and writes to [Turso](https://turso.tech) (source of truth) plus a small live **overlay** beside the serving replica; serves search with keyword (SQLite FTS5, snapshot + overlay merged at read — fresh docs are searchable in ~a minute), semantic ([turbopuffer](https://turbopuffer.com) ANN), and hybrid modes. runs as two fly process groups (`app` serves, `worker` runs the reconciler/embedder) so background work never shares a core with search (see [docs/overlay-serving.md](docs/overlay-serving.md))
3. **builder** (the same backend binary in `BUILDER_MODE`) rebuilds the keyword replica offline from Turso on a schedule and publishes it to R2 with a sha256 manifest — serving adopts verified snapshots instead of syncing in place, an architecture borrowed from [typeahead](https://tangled.sh/@zzstoatzz.io/typeahead) (see [docs/snapshot-pipeline.md](docs/snapshot-pipeline.md))
4. **labeler** — pub-search is also an AT Protocol [labeler](https://atproto.com/specs/label): an autonomous classifier watches the firehose and labels accounts that generate documents from a dataset (one per record) rather than composing them; labeled accounts are excluded from search. see [pub-search.waow.tech/labels](https://pub-search.waow.tech/labels)
5. **site** static frontend on Cloudflare Pages; its `functions/api/[[route]].js` proxies `/api/*` to the backend with a 60s edge cache (stale-while-revalidate), so repeated queries answer at the edge
6. **mcp** server for AI agents (Claude Code, etc.) — see [docs/agent-surfaces.md](docs/agent-surfaces.md)

## MCP server

search is also exposed as an MCP server for AI agents like Claude Code:

```bash
claude mcp add-json pub-search '{"type": "http", "url": "https://pub-search-by-zzstoatzz.fastmcp.app/mcp"}'
```

see [pub-search-mcp/server/README.md](pub-search-mcp/server/README.md) for local setup and usage details.

## api

```
GET /search?q=<query>&mode=keyword|semantic|hybrid&platform=<platform>&tag=<tag>&since=<date>&author=<did|handle>&format=v2
GET /similar?uri=<at-uri>&format=v2
GET /tags
GET /popular
GET /stats
GET /health
```

search returns three entity types: `article` (document in a publication), `looseleaf` (standalone document), `publication` (newsletter itself). each result includes a `platform` field (leaflet, pckt, offprint, greengale, whitewind, lemma, or other). use `format=v2` for a wrapped response with `results`, `hasMore`, `nextOffset`, and `total` fields. search leaves `total` null rather than running an expensive full-corpus count.

**modes**: `keyword` (default) uses FTS5 with BM25 + recency scoring. `semantic` uses voyage embeddings + [turbopuffer](https://turbopuffer.com) ANN. `hybrid` merges both via reciprocal rank fusion.

**visibility**: publications that set `preferences.showInDiscover=false` are indexed but excluded from every discovery surface (`/search`, `/similar`, `/document`). `include_undiscoverable=true` opts back in — meant for reading a specific author's own writing, so pair it with `author=`. Enforcement is a single response-level policy pass, not a per-endpoint filter; see [docs/visibility.md](docs/visibility.md).

**ranking**: keyword results use hybrid BM25 + recency scoring. text relevance is primary, but recent documents get a boost (~1 point per 30 days). the `since` parameter filters to documents created after the given ISO date (e.g., `since=2025-01-01`).

`/similar` uses [Voyage AI](https://voyageai.com) embeddings with [turbopuffer](https://turbopuffer.com) ANN search.

## configuration

the backend is fully configurable via environment variables:

| variable | default | description |
|----------|---------|-------------|
| `APP_NAME` | `leaflet-search` | name shown in startup logs |
| `DASHBOARD_URL` | `https://pub-search.waow.tech/dashboard.html` | redirect target for `/dashboard` |
| `INGESTER_HOST` | `leaflet-search-ingester.internal` | ingester websocket host (`/channel` protocol) |
| `INGESTER_PORT` | `2480` | ingester websocket port |
| `PORT` | `3000` | HTTP server port |
| `TURSO_URL` | - | Turso database URL (required) |
| `TURSO_TOKEN` | - | Turso auth token (required) |
| `VOYAGE_API_KEY` | - | Voyage AI API key (for embeddings) |

the backend indexes multiple ATProto platforms — currently `pub.leaflet.*`, `site.standard.*`, and `com.whtwnd.blog.entry` collections (the last for whitewind). platform is stored per-document and returned in search results.

## atlas

a 2D semantic map of the entire document index: [pub-search.waow.tech/atlas](https://pub-search.waow.tech/atlas)

documents are projected from 1024-dim voyage embeddings to 2D via PCA → UMAP, then clustered with HDBSCAN at two granularities. each point is colored by platform. zoom in to see finer cluster labels and individual document titles.

built with `scripts/build-atlas` (batch job; rebuilt every 6h by a prefect flow) → `site/atlas.json` → canvas renderer. see [docs/atlas.md](docs/atlas.md) for details.

## [stack](https://bsky.app/profile/zzstoatzz.io/post/3mbij5ip4ws2a)

- [Fly.io](https://fly.io) hosts the [Zig](https://ziglang.org) search API, content indexing, and our own verified firehose ingester ([zat](https://tangled.sh/@zzstoatzz.io/zat) for AT Protocol crypto)
- [Turso](https://turso.tech) cloud SQLite (source of truth) + local read replica (FTS queries), refreshed via sha256-verified snapshots in [R2](https://developers.cloudflare.com/r2/)
- [turbopuffer](https://turbopuffer.com) ANN vector search
- [Voyage AI](https://voyageai.com) embeddings (voyage-4-lite, 1024 dims)
- [Cloudflare Pages](https://pages.cloudflare.com) static frontend

## embeddings

documents are embedded using Voyage AI's `voyage-4-lite` model (1024 dimensions). the backend automatically generates embeddings for new documents via a background worker — no manual backfill needed. similarity search uses turbopuffer's ANN index for fast nearest-neighbor queries.
