# adopting pub-search for agents

this doc is for people wiring pub-search into an agent — a chat client, a
coding agent, a pipeline — not for people developing this repo. it explains
the surfaces you can consume, what's genuinely different between them, and
when to pick one over another.

## what you're adopting

pub-search indexes long-form writing across AT Protocol publishing platforms
(leaflet, pckt, offprint, greengale, whitewind, and self-hosted
[standard.site](https://standard.site) sites) as **one corpus**. that's the
thing no single platform's own search gives you: the network position of a
post — who else is writing about this, on which platforms, what the
most-recommended writers themselves recommend.

corpus characteristics that matter for agent design:

- ~25k documents, long-form only. small enough that exhaustive strategies
  (paginate everything, browse a whole author) are cheap.
- actively curated: bulk/scraper sources (bridgy fed mirrors, archive bots)
  are banned at ingest and never enter the corpus. results are human writing.
- three search modes: **keyword** (FTS5/BM25, ~100ms), **semantic**
  (voyage-4-lite embeddings + turbopuffer ANN, ~500ms), **hybrid** (both,
  merged via reciprocal rank fusion).

## the surfaces

### MCP server — default for chat/agent clients

```bash
# hosted (recommended — auto-deploys from this repo)
claude mcp add-json pub-search '{"type": "http", "url": "https://pub-search-by-zzstoatzz.fastmcp.app/mcp"}'

# local stdio
claude mcp add pub-search -- uvx --from 'git+https://github.com/zzstoatzz/pub-search#subdirectory=pub-search-mcp/server' pub-search
```

the MCP server is **not** a 1:1 mapping of the HTTP API. it's a curated layer
with three things the raw API doesn't have:

1. **live-record retrieval.** `get_document(uri)` fetches the actual record
   from the author's PDS (via pdsx) and flattens the platform-specific block
   structure into plaintext — the canonical, current version straight from
   the source. (for reading from the index instead, the HTTP API's
   [`/document`](api.md#document) returns the stored extracted text, batched.)
2. **composed curator tools.** `discover_focal_post` (what's notable now,
   top vs trending), `describe_cluster` (a post's semantic neighborhood with
   cross-platform/author/shared-term observations pre-computed),
   `recommended_by_top_authors` (transitive taste: what the
   most-recommended writers themselves endorse), and `author_profile` (what
   one author writes, where, and over what period). each replaces a
   multi-call assembly an agent would otherwise fumble through — the
   network-position framing is done server-side.

   every tool returns the same result shape, and every result carries a
   `snippet`: the match window for a search, otherwise the document's
   opening back-filled from the index in one batched `/document` call, so
   recommendations and browses can be triaged without a `get_document` per
   result. `contentLength` (the indexed text's length, 0 when unknown) tells
   a linkblog stub from primary writing. search results also carry `source`:
   `keyword`, `semantic`, or `keyword+semantic`.

   the surface is deliberately small. tags, popular queries and index stats
   are **resources** (`pub-search://tags`, `pub-search://popular`,
   `pub-search://stats`), not tools: they are lookups an agent reads, not
   steps it takes, and every tool's schema is re-read on each reasoning
   cycle. `search` has no `offset` for the same reason it defaults to
   hybrid — semantic ranking is ANN and does not repeat exactly, so paging is
   not a stable continuation; narrowing is.
3. **prompts.** `usage_guide` and `search_tips` ship with the server.
   caveat: many MCP clients still don't surface prompts or resources, so
   don't design your integration assuming they're visible. the tool
   docstrings carry the essential guidance redundantly for that reason.

choose MCP when the consumer is a tool-calling agent and you want it
productive immediately, with no code of yours in the loop.

### visibility: two deliberate asymmetries

Both of these are settled decisions, recorded here so they are not re-read as
the showInDiscover bug and "re-fixed".

**`get_document` returns opted-out documents; REST `/document` does not.**
The MCP tool reads the live record from the author's PDS, where it is public by
construction — `showInDiscover` is a request to *discovery surfaces*, not an
access control, and nothing private is exposed by fetching a public record you
already have the URI for. REST `/document` reads the index, which is a
discovery surface, so it withholds. The asymmetry is the point: search will not
hand you the URI of an opted-out document, and if you have one already, the
protocol never pretended it was secret.

**The opt-in is scoped to one identity, not authenticated.**
`include_undiscoverable=true` requires `author` on `/search`, requires a single
repo on `/document`, and is rejected outright on `/similar` (neighbours come
from every author, so it cannot be scoped there at all). Requests without the
scope are **rejected, not silently ignored** — a dropped flag is how the
original inconsistency stayed invisible.

This closes enumeration: there is no way to *discover* which publications have
opted out. It does not stop a caller who already knows a DID from reading that
author's unlisted writing. That gap needs authentication, which does not exist
yet — so treat the guarantee as "not listed", not "not readable".

`author_profile` takes the same flag, and needs no extra scoping because it is
already one author per call.

### HTTP API — for code-mode agents, scripts, and pipelines

base URL: `https://api.pub-search.waow.tech` — full reference in
[api.md](api.md). no auth, no key, just GET.

```bash
curl -s 'https://api.pub-search.waow.tech/search?q=relay&mode=hybrid&format=v2&limit=5' | jq
```

the API surface is small (search, similar, tags, popular, stats, dashboard),
which changes the calculus from the usual "code-mode MCP vs static tools"
debate: there's no sprawling SDK whose legibility you'd lose by flattening it
into tool definitions. the static MCP tools cover it comfortably. reach for
raw HTTP when:

- your agent **writes code** anyway (a coding agent doing research, a
  pipeline, a cron job) — one `httpx.get`/`curl` is less machinery than an
  MCP client.
- you need behavior the MCP tools deliberately don't expose (`limit` beyond
  the tool caps, pagination metadata, or the raw v1 response shape). The MCP
  `search` tool does expose `offset` for straightforward page traversal.
- you're building your own composition and want the primitives, not the
  curated layer.

search results carry snippets; for the full text, pass their `uri` values to
[`/document`](api.md#document) (comma-separated, up to 25 per request). it
serves the extracted content the indexer already stored — no PDS fetch, no
per-platform block flattening on your side. results also carry enough
metadata (`uri`, `did`, `rkey`, `basePath`, `path`, `platform`) to build web
URLs or fetch the live record from the PDS yourself
(`com.atproto.repo.getRecord`, or `uvx pdsx`) when you need the canonical
current version rather than the indexed one (see
[content-extraction.md](content-extraction.md) for what that entails).

### SDK / CLI — what exists and what doesn't, honestly

there is **no standalone published SDK and no dedicated CLI** today. the
`pub-search` package (in `pub-search-mcp/server/`) is the MCP server; its
console entrypoint starts a stdio MCP server, not a CLI. its internals
(`pub_search.client`, the pydantic result types) are usable as a lightweight
python client if you vendor them, but they're not a supported surface.

in practice the API plays the role both would: it's small, unauthenticated,
and stable enough that `curl` + `jq` *is* the CLI, and a code-mode agent
hitting it directly *is* the SDK. if the API surface grows to where a real
SDK earns its keep, the right move (per the experience elsewhere: when real
care has gone into SDK legibility, don't 1:1-map it into tool schemas) is
SDK-first with CLI and MCP as thin shells over it — that's not where this
project is yet.

## choosing, in one table

| you are building | use | why |
|---|---|---|
| a chat agent / claude code session that should search & read pubs | MCP (hosted) | zero setup beyond `claude mcp add`, full-text via `get_document`, curator tools |
| an agent in a restricted network / pinned deps | MCP (local stdio via uvx) | same tools, runs where you run, `LEAFLET_SEARCH_API_URL` to repoint |
| a coding agent, script, or scheduled pipeline | HTTP API | fewest moving parts; you control pagination, retries, composition |
| something needing full article text at scale | HTTP API `/document` | batched (25 uris/request), served from the index — no per-PDS fetch loop needed |
| a human-facing thing | the site ([pub-search.waow.tech](https://pub-search.waow.tech)) | the UI, atlas included, is the human surface |

## sharp edges (learned the hard way)

- **use `api.pub-search.waow.tech` for API calls.** the site domain
  `pub-search.waow.tech` proxies the API only under `/api/*` (edge-cached,
  rate-limited 30 req/10s per IP → 429); every other unknown path there returns
  the SPA's HTML with a 200. if you probe the site domain and trust status
  codes, you will "succeed" against a page of HTML.
- **at least one of `q`, `tag`, or `author` is required.** an empty query is
  rejected by design; the MCP `search` tool returns `[]` rather than erroring.
- **filters vary by mode.** `tag` and `since` apply to keyword only;
  `platform` and `author` work everywhere. hybrid applies each filter to the
  half that supports it. don't assume a filtered semantic search did what you
  asked — check the [matrix in api.md](api.md).
- **`author` accepts handles or DIDs**; handles resolve server-side.
  `search("", author="someone.com")` is the supported way to browse an
  author's everything.
- **freshness is bounded, not real-time.** ingestion from the firehose is
  continuous, but the keyword-serving replica is refreshed by snapshot swap.
  expect minutes-to-hours of lag, not seconds. the `pub-search://stats`
  resource / `/api/dashboard` show current counts if staleness matters to you.
- **`get_document` (MCP) reaches out to the author's PDS per call.** latency
  and availability depend on that PDS, not on pub-search. fine for reading a
  handful of focal posts; for batches, the API's `/document` serves stored
  text without touching any PDS.

## evaluating your integration

test what your agent can *accomplish* with the surface, not what the surface
technically exposes. a useful smoke set, whichever surface you chose:

1. keyword, semantic, and hybrid queries for the same topic — all three
   return results and the hybrid `source` field is populated. (exercising
   only one mode is how integrations look healthy while a mode is down.)
2. search → take a result's `uri` → fetch full content (HTTP `/document`,
   MCP `get_document`, or your own PDS fetch) — the round trip from "found"
   to "read" works, and the body is article text, not an empty string.
3. an author-scoped browse (`search("", author=...)`) — filter plumbing works.
4. a curator flow: `discover_focal_post` → `describe_cluster(focal.uri)` —
   the composed tools return non-empty neighborhoods.

inspect bodies, not status codes (see sharp edges above for why).

## further reading

- [api.md](api.md) — full endpoint reference
- [search-syntax.md](search-syntax.md) — query syntax (quotes, OR, prefix behavior)
- [content-extraction.md](content-extraction.md) — why full text is per-platform work
- `pub-search-mcp/server/README.md` — MCP server dev setup
- on the philosophy of surfaces: [get in loser, we're a-sembling](https://nate.leaflet.pub/3mnxnia4lvk2n) (SDK/CLI/MCP as shells over one core) and [mcpval](https://dev-log.prefect.io/mcpval/) (evaluate what clients accomplish with a server, not what it exposes)
