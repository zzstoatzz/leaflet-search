# pub search MCP

MCP server for [pub search](https://pub-search.waow.tech) - search ATProto publishing platforms (Leaflet, pckt, Offprint, Greengale, WhiteWind, and others using standard.site).

## usage

### hosted (recommended)

```bash
claude mcp add-json pub-search '{"type": "http", "url": "https://pub-search-by-zzstoatzz.fastmcp.app/mcp"}'
```

### local

run the MCP server locally with `uvx`:

```bash
uvx --from 'git+https://github.com/zzstoatzz/pub-search#subdirectory=pub-search-mcp/server' pub-search
```

to add it to claude code as a local stdio server:

```bash
claude mcp add pub-search -- uvx --from 'git+https://github.com/zzstoatzz/pub-search#subdirectory=pub-search-mcp/server' pub-search
```

## tools

| tool | description |
|------|-------------|
| `search` | hybrid search by default; narrow with `tag`, `platform`, `since`, `author`; `mode` picks keyword or semantic |
| `get_document` | the live record from the author's PDS, flattened to text |
| `find_similar` | documents near one document |
| `describe_cluster` | that neighborhood plus cross-platform, author, and shared-term observations |
| `discover_focal_post` | what's notable now: top or trending over a window |
| `recommended_by_top_authors` | what the most-recommended writers themselves recommend |
| `author_profile` | what one author writes, where, and over what period |

reference data is exposed as **resources**, not tools — `pub-search://tags`,
`pub-search://popular`, `pub-search://stats`. tool schemas are re-read on every
reasoning cycle, so lookups an agent reads (rather than steps it takes) do not
belong in that budget.

## results

every tool returns the same result shape, and every result carries:

- `snippet` — the window around the match for a search; for anything else
  (recommendations, a browse, neighbors) the document's opening, back-filled
  from the index in one batched call. triage from the snippet; call
  `get_document` only for what you will read.
- `contentLength` — the indexed text's length when known, 0 when not. a few
  hundred characters is a linkblog stub (a pull-quote indexed as the whole
  document), not primary writing.
- `source` on search results — `keyword` (an FTS match), `semantic` (an
  embedding neighbour), or `keyword+semantic` (both; the strongest signal in
  hybrid).

uris are canonical `site.standard.document` uris; a `pub.leaflet.document`
uri given to `find_similar` or `describe_cluster` resolves to the same
document, so the input uri may not appear verbatim in results.

## workflow

```
search("space station") → [{uri, title, snippet, source, url, ...}]
search("gated content", author="ngerakines.me") → results from that author only
search("", author="zat.dev") → browse all docs by author, keyword mode
search("labelers", since="2026-08-01", platform="leaflet") → narrowed, not paged
get_document("at://...") → {title, content: "full article text..."}
describe_cluster("at://...") → neighbors + platforms + shared_terms
```

the `author` param accepts either a handle (`nate.bsky.social`) or a DID (`did:plc:xyz`). handles are resolved server-side.

there is no `offset`: semantic ranking is approximate-nearest-neighbour and
does not repeat exactly, so page 2 is not a stable continuation of page 1.
narrow the query instead. the HTTP API has no `before` date or document
`type` filter yet, so neither does this server.

**visibility**: publications that set `preferences.showInDiscover=false` are indexed but excluded from `search` and `find_similar`. `search(..., include_undiscoverable=True)` opts back in — intended for reading through a specific author's own writing, so pair it with `author=`. `get_document` reads the record straight from the author's PDS, so it is not filtered; see [docs/visibility.md](../../docs/visibility.md).

## development

```bash
git clone https://github.com/zzstoatzz/pub-search
cd pub-search/pub-search-mcp/server
uv sync
uv run pytest
```

the hosted server deploys from `main`.
