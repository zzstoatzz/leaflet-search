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
uvx --from git+https://github.com/zzstoatzz/pub-search#subdirectory=mcp pub-search
```

to add it to claude code as a local stdio server:

```bash
claude mcp add pub-search -- uvx --from 'git+https://github.com/zzstoatzz/pub-search#subdirectory=mcp' pub-search
```

## tools

| tool | description |
|------|-------------|
| `search` | keyword search by query, tag, platform, date, or author |
| `search_semantic` | semantic search by meaning, filterable by platform or author |
| `search_hybrid` | combined keyword + semantic search with author/platform filtering |
| `get_document` | retrieve full content by AT-URI |
| `find_similar` | find semantically similar documents |
| `author_profile` | what one author writes, where, and over what period |

reference data is exposed as **resources**, not tools — `pub-search://tags`,
`pub-search://popular`, `pub-search://stats`. tool schemas are re-read on every
reasoning cycle, so lookups an agent reads (rather than steps it takes) do not
belong in that budget.

## workflow

```
search("space station") → [{uri: "at://...", title: "...", snippet: "...", url: "..."}]
search("gated content", author="ngerakines.me") → results from that author only
search("", author="zat.dev") → browse all docs by author
search_semantic("building a relay", author="zat.dev") → semantic search scoped to author
get_document("at://...") → {title: "...", content: "full article text..."}
find_similar("at://...") → [{uri: "at://...", title: "...", snippet: "..."}]
```

the `author` param accepts either a handle (`nate.bsky.social`) or a DID (`did:plc:xyz`). handles are resolved server-side.

**visibility**: publications that set `preferences.showInDiscover=false` are indexed but excluded from `search` and `find_similar`. `search(..., include_undiscoverable=True)` opts back in — intended for reading through a specific author's own writing, so pair it with `author=`. `get_document` reads the record straight from the author's PDS, so it is not filtered; see [docs/visibility.md](../../docs/visibility.md).

## development

```bash
git clone https://github.com/zzstoatzz/pub-search
cd pub-search/mcp
uv sync
uv run pytest
```
