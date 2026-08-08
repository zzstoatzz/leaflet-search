"""MCP server for searching ATProto publishing platforms."""

from __future__ import annotations

import re
from collections import Counter
from html import unescape
from typing import Any, Literal

import httpx
from fastmcp import FastMCP

from pub_search._types import (
    ClusterContext,
    Document,
    PopularSearch,
    SearchResult,
    Stats,
    Tag,
)
from pub_search.client import get_http_client

mcp = FastMCP("pub-search")


# -----------------------------------------------------------------------------
# prompts
# -----------------------------------------------------------------------------


@mcp.prompt("usage_guide")
def usage_guide() -> str:
    """instructions for using pub-search MCP tools."""
    return """\
# pub-search MCP

search long-form writing on ATProto: leaflet, pckt, offprint, greengale, whitewind.

## tools

- `search(query, mode, tag, platform, since, author)` - search with mode: keyword, semantic, or hybrid
- `get_document(uri)` - fetch full content by AT-URI
- `find_similar(uri)` - find related documents (raw)
- `discover_focal_post(window, sort)` - what's notable right now (top/trending in a window)
- `describe_cluster(uri)` - a focal post's neighborhood + cross-platform / author / shared-terms observations
- `recommended_by_top_authors(top_authors, window)` - transitive taste: what the most-recommended writers are themselves recommending. tunable resolution via the pool size.
- `get_tags()` - available tags
- `get_stats()` - index statistics
- `get_popular()` - popular queries

## workflows

**research a topic.**
1. `search("topic")` for keyword search, `search("topic", mode="hybrid")` for best results
2. `get_document(uri)` for full text
3. `find_similar(uri)` for related content

**curate / write about the network.** pub-search is the only place that sees
every long-form ATProto platform as one corpus, so the network-position of a
post (who else is writing about this, on which platforms) is information you
can't get from any single platform's UI.
1. `discover_focal_post(window="week", sort="trending")` to find what's hot
2. `describe_cluster(focal.uri)` to see the neighborhood + cross-platform observations
3. `get_document(focal.uri)` if you want the actual claim from inside the post

## search modes

- **keyword** (default): fast exact match (~100ms), supports all filters
- **semantic**: meaning-based (~500ms), good for natural language queries
- **hybrid**: both combined via rank fusion, `source` field shows how each result was found

## visibility

publications that set `preferences.showInDiscover=false` are indexed but kept
out of `search` and `find_similar`. their author asked to stay out of discovery.
`search(..., include_undiscoverable=True)` opts back in — use it when someone is
deliberately reading through a specific author's own writing, and pair it with
`author=`. do not set it on a general/global query.

## result types

- **article**: document in a publication
- **looseleaf**: standalone document
- **publication**: the publication itself

results include a `url` field for web access.
"""


@mcp.prompt("search_tips")
def search_tips() -> str:
    """tips for effective searching."""
    return """\
# search tips

- prefix matching on last word: "cat dog" matches "cat dogs"
- combine filters: `search("python", tag="tutorial", platform="leaflet")`
- filter by author: `search("python", author="nate.bsky.social")` or `search("", author="did:plc:xyz")`
- use `since="2025-01-01"` for recent content
- `search("natural language query", mode="semantic")` for meaning-based search
- `search("query", mode="hybrid")` for best of both — results show `source` field
- `find_similar(uri)` to discover related documents
- `get_tags()` to discover available tags
"""


# -----------------------------------------------------------------------------
# tools
# -----------------------------------------------------------------------------


Platform = Literal["leaflet", "pckt", "offprint", "greengale", "whitewind", "other"]


def _extract_results(data: Any) -> list[dict[str, Any]]:
    """extract results array from API response (handles both v1 and v2 formats)."""
    if isinstance(data, dict) and "results" in data:
        return data["results"]
    if isinstance(data, list):
        return data
    return []


Mode = Literal["keyword", "semantic", "hybrid"]


@mcp.tool
async def search(
    query: str = "",
    tag: str | None = None,
    platform: Platform | None = None,
    since: str | None = None,
    author: str | None = None,
    mode: Mode = "keyword",
    limit: int = 5,
    offset: int = 0,
    include_undiscoverable: bool = False,
) -> list[SearchResult]:
    """search long-form writing across ATProto publishing platforms.

    modes:
        keyword: fast exact match (~100ms), supports all filters
        semantic: meaning-based (~500ms), good for natural language queries
        hybrid: both combined via rank fusion — results include a `source` field

    args:
        query: search query (titles and content). for semantic/hybrid, natural language works well.
        tag: filter by tag (keyword mode only)
        platform: filter by platform (leaflet, pckt, offprint, greengale, whitewind, other)
        since: ISO date - only documents created after this date (keyword mode only)
        author: filter by author (DID like "did:plc:xyz" or handle like "nate.bsky.social")
        mode: search mode — keyword, semantic, or hybrid (default: keyword)
        limit: max results (default 5, max 40)
        offset: skip this many ranked results (default 0, max 1000)
        include_undiscoverable: include publications that set
            preferences.showInDiscover=false. These are indexed but excluded
            from discovery surfaces by default because their author asked to
            stay out of them. Pass True only when the user is deliberately
            looking through their own or a specific author's writing —
            pair it with `author` rather than running it globally.

    returns:
        list of results with uri, title, snippet, platform, and web url
    """
    if not query and not tag and not author:
        return []

    params: dict[str, Any] = {
        "format": "v2",
        "limit": str(limit),
        "offset": str(offset),
    }
    if query:
        params["q"] = query
    if tag:
        params["tag"] = tag
    if platform:
        params["platform"] = platform
    if since:
        params["since"] = since
    if author:
        params["author"] = author
    if mode != "keyword":
        params["mode"] = mode
    if include_undiscoverable:
        params["include_undiscoverable"] = "true"

    async with get_http_client() as client:
        response = await client.get("/search", params=params)
        response.raise_for_status()
        data = response.json()

    if isinstance(data, dict) and "error" in data:
        return []

    results = _extract_results(data)
    return [SearchResult(**r) for r in results[:limit]]


async def _resolve_pds(did: str) -> str:
    """resolve a DID to its PDS endpoint via plc.directory or did:web."""
    if did.startswith("did:plc:"):
        doc_url = f"https://plc.directory/{did}"
    elif did.startswith("did:web:"):
        host = did.removeprefix("did:web:").replace("%3A", ":")
        doc_url = f"https://{host}/.well-known/did.json"
    else:
        raise ValueError(f"unsupported DID method: {did}")

    async with httpx.AsyncClient(timeout=15.0) as client:
        response = await client.get(doc_url)
        response.raise_for_status()
        doc = response.json()

    for service in doc.get("service", []):
        if service.get("id", "").endswith("#atproto_pds"):
            return service["serviceEndpoint"]
    raise ValueError(f"no PDS endpoint in DID document for {did}")


def _strip_html(html: str) -> str:
    """crude tag strip for the PDS fallback path only.

    The index's extractor is the real one; this exists so a document that is
    not indexed yet still returns readable text instead of markup.
    """
    text = re.sub(r"<(script|style)[^>]*>.*?</\1>", " ", html, flags=re.S | re.I)
    text = re.sub(r"<[^>]+>", " ", text)
    text = unescape(text)
    return re.sub(r"[ \t]*\n\s*\n\s*", "\n\n", re.sub(r"[ \t]+", " ", text)).strip()


def _extract_content(value: dict[str, Any]) -> str:
    """pull plaintext out of a document record, whatever its shape.

    pub.leaflet.document: pages[].blocks[].block.plaintext
    site.standard.document: content.pages[].blocks[].block.plaintext
    com.whtwnd.blog.entry: content is the markdown string itself
    """
    content_obj = value.get("content")
    if isinstance(content_obj, str):
        return content_obj

    # site.standard.document carries a plaintext sibling, and its content can
    # be {"html": ...} rather than pages/blocks. Missing both is what made
    # get_document return empty text for records the index extracts fine.
    if text := value.get("textContent"):
        return text
    if isinstance(content_obj, dict) and isinstance(content_obj.get("html"), str):
        return _strip_html(content_obj["html"])

    pages = value.get("pages") or []
    if not pages and isinstance(content_obj, dict):
        pages = content_obj.get("pages") or []

    content_parts = []
    for page in pages:
        for block_wrapper in page.get("blocks") or []:
            block = block_wrapper.get("block") or {}
            plaintext = block.get("plaintext", "")
            if plaintext:
                content_parts.append(plaintext)
    return "\n\n".join(content_parts)


@mcp.tool
async def get_document(uri: str) -> Document:
    """get the full content of a document by its AT-URI.

    fetches the complete document from ATProto, including full text content.
    use this after finding documents via search to get the complete text.

    note: this reads the record directly from the author's PDS, not from the
    pub-search index, so it is not filtered by preferences.showInDiscover.
    That is deliberate — the preference is about staying out of discovery
    surfaces, not about revoking access to a public record you already have
    the URI for. Search will not hand you those URIs by default.

    args:
        uri: the AT-URI of the document (e.g., at://did:plc:.../pub.leaflet.document/...)

    returns:
        document with full content, title, tags, and metadata
    """
    # at://did:plc:xxx/collection/rkey
    parts = uri.removeprefix("at://").split("/")
    if len(parts) < 3:
        raise ValueError(f"invalid AT-URI: {uri}")
    repo, collection, rkey = parts[0], parts[1], parts[2]

    # Live record from the author's PDS — this is the point of the tool. The
    # index lags by up to a snapshot cycle, so the PDS is the only way to read
    # the CURRENT text, including documents published minutes ago.

    try:
        pds = await _resolve_pds(repo)
        async with httpx.AsyncClient(timeout=15.0) as client:
            response = await client.get(
                f"{pds}/xrpc/com.atproto.repo.getRecord",
                params={"repo": repo, "collection": collection, "rkey": rkey},
            )
            response.raise_for_status()
            record = response.json()

        value = record.get("value") or {}
        content = _extract_content(value)
        if content:
            return Document(
                uri=record.get("uri", uri),
                title=value.get("title", ""),
                content=content,
                createdAt=value.get("publishedAt") or value.get("createdAt") or "",
                # records can carry an explicit null for tags
                tags=value.get("tags") or [],
                # leaflet names its parent `publication`; site.standard `site`
                publicationUri=value.get("publication") or value.get("site") or "",
            )
    except Exception:
        pass  # PDS down, DID unresolvable, or a record shape we cannot read

    # Fallback to the index. Returning "" when the live fetch fails would look
    # like an empty document rather than a failed read — the index's stored
    # text is stale but true, which beats a convincing blank.
    async with get_http_client() as client:
        indexed = await client.get("/document", params={"uri": uri})
    documents = (indexed.json() or {}).get("documents") or [] if indexed.status_code == 200 else []
    if not documents:
        raise ValueError(f"could not read {uri} from its PDS or the index")
    doc = documents[0]
    return Document(
        uri=doc.get("uri", uri),
        title=doc.get("title", ""),
        content=doc.get("content", ""),
        createdAt=doc.get("createdAt", ""),
        tags=doc.get("tags") or [],
        publicationUri=doc.get("publicationUri", ""),
    )


@mcp.tool
async def find_similar(uri: str, limit: int = 5) -> list[SearchResult]:
    """find documents similar to a given document.

    uses vector similarity to find semantically related documents.
    great for discovering related content after finding
    an interesting document.

    publications that set preferences.showInDiscover=false are excluded, same
    as search — otherwise they would be one hop away from any neighbour.

    args:
        uri: the AT-URI of the document to find similar content for
        limit: max similar documents to return (default 5)

    returns:
        list of similar documents with uri, title, and metadata
    """
    async with get_http_client() as client:
        response = await client.get("/similar", params={"uri": uri, "format": "v2"})
        response.raise_for_status()
        data = response.json()

    results = _extract_results(data)
    return [SearchResult(**r) for r in results[:limit]]


Window = Literal["day", "week", "month", "year", "all"]
SortRecommended = Literal["top", "trending"]


@mcp.tool
async def discover_focal_post(
    window: Window = "week",
    sort: SortRecommended = "trending",
    limit: int = 3,
) -> list[SearchResult]:
    """surface what's notable right now — the focal posts a curator would write about.

    use `sort="trending"` for current momentum (recommends-per-day-since-publish,
    so newer posts with steady velocity surface over older posts with cumulative
    counts). use `sort="top"` for "what mattered most" by raw count in the window.

    pair with `describe_cluster(focal.uri)` to get the network-position context
    around a focal item — the conversation it sits inside, not just the post itself.

    args:
        window: day | week | month | year | all (default week)
        sort: top | trending (default trending)
        limit: max items (default 3 — keep small; this is for focal items, not browse)

    returns:
        list of SearchResult with recommendCount (windowed) and totalCount (all-time) populated
    """
    async with get_http_client() as client:
        response = await client.get(
            "/recommended",
            params={"since": window, "sort": sort},
        )
        response.raise_for_status()
        data = response.json()

    results = _extract_results(data)
    return [SearchResult(**r) for r in results[:limit]]


# common short / structural words to strip from cluster title overlap.
# kept tight — the goal is "what terms recur in this neighborhood", so most
# nouns/verbs should pass through.
_TITLE_STOPWORDS = frozenset(
    "the a an and or but is are was were be been being to of in on at for from "
    "with by as this that these those it its it's has have had do does did how "
    "what when where why who whom which not no yes you your we our they their"
    .split()
)


def _title_terms(title: str) -> set[str]:
    out: set[str] = set()
    for raw in title.split():
        w = raw.strip(":,.!?\"'()[]{}—–-").lower()
        if len(w) >= 4 and w not in _TITLE_STOPWORDS and not w.isdigit():
            out.add(w)
    return out


@mcp.tool
async def recommended_by_top_authors(
    top_authors: int = 10,
    window: Window = "month",
    limit: int = 10,
) -> list[SearchResult]:
    """what are the network's most-recommended writers themselves recommending?

    a transitive-taste signal distinct from raw popularity: surface what the
    people who *write* the most-recommended posts have themselves endorsed.
    pub-search is the only place that sees every long-form ATProto platform
    as one corpus, so this is uniquely available here.

    resolution knobs (think of it like tuning a microscope):
    - `top_authors`: pool size. small (5-10) = sharp, opinionated focal point;
      large (50-100) = broader consensus across the network's signal-writers.
    - `window`: when those authors made their recommendations. shorter
      (week/month) = current taste; longer (year/all) = enduring favorites.

    each result's `recommendCount` is the endorsement count from the top-author
    pool inside the window (drives rank); `totalCount` is the all-time count
    from that pool. neither is global popularity.

    args:
        top_authors: how many top writers to draw the taste-pool from (default 10)
        window: day | week | month | year | all (default month)
        limit: max results (default 10)

    returns:
        list of SearchResult sorted by endorsement count (desc), then recency
    """
    async with get_http_client() as client:
        response = await client.get(
            "/recommended-by-top-authors",
            params={"pool": top_authors, "since": window},
        )
        response.raise_for_status()
        data = response.json()

    results = _extract_results(data)
    return [SearchResult(**r) for r in results[:limit]]


@mcp.tool
async def describe_cluster(uri: str, k: int = 5) -> ClusterContext:
    """get a focal document's semantic neighborhood, with cross-platform / author observations.

    pre-computes what pub-search uniquely sees from indexing the whole long-form
    network as one corpus: which platforms the neighborhood spans, how many
    distinct authors are involved, what terms recur across the cluster's titles.

    a curator can use this to write about a focal post *as a focal post* — i.e.
    placing it inside the conversation it sits in — without making N extra calls
    to assemble the same picture from `find_similar` results.

    args:
        uri: AT-URI of the focal document (typically from `discover_focal_post()`)
        k: number of semantic neighbors to include (default 5)

    returns:
        ClusterContext with neighbors + structured observations. shared_terms is
        derived from neighbor titles only; combine with the focal's title for the
        full cluster theme picture.
    """
    async with get_http_client() as client:
        response = await client.get("/similar", params={"uri": uri, "format": "v2"})
        response.raise_for_status()
        data = response.json()

    neighbors = [SearchResult(**r) for r in _extract_results(data)[:k]]
    platforms = sorted({n.platform for n in neighbors})
    authors = {n.did for n in neighbors}
    term_counts: Counter[str] = Counter()
    for n in neighbors:
        term_counts.update(_title_terms(n.title))
    shared_terms = [t for t, c in term_counts.most_common(10) if c >= 2]
    return ClusterContext(
        focal_uri=uri,
        neighbors=neighbors,
        platforms=platforms,
        distinct_authors=len(authors),
        cross_platform=len(platforms) > 1,
        cross_author=len(authors) > 1,
        shared_terms=shared_terms,
    )


@mcp.tool
async def get_tags(limit: int = 10) -> list[Tag]:
    """list all available tags with document counts.

    returns tags sorted by document count (most popular first).
    useful for discovering topics and filtering searches.

    args:
        limit: max tags to return (default 10)

    returns:
        list of tags with their document counts
    """
    async with get_http_client() as client:
        response = await client.get("/tags", params={"format": "v2"})
        response.raise_for_status()
        data = response.json()

    results = _extract_results(data)
    return [Tag(**t) for t in results[:limit]]


@mcp.tool
async def get_stats() -> Stats:
    """get index statistics.

    returns:
        document and publication counts
    """
    async with get_http_client() as client:
        response = await client.get("/stats")
        response.raise_for_status()
        data = response.json()
        data.pop("timing", None)
        return Stats(**data)


@mcp.tool
async def get_popular(limit: int = 5) -> list[PopularSearch]:
    """get popular search queries.

    see what others are searching for.
    can inspire new research directions.

    args:
        limit: max queries to return (default 5)

    returns:
        list of popular queries with search counts
    """
    async with get_http_client() as client:
        response = await client.get("/popular", params={"format": "v2"})
        response.raise_for_status()
        data = response.json()

    results = _extract_results(data)
    return [PopularSearch(**p) for p in results[:limit]]


# -----------------------------------------------------------------------------
# resources
# -----------------------------------------------------------------------------


@mcp.resource("pub-search://stats")
async def stats_resource() -> str:
    """current index statistics."""
    stats = await get_stats()
    return f"pub search index: {stats.documents} documents, {stats.publications} publications"


# -----------------------------------------------------------------------------
# entrypoint
# -----------------------------------------------------------------------------


def main() -> None:
    """run the MCP server."""
    mcp.run()


if __name__ == "__main__":
    main()
