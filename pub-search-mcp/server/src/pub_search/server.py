"""MCP server for searching ATProto publishing platforms."""

from __future__ import annotations

import re
from collections import Counter
from html import unescape
from typing import Any, Literal

import httpx
from fastmcp import FastMCP

from pub_search._types import (
    AuthorProfile,
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

- `search(query, tag, platform, since, author)` - hybrid by default; narrow with filters rather than paging. every result carries `source` (keyword | semantic | keyword+semantic), a `snippet`, and `contentLength`
- `get_document(uri)` - the LIVE record from the author's PDS, flattened to text
- `author_profile(author)` - what one author writes, where, and over what period
- `find_similar(uri)` - documents near this one
- `describe_cluster(uri)` - that neighborhood plus cross-platform / author / shared-term observations
- `discover_focal_post(window, sort)` - what's notable right now
- `recommended_by_top_authors(top_authors, window)` - transitive taste: what the most-recommended writers themselves recommend

## resources

reference data, read when you need it, not carried in every turn:
`pub-search://stats`, `pub-search://tags`, `pub-search://popular`

## workflows

**research a topic.**
1. `search("topic")` — hybrid already, no mode to pick
2. `get_document(uri)` for the full current text
3. `describe_cluster(uri)` for who else is in this conversation

**curate / write about the network.** pub-search is the only place that sees
every long-form ATProto platform as one corpus, so the network-position of a
post (who else is writing about this, on which platforms) is information you
can't get from any single platform's UI.
1. `discover_focal_post(window="week", sort="trending")` to find what's hot
2. `describe_cluster(focal.uri)` to see the neighborhood + cross-platform observations
3. `get_document(focal.uri)` if you want the actual claim from inside the post

## search modes

`hybrid` is the default and is usually right. `keyword` for exact lookups;
`semantic` for purely conceptual questions. every filter binds in every
mode. Results carry a `source` field showing how each was found:
`keyword` (an FTS match), `semantic` (an embedding neighbour), or
`keyword+semantic` (both — the strongest signal in hybrid).

## snippets

every tool's results carry a `snippet`: the window around the match for a
search, otherwise the document's opening. `contentLength` is the indexed
text's length; a few hundred characters is a linkblog stub (a pull-quote
indexed as the whole document), not primary writing. triage
from the snippet; call `get_document` only for the ones you will read.

There is no `offset`: semantic ranking is approximate-nearest-neighbour and
does not repeat exactly, so page 2 is not a stable continuation of page 1.
Narrow the query instead.

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
- read `pub-search://tags` to see what topics the corpus actually covers
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

SNIPPET_CHARS = 200
DOCUMENT_BATCH = 25


def _lead(text: str, chars: int = SNIPPET_CHARS) -> str:
    """the opening of a document as a one-line snippet."""
    flat = " ".join(text.split())
    return flat if len(flat) <= chars else flat[:chars].rstrip() + "…"


def _canonical(uri: str) -> str:
    """the index knows a leaflet document by its site.standard uri; a semantic
    hit can still carry the older pub.leaflet form, which /document reports
    as missing."""
    return uri.replace("/pub.leaflet.document/", "/site.standard.document/", 1)


async def _documents(uris: list[str]) -> dict[str, dict[str, Any]]:
    """the index's stored documents, keyed by the uri as the caller had it,
    batched 25 per request.

    A failed or partial batch just yields fewer entries; callers treat an
    absent uri as unknown rather than failing the tool.
    """
    by_uri: dict[str, dict[str, Any]] = {}
    if not uris:
        return by_uri
    originals: dict[str, list[str]] = {}
    for uri in uris:
        originals.setdefault(_canonical(uri), []).append(uri)
    wanted = list(originals)
    async with get_http_client() as client:
        for start in range(0, len(wanted), DOCUMENT_BATCH):
            batch = ",".join(wanted[start : start + DOCUMENT_BATCH])
            response = await client.get("/document", params={"uri": batch})
            if response.status_code != 200:
                continue
            for doc in (response.json() or {}).get("documents") or []:
                for original in originals.get(doc.get("uri", ""), []):
                    by_uri[original] = doc
    return by_uri


def _enrich(result: SearchResult, doc: dict[str, Any] | None) -> None:
    """record the stored text's length, and its opening when there is no snippet."""
    content = (doc or {}).get("content") or ""
    if not content:
        return
    result.contentLength = len(content)
    if not result.snippet:
        result.snippet = _lead(content)


async def _fill_snippets(results: list[SearchResult]) -> list[SearchResult]:
    """back-fill empty snippets from the index's stored text.

    /search and /similar return a window around the match; /recommended and a
    browse without a query return no snippet at all, which forced a
    get_document round trip per result just to triage. One batched /document
    call fills the opening of each such document instead, and records the
    stored text's length so stubs can be told from primary writing.
    """
    wanted = [r for r in results if not r.snippet]
    if not wanted:
        return results
    docs = await _documents([r.uri for r in wanted])
    for r in wanted:
        _enrich(r, docs.get(r.uri))
    return results


@mcp.tool
async def search(
    query: str = "",
    tag: str | None = None,
    platform: Platform | None = None,
    since: str | None = None,
    author: str | None = None,
    mode: Mode = "hybrid",
    limit: int = 5,
    include_undiscoverable: bool = False,
) -> list[SearchResult]:
    """search long-form writing across ATProto publishing platforms.

    Defaults to hybrid (keyword + meaning, rank-fused), which is the right
    answer for almost every research question. Narrow with `tag`, `platform`,
    `since` or `author` rather than paging — there is deliberately no offset:
    semantic ranking is an approximate-nearest-neighbour search that does not
    repeat exactly, so page 2 is not a stable continuation of page 1.

    every filter binds in every mode. `since`, `platform` and `author` are
    applied by the index in all three modes; `tag` is applied by the index on
    the keyword side and enforced here on hybrid's semantic side, so a result
    you get back always carries the tag you asked for.

    args:
        query: what you are looking for. natural language works well.
        tag: only documents carrying this tag
        platform: leaflet, pckt, offprint, greengale, whitewind, other
        since: ISO date — only documents created on or after it
        author: handle ("nate.bsky.social") or DID ("did:plc:xyz")
        mode: hybrid (default), keyword for exact/filtered lookups, semantic
            for purely conceptual ones
        limit: max results (default 5, max 40)
        include_undiscoverable: include publications that set
            preferences.showInDiscover=false. These are indexed but excluded
            from discovery surfaces by default because their author asked to
            stay out of them. REQUIRES `author` — the opt-in is scoped to one
            identity per request, never the whole corpus.

    returns:
        list of results with uri, title, snippet, platform, and web url.
        `source` says how each was found: "keyword" (FTS match), "semantic"
        (embedding neighbour), or "keyword+semantic" (both, the strongest
        signal in hybrid). `snippet` is the window around the match, or the
        document's opening for a browse; `contentLength` is the indexed text's
        length — a few hundred characters is a linkblog stub, not primary
        writing. fewer than `limit` results can come back when the tag filter
        drops semantic neighbours that do not carry it.
    """
    if not query and not tag and not author:
        return []

    # Scoped to one identity. The backend rejects this too; failing here gives
    # the agent a usable message instead of a 400 it has to interpret.
    if include_undiscoverable and not author:
        raise ValueError(
            "include_undiscoverable requires an author — it opts you into one "
            "author's unlisted writing, not the whole corpus"
        )

    # Browsing (no query) is only meaningful as an exact-match listing: the
    # semantic and hybrid paths rank against a query embedding and return
    # nothing without one. Defaulting to hybrid without this made
    # search("", author=X) — the browse-an-author call — return zero results.
    effective_mode = mode if query else "keyword"

    params: dict[str, Any] = {"format": "v2", "limit": str(limit)}
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
    if effective_mode != "keyword":
        params["mode"] = effective_mode
    if include_undiscoverable:
        params["include_undiscoverable"] = "true"

    async with get_http_client() as client:
        response = await client.get("/search", params=params)
        response.raise_for_status()
        data = response.json()

    if isinstance(data, dict) and "error" in data:
        return []

    results = [SearchResult(**r) for r in _extract_results(data)[:limit]]
    docs = await _documents([r.uri for r in results])
    kept: list[SearchResult] = []
    for r in results:
        doc = docs.get(r.uri)
        if not r.source and effective_mode == "keyword":
            r.source = "keyword"
        _enrich(r, doc)
        # the index applies `tag` to the keyword side only; a neighbour the
        # semantic side alone produced is dropped unless the index shows it
        # carrying the tag, so a filter never hands back what it did not bind
        if tag and r.source == "semantic":
            if tag.lower() not in {t.lower() for t in (doc or {}).get("tags") or []}:
                continue
        kept.append(r)
    return kept


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

    a `pub.leaflet.document` uri is accepted; results name the same documents
    by their canonical `site.standard.document` uri, so do not expect the
    input uri to appear verbatim.

    args:
        uri: the AT-URI of the document to find similar content for
        limit: max similar documents to return (default 5)

    returns:
        list of similar documents with uri, title, snippet, and metadata
    """
    async with get_http_client() as client:
        response = await client.get("/similar", params={"uri": uri, "format": "v2"})
        response.raise_for_status()
        data = response.json()

    results = _extract_results(data)
    return await _fill_snippets([SearchResult(**r) for r in results[:limit]])


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
        list of SearchResult with recommendCount (windowed) and totalCount
        (all-time) populated, and `snippet` holding each document's opening
    """
    async with get_http_client() as client:
        response = await client.get(
            "/recommended",
            params={"since": window, "sort": sort},
        )
        response.raise_for_status()
        data = response.json()

    results = _extract_results(data)
    return await _fill_snippets([SearchResult(**r) for r in results[:limit]])


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
        list of SearchResult sorted by endorsement count (desc), then recency,
        each with `snippet` holding the document's opening
    """
    async with get_http_client() as client:
        response = await client.get(
            "/recommended-by-top-authors",
            params={"pool": top_authors, "since": window},
        )
        response.raise_for_status()
        data = response.json()

    results = _extract_results(data)
    return await _fill_snippets([SearchResult(**r) for r in results[:limit]])


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

    neighbors = await _fill_snippets([SearchResult(**r) for r in _extract_results(data)[:k]])
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
async def author_profile(
    author: str, limit: int = 40, include_undiscoverable: bool = False
) -> AuthorProfile:
    """what one author writes about, where they publish, and over what period.

    Answers "who is this person in this corpus?" in one call. Doing it through
    `search` means browsing the author and then counting platforms, publications
    and date ranges by hand over the results — several reasoning cycles to
    re-derive the same summary every time.

    args:
        author: handle ("nate.bsky.social") or DID ("did:plc:xyz")
        limit: how many of their documents to read for the summary (default 40)
        include_undiscoverable: include this author's publications that set
            preferences.showInDiscover=false. Already scoped — this tool takes
            one author by definition. Without it a prolific author whose main
            publication is unlisted summarizes as a handful of documents, with
            nothing saying anything was withheld.

    returns:
        counts, the platforms and publications they use, their publishing date
        range, terms recurring across their titles, and their most recent work
    """
    params: dict[str, Any] = {"format": "v2", "author": author, "limit": str(limit)}
    if include_undiscoverable:
        params["include_undiscoverable"] = "true"
    async with get_http_client() as client:
        response = await client.get("/search", params=params)
        response.raise_for_status()
        data = response.json()

    if isinstance(data, dict) and "error" in data:
        return AuthorProfile(author=author, document_count=0, platforms=[], publications=[])

    rows = [SearchResult(**r) for r in _extract_results(data)]
    docs = [r for r in rows if r.type != "publication"]
    await _fill_snippets(docs[:5])

    dates = sorted(d.createdAt for d in docs if d.createdAt)
    terms = Counter(w for d in docs for w in _title_terms(d.title))

    return AuthorProfile(
        author=author,
        document_count=len(docs),
        platforms=sorted({d.platform for d in docs}),
        publications=sorted({d.publicationName for d in docs if d.publicationName}),
        first_published=dates[0] if dates else "",
        last_published=dates[-1] if dates else "",
        recurring_terms=[w for w, n in terms.most_common(8) if n > 1],
        recent=docs[:5],
    )


# -----------------------------------------------------------------------------
# resources — reference data, not actions
#
# tags / popular / stats were tools. They are lookups an agent reads, not steps
# it takes, and every tool's schema is re-read on each reasoning cycle — so
# three of them charged a per-turn token tax to answer questions nobody asks
# mid-task. As resources they stay available without competing with the tools
# that do work.
# -----------------------------------------------------------------------------


async def _fetch_tags(limit: int = 40) -> list[Tag]:
    async with get_http_client() as client:
        response = await client.get("/tags", params={"format": "v2"})
        response.raise_for_status()
        data = response.json()
    return [Tag(**t) for t in _extract_results(data)[:limit]]


async def _fetch_stats() -> Stats:
    async with get_http_client() as client:
        response = await client.get("/stats")
        response.raise_for_status()
        data = response.json()
        data.pop("timing", None)
        return Stats(**data)


@mcp.resource("pub-search://stats")
async def stats_resource() -> str:
    """size and shape of the corpus."""
    stats = await _fetch_stats()
    return f"pub-search index: {stats.documents} documents, {stats.publications} publications"


@mcp.resource("pub-search://tags")
async def tags_resource() -> str:
    """the tag vocabulary, most-used first — the topics this corpus actually covers."""
    tags = await _fetch_tags()
    lines = [f"{t.tag} ({t.count})" for t in tags]
    return "tags by document count:\n" + "\n".join(lines)


@mcp.resource("pub-search://popular")
async def popular_resource() -> str:
    """what other people are searching for right now."""
    async with get_http_client() as client:
        response = await client.get("/popular", params={"format": "v2"})
        response.raise_for_status()
        data = response.json()
    rows = [PopularSearch(**p) for p in _extract_results(data)[:10]]
    return "popular queries:\n" + "\n".join(f"{p.query} ({p.count})" for p in rows)


# -----------------------------------------------------------------------------
# entrypoint
# -----------------------------------------------------------------------------


def main() -> None:
    """run the MCP server."""
    mcp.run()


if __name__ == "__main__":
    main()
