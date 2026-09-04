"""Type definitions for Leaflet MCP responses."""

from typing import Literal

from pydantic import BaseModel, field_validator


class SearchResult(BaseModel):
    """A search result from the Leaflet API."""

    type: Literal["article", "looseleaf", "publication"]
    uri: str
    did: str
    title: str
    # /search and /similar return a match window; /recommended and a browse
    # (no query) return nothing here, and the server back-fills the opening
    # of the stored text so every tool's results can be triaged in place.
    snippet: str = ""
    # length of the indexed text in characters when known, 0 when not. a
    # linkblog stub indexes as a pull-quote of a few hundred characters; this
    # lets a caller skip those when hunting for primary writing.
    contentLength: int = 0
    createdAt: str = ""
    rkey: str
    basePath: str = ""
    platform: Literal["leaflet", "pckt", "offprint", "greengale", "whitewind", "other"] = "leaflet"
    path: str = ""
    source: str = ""
    publicationName: str = ""
    url: str = ""
    # populated by /recommended (windowed count and all-time count); 0 elsewhere.
    recommendCount: int = 0
    totalCount: int = 0

    @field_validator("snippet", mode="before")
    @classmethod
    def _none_snippet_is_empty(cls, value: object) -> object:
        return "" if value is None else value


class ClusterContext(BaseModel):
    """A focal document's neighborhood across the long-form ATProto network.

    Pre-computes the cross-platform / cross-author / shared-terms observations
    that pub-search uniquely sees from indexing every long-form platform as one
    corpus. Designed to give a curator (e.g. an agent) the network-position
    context for a synthesis without N extra calls.
    """

    focal_uri: str
    neighbors: list["SearchResult"]
    platforms: list[str]
    distinct_authors: int
    cross_platform: bool
    cross_author: bool
    shared_terms: list[str]


class Tag(BaseModel):
    """A tag with document count."""

    tag: str
    count: int


class PopularSearch(BaseModel):
    """A popular search query with count."""

    query: str
    count: int


class EndpointTiming(BaseModel):
    """Timing stats for a single endpoint."""

    count: int = 0
    avg_ms: float = 0.0
    p50_ms: float = 0.0
    p95_ms: float = 0.0
    p99_ms: float = 0.0
    max_ms: float = 0.0


class Stats(BaseModel):
    """Index statistics."""

    documents: int
    publications: int
    embeddings: int = 0
    searches: int = 0
    errors: int = 0
    started_at: int = 0
    cache_hits: int = 0
    cache_misses: int = 0
    timing: dict[str, EndpointTiming] = {}


class Document(BaseModel):
    """Full document content from ATProto."""

    uri: str
    title: str
    content: str
    createdAt: str = ""
    tags: list[str] = []
    publicationUri: str = ""


class AuthorProfile(BaseModel):
    """What one author writes, where, and when.

    Answers "who is this person, in this corpus?" — a question that otherwise
    costs an agent a browse plus several passes of counting over the results.
    """

    author: str
    document_count: int
    platforms: list[str]
    publications: list[str]
    first_published: str = ""
    last_published: str = ""
    recurring_terms: list[str] = []
    recent: list["SearchResult"] = []
