# API reference

base URL: `https://leaflet-search-backend.fly.dev`

## endpoints

### search

```
GET /search?q=<query>&tag=<tag>&platform=<platform>&since=<date>&author=<did|handle>&mode=<mode>
```

full-text search across documents and publications.

**parameters:**
| param | type | required | description |
|-------|------|----------|-------------|
| `q` | string | no* | search query (titles and content) |
| `tag` | string | no | filter by tag (documents only) |
| `platform` | string | no | filter by platform: `leaflet`, `pckt`, `offprint`, `greengale`, `whitewind`, `lemma`, `other` |
| `since` | string | no | ISO date, filter to documents created after |
| `author` | string | no | filter by author: DID (`did:plc:xyz`) or handle (`nate.bsky.social`). handles are resolved server-side via AT Protocol. |
| `mode` | string | no | `keyword` (default), `semantic`, or `hybrid`. semantic uses voyage-4-lite embeddings + turbopuffer ANN. hybrid merges keyword + semantic via reciprocal rank fusion. |
| `format` | string | no | `v2` wraps the page with pagination metadata |
| `limit` | int | no | page size (default 20, max 40) |
| `offset` | int | no | ranked-result offset (default 0, max 1000) |
| `include_undiscoverable` | bool | no | `true` includes publications that set `preferences.showInDiscover=false`. **Requires `author`** — 400 without it. Default excludes them. |

*at least one of `q`, `tag`, or `author` required

**visibility:** publications that set `preferences.showInDiscover=false` are
indexed but excluded from every discovery surface — `/search` (all modes),
`/similar`, and `/document`. Their author asked to stay out of discovery, not
to be forgotten. `include_undiscoverable=true` opts back in, scoped to ONE
identity per request:

| surface | rule |
|---|---|
| `/search` | requires `author=`, else `400` |
| `/document` | every uri must belong to the same repo, else `400` |
| `/similar` | rejected — neighbours come from every author, so it cannot be scoped |

Rejected rather than silently ignored: a dropped flag is how the original
inconsistency stayed invisible. This is scoping, not authentication — it closes
enumeration, but a caller who already knows a DID can still read that author's
unlisted writing. During the brief startup window before the
visibility set loads, these endpoints answer `503` rather than returning a
result set that would look like "nothing matched". See
[visibility.md](visibility.md).

**filter behavior by mode:**
- **keyword**: respects all filters (`tag`, `platform`, `since`, `author`)
- **semantic**: respects `platform`, `since`, and `author`. ignores `tag`.
- **hybrid**: keyword half respects all filters; semantic half respects `platform`, `since`, and `author`. results merged via RRF.

**response:**
```json
[
  {
    "type": "article|looseleaf|publication",
    "uri": "at://did:plc:.../collection/rkey",
    "did": "did:plc:...",
    "title": "document title",
    "snippet": "...matched text...",
    "createdAt": "2025-01-15T...",
    "rkey": "abc123",
    "basePath": "gyst.leaflet.pub",
    "platform": "leaflet",
    "path": "/001",
    "coverImage": "",
    "handle": "@user.bsky.social"
  }
]
```

with `format=v2`:
```json
{
  "results": [ /* same as above */ ],
  "total": null,
  "hasMore": true,
  "nextOffset": 20
}
```

`hasMore` is determined by retrieving one result beyond the requested page.
`total` is `null` because search does not run an expensive full-corpus count.
`nextOffset` is `null` when the retrieved ranking is exhausted. Other v2 list
endpoints may return an exact integer `total` when they already hold a complete
bounded result array.

Hybrid fusion uses a fixed 200-result source depth so its ranking cannot shift
between page requests; hybrid pages are therefore limited to that top-200
window. Keyword and semantic search accept offsets through 1000.

hybrid mode adds `source` and `score` fields:
```json
{
  "source": "keyword+semantic",
  "score": 0.85
}
```

**result types:**
- `article`: document in a publication
- `looseleaf`: standalone document (no publication)
- `publication`: the publication itself (only returned for text queries, not tag/platform filters)

**ranking:** hybrid BM25 + recency. text relevance primary, recent docs boosted (~1 point per 30 days).

### document

```
GET /document?uri=<at-uri>[,<at-uri>...]
```

full extracted text for documents by AT-URI. search results carry snippets
only; pass their `uri` values here to read the complete articles without
fetching and flattening records from the author's PDS yourself. served from
the local replica, so it inherits the same snapshot freshness as keyword
search.

**parameters:**
| param | type | required | description |
|-------|------|----------|-------------|
| `uri` | string | yes | comma-separated AT-URIs, max 25 per request |

**response:**
```json
{
  "documents": [
    {
      "type": "article",
      "uri": "at://did:plc:.../pub.leaflet.document/...",
      "did": "did:plc:...",
      "rkey": "...",
      "title": "...",
      "createdAt": "2026-01-01T00:00:00Z",
      "platform": "leaflet",
      "basePath": "example.leaflet.pub",
      "path": "",
      "publicationName": "...",
      "coverImage": "",
      "url": "https://example.leaflet.pub/...",
      "tags": ["..."],
      "content": "full extracted text..."
    }
  ],
  "missing": ["at://..."]
}
```

`documents` preserves request order. uris that are unknown, malformed, or
policy-excluded (banned/labeled authors, bridgy fed, dead urls — the same
set search hides) come back under `missing` rather than erroring the batch.
`400` for a missing/oversized `uri` param, `503` while the replica is
adopting a snapshot (retry shortly).

### similar

```
GET /similar?uri=<at-uri>
```

find semantically similar documents using vector similarity (voyage-4-lite embeddings + turbopuffer ANN).

**parameters:**
| param | type | required | description |
|-------|------|----------|-------------|
| `uri` | string | yes | AT-URI of source document |

**response:** same format as search (array of results)

### tags

```
GET /tags
```

list all tags with document counts, sorted by popularity.

**response:**
```json
[
  {"tag": "programming", "count": 42},
  {"tag": "rust", "count": 15}
]
```

### popular

```
GET /popular
```

popular search queries.

**response:**
```json
[
  {"query": "rust async", "count": 12},
  {"query": "leaflet", "count": 8}
]
```

### stats

```
GET /stats
```

index statistics and request timing.

**response:**
```json
{
  "documents": 11445,
  "publications": 2603,
  "embeddings": 10900,
  "searches": 5000,
  "errors": 5,
  "cache_hits": 1200,
  "cache_misses": 800,
  "timing": {
    "search_keyword": {"count": 1000, "avg_ms": 25, "p50_ms": 20, "p95_ms": 50, "p99_ms": 80, "max_ms": 150},
    "search_semantic": {"count": 100, "avg_ms": 350, "p50_ms": 340, ...},
    "search_hybrid": {"count": 50, "avg_ms": 380, ...},
    "similar": {"count": 200, "avg_ms": 150, ...},
    "tags": {"count": 500, "avg_ms": 5, ...},
    "popular": {"count": 300, "avg_ms": 3, ...}
  }
}
```

### activity

```
GET /activity
```

hourly activity counts (last 24 hours).

**response:**
```json
[12, 8, 5, 3, 2, 1, 0, 0, 1, 5, 15, 25, 30, 28, 22, 18, 20, 25, 30, 35, 28, 20, 15, 10]
```

### dashboard

```
GET /api/dashboard
```

rich dashboard data for analytics UI. includes platform counts (no separate `/platforms` endpoint).

**response:**
```json
{
  "startedAt": 1705000000,
  "searches": 5000,
  "publications": 2603,
  "documents": 11445,
  "platforms": [{"platform": "leaflet", "count": 5399}],
  "tags": [{"tag": "programming", "count": 42}],
  "timeline": [{"date": "2025-01-15", "count": 25}],
  "topPubs": [{"name": "gyst", "basePath": "gyst.leaflet.pub", "count": 150}],
  "timing": {...}
}
```

### subscribed

```
GET /subscribed?view=<publications|people>&since=<window>
```

subscription leaderboards. `view=publications` (default) ranks publications by
distinct subscribers; `view=people` ranks publication owners. `since` windows the
ranking: `day`, `week`, `month`, `year`, or `all` (default). Cached per (view, window).

Subscriptions match publications by `(did, rkey)`, not the full at-uri — leaflet
dual-writes each publication under both `pub.leaflet.publication` and
`site.standard.publication` at the same rkey, so an exact-uri match would miss the
half a subscription happens to reference. See `src/server/pubkey.zig`.

**response (`publications`):**
```json
[{"type": "publication", "uri": "at://…", "ownerDid": "did:plc:…", "name": "…",
  "basePath": "lab.leaflet.pub", "platform": "leaflet", "url": "https://lab.leaflet.pub",
  "subscriberCount": 418, "totalCount": 418}]
```
`subscriberCount` is within the window; `totalCount` is all-time. `people` rows are
`{"type": "person", "did", "subscriberCount", "totalCount", "pubCount"}`.

### subscribers

```
GET /subscribers?publication=<at-uri>
GET /subscribers?owner=<did>
```

the subscriber DIDs behind one publication (or every publication a DID owns).
Collection-agnostic on the publication side (same `(did, rkey)` match as
`/subscribed`); capped at 200, newest-subscribed first.

**response:**
```json
[{"did": "did:plc:…", "subscribedAt": "2026-05-01T12:00:00"}]
```

### wrapped

```
GET /wrapped?did=<did>
GET /wrapped?handle=<handle>
```

one identity's standing across the standard.site graph, in three lenses:
publisher (distinct subscribers + rank among owners), curator (recommends + rank),
reader (publications subscribed to + a recent slice). Local-replica only, no cache.

**response:**
```json
{
  "did": "did:plc:…",
  "publisher": {"pubCount": 3, "totalSubscribers": 44, "rank": 39, "totalOwners": 1008,
    "topPublication": {"uri": "at://…", "name": "…", "basePath": "…", "subscribers": 30},
    "publications": ["host.example"]},
  "curator": {"totalRecommends": 53, "uniqueDocs": 53, "rank": 5, "totalCurators": 855,
    "firstAt": "2026-02-…", "lastAt": "2026-06-…"},
  "reader": {"subscriptionCount": 11, "firstAt": "2026-01-…",
    "following": [{"uri": "at://…", "ownerDid": "did:plc:…", "name": "…", "basePath": "…"}]}
}
```

### labeler

```
GET /api/labeler
```

read-only summary of the bulk-generated labeler's state: counts by review
state plus every decided author (site, state, model reason, heuristic score,
title patterns). backs the [/labels](https://pub-search.waow.tech/labels)
page. the labels themselves are served over standard XRPC at
`labeler.pub-search.waow.tech` (`com.atproto.label.queryLabels` /
`subscribeLabels`).

`queryLabels` takes the lexicon's parameters: `uriPatterns` (repeatable, and
comma-joined values are also accepted — an entry may be an exact subject, a
`prefix*` pattern, or a bare `*` for every subject), optional `sources`
(repeatable labeler DIDs), `limit` (1–250, default 50) and `cursor`. the
response carries a `cursor` whenever a full page was returned.

```
GET https://labeler.pub-search.waow.tech/xrpc/com.atproto.label.queryLabels?uriPatterns=*&limit=250
```

`GET /admin/label?token=…&did=…&val=bulk-generated&neg=0|1` (BACKFILL_TOKEN-
gated) emits or negates a label manually — the negation path is the appeal /
correction lever; it also updates the classifier's state so the account is
never re-flagged.

`POST /admin/reconcile-document?token=…&action=upsert|delete&did=…&collection=site.standard.document&rkey=…&pds=…&expected_cid=…`
is the item-scoped corpus-repair endpoint. It is disabled unless
`BACKFILL_TOKEN` is configured. Upserts re-fetch the PDS record and require an
exact CID match; deletes only proceed on a fresh authoritative 400/404. The
bounded ledger runner is the intended caller—this is not a bulk API.

### health

```
GET /health
```

**response:**
```json
{"status": "ok"}
```

## building URLs

documents can be accessed on the web via their `basePath` and platform-specific patterns:

| platform | URL pattern | example |
|----------|-------------|---------|
| leaflet | `https://{basePath}/{rkey}` | `https://gyst.leaflet.pub/3ldasifz7bs2l` |
| pckt | `https://{basePath}{path}` | `https://devlog.pckt.blog/some-slug` |
| offprint | `https://{basePath}{path}` | `https://dalisay.offprint.app/a/3me5ucj7vxf23-title-slug` |
| greengale | `https://{basePath}{path}` | `https://3fz.greengale.app/001` |
| whitewind | `https://whtwnd.com/{did}/{rkey}` | `https://whtwnd.com/did:plc:.../3abc123` |
| publications | `https://{basePath}` | `https://gyst.leaflet.pub` |
