# how pub search works

a search engine for content published on the [AT Protocol](https://atproto.com) — the open network behind [Bluesky](https://bsky.app). it indexes posts from publishing platforms like [leaflet](https://leaflet.pub), [pckt](https://pckt.blog), [offprint](https://offprint.app), [greengale](https://greengale.app), and [whitewind](https://whtwnd.com), all of which use the [standard.site](https://standard.site) schema.

**live at [pub-search.waow.tech](https://pub-search.waow.tech)**

## the big picture

```
ATProto firehose (every post, everywhere)
     ↓ filtered by collection, cryptographically verified
ingester (our own firehose consumer — signature + MST diff verification)
     ↓ documents + publications over a websocket channel
backend (zig)
     ├── turso (cloud sqlite — source of truth)
     ├── local sqlite replica (fast keyword search via FTS5)
     │      ↑ refreshed by the snapshot builder: offline build from turso
     │        → sha256 manifest → R2 → verified adoption (scaling-plan.md)
     ├── voyage AI embeddings → turbopuffer (semantic search)
     └── HTTP API
           ↓
static frontend (cloudflare pages)
```

content flows in one direction: the firehose broadcasts every AT Protocol event in real-time, the [ingester](../ingester/) filters for publishing-related records and verifies each commit cryptographically (it replaced bluesky's indigo tap on 2026-06-09 — non-canonical repos like bridgy fed mirrors are dropped at the door), and the backend indexes them into turso. the serving replica is rebuilt offline and adopted atomically rather than synced in place — bulk data movement never touches the serving box. a [reconciler](reconciliation.md) periodically verifies documents still exist at their source, catching missed deletions.

## how searching works

there are three search modes, each using different technology:

### keyword search

uses [SQLite FTS5](https://www.sqlite.org/fts5.html) — a built-in full-text search engine. when a document is indexed, FTS5 builds an inverted index (a map from every word to every document containing it). queries use [BM25](https://en.wikipedia.org/wiki/Okapi_BM25) ranking — a standard relevance scoring algorithm that considers term frequency and document length. recent documents get a small boost.

this is not something custom — FTS5 is a well-established tool built into SQLite. the custom part is building the index (deciding what to index, how to tokenize, how to rank) and the query syntax (OR between terms for recall, prefix matching on the last word for a type-ahead feel).

keyword search runs against a **local SQLite replica** on the same machine as the backend, not over the network to the database. this keeps latency around ~9ms.

### semantic search

uses [Voyage AI](https://voyageai.com) embeddings (voyage-4-lite, 1024 dimensions) to convert text into vectors — arrays of numbers that capture meaning. similar texts produce similar vectors, even if they don't share any words.

these vectors are stored in [turbopuffer](https://turbopuffer.com), a vector database optimized for approximate nearest-neighbor (ANN) search. when you search semantically, your query is embedded into a vector, and turbopuffer finds the documents whose vectors are closest.

this is how a search for `"loosely about cooking"` can find a post titled `"my grandmother's kitchen"` — keyword search would miss it entirely because the words don't overlap, but the meaning is close.

### hybrid search

runs both keyword and semantic in parallel, then merges results using [reciprocal rank fusion](https://plg.uwaterloo.ca/~gvcormac/cormacksigir09-rrf.pdf) (RRF, k=60). documents found by both methods rank highest. each result is annotated with its source: `"keyword"`, `"semantic"`, or `"keyword+semantic"`.

## the content extraction problem

every platform on standard.site stores document content differently. this is the most fiddly part of the system.

- **pckt, offprint, greengale** provide a `textContent` field with pre-flattened plaintext — easy
- **leaflet** omits `textContent` to save record size. content lives nested inside `content.pages[].blocks[].block.plaintext` — requires block-by-block extraction
- **whitewind** stores markdown directly in a `content` string field

the backend handles all of this in the [content extraction](content-extraction.md) layer, producing a uniform plaintext blob for indexing regardless of source platform.

## what's custom vs off-the-shelf

| component | off-the-shelf | custom |
|-----------|---------------|--------|
| full-text matching | SQLite FTS5 (BM25 ranking, inverted index) | query construction, tokenization rules, recency scoring |
| vector similarity | Voyage AI (embeddings), turbopuffer (ANN search) | hybrid fusion, result merging, snippet extraction |
| firehose sync | [zat](https://tangled.sh/@zzstoatzz.io/zat) (AT Protocol crypto: signatures, MST diff verification) | the entire ingester (relay subscription, verification policy, channel protocol), content extraction per platform, deduplication |
| data storage | Turso (cloud SQLite), local SQLite replica, R2 (snapshot artifacts) | schema design, snapshot builder + manifest gates, replica adoption |
| schema migrations | [zug](https://tangled.sh/@zzstoatzz.io/zug) (Zig 0.16 SQLite migration runner) | migration list, bootstrap path for the existing turso DB, `MigrationConn` adapter to zug's connection trait |
| frontend | Cloudflare Pages (hosting) | the entire UI and search experience |

the tools are popular and well-established. the assembly — wiring the firehose to content extraction to multi-modal search across heterogeneous publishing platforms — is very custom.

## further reading

- [search-syntax.md](search-syntax.md) — query syntax reference (quotes, OR, filters, modes)
- [search-architecture.md](search-architecture.md) — FTS5 details, scaling considerations, future options
- [content-extraction.md](content-extraction.md) — how content is extracted from each platform
- [api.md](api.md) — API endpoint reference
- [exclusions.md](exclusions.md) — the registry of manually excluded authors: the policy line (composed vs generated), enforcement layers, and the evidence for each ban
- [spam-detection-plan.md](spam-detection-plan.md) — the labeler: pub-search autonomously labels accounts that generate documents from datasets (`bulk-generated`, see [/labels](https://pub-search.waow.tech/labels)); doc is the original plan with an as-built status header
- [retro-2026-09-05-rescore-label-spill.md](retro-2026-09-05-rescore-label-spill.md) — a scoring rescore wiped `author_stats` and un-labeled every bulk-generated account for 25 minutes; a rescore now rebuilds counters in place and never drops a verdict
- [agent-surfaces.md](agent-surfaces.md) — adopting pub-search for agents: MCP vs HTTP API, when to use which
- [snapshot-pipeline.md](snapshot-pipeline.md) — how the keyword index ships (builder → manifest → R2 → verified adoption), what scales, and how to do production data surgery
- [scaling-plan.md](scaling-plan.md) — the plan of record: snapshot builder → R2 → verified swap → live overlay (largely executed; see status header)
- [retro-2026-06-10-cutover-cascade.md](retro-2026-06-10-cutover-cascade.md) — the outage night that produced the invariants behind that plan
- [reconciliation.md](reconciliation.md) — stale document detection and cleanup
- [turso-hrana.md](turso-hrana.md) — Turso's HTTP protocol for database queries
- [migrations.md](migrations.md) — schema migration system (zug + adapter + bootstrap)
- [performance-saga.md](performance-saga.md) — a debugging story about latency spikes
- [access-pattern-audit.md](access-pattern-audit.md) — a debugging story about turso row-read cost (the cost-side companion to the latency saga)
