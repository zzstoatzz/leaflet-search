# search architecture

current state, rationale, and future options.

## current: SQLite FTS5

keyword search uses SQLite's FTS5 on a local read replica built from Turso (the source of truth). the replica is an immutable snapshot, rebuilt hourly offline and adopted atomically after verification — see [snapshot-pipeline.md](snapshot-pipeline.md) for the pipeline, its scaling properties, and how production edits propagate.

### why FTS5 works for now

- **scale**: ~18k documents. FTS5 handles this trivially.
- **latency**: keyword p50 ~9ms (local SQLite replica), semantic p50 ~345ms (voyage + turbopuffer), hybrid p50 ~360ms.
- **cost**: $0. included with Turso free tier.
- **ops**: zero. no separate service to run.
- **simplicity**: Turso as source of truth, immutable local SQLite snapshot for FTS queries (no live writers on the serving file — ever).

### how it works

```
user query: "crypto-casino"
     ↓
buildFtsQuery(): "crypto OR casino*"
     ↓
FTS5 MATCH query with BM25 + recency decay (on local SQLite replica)
     ↓
results with snippet()
```

key decisions (see [search-syntax.md](search-syntax.md) for the user-facing reference):
- **OR between terms** for better recall (deliberate, see commit 35ad4b5)
- **quoted phrases** passed through to FTS5 for exact matching
- **prefix match on last word** for type-ahead feel (bare words only, not phrases)
- **unicode61 tokenizer** splits on non-alphanumeric (we match this in buildFtsQuery)
- **recency decay** boosts recent docs: `ORDER BY rank + (days_old / 30)`

### local-first routing (2026-08-04)

`searchKeyword` tries the local replica first (`searchLocal`) and falls back to
Turso only on error or when the replica isn't ready. Served locally:

- **FTS text queries** (with any of platform/since/author): BM25 + recency
- **tag browse** (tag, no text): ranked `months_old − RECOMMEND_LIFT·ln(1+recommenders)`
  — the replica carries `document_tags` and `recommends`, so one recommend
  outranks ~2 months of freshness while unrecommended docs stay newest-first
- **text within a tag**: BM25 + recency restricted to the tag's documents.
  ⚠️ the tag restriction is an `IN` subquery, NOT a join — a join let SQLite
  drive from the tag index and probe FTS per row (~8s on the real corpus).
  An `EXPLAIN QUERY PLAN` test asserts the FTS index drives; keep it passing.

Author-only browse still goes to Turso. Before 2026-08-04 every tag query did
too (2–8s per request); local serving is double-digit ms.

### what's coupled to FTS5

all in `backend/src/server/search.zig`:

| component | FTS5-specific |
|-----------|---------------|
| 14 query definitions | `MATCH`, `snippet()`, `ORDER BY rank` |
| `buildFtsQuery()` | constructs FTS5 syntax |
| schema | `documents_fts`, `publications_fts` virtual tables |

### what's already decoupled

- result types (`SearchResultJson`, `Doc`, `Pub`)
- similarity search (uses voyage-4-lite embeddings + turbopuffer ANN, not FTS5)
- hybrid mode (merges keyword + semantic via reciprocal rank fusion, k=60)
- search-time dedup by `(did, title)` — collapses cross-platform duplicates
- ingestion-time dedup by content hash — prevents duplicates at write time
- caching logic
- HTTP layer (server/mod.zig just calls `search()`)

### known limitations

- **no typo tolerance**: "leafet" won't find "leaflet"
- **no relevance tuning**: can't boost title vs content
- **single writer**: SQLite write lock
- **no horizontal scaling**: single database

these aren't problems at current scale.

## future: if we need to scale

### when to consider switching

- search latency consistently >100ms
- write contention from indexing
- need typo tolerance or better relevance
- millions of documents

### recommended: Elasticsearch

Elasticsearch is the battle-tested choice for production search:

- proven at massive scale (Wikipedia, GitHub, Stack Overflow)
- rich query DSL, analyzers, aggregations
- typo tolerance via fuzzy matching
- horizontal scaling built-in
- extensive tooling and community

trade-offs:
- operational complexity (JVM, cluster management)
- resource hungry (~2GB+ RAM minimum)
- cost: $50-500/month depending on scale

### alternatives considered

**Meilisearch/Typesense**: simpler, lighter, great defaults. good for straightforward search but less proven at scale. would work fine for this use case but Elasticsearch has more headroom.

**Algolia**: fully managed, excellent but expensive. makes sense if you want zero ops.

**PostgreSQL full-text**: if already on Postgres. not as good as FTS5 or Elasticsearch but one less system.

### migration path

1. keep Turso as source of truth
2. add Elasticsearch as search index
3. sync documents to ES on write (async)
4. point `/search` at Elasticsearch
5. keep `/similar` on turbopuffer (vector search)

the `search()` function would change from SQL queries to ES client calls. result types stay the same. HTTP layer unchanged.

estimated effort: 1-2 days to swap search backend.

### vector search scaling

similarity search currently uses voyage-4-lite embeddings (1024 dims) with turbopuffer ANN index. this handles ~18k docs well. at larger scale:

- **Elasticsearch**: has vector search (dense_vector + kNN)
- **dedicated vector DB**: Qdrant, Pinecone, Weaviate
- **pgvector**: if on Postgres

could consolidate text + vector in Elasticsearch, or keep them separate. turbopuffer scales well so may not need to change.

## summary

| scale | recommendation |
|-------|----------------|
| <10k docs | keep FTS5 (current) |
| 10k-100k docs | still probably fine, monitor latency |
| 100k+ docs | consider Elasticsearch |
| millions + sub-ms latency | Elasticsearch cluster + caching layer |

we're in the "keep FTS5" zone. the code is structured to swap later if needed.
