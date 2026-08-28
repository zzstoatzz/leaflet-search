# content extraction for site.standard.document

lessons learned from implementing cross-platform content extraction.

## the problem

[eli mallon raised this question](https://bsky.app/profile/iame.li/post/3md4s4vm2os2y):

> The `site.standard.document` "content" field kinda confuses me. I see my leaflet posts have a $type field of "pub.leaflet.content". So if I were writing a renderer for site.standard.document records, presumably I'd have to know about separate things for leaflet, pckt, and offprint.

short answer: yes. but once you handle `content.pages` extraction, it's straightforward.

## textContent: platform-dependent

`site.standard.document` has a `textContent` field for pre-flattened plaintext:

```json
{
  "title": "my post",
  "textContent": "the full text content, ready for indexing...",
  "content": {
    "$type": "blog.pckt.content",
    "items": [ /* platform-specific blocks */ ]
  }
}
```

**pckt, offprint, greengale** populate `textContent`. extraction is trivial.

**leaflet** intentionally leaves `textContent` null to avoid inflating record size. content lives in `content.pages[].blocks[].block.plaintext`.

## extraction strategy

priority order (in `extractor.zig`):

1. `textContent` - use if present
2. `pages` - top-level blocks (pub.leaflet.document)
3. `content.pages` - nested blocks (site.standard.document with pub.leaflet.content)

```zig
// try textContent first
if (zat.json.getString(record, "textContent")) |text| {
    return text;
}

// fall back to block parsing
const pages = zat.json.getArray(record, "pages") orelse
    zat.json.getArray(record, "content.pages");
```

the key insight: if you extract from `content.pages` correctly, you're good. no need for extra network calls.

## cross-collection identity

documents can appear in both collections with identical `(did, rkey)`:
- `site.standard.document`
- `pub.leaflet.document`

handle with `ON CONFLICT`:

```sql
INSERT INTO documents (uri, ...)
ON CONFLICT(uri) DO UPDATE SET ...
```

note: leaflet is phasing out `pub.leaflet.document` records, keeping old ones for backwards compat.

## platform detection

collection name doesn't indicate platform for `site.standard.*` records. detection order:

1. **basePath** - infer from publication basePath:

| basePath contains | platform |
|-------------------|----------|
| `leaflet.pub` | leaflet |
| `pckt.blog` | pckt |
| `offprint.app` | offprint |
| `greengale.app` | greengale |

2. **content.$type** - fallback for custom domains (e.g., `cailean.journal.ewancroft.uk`):

| content.$type starts with | platform |
|---------------------------|----------|
| `pub.leaflet.` | leaflet |

3. if neither matches → `other`

## whitewind

[WhiteWind](https://whtwnd.com) (`com.whtwnd.blog.entry`) stores content as markdown in the `content` field (a string, not a blocks structure). extraction is trivial — just use the string directly. author-only posts (`visibility: "author"`) are skipped.

## deduplication

three mechanisms address different duplicate shapes:

1. **republish identity at ingestion**: within one collection, `(did, base_path, path)` identifies a logical document across rkeys. A newer TID rkey supersedes the existing row; replaying an older rkey is ignored. The new row commits before the old row and vector are removed, so a failed write cannot erase both versions.
2. **content hash at ingestion**: wyhash of `title + \x00 + content` per author prevents identical content published across platforms under different identities from being indexed twice.
3. **search-time collapse**: `(did, title)` dedup hides any remaining historical or cross-platform duplicates in results.

The republish rule is deliberately newest-wins. Some site generators publish every document again under a fresh rkey without deleting the previous records. Keeping the old content-hash match would make a normal create-then-delete rename unsafe: dropping the new record and then processing the old record's delete would remove both. Superseding the old rkey makes that trailing delete an idempotent no-op.

### 2026-08-27 republish cleanup

Before the identity rule, two publishers had inflated the corpus from real document count to nearly 110k rows:

| publication | stale rows removed | observed behavior |
|-------------|-------------------:|-------------------|
| `jcrt.org` | 25,024 | about 26 live rkeys per article |
| `blog.localstack.cloud` | 2,052 | about 11 live rkeys per article |
| **total** | **27,076** | |

`scripts/purge-republish-duplicates` removed the older rows from Turso and turbopuffer while retaining the newest rkey for each identity. It ran in paced 100-row batches with a `SELECT 1` canary between batches; an immediate rerun found zero remaining duplicates. The dashboard change was therefore:

```text
109,994 rows before
-27,076 stale republish rows
=82,918 documents after
```

This was cardinality correction, not loss of distinct documents. JCRT retained 1,821 documents, and the post-cleanup dashboard reported equal document and embedding counts (82,918 each). Future republishes are bounded by the ingest identity rule in `backend/src/ingest/indexer.zig`; the cleanup script is retained only as an auditable one-time operation.

## summary

- **pckt/offprint/greengale**: use `textContent` directly
- **leaflet**: extract from `content.pages[].blocks[].block.plaintext`
- **whitewind**: use `content` string directly (markdown)
- **deduplication**: newest-rkey republish identity + content hash at ingestion, then `(did, title)` at search time
- **platform**: infer from basePath, fallback to content.$type for custom domains

## code references

- `backend/src/ingest/extractor.zig` - content extraction logic, content_type field
- `backend/src/ingest/indexer.zig` - platform detection, republish identity, and content-hash dedup
- `scripts/purge-republish-duplicates` - auditable one-time cleanup for rows accumulated before the identity rule
