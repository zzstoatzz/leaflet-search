# retro: keyword search ran every FTS match twice

**date:** 2026-09-05 · **impact:** common-word keyword searches took 17 to 20
seconds; the one-letter query "a" ran for 42 minutes and held the machine's
only CPU the whole time, slowing every other request 2 to 6x · **cause:** the
statements that produce snippets re-ran the MATCH in their outer query, and
FTS5 executes that as a second full scan of the query's posting union.

## what we saw

the stats page showed keyword p95 near 3 seconds after a day of deploys. a
direct probe reproduced it: `the` took 20.5 s cold, then 90 ms from the origin
memo; `a` never returned. load average sat at 4 on a `shared-cpu-1x` machine
until the statement finished.

## what it was not

the first theory was ranking cost: BM25 over 62k matching documents. measured
warm on the replica with the sqlite shell, ranking `the` to a top-2000 candidate
set takes 179 ms and even `a*` (39,615 terms, 1.46M postings) counts in 480 ms.
FTS5 was fine. the plan for the server's own statement was not.

## the plan

the snippet-producing statements had this shape:

```sql
SELECT ..., snippet(documents_fts, ...), rank + recency
FROM documents_fts f JOIN documents d ON d.rowid = f.rowid
WHERE documents_fts MATCH ?            -- second MATCH, for snippet()'s cursor
  AND f.rowid IN (SELECT rid FROM (   -- the real candidate pass
        SELECT rowid AS rid, rank FROM documents_fts WHERE documents_fts MATCH ?
        ORDER BY rank LIMIT 2000) ... LIMIT 83)
```

```
|--SCAN f VIRTUAL TABLE INDEX 0:=M3      ← full match scan, all 64k rows
|--LIST SUBQUERY 3
|  |--MATERIALIZE c
|  |  `--SCAN documents_fts VIRTUAL TABLE INDEX 32:M3
```

FTS5 accepts `rowid = ?` as a constraint but not `rowid IN (...)`, so the outer
MATCH is a second complete scan of the posting union, with the IN-list applied
as a filter afterwards. same candidates, same inner pass: the snippet-free
variant of the statement ran in 493 ms; the snippet variant in 17.7 s. for a
prefix term the outer scan re-merges every term's doclist, which is why `a*`
took minutes rather than seconds.

## fix

- every keyword statement has exactly one MATCH. the candidate pass ranks
  inside the FTS index; the output pass joins `documents` by rowid seek.
- the snippet is built in Zig from the document's own text
  (`snippetFromContent`): the window around the first query hit, or the
  opening. it runs only for rows that are emitted. `substr(content, 1, 8000)`
  rides along in the projection so the column shape is unchanged.
- hybrid fusion's per-row snippet probe (`MATCH ? AND rowid = ?`) is gone
  too: for a prefix term each probe re-merged the prefix, 120 ms apiece.
- the type-ahead prefix star needs a final word of at least three letters.
  `a*` is most of the corpus; `an*` is 5,283 terms.
- the query-plan test asserts one `SCAN documents_fts` per statement.

what stayed: the tag-filtered statement still uses FTS5's `snippet()` inside
its single scan, and the Turso fallback statements are untouched.

## what we keep

- the plan is the evidence. a slow query is a `EXPLAIN QUERY PLAN` away from
  its cause; do that before reaching for a cap or a stop-word list.
- measure the statement the server runs, not a simplified one. the inner
  query alone was fast; only the real shape showed the second scan.
- a runaway statement on a one-CPU machine is an outage for everyone, and
  the shared-CPU burst budget makes every later measurement lie. look at
  `/proc/loadavg` before trusting a number.
