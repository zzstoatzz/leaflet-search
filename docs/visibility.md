# serving-time visibility (`showInDiscover`)

How pub-search honors `site.standard.publication` → `preferences.showInDiscover`,
and why the enforcement lives where it does.

## the rule

A publication with `showInDiscover: false` is **indexed but excluded from every
discovery surface**: `/search` (all modes), `/similar`, and `/document`. The
author asked to stay out of discovery, not to be forgotten — the corpus still
carries the documents, and `include_undiscoverable=true` opts back in.

Default is exclusion. A retrieval path that cannot resolve the preference
**excludes**, and if the policy set has not loaded at all the request is
refused with `503` rather than answered with a smaller result set.

## the bug this design exists to prevent

Enforcement used to be a point lookup against the local replica, keyed on the
document uri:

```sql
SELECT 1 FROM documents d
JOIN publications p ON d.publication_uri = p.uri
WHERE d.uri = ? AND COALESCE(p.show_in_discover, 1) = 0
```

Every retrieval path called it. The predicate was never missing. The *data*
was: the replica is a point-in-time snapshot of everything at or below a
watermark, but the rows being filtered arrive from turso and turbopuffer, which
carry URIs the current replica does not —

- documents indexed **above** the snapshot watermark (up to a full build cycle
  of fresh content), and
- **superseded rkeys** that a rename left behind, which the reconciler has not
  yet reaped.

No row meant "not hidden", so opted-out documents ranked in anonymous semantic
search. Keyword search appeared to work only because it is served *from* the
replica, where the filter's data actually is.

Observed 2026-08-08: an opted-out publication ranked #1 and #3 for an anonymous
semantic query, and the leak closed by itself at 04:55:54Z when snapshot
`b1786164194-a278` was adopted and the replica happened to gain the rows. A
policy that enforces itself only when a background build catches up is not
enforcing anything.

## the fix: a set, not a lookup

The question changed. Instead of asking an incomplete per-document index *"is
this row hidden?"*, hold the **complete set of opted-out publications** and ask
*"does this row's publication belong to it?"*

That question is answerable for every row — ghost, fresh, or replica-missing —
because the set is complete even when the document index is not. There are
~8.5k publications and the opted-out subset is tiny, so the set is small enough
to hold in memory and cheap enough to rebuild often.

`backend/src/visibility.zig` owns it:

| | |
|---|---|
| seed | from the local replica at startup — no network, complete at or below the watermark |
| refresh | from turso every 5 min in the background — picks up opt-outs newer than the snapshot and preference flips |
| failure | fail static: a failed refresh keeps the last known good set |
| read | a hash lookup under a mutex. **no I/O, ever, on the request path** |

Keyed two ways, because different paths carry different identity: by
publication uri (SQL paths know it) and by `did` + `base_path` (turbopuffer
results carry that, and keying on it avoids making a 64k-vector attribute
backfill load-bearing for a policy check).

## why not the alternatives

- **turso lookup on the request path** — violates the invariant this codebase
  repeats in eight places (six background caches, the turso slot shedder, the
  local replica itself): a user request never blocks on turso. Measured over
  35,629 queries in 24h: p50 10ms, p95 444ms, **p99 3.96s**, max 278s.
- **a mutable overlay** (typeahead's `actor_overlay`) — maintained state that
  drifts; typeahead carries a live-table "safety belt" for exactly that reason.
  We would be importing the drift and the belt.
- **a turbopuffer `discoverable` attribute** — the embedder writes a vector
  once, so the attribute goes stale the moment a publication toggles the
  preference. Same fail-open trap, relocated.

## the borrowed invariant

From `zat.dev/stream`'s `docs/invariants.md`:

> **An internal failure must never look like an absence.** A read error is not
> a missing key; a planner failure is not "no matching data". Fail closed, or
> answer `5xx` — never return a smaller truthful-looking answer, because a
> client cannot tell the difference.

pub-search's bug was the dual: an *absence* read as a *permission*. One rule
covers both — never conflate "I don't know" with "no". That is why an unloaded
set returns `503` instead of an empty array: an empty array is exactly the
smaller truthful-looking answer the invariant forbids.

## testing

The regression tests construct the condition that actually broke — a document
whose publication is opted out but whose uri the replica does not have. A test
against a complete replica passes against the old code too, which is why the
original filter looked correct for months.

See `visibility.zig` tests and `server/search.zig`:
- unloaded set fails closed, and `search()` refuses rather than returning `[]`
- membership by publication uri, and by did+base_path with no publication uri
- looseleaf documents (no publication) have no preference to honor

## related

- `docs/scaling-plan.md` — why the replica is frozen between adoptions
- `backend/src/promote.zig` — snapshot adoption and `source_watermark`
- `backend/src/ingest/reconciler.zig` — reaps superseded rkeys; a data-quality
  process, deliberately **not** load-bearing for this policy
