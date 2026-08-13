# the snapshot pipeline: freshness, scale, and production surgery

how the keyword index gets built, shipped, verified, and adopted — and what
that design means for editing production data after the fact.

shipped 2026-06-11 (`db0241f` builder, `32dca94` promote watcher, `0ef278c`
the flip). architecture inspired by [typeahead](https://tangled.sh/@zzstoatzz.io/typeahead),
the same author's 7M-actor autocomplete service, which arrived at the
operating invariant this whole pipeline encodes:

> serving reads immutable, already-built artifacts. corpus-scale data
> movement happens off the serving box, always.

**2026-08-13 update**: snapshots are no longer the freshness mechanism — a
live overlay (`/data/overlay.db`) carries docs newer than the adopted
snapshot's watermark and is merged at read time, then compacted to the new
watermark on each adoption. the invariant above is unchanged (the overlay is
bounded and local; the snapshot stays gated + atomic); what changed is that
adoption cadence can now stretch without costing freshness. see
[overlay-serving.md](overlay-serving.md).

## why: the patents incident

in june 2026 a single patent-mirroring bot
(`did:plc:oql6ds5vnff4ugar6rruliwd`, ~96k documents on its PDS) tripled the
corpus in days and became **44% of the index**. ripping it out is what
exposed every weak joint at once:

- per-record FTS maintenance was O(corpus) (fts5's `uri` column is
  unindexed — every delete-by-uri scanned the whole table)
- the serving replica shared a SQLite file with a bulk writer, so cleanup
  writes convoyed live searches
- bulk deletes against turso degraded it enough that `SELECT 1` timed out —
  and with no http timeouts in the client, a slow source of truth froze
  serving entirely

the lesson wasn't "patents are bad." it was that **corpus composition is
adversarial** — one actor can grow the index faster than any in-place
maintenance can cope — and that the only durable defense is to make corpus
size irrelevant to the serving path. full story:
[retro-2026-06-10-cutover-cascade.md](retro-2026-06-10-cutover-cascade.md);
the resulting invariants: [scaling-plan.md](scaling-plan.md).

## the pipeline

```
turso (source of truth)
  │  hourly, off-box (ephemeral fly machine, BUILDER_MODE=1)
  ▼
builder: keyset-paginated read, paced; policy filters
  (banned DIDs + bridgy NEVER enter a snapshot, even though
   turso history may briefly hold their rows)
  │  gates: doc-count vs turso, FTS sentinel, quick_check
  ▼
artifact: replica.db (VACUUMed) + manifest
  { build_id, sha256, byte_size, schema_version,
    source_watermark, doc/pub/tag counts }
  │  rclone → R2; latest.json pointer uploaded LAST
  ▼
R2 bucket leaflet-search-index
  channels: prod (builds/, latest.json — publish requires
  BUILDER_ALLOW_PROD=1) and staging (staging/, latest.staging.json)
  lifecycle: prod artifacts expire after 7d, staging 3d
  │  polled every 5min by the promote watcher (serving box,
  │  ENABLE_SNAPSHOT_PROMOTE=1, channel-pinned)
  ▼
verify: size + sha256 vs manifest, schema_version match,
  quick_check, EXACT doc count, zero banned/bridgy rows,
  FTS answers, platforms present   ← rejects, never best-effort
  ▼
stage local.db.new + manifest sidecar → clean exit →
  restart (policy: always) → atomic rename at boot
  previous snapshot kept as local.db.prev (one-command rollback)
```

key properties, mostly stolen from typeahead:

- **the manifest is a contract, not a hint.** the build is pinned to
  `indexed_at <= source_watermark`, so the snapshot contains every
  policy-passing doc at or before the watermark and nothing after. the
  watcher verifies the *exact* doc count — there is no race to tolerate.
- **the pointer is uploaded last.** a reader can never see `latest.json`
  pointing at a partially-uploaded artifact.
- **channel discipline.** a staging build cannot be adopted by a prod
  watcher even if uploaded to the wrong key — the manifest's channel field
  is checked, and prod *publishing* requires a second arming flag.
- **adoption is attested.** boot skips the (minutes-long, cold-cache)
  integrity scan when the manifest sidecar attests the file was
  sha256-verified at stage time — adopt restarts cost ~1 second of db open.
- **rollback is a file rename**, not a debugging session:
  `mv local.db.prev local.db.new` + restart. or disable the watcher flag
  and the system freezes on the current snapshot indefinitely.

observability: `GET /snapshot` on the backend returns the live replica's
manifest (what build is prod actually serving). the builder has a 45-minute
process deadline; an independent 15-minute guardian stops runs over 50 minutes
and starts a missed run when snapshot age exceeds 75 minutes. after a builder
exits, the guardian allows 15 minutes for download and atomic adoption before
starting another run. the watchdog alerts when age exceeds 120 minutes. see
[the July 15 stale-snapshot retro](retro-2026-07-15-stale-snapshot-builder-hang.md).

## what happens at arbitrary scale

the request path does no work proportional to corpus size — that's the
invariant, and it's what makes 10x boring. what actually grows:

| component | growth | status |
|---|---|---|
| serving reads (FTS, point lookups) | flat — bounded queries on an immutable file | done |
| semantic search | flat — remote ANN (turbopuffer), vectors built offline | done |
| ingest | per-event, corpus-independent | done |
| snapshot build + upload + download | linear, but **offline and schedulable** — a slow build costs nothing user-visible | done; hourly at 25k docs ≈ 10min |
| boot adoption | ~constant (attested boots skip the scan) | done |
| FTS **updates** on turso | O(corpus) per edited document (fts5 uri unindexed; creates skip it via existence gate) | **owed: rowid-keyed FTS migration** — prerequisite for edit-heavy backfills |
| background cache aggregates (COUNTs, GROUP BYs) | linear; off the request path | fine until it isn't — counter tables when refresh gets slow |
| `/popular` | live turso query per request | known gap; cache like the rest |
| replica size | linear; multi-GB at 10x means volume/RAM sizing or contentless FTS (only FTS needs content locally) | sized fine today (1GB box, 350MB replica) |
| freshness | hourly rebuild stops being "fresh enough" as builds slow | **owed: live overlay** — small delta db over the snapshot watermark, merged at query time, dropped at each adopt |

the deeper point: a drivepatents-scale inflow today would land in turso,
get excluded (if banned) or included (if legit) by the next build, and
serving would notice nothing either way. the failure mode that took a
night of firefighting in june is now a non-event by construction.

## staleness and production surgery

turso is the source of truth; everything else is a derived view with its
own refresh cycle. to do surgery, edit the source and know the
propagation times:

| view | refresh | worst-case staleness |
|---|---|---|
| keyword (replica) | hourly build + ≤5min promote poll | ~75min hands-off; **~15min if you trigger a build manually** |
| semantic ranking (tpuf vector) | only on re-embed | until you act (see below) |
| semantic *display* fields (title/url) | read from the replica at query time | same as keyword |
| recommended / curators | turso queried via background caches | minutes |
| recommended-by-top-authors | replica (recommends ship in the snapshot, schema v2) | one snapshot (~75min) |
| tags / dashboard / timeline | replica + 60s–5min caches | one snapshot + one cache tick |
| atlas | prefect rebuild every 6h from tpuf | ≤6h, or trigger the flow |

### the playbooks

**fix a document's content/title/metadata**: update the row in turso.
keyword + display fields heal at the next snapshot. if the *meaning*
changed enough to affect semantic ranking, also clear its `embedded_at` —
the embedder re-embeds and upserts the vector.

**delete a document**: delete the turso row (and its `document_tags` /
`recommends` rows) AND delete its tpuf vector. the snapshot pipeline only
heals keyword — a forgotten vector keeps the doc alive in semantic,
similar, and the next atlas build. this dual-store step is the #1
surgery footgun.

**ban an author** (the full drivepatents/bridgy treatment):
1. add the DID to `backend/src/policy.zig` `BANNED_DIDS` → deploy. the
   indexer refuses new inserts, the builder excludes existing rows, and
   the promote watcher will reject any snapshot containing them.
2. deploy the backend, then trigger a build:
   `prefect deployment run 'pub-search-snapshot/pub-search-snapshot' --watch`.
   the flow clones fresh, so the new ban list is in the binary it builds.
3. purge vectors (`scripts/purge-bridgyfed-vectors` handles the banned
   list) and, eventually, turso history (`scripts/purge-banned-turso` —
   paced, canary-gated; turso rows are invisible to serving meanwhile,
   the builder filters them every build).

**emergency takedown** (minutes matter): edit turso, then trigger a build
immediately instead of waiting for the 2h schedule:
`prefect deployment run 'pub-search-snapshot/pub-search-snapshot' --watch`.
the watcher adopts within its next 5-minute poll. ~25 minutes end to end,
all through the verified path. also delete the tpuf vector for instant
semantic removal.

### the limitations, honestly

- **no instant keyword takedown.** the floor is one build + one poll
  (~15min). editing `/data/local.db` directly is not a path — the file is
  the *output* of the pipeline and gets atomically replaced within the
  hour; hand-edits silently vanish. if sub-minute removal ever matters,
  it belongs in the live overlay as a suppression entry (the overlay
  design carries deletes/suppressions precisely for this).
- **dual-store deletes are manual.** keyword heals itself from turso;
  vectors don't. until that's automated (e.g. the reconciler diffing tpuf
  against turso), every delete is a two-step.
- **document edits are expensive turso-side** (the O(corpus) FTS update)
  until the rowid-FTS migration lands. fine at edit rates of today;
  not fine for a mass-rewrite backfill.
- **ban propagation requires a deploy.** policy is code, deliberately -- but
  it means the ban list isn't a runtime knob. CI advances serving and builder
  from the same image digest.
- **adopt restarts drop in-flight requests** (once per hour, ~seconds).
  the zero-downtime answer is a leased read pool + live ATTACH swap
  (typeahead's design); the boot-adopt path was chosen first because it
  reuses the rename choreography every deploy already exercises.

## further reading

- [scaling-plan.md](scaling-plan.md) — the invariants and the original plan of record
- [retro-2026-06-10-cutover-cascade.md](retro-2026-06-10-cutover-cascade.md) — the night that motivated all of this
- [retro-2026-07-15-stale-snapshot-builder-hang.md](retro-2026-07-15-stale-snapshot-builder-hang.md) — why schedule, timeout, deploy, and paging are separate guarantees
- [search-architecture.md](search-architecture.md) — FTS5 internals and the if-we-outgrow-sqlite options
- typeahead: [tangled.sh/@zzstoatzz.io/typeahead](https://tangled.sh/@zzstoatzz.io/typeahead) — the architectural prior art (builder/manifest/promote/overlay at 7M-actor scale)
