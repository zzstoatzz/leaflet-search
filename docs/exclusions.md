# exclusions: what we keep out of the corpus, and why

pub-search indexes **composed writing** — documents an author (a person or
an AI) wrote because they had something to say. some actors publish valid,
well-signed AT Protocol records that are nonetheless not that —
machine-generated registry mirrors, scraper bridges, bulk archives. this
file is the registry of every manual exclusion: who, when, why, and the
evidence. if we're going to editorialize, we explain ourselves.

## what qualifies (the policy line)

the line is **composed vs. generated**, not human vs. machine: most banned
content was human-written at the source (patents, recall notices,
transcripts), and original writing by an AI is welcome. an author is
excluded when **all** of these hold:

1. **generated, not composed** — each document exists because a record
   exists in some dataset (external or the account's own), with a template
   determining the text; not because an author chose to write it
2. **bulk scale** — the volume materially distorts the corpus or a topic's
   search results (rule of thumb: would be >1% of the corpus, or owns a
   topic's semantic results)
3. **no curation signal lost** — nobody recommends them; excluding them
   removes noise, not voices

an author is NOT excluded for: low quality, controversial content, high
volume, self-promotion, or being an AI. the line is how the documents come
to exist, not taste and not the author's species.

taste IS allowed on the keep side: `/kept-dids.txt` lists accounts
pub-search deliberately keeps despite a true `bulk-generated` label
(e.g. thefestivusproject.com — a Seinfeld episode catalog, kept because it
is delightful). the label stays on the public stream (it's a fact); the
exemption is pub-search's own consumer policy. `policy.isKept` gates the
hard-drop; /labels shows these as labeled · kept.

## how exclusions are enforced (the new architecture)

a ban acts at every layer, so no single bug re-admits an excluded author:

| layer | mechanism | effect |
|---|---|---|
| ingester | `BANNED_DIDS` in `ingester/src/main.zig` | dropped at the firehose, never emitted |
| indexer | `backend/src/policy.zig` via `isBanned` | refuses inserts (replays, backfills, the `/admin/backfill` side door) |
| snapshot builder | `policy.banned_dids_sql` in the build query | excluded from every snapshot even while turso history still holds rows |
| promote watcher | per-DID zero-count gate | a snapshot containing a banned row is REJECTED, never adopted |
| bridgy fed (network-level) | PDS host check in the ingester verifier + backfill | any `*.brid.gy`-hosted repo, no list needed |

cleanup after a ban (the stores that don't self-heal):

| store | tool | notes |
|---|---|---|
| turbopuffer vectors | `scripts/purge-bridgyfed-vectors` (handles the banned set) | do this FIRST — semantic pollution is live until vectors die |
| turso history | `scripts/purge-banned-turso` | paced + canary-gated; not urgent (builder filters every build) but keyword's turso fallback during restarts reads it |
| atlas | trigger `rebuild-atlas` prefect flow (or wait ≤6h) | rebuilt from tpuf, so purge vectors first |
| keyword replica | nothing — next hourly build + adopt is clean | |

**operational steps for a new ban** (see also
[snapshot-pipeline.md](snapshot-pipeline.md) playbooks):
1. add the DID to all four lists: `ingester/src/main.zig`,
   `backend/src/policy.zig` (+ its tests), `scripts/purge-banned-turso`,
   `scripts/purge-bridgyfed-vectors` — and an entry HERE with evidence
2. deploy backend (push) AND ingester (`cd ingester && fly deploy`)
3. **recreate the `snapshot-builder-hourly` machine** — it pins its image
   and will keep building with the old policy until you do
4. purge vectors, trigger atlas rebuild, run the paced turso purge
5. verify: semantic query for the author's topic, `/snapshot` after next
   adopt, atlas point count

> single source of truth (2026-06-29): the DID list lives once in
> `/banned-dids.txt` at the repo root. Zig (backend + ingester) `@embedFile`s
> it at comptime via build.zig; the purge scripts read it at runtime. Adding a
> ban = editing one file (+ a registry entry here). Propagation still needs a
> deploy (the list is compiled in); the eventual next step, if bans get
> frequent, is a `banned_dids` turso table read at runtime so a ban becomes a
> data change instead of a deploy.
>
> that labeler now EXISTS (2026-06-30): pub-search emits signed
> `bulk-generated` account labels autonomously — a firehose classifier
> nominates, a model gate confirms (majority-of-3 votes), and a mislabel is
> corrected by negation (`/admin/label?neg=1`), not a review queue. labels are
> public at labeler.pub-search.waow.tech and explained at
> [pub-search.waow.tech/labels](https://pub-search.waow.tech/labels). the
> `banned_dids`-backed hard-drop is still pending (gated on the notify/appeal
> loop). see [spam-detection-plan.md](spam-detection-plan.md) for the original
> plan.

## the registry

### did:plc:oql6ds5vnff4ugar6rruliwd — drivepatents.com

- **banned**: 2026-06-10
- **what**: automotive patent bot mirroring the patent database as
  `site.standard.document` records (~96k docs on its PDS)
- **evidence at ban time**: 25,111 docs indexed = **~44% of the corpus**;
  12/12 semantic results for EV-related queries were patents; standard
  registry boilerplate, no human authorship
- **cleanup**: tpuf 25,096 vectors (2026-06-10), turso 25,111 docs + 1 pub
  via paced purge (2026-06-11, 84 canary-gated batches, zero canary trips),
  replica rebuilt clean, atlas clean

### did:plc:2s32mlusc66sjb256aenynfc — destinationcharged.com

- **banned**: 2026-06-11
- **what**: EV/automotive site bulk-publishing auto-generated NHTSA
  vehicle-recall summaries ("Nissan Leaf recall (2016179)", one doc per
  recall record) as `site.standard.document`
- **evidence at ban time**: 1,262 docs (~4.7% of corpus) — quiet trickle
  since 2026-05-29, then an **819-doc burst in ~80 minutes** on 2026-06-11;
  semantic "tesla recall" returned 5/5 destinationcharged; 443 points in
  the atlas; hosted on a genuine bsky PDS (agaric.us-west.host.bsky.network),
  validly signed — caught because post-watermark turso growth ran 35x the
  day's organic rate
- **call**: same class as drivepatents (registry mirror, zero human
  authorship, owns a topic's semantic results). NHTSA's database is tens
  of thousands of recalls; trajectory said drivepatents-scale within days
- **cleanup**: this ban (see git history for execution)

### did:plc:llnmp5t7s3u4dzjqyhp76h62 — crownnote.com

- **banned**: 2026-06-29
- **what**: music-charts website (Kworb-style) auto-exporting its entire
  site to ATProto — one `site.standard.publication` per user/chart page
  (`/users/<name>`, `/charts/<name>`) and one `site.standard.document` per
  daily chart. Records carry `content: null` and a templated description
  ("A Crownnote album chart by <user> from <date>, with '<track>' at #1").
- **evidence at ban time**: 4,801 docs across 628 publications =
  **~12% of the 39,882-doc corpus**, all since 2026-06-14 (~300/day, bursts
  of ~950); `charts` keyword query returned 5/10 from this DID; **0
  recommends, 0 subscriptions** to any of its publications. Hosted on a
  genuine bsky PDS (stinkhorn.us-west.host.bsky.network), validly signed.
- **call**: same class as drivepatents/destinationcharged — registry mirror,
  zero human authorship, owns a topic's results, no curation signal lost.
  Surfaced as the 621-publication `/wrapped` latency outlier earlier the
  same session — the latency anomaly was the first symptom of the ramp.
- **cleanup**: this ban (see git history for execution)

### did:plc:6dis2hlap2lhwohabb2tniln — vccbusiness.bsky.social

- **banned**: 2026-09-04
- **what**: an SEO article farm on whitewind. every document is a long
  (~16–25k character) machine-written "how to" piece rotating between two
  product families — link-building software and virtual credit cards —
  with titles like "How to Use AI powered link building Without Losing
  Editorial Trust" and "How to buy VCC with crypto Without Disrupting Your
  Operating Cash Flow". the writing exists to rank for keywords, not
  because anyone had something to say.
- **evidence at ban time**: 124 documents since 2026-07-30 (~3.4/day,
  sustained); held 13 of the top 20 keyword results for `link` and 10 of 10
  hybrid results for `link building`; **0 subscribers** to the account's
  publications. surfaced by an agent session that spent six queries trying
  to find writing about the "Free Link" moderation situation through this
  noise (2026-09-04).
- **call**: generated, bulk relative to its topics, no curation signal. the
  classifier's heuristic scored it 0.05 against a 0.50 threshold — its
  signals (thin bodies, templated or digit titles, registry-scale volume)
  describe registry mirrors, and this is the opposite shape: fluent, long,
  varied titles, modest volume. nothing routed it to the model judge below
  the 5,000-document floor. see the labeler notes for the gate change.
- **cleanup**: this ban (see git history for execution)
