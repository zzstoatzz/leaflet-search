# spam / bulk-mirror detection + a pub-search labeler

status: **BUILT** (labeler live 2026-06-30; this doc is the original plan and
drifts from the as-built system). as built: the label is `bulk-generated`(renamed from `bulk-mirror`), the classifier is autonomous
(`backend/src/ingest/classifier.zig`: heuristic pre-filter → majority-of-3
model votes → emit; the judge is OpenAI's gpt-5.6-luna since 2026-09-04, after co/core dropped the served model id twice; `REVIEW_PROVIDER=anthropic` switches to Claude Haiku, and any chat-completions endpoint still works through the openai provider; since 2026-09-04 a velocity gate — ≥2 docs/day over an author's date span at the 50-doc floor — sends fluent farms the title heuristic misses to the judge, under a `REVIEW_DAILY_BUDGET` of 10 authors per day), and the policy line evolved to **composed vs
generated** (see [exclusions.md](exclusions.md)). the public face is
[pub-search.waow.tech/labels](https://pub-search.waow.tech/labels). hard-drop
(acting on labels) is still pending the notify/appeal loop.

the design, in AT Protocol terms ([labels spec](https://atproto.com/specs/label)):
labeling **is** the primitive — there is no review queue. a labeler *emits*
signed annotations on accounts/records; consumers *subscribe* and independently
decide what to do. so this is one mechanism with a clean seam:

1. **emit** — a pub-search labeler watches the firehose, classifies publishers,
   and emits a descriptive `bulk-mirror` account label (a claim, not a verdict;
   negatable and reversible).
2. **act** — pub-search subscribes to that labeler (and others) and applies its
   *own* policy: exclude `bulk-mirror`-labeled accounts from the corpus. other
   readers can apply a different policy, or ignore the labeler entirely.

emitting a label does not silence anyone — it annotates. a mislabel is a
negation (`neg: true`), not an outage. that's why emission can be automated and
firehose-driven; the conservatism lives in the *emission threshold* and in each
consumer's policy, not in a human gate.

## the data forced the design

a per-DID sweep of the corpus (2026-06-29, `is_bridgyfed = 0`):

| DID stem | site | docs | shape | call |
|---|---|---:|---|---|
| sttgf… | coryd.dev | 941 | real blog + music writing | **keep** |
| 77mn3… | frankhecker.com | 753 | real blog backfill | **keep** |
| rkjxb… | den.dev | 313 | real dev blog | **keep** |
| icpcpp… | mikebifulco.com | 203 | real blog | **keep** |
| jgg4d… | chicagotransitalerts.app | 555 | "Train 807 Stopped" alert feed | **flag** |
| 5swhf… | prideraiser.org | 665 | bulk fundraiser-campaign records | **flag** |
| esvvys… | eligundry.com | 643 | "I felt rad" / "7 days ago I felt good" mood tracker | **flag** |
| gvudfu… | alekslessmann.de | 272 | empty titles | **flag** |

two lessons that kill the obvious detectors:

- **volume and burst are useless on their own.** the corpus's #1 author by
  volume (coryd.dev, 941) is a beloved human. den.dev / frankhecker backfilled
  hundreds of genuine posts in a single day. a "high count" or "single-day
  burst" rule bans real people.
- **absence of curation proves nothing — this corpus's recommend/subscription
  graph is sparse.** coryd.dev: 0 recommends, 1 subscription. frankhecker,
  den.dev, mikebifulco: 0/0 — *identical to the transit bot.* so the policy's
  "no curation signal lost" prong can only be used as a **veto** (if it HAS
  curation, never flag), never as positive evidence of spam.

what's left as the real discriminator is **authorship / content shape** —
templated, machine-generated registry/feed output vs human long-form writing.
that's the judgment the labeler's emission logic has to make.

## part 1 — what the labeler emits on

the labeler classifies publishers and emits `bulk-mirror`. classification
signals, in order of weight:

1. **content / authorship** (the decisive axis)
   - title-template similarity: shingle each author's titles; "Train #N
     Delayed", "I felt X" collapse to one template → high self-similarity is the
     tell.
   - null / templated-content fraction, avg content length.
2. **model classification**: sample N docs, ask "human-authored long-form, or
   templated registry/feed output?" → verdict + rationale, attached to the
   label's rationale for transparency.
3. **volume / burst**: docs, docs/day, recency concentration — corroborating,
   not decisive (real blogs backfill in bursts — see the keep-list above).
4. **curation veto** (negative only): any recommends or subscriptions from
   distinct real accounts → never emit.

error handling is the protocol's, not a queue's: tune the emission threshold for
precision, and **negate** (`neg: true`) any label later judged wrong. a
mislabeled account is annotated, not deleted — consumers that disagree just
don't act on it.

### known bulk-mirror accounts as of 2026-06-29

already banned (these become the labeler's seed emissions — see
[exclusions.md](exclusions.md) registry): drivepatents.com, destinationcharged.com,
crownnote.com.

newly surfaced this session (strong `bulk-mirror` candidates):

- **chicagotransitalerts.app** (`did:plc:jgg4dtdflzzemyvnybucnzdw`) — transit
  alert feed, 555 docs in one day.
- **prideraiser.org** (`did:plc:5swhfspkrynnbidlkrkch3lh`) — bulk campaign
  records, 665 in one day.
- **eligundry.com** (`did:plc:esvvys4mvoclui34shb23l5w`) — "I felt X" mood-tracker
  auto-entries, 643.
- **alekslessmann.de** (`did:plc:gvudfu6dhl5cbpinhofa325s`) — empty titles, 272.

do NOT emit on the keep-list (coryd.dev, frankhecker.com, den.dev,
mikebifulco.com) — real humans the volume/burst signals alias onto.

surfaced by the classifier (`scripts/classify-bulk-mirror`), not by hand —
evidence the signals generalize:

- **atlantatransitalerts.app** (`did:plc:wkz6gknpvc44kijydpta23ez`) — sibling of
  chicagotransitalerts; "Green Line delays", "Route 114 detour".
- **thefestivusproject.com** (`did:plc:4z33k5fjzw2ew3u373pg7ku5`) — Seinfeld
  episode mirror; "S07E14: The Cadillac", "S05E08: The Barber".
- **tcpinball.org** (`did:plc:uyph4xrcc6m3zouwnff4pifu`) — pinball league
  results; "April 6 Results", "May 16 Results".
- `did:plc:vd3vzujxkxsthkswrc2zzupm` (score 0.377, borderline) — court-transcript
  mirror; the kind of case the model pass should adjudicate, not the heuristic.

### the classifier (phase 1, built 2026-06-29)

what's built is the **decision function** — "given a DID's records, is this a
bulk-mirror?" — and an offline harness to prove it. features per DID:
title-template self-similarity (scaffold coverage + normalized-distinct ratio),
empty/digit-title fraction, content thinness, volume (corroborating only),
curation veto. composite score; emit threshold 0.35.

`scripts/classify-bulk-mirror` — read-only `uv run` script that feeds the
decision function from turso. it reads turso for two reasons only: (1) to
validate the scoring against the known set offline (fast iteration), and (2) the
one-time **backfill** — ~40k docs were indexed before the labeler exists, so
something must score the existing corpus once to emit labels on DIDs already
here. validates: all 4 FLAG ≥ 0.35, all 4 KEEP below, **+0.283 margin (PASS)**;
coryd.dev → 0.0 (vetoed by its one subscription).

at steady state there is **no batch job and no turso**: the labeler is a
firehose listener (next section).

## part 2 — a first-class pub-search labeler

we will run our own AT Protocol labeler (not a fork of
[labelz](https://tangled.org/zzstoatzz.io/labelz) — labelz is the reference for
how: jetstream/`zat` → secp256k1-sign → serve `queryLabels` + `subscribeLabels`,
deployed with a registered DID identity). ours differs in two ways:

- **account-level labels.** subject is the publisher's **DID** (uri = the did,
  no cid), not a record URI. labelz labels post records; we label accounts — one
  label covers all of a DID's records.
- **aggregate decision, not per-record keyword.** a single record can't be
  judged ("S07E14: The Cadillac" is innocent alone); only the pattern across a
  DID's records reveals the mirror. so emission is classifier-driven, not a
  keyword match.

### how the labeler decides (firehose listener)

steady state is pure firehose — same loop as labelz (jetstream → store), but the
store maintains a **rolling per-DID aggregate** (record count, title shingles,
content stats) instead of just matching keywords. when a DID accumulates enough
records to judge, the labeler runs the part-1 decision function over its
aggregate and, if the score crosses threshold, emits a `bulk-mirror` account
label (and negates if a later judgment flips). no turso, no cron.

the only non-firehose pass is the **one-time backfill**: DIDs indexed before the
labeler existed are scored once from the corpus (`scripts/classify-bulk-mirror`)
and seeded. after that, the firehose carries it.

### label vocabulary (self-declared in the labeler-service record)

- `bulk-mirror` — descriptive: machine-generated registry/feed mirror, no human
  authorship (drivepatents / NHTSA / transit-alert class). severity: inform.

a *descriptive* value, not a behavioral one (`!takedown`/`!warn`) — per the
spec, the labeler states what a thing **is**; each consumer decides what to
**do**. one value to start; resist a taxonomy until we need it.

### how pub-search acts on labels (the consumer side)

the ban source moves from `banned-dids.txt` (compiled in, needs a deploy) to the
labeler subscription: the backend ingests `bulk-mirror` account labels (its own
labeler + any others it trusts) via `subscribeLabels` into a `banned_dids`
table, and the four enforcement layers in [exclusions.md](exclusions.md) read
that table. a ban becomes a **data change** (a label arrives) instead of an edit
+ redeploy of every service — and a *negation* lifts it the same way. this is
exactly the migration exclusions.md anticipated.

exclusion is pub-search's chosen *action*; the label itself is neutral signal.
`banned-dids.txt` stays as the bootstrap / break-glass seed until the
subscription is proven.

### ecosystem leverage (the reason to publish, not keep it internal)

register the labeler two ways so the curation is a public good, not just ours:

- **standard-reader** already defines `app.standard-reader.labeler.service` and
  mirrors any registered labeler's `queryLabels` into its read model every 2
  min, letting its readers subscribe + filter. registering there means
  standard.site readers inherit our bulk-mirror calls.
  - open integration question: standard-reader keys `document_labels` by
    document URI; account-level labels (uri = did) need a mapping from author →
    their docs on their side. flag this with them.
- **Bluesky** `app.bsky.labeler.service` declaration → the labeler shows up in
  bsky's own moderation UI; account labels apply natively there.

### identity setup (one-time, mirrors labelz)

new DID + PDS account + labeler declaration record (labelz did this via pds.js on
cloudflare workers + goat for PLC ops; secp256k1 label-signing key separate from
the P-256 repo key). budget for this as discrete setup work.

## phased rollout

1. **decision function** ✓ (2026-06-29) — the part-1 scoring, validated offline
   against the known set (`scripts/classify-bulk-mirror`): fires on the 4
   surfaced accounts, silent on the keep-list, +0.283 margin.
2. **labeler, emitting** — stand up the labeler identity; firehose listener with
   a rolling per-DID aggregate runs the decision function and emits `bulk-mirror`
   account labels live. backfill the existing corpus once from the script.
3. **consume** — backend subscribes to its own labeler, mirrors labels into
   `banned_dids`, the enforcement layers read it; `banned-dids.txt` demoted to
   seed/break-glass.
4. **publish** — register with standard-reader + bsky so other readers can
   subscribe; coordinate the account-label→document mapping with standard-reader.
5. **federate (future)** — subscribe to other community standard.site labelers
   so the signal is collaborative, not unilateral.

## open questions

- model: which model + cost for the classification pass (corpus ~40k docs;
  candidates are few — sampling keeps it cheap).
- emission threshold: how high to set precision before auto-emitting vs leaving
  borderline accounts unlabeled (negation is the safety net either way).
- account-label semantics in standard-reader (the uri=did → documents mapping).
