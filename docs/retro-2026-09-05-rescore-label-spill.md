# retro: a scoring rescore un-labeled every bulk-generated account

**date:** 2026-09-05, 02:52–03:20 UTC · **impact:** three previously labeled
accounts (about 10k documents, mostly one 9.6k-doc news mirror) were served
by search for roughly 25 minutes · **cause:** the classifier's scoring-version
rescore wiped `author_stats` and rebuilt it, and serving filters on that table
live.

## what happened

the velocity gate shipped with `SCORING_VERSION` 12. on boot the classifier
saw the version change and ran its rescore: negate every emitted label,
`DELETE FROM author_stats`, re-observe the corpus, then restore the verdicts it
had snapshotted. the snapshot only covered REJECTED and VETOED (the verdicts
the velocity change had just taught it to keep); LABELED rows were dropped by
design so a now-below-threshold author would not keep a stale label.

that design predates the model judge. every label today is a model, operator,
or registry verdict, none of which depends on the heuristic. dropping them
meant each labeled account had to re-qualify through the heuristic and the
judge, now under a ten-per-day review budget, and one of them (a curated news
mirror) re-evaluated to VETOED instead of LABELED because curation had arrived
after its original label.

`search.zig` and `documents.zig` drop rows whose author is `STATE_LABELED` in
`author_stats` at query time. between the delete and the re-judgement, those
authors were ordinary authors.

## detection

nate, reading the deploy report: 29 labels negated on a rescore is a spill
unless something proves otherwise. nothing did.

## mitigation

the label store (`/data/labels.db`) still held the history, so the set of
accounts whose latest `bulk-generated` label was a negation with an earlier
positive gave the true spill set: three accounts with replica presence and no
current label. each was re-emitted through `/admin/label` (which also writes
`STATE_LABELED`), and an author-filtered search for each returned zero rows
again. the other 26 negations were accounts that were already REJECTED or
VETOED (`labeled = 1` is set for every decided state, so the negate loop
retracted labels that had never been emitted) plus the four banned-registry
seeds, which re-emit at the end of every bootstrap.

## fix

`resetForRescore` replaces both wipe-then-restore paths (the scoring-version
rescore and the aggregation rebuild). decided rows are never deleted: their
counters are zeroed in place and `labeled`, `state`, `reason`, `site`, and
`review_attempts` stay, so `isLabeledDid` is true for every labeled account at
every instant of the rebuild and the evaluator never re-queues them. the
negate loop is gone. observing and pending rows are still dropped and must
qualify again from their real document count.

a regression test drives the reset and the re-observation against an
in-memory database and asserts the labeled row is labeled before, during, and
after.

## what we keep

- a verdict is a verdict. scoring changes re-derive *nominations*, never
  decisions. anything that unlabels an account is an operator action or a
  fresh model verdict, with the label store as the audit trail.
- serving state that is rebuilt must be rebuilt in place. a delete-then-fill
  of a table the request path reads is a window, however short the fill.
- "cleared N prior labels" is not a routine log line. the next deploy report
  that shows a mass negation is an incident until proven otherwise.
