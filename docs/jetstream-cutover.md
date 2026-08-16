# jetstream ingest cutover

Replace the `leaflet-search-ingester` fly app (relay firehose → verify →
`/channel` websocket) with direct Jetstream V2 consumption in the backend,
at functional parity, then delete the app.

## why this preserves verification

A Jetstream consumer cannot re-verify commits itself — the commit and MST
blocks are not on the wire. But every host in our list runs full Sync 1.1
signature/MST verification at *its own* ingest and withholds unverified data:

- **stream.waow.tech** (primary): `verify.zig` — "no event is served or
  archived unverified when the DID resolves"; failed signatures dropped,
  chain gaps trigger authenticated whole-repo repair.
- **Bluesky's hosted Jetstream V2 instances** (fallback): offline Sync 1.1
  signature verification in the production live path; `#sync` divergence
  resyncs through the sync verifier (bluesky-social/jetstream
  `specs/oracle.md`, `docs/README.md`).

Verification moves one hop upstream; the trust step is operator trust of
verifying implementations. Jetstream **v1** instances do NOT verify — never
add one as a fallback host.

## how it works (`backend/src/ingest/jetstream.zig`)

- `INGEST_SOURCE=jetstream` on the `app` process group selects the consumer
  (default `channel` = the old fly-app path, kept as rollback).
- Subscribes `/subscribe` with `wantedCollections` for our 9 collections;
  events normalize into the same `dispatchRecord` path as /channel frames,
  so both sources index identically by construction.
- At-least-once: synchronous processing on the read loop; the durable cursor
  (`/data/jetstream-cursor`, `time_us`) is persisted only after dispatch,
  rewound 5s on every reconnect; redelivery is absorbed by idempotent
  upserts. First boot with no cursor file seeds from wall clock − 60s so the
  deploy gap replays. No ack protocol, no outbox, no shedding.
- Policy parity in-process: banned DIDs via `policy.isBanned`; bridgy fed
  via a cached DID→PDS check (`ingester.resolvePds` + `isBridgyPds`).
  Resolution failure admits the event (bridgy repos are did:plc and resolve
  reliably; the reconciler re-checks PDS hosting later).
- Staleness watchdog at 15 min (`JETSTREAM_STALE_SECS`) — our collections
  are quiet, a firehose-style 90s watchdog would false-trigger.
- Env: `JETSTREAM_HOSTS` (csv override), `JETSTREAM_CURSOR_PATH`.

## rollout

1. Deploy inert (default `channel`), verify healthy.
2. Set `INGEST_SOURCE=jetstream` in `backend/fly.toml` [env], deploy.
3. Soak: compare `ingest.index_record` rate and `ingest.dropped` reasons in
   logfire against the /channel baseline; spot-check fresh docs land in
   search.
4. Rollback at any point = revert the env var.
5. When clean: `fly apps destroy leaflet-search-ingester`, delete
   `ingester/`, prune the ingester deploy docs.
