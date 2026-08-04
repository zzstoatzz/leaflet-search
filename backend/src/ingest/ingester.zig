const std = @import("std");
const mem = std.mem;
const http = std.http;
const json = std.json;
const Allocator = mem.Allocator;
const websocket = @import("websocket");
const zat = @import("zat");
const logfire = @import("logfire");
const poolio = @import("poolio");
const indexer = @import("indexer.zig");
const extractor = @import("extractor.zig");
const classifier = @import("classifier.zig");
const Io = std.Io;
const tpuf = @import("../tpuf.zig");

// leaflet-specific collections
const LEAFLET_DOCUMENT = "pub.leaflet.document";
const LEAFLET_PUBLICATION = "pub.leaflet.publication";

// standard.site collections (cross-platform)
const STANDARD_DOCUMENT = "site.standard.document";
const STANDARD_PUBLICATION = "site.standard.publication";
const STANDARD_RECOMMEND = "site.standard.graph.recommend";

// standard.site's publication-subscription graph record. No leaflet variant
// is ingested by design — subscriptions are a cross-platform standard.site
// concept (see /subscribed). Field `publication` is the subscribed at-uri.
const STANDARD_SUBSCRIPTION = "site.standard.graph.subscription";

// leaflet's parallel "interactions" lexicon. Most leaflet UI writes
// recommends here (.subject), not to the cross-platform standard one.
const LEAFLET_RECOMMEND = "pub.leaflet.interactions.recommend";

// whitewind blog entries
const WHITEWIND_ENTRY = "com.whtwnd.blog.entry";

// pckt dual-writes each publication under both site.standard.publication (which
// we index) and this native sidecar at the same rkey. The sidecar's
// `publication.uri` strongRef points back at the site.standard.publication. We
// use it to platform-tag pckt blogs on custom domains, whose host lacks
// `pckt.blog` so base_path detection alone can't recognize them.
const PCKT_PUBLICATION = "blog.pckt.publication";

fn isDocumentCollection(collection: []const u8) bool {
    return mem.eql(u8, collection, LEAFLET_DOCUMENT) or
        mem.eql(u8, collection, STANDARD_DOCUMENT) or
        mem.eql(u8, collection, WHITEWIND_ENTRY);
}

fn isPublicationCollection(collection: []const u8) bool {
    return mem.eql(u8, collection, LEAFLET_PUBLICATION) or
        mem.eql(u8, collection, STANDARD_PUBLICATION);
}

fn isRecommendCollection(collection: []const u8) bool {
    return mem.eql(u8, collection, STANDARD_RECOMMEND) or
        mem.eql(u8, collection, LEAFLET_RECOMMEND);
}

fn isSubscriptionCollection(collection: []const u8) bool {
    return mem.eql(u8, collection, STANDARD_SUBSCRIPTION);
}

/// Lexicons disagree on the field name for the recommended doc:
///   site.standard.graph.recommend -> .document
///   pub.leaflet.interactions.recommend -> .subject
/// Normalize at the ingest boundary so downstream code reads one column.
fn recommendDocFieldName(collection: []const u8) []const u8 {
    return if (mem.eql(u8, collection, LEAFLET_RECOMMEND)) "subject" else "document";
}

fn getIngesterHost() []const u8 {
    return if (std.c.getenv("INGESTER_HOST")) |p| std.mem.span(p) else "leaflet-search-ingester.fly.dev";
}

fn getIngesterPort() u16 {
    const port_str = if (std.c.getenv("INGESTER_PORT")) |p| std.mem.span(p) else "443";
    return std.fmt.parseInt(u16, port_str, 10) catch 443;
}

fn useTls() bool {
    return getIngesterPort() == 443;
}

/// Bounded queue for decoupling websocket readLoop from turso writes.
/// Processing (and the post-commit ACK) happens in the worker thread. If the
/// queue is full (turso is slow), the OLDEST queued frame is shed — it was
/// never ACKed, so the ingester's outbox retransmits it (~60s). Sized to
/// absorb archive-backfill bursts (a publisher re-putting tens of thousands
/// of records in minutes) — frames are small; the real defense is write-path
/// throughput (batched pipeline requests in indexer.zig), this buffer is
/// insurance.
const QUEUE_CAPACITY = 4096;

const IngesterCtx = struct {
    allocator: Allocator,
    handler: ?*Handler = null,
    drop_count: std.atomic.Value(usize) = std.atomic.Value(usize).init(0),
    no_id_count: std.atomic.Value(usize) = std.atomic.Value(usize).init(0),

    fn process(self: *IngesterCtx, _: Io, frame: []u8) void {
        defer self.allocator.free(frame);
        processMessage(self.allocator, frame) catch |err| {
            // NO ack on failure: the ingester's outbox retransmits the frame
            // (~60s), so a transient turso error is retried instead of lost.
            logfire.err("message processing error: {}", .{err});
            return;
        };
        // ack only after the write batch committed — the ingester releases
        // the frame from its outbox on this ack (at-least-once, both sides).
        // this worker is the ONLY ack writer (workers=1); bump workers and
        // this needs a write lock around the websocket client.
        const msg_id = extractMessageId(self.allocator, frame) orelse {
            const n = self.no_id_count.fetchAdd(1, .monotonic) + 1;
            if (n <= 5) {
                logfire.warn("ingester: message has no id, first {d} bytes: {s}", .{ @min(frame.len, 100), frame[0..@min(frame.len, 100)] });
            }
            return;
        };
        if (self.handler) |h| h.sendAck(msg_id);
    }

    fn onDrop(self: *IngesterCtx, frame: []u8) void {
        defer self.allocator.free(frame);
        // shed frames were never acked, so the ingester's outbox retransmit
        // redelivers them — sheds under burst now cost latency, not data.
        // still loud: sustained shedding means the write path can't keep up.
        const n = self.drop_count.fetchAdd(1, .monotonic) + 1;
        if (n <= 10 or n % 100 == 0) {
            logfire.warn("ingester: shed unprocessed frame under backpressure ({d} total, outbox will retransmit)", .{n});
        }
    }
};

const IngesterPool = poolio.Pool([]u8, IngesterCtx);

pub fn consumer(allocator: Allocator, io: Io) void {
    var backoff: u64 = 1;
    const max_backoff: u64 = 30;

    while (true) {
        const connected = connect(allocator, io);
        if (connected) |_| {
            // connection succeeded then closed - reset backoff
            backoff = 1;
            logfire.info("ingester connection closed, reconnecting immediately", .{});
        } else |err| {
            // connection failed - backoff
            logfire.warn("ingester error: {}, reconnecting in {d}s", .{ err, backoff });
            io.sleep(Io.Duration.fromSeconds(@intCast(backoff)), .awake) catch {};
            backoff = @min(backoff * 2, max_backoff);
        }
    }
}

const Handler = struct {
    allocator: Allocator,
    io: Io,
    client: *websocket.Client,
    pool: *IngesterPool,
    // atomic: read by the staleness watchdog thread
    msg_count: std.atomic.Value(usize) = std.atomic.Value(usize).init(0),
    ack_count: usize = 0,
    ack_buf: [64]u8 = undefined,

    pub fn serverMessage(self: *Handler, data: []const u8) !void {
        const count = self.msg_count.fetchAdd(1, .monotonic) + 1;
        if (count % 1000 == 0) {
            const c = self.pool.counters(self.io);
            logfire.info("ingester: recv {d}, acks {d}, accepted {d}, processed {d}, dropped {d}, queued {d}", .{
                count, self.ack_count, c.accepted, c.processed, c.dropped, c.queued,
            });
        }

        // NO ack here: the worker acks after its turso batch commits (and owns
        // the no-id diagnostic), so the ingester's outbox holds the frame until
        // it's durably processed — a shed frame gets retransmitted instead of
        // silently lost. no JSON parse on the read thread at all.
        // dupe message data (websocket reuses the buffer) and offer to processing pool.
        // drop_oldest shedding means full pool evicts the oldest queued frame, not the new one.
        const data_copy = self.allocator.dupe(u8, data) catch |err| {
            logfire.err("ingester: failed to dupe message: {}", .{err});
            return;
        };
        _ = self.pool.offer(self.io, data_copy);
    }

    fn sendAck(self: *Handler, msg_id: i64) void {
        const ack_json = std.fmt.bufPrint(&self.ack_buf, "{{\"type\":\"ack\",\"id\":{d}}}", .{msg_id}) catch |err| {
            logfire.err("ingester: ACK format error: {}", .{err});
            return;
        };
        // log before write — websocket.zig masks the buffer in-place
        if (self.ack_count < 3) {
            logfire.info("ingester: sending ACK for id={d}", .{msg_id});
        }
        self.client.write(@constCast(ack_json)) catch |err| {
            logfire.err("ingester: failed to send ACK: {}", .{err});
            return;
        };
        self.ack_count += 1;
    }

    pub fn close(_: *Handler) void {}
};

/// Detects half-open channel sockets. The ingester restarting (deploy, crash)
/// leaves an idle peer with no RST — and since we only write ACKs in response
/// to frames, readLoop would block forever and ingestion silently stops
/// (observed at cutover, 2026-06-09). The ingester heartbeats every 20s, so
/// "no frames for ~90s" reliably means the connection is dead: force-close it
/// and let the consumer loop re-dial.
const Watchdog = struct {
    io: Io,
    client: *websocket.Client,
    handler: *Handler,
    stop: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),

    const TICK_SECONDS = 1;
    const STALE_TICKS = 90;

    fn run(self: *Watchdog) void {
        var last: usize = 0;
        var stale: u32 = 0;
        while (!self.stop.load(.acquire)) {
            self.io.sleep(Io.Duration.fromSeconds(TICK_SECONDS), .awake) catch {};
            const n = self.handler.msg_count.load(.monotonic);
            if (n != last) {
                last = n;
                stale = 0;
                continue;
            }
            stale += 1;
            if (stale >= STALE_TICKS) {
                logfire.warn("ingester: no frames for {d}s, closing connection to force reconnect", .{STALE_TICKS * TICK_SECONDS});
                self.client.close(.{}) catch {};
                return;
            }
        }
    }
};

fn extractMessageId(allocator: Allocator, payload: []const u8) ?i64 {
    const parsed = json.parseFromSlice(json.Value, allocator, payload, .{}) catch return null;
    defer parsed.deinit();
    return zat.json.getInt(parsed.value, "id");
}

fn connect(allocator: Allocator, io: Io) !void {
    const host = getIngesterHost();
    const port = getIngesterPort();
    const tls = useTls();
    const path = "/channel";

    logfire.info("connecting to {s}://{s}:{d}{s}", .{ if (tls) "wss" else "ws", host, port, path });

    var client = websocket.Client.init(io, allocator, .{
        .host = host,
        .port = port,
        .tls = tls,
        // must cover the channel server's max_message_size (5MB,
        // ingester/src/channel.zig) or oversized doc frames error the read loop
        .max_size = 5 * 1024 * 1024,
    }) catch |err| {
        logfire.err("websocket client init failed: {}", .{err});
        return err;
    };
    defer client.deinit();

    var host_header_buf: [256]u8 = undefined;
    const host_header = std.fmt.bufPrint(&host_header_buf, "Host: {s}\r\n", .{host}) catch host;

    client.handshake(path, .{ .headers = host_header }) catch |err| {
        logfire.err("websocket handshake failed: {}", .{err});
        return err;
    };

    logfire.info("ingester connected", .{});

    // processing pool: decouples readLoop from turso writes so a slow/hung turso
    // request never blocks ACKs. drop_oldest shedding keeps the freshest data
    // when turso lags (older frames have already been ACK'd to the ingester outbox).
    var ctx = IngesterCtx{ .allocator = allocator };
    var pool = IngesterPool.init(allocator, .{
        .queue_capacity = QUEUE_CAPACITY,
        .workers = 1,
        .ctx = &ctx,
        .process = IngesterCtx.process,
        .on_drop = IngesterCtx.onDrop,
        .shedding = .drop_oldest,
    }) catch |err| {
        logfire.err("ingester: failed to init pool: {}", .{err});
        return err;
    };
    defer pool.deinit();

    pool.start(io) catch |err| {
        logfire.err("ingester: failed to start pool: {}", .{err});
        return err;
    };
    defer pool.shutdown(io);

    var handler = Handler{ .allocator = allocator, .io = io, .client = &client, .pool = &pool };
    // the worker acks through the handler after each commit; safe to set
    // post-start because no frame arrives until readLoop below begins, and
    // pool.shutdown (deferred) stops workers before handler leaves scope.
    ctx.handler = &handler;

    var watchdog = Watchdog{ .io = io, .client = &client, .handler = &handler };
    const wd_thread = std.Thread.spawn(.{}, Watchdog.run, .{&watchdog}) catch |err| {
        logfire.err("ingester: failed to spawn watchdog: {}", .{err});
        return err;
    };
    defer {
        // joined, not detached — the watchdog borrows stack locals (client,
        // handler) that die when this function returns. 1s tick bounds the wait.
        watchdog.stop.store(true, .release);
        wd_thread.join();
    }

    client.readLoop(&handler) catch |err| {
        logfire.err("websocket read loop error: {}", .{err});
        return err;
    };
}

/// ingester record envelope - extracted via zat.json.extractAt
const IngesterRecord = struct {
    collection: []const u8,
    action: []const u8, // "create", "update", "delete"
    did: []const u8,
    rkey: []const u8,
    cid: ?[]const u8 = null,

    pub fn isCreate(self: IngesterRecord) bool {
        return mem.eql(u8, self.action, "create");
    }
    pub fn isUpdate(self: IngesterRecord) bool {
        return mem.eql(u8, self.action, "update");
    }
    pub fn isDelete(self: IngesterRecord) bool {
        return mem.eql(u8, self.action, "delete");
    }
};

/// Leaflet publication fields
const LeafletPublication = struct {
    name: []const u8,
    description: ?[]const u8 = null,
    base_path: ?[]const u8 = null,
};

fn processMessage(allocator: Allocator, payload: []const u8) !void {
    const parsed = json.parseFromSlice(json.Value, allocator, payload, .{}) catch {
        logfire.err("ingester: JSON parse failed, first 100 bytes: {s}", .{payload[0..@min(payload.len, 100)]});
        return;
    };
    defer parsed.deinit();

    // check message type
    const msg_type = zat.json.getString(parsed.value, "type") orelse {
        logfire.warn("ingester: no type field in message", .{});
        return;
    };

    if (!mem.eql(u8, msg_type, "record")) return;

    // extract record envelope (extractAt ignores extra fields like live, rev, cid)
    const rec = zat.json.extractAt(IngesterRecord, allocator, parsed.value, .{"record"}) catch |err| {
        logfire.warn("ingester: failed to extract record: {}", .{err});
        return;
    };

    // validate DID
    const did = zat.Did.parse(rec.did) orelse {
        logfire.span("ingest.dropped", .{ .reason = "invalid_did", .collection = rec.collection }).end();
        return;
    };

    // note: bridgy fed content is no longer filtered — the indexer's HTTP site URL
    // fallback resolves base_path from the publication's "site" field, so we can
    // build working links for bridged standard.site documents.

    // build AT-URI string (no allocation - uses stack buffer)
    var uri_buf: [256]u8 = undefined;
    const uri = zat.AtUri.format(&uri_buf, did.raw, rec.collection, rec.rkey) orelse {
        logfire.span("ingest.dropped", .{ .reason = "uri_too_long", .collection = rec.collection }).end();
        return;
    };

    // span for the actual indexing work
    const span = logfire.span("ingest.index_record", .{});
    defer span.end();

    if (rec.isCreate() or rec.isUpdate()) {
        const inner_record = zat.json.getObject(parsed.value, "record.record") orelse {
            logfire.span("ingest.dropped", .{ .reason = "no_inner_record", .collection = rec.collection, .uri = uri }).end();
            return;
        };

        if (isDocumentCollection(rec.collection)) {
            // skip author-only whitewind entries (public + url are both publicly accessible)
            if (mem.eql(u8, rec.collection, WHITEWIND_ENTRY)) {
                const record_val: json.Value = .{ .object = inner_record };
                const visibility = zat.json.getString(record_val, "visibility") orelse "public";
                if (mem.eql(u8, visibility, "author")) {
                    logfire.span("ingest.dropped", .{ .reason = "author_only", .collection = rec.collection, .uri = uri }).end();
                    return;
                }
            }

            processDocument(allocator, uri, did.raw, rec.rkey, inner_record, rec.collection, rec.cid) catch |err| {
                logfire.err("document processing error: {}", .{err});
            };
        } else if (isPublicationCollection(rec.collection)) {
            processPublication(allocator, uri, did.raw, rec.rkey, inner_record) catch |err| {
                logfire.err("publication processing error: {}", .{err});
            };
        } else if (mem.eql(u8, rec.collection, PCKT_PUBLICATION)) {
            processPcktPublication(inner_record) catch |err| {
                logfire.err("pckt publication processing error: {}", .{err});
            };
        } else if (isRecommendCollection(rec.collection)) {
            processRecommend(uri, did.raw, rec.rkey, rec.collection, inner_record) catch |err| {
                logfire.err("recommend processing error: {}", .{err});
            };
        } else if (isSubscriptionCollection(rec.collection)) {
            processSubscription(uri, did.raw, rec.rkey, inner_record) catch |err| {
                logfire.err("subscription processing error: {}", .{err});
            };
        }
    } else if (rec.isDelete()) {
        if (isDocumentCollection(rec.collection)) {
            indexer.deleteDocument(uri);
            // also clean up turbopuffer vector (deleteDocument only handles turso)
            const hashed = tpuf.hashId(uri);
            tpuf.delete(allocator, &.{&hashed}) catch {};
        } else if (isPublicationCollection(rec.collection)) {
            indexer.deletePublication(uri);
        } else if (isRecommendCollection(rec.collection)) {
            indexer.deleteRecommend(uri);
        } else if (isSubscriptionCollection(rec.collection)) {
            indexer.deleteSubscription(uri);
        }
    }
}

fn processDocument(allocator: Allocator, uri: []const u8, did: []const u8, rkey: []const u8, record: json.ObjectMap, collection: []const u8, source_cid: ?[]const u8) !void {
    var doc = extractor.extractDocument(allocator, record, collection) catch |err| {
        if (err == error.MissingTitle) {
            logfire.span("ingest.dropped", .{ .reason = "missing_title", .collection = collection, .uri = uri }).end();
        } else if (err == error.NoContent) {
            logfire.span("ingest.dropped", .{ .reason = "no_content", .collection = collection, .uri = uri }).end();
        } else {
            logfire.span("ingest.dropped", .{ .reason = "extraction_error", .collection = collection, .uri = uri }).end();
        }
        return;
    };
    defer doc.deinit();

    try indexer.insertDocument(
        uri,
        did,
        rkey,
        doc.title,
        doc.content,
        doc.created_at,
        doc.publication_uri,
        doc.tags,
        doc.platformName(),
        doc.source_collection,
        doc.path,
        doc.content_type,
        doc.cover_image,
        source_cid,
    );
    logfire.counter("ingest.documents_indexed", 1);

    // feed the autonomous labeler: it keeps a rolling per-DID aggregate and
    // emits bulk-mirror on its own when an author crosses the threshold. Local
    // sqlite only — never blocks the firehose.
    classifier.observe(did, doc.title, doc.content);
}

fn processPublication(_: Allocator, uri: []const u8, did: []const u8, rkey: []const u8, record: json.ObjectMap) !void {
    const record_val: json.Value = .{ .object = record };

    // extract required field
    const name = zat.json.getString(record_val, "name") orelse {
        logfire.span("ingest.dropped", .{ .reason = "pub_missing_name", .uri = uri }).end();
        return;
    };
    const description = zat.json.getString(record_val, "description");

    // base_path: try leaflet's "base_path", then site.standard's "url"
    // url is full URL like "https://devlog.pckt.blog", we need just the host
    const base_path = zat.json.getString(record_val, "base_path") orelse
        stripUrlScheme(zat.json.getString(record_val, "url"));

    // skip .test domains (dev/staging data)
    if (base_path) |bp| {
        if (mem.endsWith(u8, bp, ".test")) {
            logfire.span("ingest.dropped", .{ .reason = "test_domain", .uri = uri }).end();
            return;
        }
    }

    // site.standard.publication preferences.showInDiscover: absent means
    // discoverable (matches ecosystem default — most records omit it).
    const show_in_discover = zat.json.getBool(record_val, "preferences.showInDiscover") orelse true;

    try indexer.insertPublication(uri, did, rkey, name, description, base_path, show_in_discover);
    logfire.counter("ingest.publications_indexed", 1);
}

/// blog.pckt.publication sidecar: tag the referenced site.standard.publication
/// (and its already-indexed docs) as platform pckt. New docs are then tagged at
/// index time via the publication's platform (see indexer base_path lookup).
fn processPcktPublication(record: json.ObjectMap) !void {
    const record_val: json.Value = .{ .object = record };
    const target = zat.json.getString(record_val, "publication.uri") orelse {
        logfire.span("ingest.dropped", .{ .reason = "pckt_pub_missing_ref" }).end();
        return;
    };
    try indexer.markPublicationPckt(target);
    logfire.counter("ingest.pckt_publications_tagged", 1);
}

fn processRecommend(uri: []const u8, did: []const u8, rkey: []const u8, collection: []const u8, record: json.ObjectMap) !void {
    const record_val: json.Value = .{ .object = record };

    const field = recommendDocFieldName(collection);
    const document_uri = zat.json.getString(record_val, field) orelse {
        logfire.span("ingest.dropped", .{ .reason = "recommend_missing_document", .uri = uri, .collection = collection }).end();
        return;
    };
    const created_at = zat.json.getString(record_val, "createdAt");

    try indexer.insertRecommend(uri, did, rkey, document_uri, created_at);
    logfire.counter("ingest.recommends_indexed", 1);
}

fn processSubscription(uri: []const u8, did: []const u8, rkey: []const u8, record: json.ObjectMap) !void {
    const record_val: json.Value = .{ .object = record };

    const publication_uri = zat.json.getString(record_val, "publication") orelse {
        logfire.span("ingest.dropped", .{ .reason = "subscription_missing_publication", .uri = uri }).end();
        return;
    };
    // createdAt is optional on this lexicon and usually absent. Fall back to
    // the rkey TID time (the record's true creation time) rather than letting
    // it default to indexed_at downstream — otherwise /subscribed's time filter
    // collapses for any record ingested long after it was created (backfill).
    var tid_buf: [24]u8 = undefined;
    const created_at: ?[]const u8 = zat.json.getString(record_val, "createdAt") orelse
        tidToIso(&tid_buf, rkey);

    try indexer.insertSubscription(uri, did, rkey, publication_uri, created_at);
    logfire.counter("ingest.subscriptions_indexed", 1);
}

/// Decode an atproto TID rkey into an ISO-8601 UTC timestamp. Returns null if
/// the rkey isn't a valid TID. Used as the createdAt fallback for records that
/// omit it (the rkey itself encodes creation time).
fn tidToIso(buf: []u8, rkey: []const u8) ?[]const u8 {
    const tid = zat.Tid.parse(rkey) orelse return null;
    const secs: u64 = tid.timestamp() / std.time.us_per_s;
    const es = std.time.epoch.EpochSeconds{ .secs = secs };
    const yd = es.getEpochDay().calculateYearDay();
    const md = yd.calculateMonthDay();
    const ds = es.getDaySeconds();
    return std.fmt.bufPrint(buf, "{d:0>4}-{d:0>2}-{d:0>2}T{d:0>2}:{d:0>2}:{d:0>2}Z", .{
        yd.year,              md.month.numeric(),      md.day_index + 1,
        ds.getHoursIntoDay(), ds.getMinutesIntoHour(), ds.getSecondsIntoMinute(),
    }) catch null;
}

// ---------------------------------------------------------------------------
// Targeted backfill: pull every record of our collections for a single repo
// directly from its PDS and run it through the SAME extract+index path the
// firehose uses. Lets us ingest a specific author on demand without waiting
// on the ingester's serial resync queue (which can sit behind a whole-network sweep).
// ---------------------------------------------------------------------------

const BACKFILL_COLLECTIONS = [_][]const u8{
    LEAFLET_DOCUMENT,    STANDARD_DOCUMENT,     WHITEWIND_ENTRY,
    LEAFLET_PUBLICATION, STANDARD_PUBLICATION,  STANDARD_RECOMMEND,
    LEAFLET_RECOMMEND,   STANDARD_SUBSCRIPTION,
};

pub const BackfillCounts = struct {
    documents: usize = 0,
    publications: usize = 0,
    recommends: usize = 0,
    subscriptions: usize = 0,
    skipped: usize = 0,
};

pub const TargetedAction = enum { upsert, delete };
pub const TargetedOutcome = enum {
    upserted,
    deleted,
    source_changed,
    source_exists,
    source_absent,
};

pub const TargetedResult = struct {
    outcome: TargetedOutcome,
    source_cid: ?[]u8 = null,
};

const RecordFetch = union(enum) {
    found: []u8,
    missing,
};

fn fetchRecord(
    allocator: Allocator,
    io: Io,
    pds: []const u8,
    did: []const u8,
    collection: []const u8,
    rkey: []const u8,
) !RecordFetch {
    if (!mem.startsWith(u8, pds, "https://")) return error.InsecurePds;

    var url_buf: [768]u8 = undefined;
    const url = std.fmt.bufPrint(
        &url_buf,
        "{s}/xrpc/com.atproto.repo.getRecord?repo={s}&collection={s}&rkey={s}",
        .{ pds, did, collection, rkey },
    ) catch return error.UrlTooLong;

    var http_client: http.Client = .{ .allocator = allocator, .io = io };
    defer http_client.deinit();
    var sink: std.Io.Writer.Allocating = .init(allocator);
    defer sink.deinit();

    const response = try http_client.fetch(.{
        .location = .{ .url = url },
        .method = .GET,
        .response_writer = &sink.writer,
    });
    const status: u10 = @intFromEnum(response.status);
    if (status == 400 or status == 404) return .missing;
    if (status < 200 or status >= 300) return error.PdsUnavailable;
    return .{ .found = try sink.toOwnedSlice() };
}

/// Apply one ledger item through the normal extraction/index path. The caller
/// supplies a freshly DID-resolved PDS and the audit CID. The PDS response is
/// fetched again here, immediately before the write, so a changed source is a
/// no-op rather than a stale overwrite.
pub fn applyDocumentReconciliation(
    allocator: Allocator,
    io: Io,
    did_str: []const u8,
    collection: []const u8,
    rkey: []const u8,
    pds: []const u8,
    expected_cid: ?[]const u8,
    action: TargetedAction,
    observe_classifier: bool,
) !TargetedResult {
    if (!mem.eql(u8, collection, STANDARD_DOCUMENT)) return error.UnsupportedCollection;
    const did = zat.Did.parse(did_str) orelse return error.InvalidDid;
    if (isBridgyPds(pds) and action == .upsert) return error.BridgedRepo;

    var uri_buf: [256]u8 = undefined;
    const uri = zat.AtUri.format(&uri_buf, did.raw, collection, rkey) orelse return error.InvalidAtUri;
    const fetched = try fetchRecord(allocator, io, pds, did.raw, collection, rkey);

    switch (fetched) {
        .missing => {
            if (action == .upsert) return .{ .outcome = .source_absent };
            indexer.deleteDocument(uri);
            const hashed = tpuf.hashId(uri);
            // A vector failure makes the item retryable. The Turso delete is
            // idempotent, so the next attempt safely finishes vector cleanup.
            if (tpuf.isEnabled()) try tpuf.delete(allocator, &.{&hashed});
            return .{ .outcome = .deleted };
        },
        .found => |body| {
            defer allocator.free(body);
            const parsed = json.parseFromSlice(json.Value, allocator, body, .{}) catch return error.BadRecordResponse;
            defer parsed.deinit();
            const actual_cid = zat.json.getString(parsed.value, "cid") orelse return error.BadRecordResponse;
            const cid_owned = try allocator.dupe(u8, actual_cid);
            errdefer allocator.free(cid_owned);

            if (action == .delete) {
                return .{ .outcome = .source_exists, .source_cid = cid_owned };
            }
            const expected = expected_cid orelse return error.MissingExpectedCid;
            if (!mem.eql(u8, expected, actual_cid)) {
                return .{ .outcome = .source_changed, .source_cid = cid_owned };
            }
            const response_uri = zat.json.getString(parsed.value, "uri") orelse return error.BadRecordResponse;
            if (!mem.eql(u8, response_uri, uri)) return error.RecordUriMismatch;
            const value = parsed.value.object.get("value") orelse return error.BadRecordResponse;
            if (value != .object) return error.BadRecordResponse;
            if (observe_classifier) {
                try processDocument(allocator, uri, did.raw, rkey, value.object, collection, actual_cid);
            } else {
                var doc = try extractor.extractDocument(allocator, value.object, collection);
                defer doc.deinit();
                try indexer.insertDocument(
                    uri, did.raw, rkey, doc.title, doc.content, doc.created_at,
                    doc.publication_uri, doc.tags, doc.platformName(), doc.source_collection,
                    doc.path, doc.content_type, doc.cover_image, actual_cid,
                );
                logfire.counter("ingest.documents_indexed", 1);
            }
            return .{ .outcome = .upserted, .source_cid = cid_owned };
        },
    }
}

test "targeted reconciliation rejects unsafe inputs before network access" {
    const allocator = std.testing.allocator;
    const io: Io = undefined;
    const did = "did:plc:ztjsajckkmfscs3tshez4ath";

    try std.testing.expectError(
        error.UnsupportedCollection,
        applyDocumentReconciliation(allocator, io, did, LEAFLET_DOCUMENT, "rkey", "https://pds.example", "bafy", .upsert, false),
    );
    try std.testing.expectError(
        error.InvalidDid,
        applyDocumentReconciliation(allocator, io, "not-a-did", STANDARD_DOCUMENT, "rkey", "https://pds.example", "bafy", .upsert, false),
    );
    try std.testing.expectError(
        error.BridgedRepo,
        applyDocumentReconciliation(allocator, io, did, STANDARD_DOCUMENT, "rkey", "https://atproto.brid.gy", "bafy", .upsert, false),
    );
    try std.testing.expectError(
        error.InsecurePds,
        applyDocumentReconciliation(allocator, io, did, STANDARD_DOCUMENT, "rkey", "http://pds.example", "bafy", .upsert, false),
    );
}

/// Backfill one repo by DID. If `collection_filter` is non-null, only that
/// collection is walked; otherwise all of BACKFILL_COLLECTIONS. Idempotent —
/// indexer upserts, so re-running is safe.
pub fn backfillRepo(
    allocator: Allocator,
    io: Io,
    did_str: []const u8,
    collection_filter: ?[]const u8,
) !BackfillCounts {
    const span = logfire.span("backfill.repo", .{ .did = did_str });
    defer span.end();

    const did = zat.Did.parse(did_str) orelse return error.InvalidDid;

    const pds = try resolvePds(allocator, io, did.raw);
    defer allocator.free(pds);
    logfire.info("backfill: {s} → pds {s}", .{ did.raw, pds });

    // the side door enforces the same policy as the front door: bridgy fed
    // never enters the corpus (the ingester drops it via the same PDS check)
    if (isBridgyPds(pds)) {
        logfire.warn("backfill: {s} rejected — bridgy-hosted PDS {s}", .{ did.raw, pds });
        return error.BridgedRepo;
    }

    var counts: BackfillCounts = .{};
    for (BACKFILL_COLLECTIONS) |collection| {
        if (collection_filter) |f| {
            if (!mem.eql(u8, f, collection)) continue;
        }
        backfillCollection(allocator, io, pds, did.raw, collection, &counts) catch |err| {
            logfire.warn("backfill: collection {s} for {s} failed: {}", .{ collection, did.raw, err });
        };
    }

    logfire.info("backfill: {s} done — docs {d}, pubs {d}, recs {d}, subs {d}, skipped {d}", .{
        did.raw, counts.documents, counts.publications, counts.recommends, counts.subscriptions, counts.skipped,
    });
    return counts;
}

fn backfillCollection(
    allocator: Allocator,
    io: Io,
    pds: []const u8,
    did: []const u8,
    collection: []const u8,
    counts: *BackfillCounts,
) !void {
    var cursor: ?[]const u8 = null;
    defer if (cursor) |c| allocator.free(c);

    while (true) {
        const body = try listRecords(allocator, io, pds, did, collection, cursor);
        defer allocator.free(body);

        const parsed = json.parseFromSlice(json.Value, allocator, body, .{}) catch return error.BadListResponse;
        defer parsed.deinit();

        const records = parsed.value.object.get("records") orelse return;
        if (records != .array) return;

        for (records.array.items) |entry| {
            if (entry != .object) continue;
            const rec_uri = zat.json.getString(entry, "uri") orelse continue;
            const rkey = rkeyFromUri(rec_uri) orelse continue;
            const value = entry.object.get("value") orelse continue;
            if (value != .object) continue;
            const inner = value.object;

            var uri_buf: [256]u8 = undefined;
            const uri = zat.AtUri.format(&uri_buf, did, collection, rkey) orelse {
                counts.skipped += 1;
                continue;
            };

            if (isDocumentCollection(collection)) {
                if (mem.eql(u8, collection, WHITEWIND_ENTRY)) {
                    const visibility = zat.json.getString(value, "visibility") orelse "public";
                    if (mem.eql(u8, visibility, "author")) {
                        counts.skipped += 1;
                        continue;
                    }
                }
                const source_cid = zat.json.getString(entry, "cid");
                processDocument(allocator, uri, did, rkey, inner, collection, source_cid) catch {
                    counts.skipped += 1;
                    continue;
                };
                counts.documents += 1;
            } else if (isPublicationCollection(collection)) {
                processPublication(allocator, uri, did, rkey, inner) catch {
                    counts.skipped += 1;
                    continue;
                };
                counts.publications += 1;
            } else if (isRecommendCollection(collection)) {
                processRecommend(uri, did, rkey, collection, inner) catch {
                    counts.skipped += 1;
                    continue;
                };
                counts.recommends += 1;
            } else if (isSubscriptionCollection(collection)) {
                processSubscription(uri, did, rkey, inner) catch {
                    counts.skipped += 1;
                    continue;
                };
                counts.subscriptions += 1;
            }
        }

        // advance cursor; stop when the PDS stops returning one
        const next = zat.json.getString(parsed.value, "cursor") orelse break;
        if (next.len == 0) break;
        const owned = try allocator.dupe(u8, next);
        if (cursor) |c| allocator.free(c);
        cursor = owned;

        // gentle on the PDS
        io.sleep(Io.Duration.fromMilliseconds(100), .awake) catch {};
    }
}

fn listRecords(
    allocator: Allocator,
    io: Io,
    pds: []const u8,
    did: []const u8,
    collection: []const u8,
    cursor: ?[]const u8,
) ![]u8 {
    var url_buf: [768]u8 = undefined;
    const url = if (cursor) |c|
        std.fmt.bufPrint(&url_buf, "{s}/xrpc/com.atproto.repo.listRecords?repo={s}&collection={s}&limit=100&cursor={s}", .{ pds, did, collection, c }) catch return error.UrlTooLong
    else
        std.fmt.bufPrint(&url_buf, "{s}/xrpc/com.atproto.repo.listRecords?repo={s}&collection={s}&limit=100", .{ pds, did, collection }) catch return error.UrlTooLong;

    var http_client: http.Client = .{ .allocator = allocator, .io = io };
    defer http_client.deinit();

    var sink: std.Io.Writer.Allocating = .init(allocator);
    defer sink.deinit();

    const res = try http_client.fetch(.{
        .location = .{ .url = url },
        .method = .GET,
        .response_writer = &sink.writer,
    });
    if (@intFromEnum(res.status) < 200 or @intFromEnum(res.status) >= 300) return error.ListRecordsFailed;

    return sink.toOwnedSlice();
}

/// Resolve a DID to its PDS endpoint via plc.directory. Caller owns the result.
fn resolvePds(allocator: Allocator, io: Io, did: []const u8) ![]u8 {
    var url_buf: [256]u8 = undefined;
    const url = std.fmt.bufPrint(&url_buf, "https://plc.directory/{s}", .{did}) catch return error.UrlTooLong;

    var http_client: http.Client = .{ .allocator = allocator, .io = io };
    defer http_client.deinit();

    var sink: std.Io.Writer.Allocating = .init(allocator);
    defer sink.deinit();

    const res = try http_client.fetch(.{
        .location = .{ .url = url },
        .method = .GET,
        .response_writer = &sink.writer,
    });
    if (res.status != .ok) return error.PlcLookupFailed;

    const body = try sink.toOwnedSlice();
    defer allocator.free(body);

    const parsed = json.parseFromSlice(json.Value, allocator, body, .{}) catch return error.BadPlcResponse;
    defer parsed.deinit();

    const services = parsed.value.object.get("service") orelse return error.NoPds;
    if (services != .array) return error.NoPds;
    for (services.array.items) |svc| {
        if (svc != .object) continue;
        const svc_type = svc.object.get("type") orelse continue;
        if (svc_type != .string or !mem.eql(u8, svc_type.string, "AtprotoPersonalDataServer")) continue;
        const endpoint = svc.object.get("serviceEndpoint") orelse continue;
        if (endpoint != .string) continue;
        return allocator.dupe(u8, endpoint.string);
    }
    return error.NoPds;
}

/// Host-match a PDS endpoint against brid.gy (mirrors ingester/src/verifier.zig).
fn isBridgyPds(endpoint: []const u8) bool {
    var host = endpoint;
    if (mem.indexOf(u8, host, "://")) |i| host = host[i + 3 ..];
    if (mem.indexOfScalar(u8, host, '/')) |i| host = host[0..i];
    if (mem.indexOfScalar(u8, host, ':')) |i| host = host[0..i];
    return mem.eql(u8, host, "brid.gy") or mem.endsWith(u8, host, ".brid.gy");
}

test "isBridgyPds matches bridgy hosts only" {
    try std.testing.expect(isBridgyPds("https://atproto.brid.gy"));
    try std.testing.expect(isBridgyPds("https://atproto.brid.gy/"));
    try std.testing.expect(isBridgyPds("https://brid.gy:443/path"));
    try std.testing.expect(!isBridgyPds("https://bsky.social"));
    try std.testing.expect(!isBridgyPds("https://notbrid.gy.example.com"));
    try std.testing.expect(!isBridgyPds("https://mybrid.gy.evil/x"));
    try std.testing.expect(!isBridgyPds("https://pds.example.com/brid.gy"));
}

test "stripUrlScheme drops fragments (regression: zat.dev devlog hash-route base_path)" {
    try std.testing.expectEqualStrings("zat.dev", stripUrlScheme("https://zat.dev/#devlog/index").?);
    try std.testing.expectEqualStrings("zat.dev", stripUrlScheme("https://zat.dev/").?);
    try std.testing.expectEqualStrings("notes.example.com", stripUrlScheme("https://notes.example.com").?);
    try std.testing.expectEqualStrings("a.dev/blog", stripUrlScheme("http://a.dev/blog#section").?);
}

test "showInDiscover: false is extracted, absent defaults to discoverable" {
    const opted_out =
        \\{"$type":"site.standard.publication","url":"https://notes.example.com","name":"notes","preferences":{"showInDiscover":false}}
    ;
    var parsed_out = try std.json.parseFromSlice(std.json.Value, std.testing.allocator, opted_out, .{});
    defer parsed_out.deinit();
    try std.testing.expectEqual(false, zat.json.getBool(parsed_out.value, "preferences.showInDiscover") orelse true);

    const no_prefs =
        \\{"$type":"site.standard.publication","url":"https://notes.example.com","name":"notes"}
    ;
    var parsed_abs = try std.json.parseFromSlice(std.json.Value, std.testing.allocator, no_prefs, .{});
    defer parsed_abs.deinit();
    try std.testing.expectEqual(true, zat.json.getBool(parsed_abs.value, "preferences.showInDiscover") orelse true);
}

test "pckt sidecar exposes the referenced publication uri" {
    const src =
        \\{"$type":"blog.pckt.publication","publication":{"uri":"at://did:plc:brookie/site.standard.publication/abc","cid":"bafy"}}
    ;
    var parsed = try std.json.parseFromSlice(std.json.Value, std.testing.allocator, src, .{});
    defer parsed.deinit();
    const target = zat.json.getString(parsed.value, "publication.uri");
    try std.testing.expectEqualStrings("at://did:plc:brookie/site.standard.publication/abc", target.?);
}

/// Last path segment of an AT-URI ("at://did/collection/rkey" → "rkey").
fn rkeyFromUri(uri: []const u8) ?[]const u8 {
    const last_slash = mem.lastIndexOfScalar(u8, uri, '/') orelse return null;
    const rkey = uri[last_slash + 1 ..];
    return if (rkey.len == 0) null else rkey;
}

test "rkeyFromUri" {
    const t = std.testing;
    try t.expectEqualStrings("3mn3z7u7jgsgl", rkeyFromUri("at://did:plc:abc/site.standard.document/3mn3z7u7jgsgl").?);
    try t.expectEqual(@as(?[]const u8, null), rkeyFromUri("no-slashes"));
    try t.expectEqual(@as(?[]const u8, null), rkeyFromUri("at://did:plc:abc/coll/"));
}

fn stripUrlScheme(url: ?[]const u8) ?[]const u8 {
    const u = url orelse return null;
    var without_scheme = if (mem.startsWith(u8, u, "https://"))
        u["https://".len..]
    else if (mem.startsWith(u8, u, "http://"))
        u["http://".len..]
    else
        u;
    // fragments are client-side routes, never part of the base: a publication
    // url like https://zat.dev/#devlog/index would otherwise bake "#devlog/…"
    // into base_path and every derived doc link becomes a broken SPA route
    if (mem.indexOfScalar(u8, without_scheme, '#')) |hash|
        without_scheme = without_scheme[0..hash];
    // strip trailing slash to avoid double-slash when combined with path
    if (without_scheme.len > 1 and without_scheme[without_scheme.len - 1] == '/')
        return without_scheme[0 .. without_scheme.len - 1];
    return without_scheme;
}
