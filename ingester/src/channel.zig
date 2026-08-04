//! Firehose `/channel` websocket server.
//!
//! Emits the exact frame shape the backend's ingester consumer expects:
//!   {"id":<seq>,"type":"record","record":{"action":..,"did":..,"collection":..,"rkey":..[,"cid":..,"record":<value>]}}
//!
//! Delivery is at-least-once via a bounded in-memory OUTBOX: every record
//! frame stays queued until the backend acks it ({"type":"ack","id":<seq>},
//! sent only AFTER its turso batch commits — see backend ingest/ingester.zig).
//! Unacked frames are redelivered on reconnect and retransmitted on a timer,
//! so a frame the backend received but then shed under backpressure is not
//! lost; the backend's upserts absorb the resulting duplicates. In steady
//! state an ack lands within ms and the outbox hovers near empty; while the
//! backend is down it grows to its budget and evicts oldest (the durable
//! cursor stays pinned at/below the oldest unacked seq, so a restart replays
//! evicted frames from the relay). Beyond budget+relay-retention is
//! `/admin/backfill` territory.
//!
//! The verify workers call broadcast(); the websocket worker thread handles
//! acks. Everything mutating shared state holds the same spinlock.

const std = @import("std");
const Io = std.Io;
const ws = @import("websocket");
const zat = @import("zat");
const logfire = @import("logfire");
const cbor_json = @import("cbor_json.zig");

const MAX_CLIENTS = 8;
const OUTBOX_MAX_ENTRIES = 8192;
const OUTBOX_MAX_BYTES = 64 * 1024 * 1024;
/// resend a delivered-but-unacked frame after this long. covers the backend
/// shedding a frame it never got to process; duplicates are idempotent.
const RETRANSMIT_AFTER_NS: i64 = 60 * std.time.ns_per_s;

pub const Channel = struct {
    allocator: std.mem.Allocator,
    io: Io,
    // tiny spinlock — std.Thread.Mutex is gone in 0.16 and Io.Mutex needs an
    // io handle the ws worker threads don't carry. Contention is near-zero
    // (one backend client, a few broadcasts/sec), so a spinlock is fine.
    locked: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),
    conns: [MAX_CLIENTS]?*ws.Conn = .{null} ** MAX_CLIENTS,
    acks: u64 = 0,
    // FIFO outbox (linked list): appended by broadcast, pruned by acks,
    // evicted oldest-first under budget pressure. Entries keep their firehose
    // seq so the cursor checkpoint can refuse to advance past unacked work
    // (see pendingSeq).
    head: ?*Entry = null,
    tail: ?*Entry = null,
    outbox_len: usize = 0,
    outbox_bytes: usize = 0,
    outbox_dropped: u64 = 0,

    const Entry = struct {
        frame: []u8,
        seq: i64,
        /// monotonic ns of last write to a client; 0 = never delivered
        sent_at: i64 = 0,
        next: ?*Entry = null,
    };

    fn lock(self: *Channel) void {
        while (self.locked.swap(true, .acquire)) std.atomic.spinLoopHint();
    }
    fn unlock(self: *Channel) void {
        self.locked.store(false, .release);
    }

    fn nowNs(self: *Channel) i64 {
        return @intCast(Io.Timestamp.now(self.io, .awake).nanoseconds);
    }

    pub fn register(self: *Channel, conn: *ws.Conn) void {
        const now_ns = self.nowNs();
        self.lock();
        defer self.unlock();
        for (&self.conns) |*slot| {
            if (slot.* == null) {
                slot.* = conn;
                const buffered = self.outbox_len;
                self.redeliverLocked(conn, now_ns, true);
                logfire.info("channel: client connected, redelivered {d} unacked frame(s) (evicted while down: {d})", .{ buffered, self.outbox_dropped });
                self.outbox_dropped = 0;
                return;
            }
        }
        logfire.warn("channel: client slots full, dropping registration", .{});
    }

    pub fn unregister(self: *Channel, conn: *ws.Conn) void {
        self.lock();
        defer self.unlock();
        for (&self.conns) |*slot| {
            if (slot.* == conn) {
                slot.* = null;
                logfire.info("channel: client disconnected", .{});
                return;
            }
        }
    }

    /// Queue a record frame in the outbox and write it to every connected
    /// client. The entry stays queued until acked. Prunes any conn whose
    /// write fails. Returns the number of clients written.
    pub fn broadcast(self: *Channel, frame: []const u8, seq: i64) usize {
        const now_ns = self.nowNs();
        self.lock();
        defer self.unlock();

        self.evictToBudgetLocked(frame.len);
        const entry = blk: {
            const copy = self.allocator.dupe(u8, frame) catch {
                self.outbox_dropped += 1;
                return 0;
            };
            const e = self.allocator.create(Entry) catch {
                self.allocator.free(copy);
                self.outbox_dropped += 1;
                return 0;
            };
            e.* = .{ .frame = copy, .seq = seq };
            break :blk e;
        };
        if (self.tail) |t| t.next = entry else self.head = entry;
        self.tail = entry;
        self.outbox_len += 1;
        self.outbox_bytes += entry.frame.len;

        var n: usize = 0;
        for (&self.conns) |*slot| {
            if (slot.*) |conn| {
                conn.write(entry.frame) catch {
                    slot.* = null;
                    continue;
                };
                n += 1;
            }
        }
        if (n > 0) entry.sent_at = now_ns;
        return n;
    }

    /// Ack from the backend: its turso batch containing this frame committed.
    /// Remove the first (oldest) outbox entry with the acked seq — multiple
    /// ops in one commit share a seq and are acked frame-by-frame, so one ack
    /// releases one entry.
    pub fn ack(self: *Channel, seq: i64) void {
        self.lock();
        defer self.unlock();
        self.acks += 1;
        var prev: ?*Entry = null;
        var cur = self.head;
        while (cur) |entry| {
            if (entry.seq == seq) {
                if (prev) |p| p.next = entry.next else self.head = entry.next;
                if (self.tail == entry) self.tail = prev;
                self.outbox_len -= 1;
                self.outbox_bytes -= entry.frame.len;
                self.allocator.free(entry.frame);
                self.allocator.destroy(entry);
                return;
            }
            prev = entry;
            cur = entry.next;
        }
    }

    /// Oldest unacked seq, or null when the outbox is empty. The cursor
    /// checkpoint must not advance past this: the spec's contract is "last
    /// sequence number received AND successfully processed", and for a
    /// forwarder "processed" means acked, not read off the relay.
    /// (2026-06-09: checkpointing the read position lost 351 events — the
    /// buffer died with a restart and the resume skipped straight over them.)
    /// Entries can be mildly out of seq order (per-DID verify reordering), so
    /// scan for the true min rather than trusting the head.
    pub fn pendingSeq(self: *Channel) ?i64 {
        self.lock();
        defer self.unlock();
        var min: ?i64 = null;
        var cur = self.head;
        while (cur) |entry| {
            if (min == null or entry.seq < min.?) min = entry.seq;
            cur = entry.next;
        }
        return min;
    }

    /// Evict oldest entries until the outbox fits the budgets with room for
    /// one more frame of `incoming` bytes. Caller holds the lock. An evicted
    /// frame is lost for this process's lifetime — but the durable cursor is
    /// pinned at/below it, so a restart replays it from the relay. Surface
    /// loudly anyway.
    fn evictToBudgetLocked(self: *Channel, incoming: usize) void {
        while (self.outbox_len > 0 and (self.outbox_len >= OUTBOX_MAX_ENTRIES or self.outbox_bytes + incoming > OUTBOX_MAX_BYTES)) {
            const entry = self.head.?;
            self.head = entry.next;
            if (self.head == null) self.tail = null;
            self.outbox_len -= 1;
            self.outbox_bytes -= entry.frame.len;
            self.allocator.free(entry.frame);
            self.allocator.destroy(entry);
            self.outbox_dropped += 1;
            if (self.outbox_dropped == 1 or self.outbox_dropped % 1000 == 0) {
                logfire.warn("channel: outbox overflow, {d} frame(s) evicted unacked (restart replays them from the relay)", .{self.outbox_dropped});
            }
        }
    }

    /// (Re)send outbox entries: everything on reconnect (`all`), otherwise
    /// only entries whose last delivery is older than the retransmit window
    /// (covers frames the backend received but shed before processing).
    /// Caller holds the lock. On write failure the conn is dead: prune it and
    /// keep the entries for the next attempt.
    fn redeliverLocked(self: *Channel, conn: *ws.Conn, now_ns: i64, all: bool) void {
        var cur = self.head;
        while (cur) |entry| {
            const due = all or entry.sent_at == 0 or (now_ns - entry.sent_at) > RETRANSMIT_AFTER_NS;
            if (due) {
                conn.write(entry.frame) catch {
                    for (&self.conns) |*slot| {
                        if (slot.* == conn) slot.* = null;
                    }
                    return;
                };
                entry.sent_at = now_ns;
            }
            cur = entry.next;
        }
    }

    /// Periodic retransmit sweep (called from the heartbeat runner).
    pub fn retransmit(self: *Channel) void {
        const now_ns = self.nowNs();
        self.lock();
        defer self.unlock();
        for (&self.conns) |*slot| {
            if (slot.*) |conn| {
                self.redeliverLocked(conn, now_ns, false);
                return; // one live conn is enough — the backend is a single consumer
            }
        }
    }

    /// App-level heartbeat to every connected client. Never buffered — a
    /// heartbeat only means something live. This is what lets the backend's
    /// staleness watchdog distinguish "quiet firehose" from "dead socket":
    /// as long as we're up, frames arrive at least every heartbeat interval.
    pub fn ping(self: *Channel) void {
        self.lock();
        defer self.unlock();
        for (&self.conns) |*slot| {
            if (slot.*) |conn| {
                conn.write("{\"id\":0,\"type\":\"ping\"}") catch {
                    slot.* = null;
                };
            }
        }
    }

};

/// Parse the id out of the backend's fixed ack shape {"type":"ack","id":N}.
/// The producer is our own backend (ingest/ingester.zig sendAck) so a fixed
/// scan beats pulling in a JSON parser on the ack hot path.
pub fn parseAckId(data: []const u8) ?i64 {
    const marker = "\"id\":";
    const at = std.mem.indexOf(u8, data, marker) orelse return null;
    var end = at + marker.len;
    while (end < data.len and (data[end] == '-' or std.ascii.isDigit(data[end]))) end += 1;
    return std.fmt.parseInt(i64, data[at + marker.len .. end], 10) catch null;
}

/// Per-connection handler. Registers on connect, deregisters on close.
pub const Handler = struct {
    channel: *Channel,
    conn: *ws.Conn,

    pub fn init(h: *ws.Handshake, conn: *ws.Conn, channel: *Channel) !Handler {
        _ = h;
        return .{ .channel = channel, .conn = conn };
    }

    // register (and redeliver the outbox) only AFTER the server has written
    // the HTTP 101 handshake reply — registering in init() puts replayed
    // frames on the wire ahead of the upgrade response, corrupting the
    // handshake.
    pub fn afterInit(self: *Handler) !void {
        self.channel.register(self.conn);
    }

    pub fn clientMessage(self: *Handler, data: []const u8) !void {
        // backend acks a frame only after its turso batch commits; the ack
        // releases the frame from the outbox. id 0 is a heartbeat echo.
        const id = parseAckId(data) orelse return;
        if (id > 0) self.channel.ack(id);
    }

    pub fn close(self: *Handler) void {
        self.channel.unregister(self.conn);
    }
};

/// Build one `/channel` record frame into `out`. For deletes, `record` is null.
pub fn buildRecordFrame(
    out: *std.Io.Writer.Allocating,
    alloc: std.mem.Allocator,
    id: i64,
    action: []const u8,
    did: []const u8,
    collection: []const u8,
    rkey: []const u8,
    cid: ?zat.cbor.Cid,
    record: ?zat.cbor.Value,
) !void {
    const w = &out.writer;
    try w.print("{{\"id\":{d},\"type\":\"record\",\"record\":{{\"action\":\"{s}\",\"did\":\"{s}\",\"collection\":\"{s}\",\"rkey\":\"{s}\"", .{
        id, action, did, collection, rkey,
    });
    if (cid) |value| {
        const b32 = try zat.multibase.encode(alloc, .base32lower, value.raw);
        defer alloc.free(b32);
        try w.writeAll(",\"cid\":\"");
        try w.writeAll(b32);
        try w.writeByte('"');
    }
    if (record) |value| {
        try w.writeAll(",\"record\":");
        try cbor_json.writeValue(w, alloc, value);
    }
    try w.writeAll("}}");
}

pub const Server = ws.Server(Handler);

/// Run the websocket server (blocks). karlseguin's server manages its own
/// worker pool, so it coexists with the firehose thread fine.
pub fn serve(allocator: std.mem.Allocator, io: Io, channel: *Channel, port: u16) !void {
    var server = try Server.init(allocator, io, .{
        .port = port,
        // fly 6PN (.internal) is IPv6-only — "::" binds both families.
        .address = "::",
        .max_conn = 64,
        .max_message_size = 5 * 1024 * 1024,
    });
    defer server.deinit();
    try server.listen(channel);
}

test "outbox: frames queue until acked, pendingSeq is the true min, eviction respects budget" {
    var threaded = Io.Threaded.init(std.testing.allocator, .{});
    var chan = Channel{ .allocator = std.testing.allocator, .io = threaded.io() };
    defer {
        var cur = chan.head;
        while (cur) |e| {
            cur = e.next;
            std.testing.allocator.free(e.frame);
            std.testing.allocator.destroy(e);
        }
    }

    // no clients connected: frames queue unacked
    _ = chan.broadcast("frame-a", 100);
    _ = chan.broadcast("frame-b", 102);
    _ = chan.broadcast("frame-c", 101); // per-DID verify reordering: out of seq order
    try std.testing.expectEqual(@as(usize, 3), chan.outbox_len);
    // pendingSeq scans for the true min, not the head
    try std.testing.expectEqual(@as(i64, 100), chan.pendingSeq().?);

    // ack releases exactly one entry per ack, by seq
    chan.ack(100);
    try std.testing.expectEqual(@as(usize, 2), chan.outbox_len);
    try std.testing.expectEqual(@as(i64, 101), chan.pendingSeq().?);
    chan.ack(999); // unknown seq: no-op
    try std.testing.expectEqual(@as(usize, 2), chan.outbox_len);
    chan.ack(102);
    chan.ack(101);
    try std.testing.expect(chan.pendingSeq() == null);
    try std.testing.expectEqual(@as(usize, 0), chan.outbox_len);
    try std.testing.expectEqual(@as(usize, 0), chan.outbox_bytes);

    // same seq twice (two ops in one commit): each ack releases one
    _ = chan.broadcast("op-1", 200);
    _ = chan.broadcast("op-2", 200);
    chan.ack(200);
    try std.testing.expectEqual(@as(usize, 1), chan.outbox_len);
    chan.ack(200);
    try std.testing.expectEqual(@as(usize, 0), chan.outbox_len);
}

test "parseAckId: backend ack shape, heartbeat echo, garbage" {
    try std.testing.expectEqual(@as(i64, 42), parseAckId("{\"type\":\"ack\",\"id\":42}").?);
    try std.testing.expectEqual(@as(i64, 0), parseAckId("{\"type\":\"ack\",\"id\":0}").?);
    try std.testing.expect(parseAckId("{\"type\":\"ack\"}") == null);
    try std.testing.expect(parseAckId("not json") == null);
}

test "record frame carries a parseable source CID" {
    const allocator = std.testing.allocator;
    const raw = [_]u8{ 1, 0x71, 0x12, 0x20, 1, 2, 3, 4 };
    var out: std.Io.Writer.Allocating = .init(allocator);
    defer out.deinit();

    try buildRecordFrame(
        &out,
        allocator,
        42,
        "create",
        "did:plc:test",
        "site.standard.document",
        "abc",
        .{ .raw = &raw },
        null,
    );

    const parsed = try std.json.parseFromSlice(std.json.Value, allocator, out.written(), .{});
    defer parsed.deinit();
    const cid = zat.json.getString(parsed.value, "record.cid") orelse return error.MissingCid;
    try std.testing.expect(cid.len > 1);
    try std.testing.expectEqual(@as(u8, 'b'), cid[0]);
    try std.testing.expect(!std.mem.startsWith(u8, cid, "bb"));
}
