//! verify pipeline: thin reader → per-DID FIFO chains → verify workers.
//!
//! adapted from zat.dev/stream's live scheduler (docs/live-scheduler.md there;
//! plan: docs/scale-300k-plan.md §1e). the firehose read thread only
//! classifies frames and hands tracked commits here as owned raw bytes; the
//! expensive work — DAG-CBOR decode, signature + MST verification, and DID
//! key resolution (up to a 10s PLC roundtrip) — happens on a small worker
//! pool. a slow PLC now stalls one worker, not the firehose.
//!
//! ordering: a worker owns one DID's chain until it drains, so events for one
//! author are processed in arrival order; cross-DID completion order is
//! unspecified (the backend's upserts don't care). bounding is per DID
//! (drop-oldest at MAX_PENDING_PER_DID), so one hot repo mass-re-putting an
//! archive can't evict other authors' events.
//!
//! cursor contract: an event's seq is "inflight" from submit until its
//! records reach the channel (delivered or ring-buffered). the durable relay
//! cursor must never advance past min(inflight)-1 — see main.zig's
//! checkpoint, which takes the min of this and the channel ring's pending
//! seq. dropped events (per-DID overflow) leave inflight immediately: the
//! cursor may advance past them, which is exactly the documented loss the
//! drop counter surfaces.

const std = @import("std");
const Io = std.Io;
const zat = @import("zat");
const logfire = @import("logfire");
const ch = @import("channel.zig");
const vf = @import("verifier.zig");

/// per-DID pending bound. stream uses 64 for the whole network; ours is the
/// same because the failure mode it bounds (one repo re-putting its archive
/// faster than we verify) is identical. NOTE (stream's "one constant" trap):
/// this and the worker count are independent here because chains queue in an
/// unbounded ready list — a full chain sheds its own oldest, never blocks
/// the reader.
const MAX_PENDING_PER_DID = 64;

const MAX_DID_LEN = 256;

fn workerCount() usize {
    const raw = if (std.c.getenv("VERIFY_WORKERS")) |p| std.mem.span(p) else "8";
    const n = std.fmt.parseInt(usize, raw, 10) catch 8;
    return @max(1, @min(n, 32));
}

const Item = struct {
    frame: []u8,
    seq: i64,
    next: ?*Item = null,
};

const Chain = struct {
    did_buf: [MAX_DID_LEN]u8 = undefined,
    did_len: usize = 0,
    head: ?*Item = null,
    tail: ?*Item = null,
    len: usize = 0,
    /// a worker currently owns this chain (it is not in the ready list)
    owned: bool = false,
    next_ready: ?*Chain = null,

    fn did(self: *const Chain) []const u8 {
        return self.did_buf[0..self.did_len];
    }
};

pub const Pipeline = struct {
    allocator: std.mem.Allocator,
    io: Io,
    channel: *ch.Channel,
    verifier: *vf.Verifier,

    mutex: Io.Mutex = Io.Mutex.init,
    cond: Io.Condition = Io.Condition.init,
    /// active chains keyed by DID (key slices point into the chain's own buf)
    chains: std.StringHashMapUnmanaged(*Chain) = .empty,
    ready_head: ?*Chain = null,
    ready_tail: ?*Chain = null,
    /// seqs submitted but not yet delivered to the channel
    inflight: std.AutoHashMapUnmanaged(i64, void) = .empty,

    submitted: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),
    processed: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),
    dropped: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),

    pub fn start(self: *Pipeline) !void {
        const n = workerCount();
        logfire.info("pipeline: starting {d} verify worker(s), per-DID bound {d}", .{ n, MAX_PENDING_PER_DID });
        for (0..n) |_| {
            const t = try std.Thread.spawn(.{}, workerLoop, .{self});
            t.detach();
        }
    }

    /// reader thread: hand a tracked commit's raw frame to the pool. copies
    /// `frame`; returns without blocking (a full chain sheds its own oldest).
    pub fn submit(self: *Pipeline, did: []const u8, seq: i64, frame: []const u8) void {
        if (did.len == 0 or did.len > MAX_DID_LEN) return;
        const copy = self.allocator.dupe(u8, frame) catch return;
        const item = self.allocator.create(Item) catch {
            self.allocator.free(copy);
            return;
        };
        item.* = .{ .frame = copy, .seq = seq };

        self.mutex.lockUncancelable(self.io);
        defer self.mutex.unlock(self.io);

        const gop = self.chains.getOrPut(self.allocator, did) catch {
            self.allocator.free(copy);
            self.allocator.destroy(item);
            return;
        };
        if (!gop.found_existing) {
            const chain = self.allocator.create(Chain) catch {
                self.chains.removeByPtr(gop.key_ptr);
                self.allocator.free(copy);
                self.allocator.destroy(item);
                return;
            };
            chain.* = .{};
            @memcpy(chain.did_buf[0..did.len], did);
            chain.did_len = did.len;
            gop.key_ptr.* = chain.did();
            gop.value_ptr.* = chain;
        }
        const chain = gop.value_ptr.*;

        // per-DID bound: shed THIS repo's oldest, never another author's
        if (chain.len >= MAX_PENDING_PER_DID) {
            if (chain.head) |oldest| {
                chain.head = oldest.next;
                if (chain.head == null) chain.tail = null;
                chain.len -= 1;
                _ = self.inflight.remove(oldest.seq);
                self.allocator.free(oldest.frame);
                self.allocator.destroy(oldest);
                const n = self.dropped.fetchAdd(1, .monotonic) + 1;
                if (n <= 10 or n % 100 == 0) {
                    logfire.warn("pipeline: per-DID overflow, dropped oldest for {s} ({d} total dropped)", .{ did, n });
                }
            }
        }

        if (chain.tail) |t| t.next = item else chain.head = item;
        chain.tail = item;
        chain.len += 1;
        self.inflight.put(self.allocator, seq, {}) catch {};
        _ = self.submitted.fetchAdd(1, .monotonic);

        const in_ready = chain.next_ready != null or self.ready_tail == chain;
        if (!chain.owned and !in_ready) {
            self.pushReady(chain);
            self.cond.signal(self.io);
        }
    }

    /// oldest seq still being verified, or null. the durable cursor
    /// checkpoint must stay below this (main.zig).
    pub fn minInflight(self: *Pipeline) ?i64 {
        self.mutex.lockUncancelable(self.io);
        defer self.mutex.unlock(self.io);
        var min: ?i64 = null;
        var it = self.inflight.keyIterator();
        while (it.next()) |k| {
            if (min == null or k.* < min.?) min = k.*;
        }
        return min;
    }

    fn pushReady(self: *Pipeline, chain: *Chain) void {
        chain.next_ready = null;
        if (self.ready_tail) |t| t.next_ready = chain else self.ready_head = chain;
        self.ready_tail = chain;
    }

    fn popReady(self: *Pipeline) ?*Chain {
        const chain = self.ready_head orelse return null;
        self.ready_head = chain.next_ready;
        if (self.ready_head == null) self.ready_tail = null;
        chain.next_ready = null;
        return chain;
    }

    fn workerLoop(self: *Pipeline) void {
        while (true) {
            self.mutex.lockUncancelable(self.io);
            const chain: *Chain = while (true) {
                if (self.popReady()) |c| break c;
                self.cond.waitUncancelable(self.io, &self.mutex);
            };
            chain.owned = true;

            // own the chain until it drains: items process in FIFO order,
            // re-taking the lock between items so the reader can append (and
            // shed) concurrently.
            while (true) {
                const item = self.takeItemLocked(chain) orelse break;
                self.mutex.unlock(self.io);

                self.process(item);

                self.mutex.lockUncancelable(self.io);
                self.completeItemLocked(item);
            }
            self.mutex.unlock(self.io);
        }
    }

    /// pop the chain's next item, or retire the drained chain (removes it
    /// from the map so it doesn't grow with every author ever seen). caller
    /// holds the lock. null = chain drained and destroyed.
    fn takeItemLocked(self: *Pipeline, chain: *Chain) ?*Item {
        const item = chain.head orelse {
            const removed = self.chains.remove(chain.did());
            std.debug.assert(removed);
            self.allocator.destroy(chain);
            return null;
        };
        chain.head = item.next;
        if (chain.head == null) chain.tail = null;
        chain.len -= 1;
        return item;
    }

    /// release a processed item's seq from the inflight set (letting the
    /// cursor advance past it) and free it. caller holds the lock.
    fn completeItemLocked(self: *Pipeline, item: *Item) void {
        _ = self.inflight.remove(item.seq);
        self.allocator.free(item.frame);
        self.allocator.destroy(item);
        _ = self.processed.fetchAdd(1, .monotonic);
    }

    /// decode + verify + emit one raw frame. runs unlocked on a worker.
    fn process(self: *Pipeline, item: *Item) void {
        var arena = std.heap.ArenaAllocator.init(self.allocator);
        defer arena.deinit();
        const a = arena.allocator();

        const event = zat.firehose.decodeFrame(a, item.frame) catch |err| {
            logfire.warn("pipeline: frame re-decode failed seq={d}: {s}", .{ item.seq, @errorName(err) });
            return;
        };
        const commit = switch (event) {
            .commit => |c| c,
            else => return,
        };

        // rejected/bridged commits are still logged as captured so coverage
        // stays auditable, but they never reach /channel.
        const verdict = self.verifier.verifyCommit(commit);
        for (commit.ops) |op| {
            if (!isTracked(op.collection)) continue;
            logfire.info("ingester.captured action={s} collection={s} did={s} rkey={s} seq={d} verified={s}", .{
                @tagName(op.action), op.collection, commit.repo, op.rkey, commit.seq, @tagName(verdict),
            });
            switch (verdict) {
                .full, .sig_only => self.emit(op, commit.repo, commit.seq),
                .bridged, .rejected => {},
            }
        }
    }

    fn emit(self: *Pipeline, op: zat.firehose.RepoOp, did: []const u8, seq: i64) void {
        var arena = std.heap.ArenaAllocator.init(self.allocator);
        defer arena.deinit();
        const a = arena.allocator();
        var out: std.Io.Writer.Allocating = .init(a);
        ch.buildRecordFrame(&out, a, seq, @tagName(op.action), did, op.collection, op.rkey, op.cid, op.record) catch |err| {
            logfire.warn("channel: frame build failed: {s}", .{@errorName(err)});
            return;
        };
        _ = self.channel.broadcast(out.written(), seq);
    }
};

// Collections we index — mirrors the backend's ingester collection filters.
pub const COLLECTIONS = [_][]const u8{
    "pub.leaflet.document",
    "pub.leaflet.publication",
    "pub.leaflet.interactions.recommend",
    "site.standard.document",
    "site.standard.publication",
    "site.standard.graph.recommend",
    // pckt sidecar: marks which site.standard.publication is a pckt blog so the
    // backend can platform-tag custom-domain pckt docs (their host lacks pckt.blog).
    "blog.pckt.publication",
    "com.whtwnd.blog.entry",
};

pub fn isTracked(collection: []const u8) bool {
    for (COLLECTIONS) |c| {
        if (std.mem.eql(u8, c, collection)) return true;
    }
    return false;
}

// --- tests: submit/shed/drain bookkeeping, driven without worker threads ---

fn testPipeline(threaded: *Io.Threaded) Pipeline {
    return .{
        .allocator = std.testing.allocator,
        .io = threaded.io(),
        .channel = undefined, // never dereferenced: process() is not called
        .verifier = undefined,
    };
}

test "per-DID FIFO with drop-oldest: order kept, bound enforced, cursor floor correct" {
    var threaded = Io.Threaded.init(std.testing.allocator, .{});
    var p = testPipeline(&threaded);
    defer {
        p.chains.deinit(std.testing.allocator);
        p.inflight.deinit(std.testing.allocator);
    }

    // overfill one DID: seqs 1..(bound+3)
    const extra = 3;
    var seq: i64 = 1;
    while (seq <= MAX_PENDING_PER_DID + extra) : (seq += 1) {
        p.submit("did:plc:hot", seq, "frame");
    }
    // a second author is unaffected by the hot repo's shedding
    p.submit("did:plc:calm", 1000, "frame");

    try std.testing.expectEqual(@as(u64, extra), p.dropped.load(.monotonic));
    try std.testing.expectEqual(@as(usize, 2), p.chains.count());
    const hot = p.chains.get("did:plc:hot").?;
    try std.testing.expectEqual(@as(usize, MAX_PENDING_PER_DID), hot.len);
    // oldest 3 were shed → cursor floor is the oldest SURVIVING seq
    try std.testing.expectEqual(@as(i64, extra + 1), p.minInflight().?);

    // drain the hot chain the way a worker would; FIFO order must hold
    p.mutex.lockUncancelable(p.io);
    var expect_seq: i64 = extra + 1;
    while (p.takeItemLocked(hot)) |item| {
        try std.testing.expectEqual(expect_seq, item.seq);
        expect_seq += 1;
        p.completeItemLocked(item);
    }
    p.mutex.unlock(p.io);

    try std.testing.expectEqual(@as(u64, MAX_PENDING_PER_DID), p.processed.load(.monotonic));
    // hot chain retired from the map; calm remains, and pins the cursor floor
    try std.testing.expectEqual(@as(usize, 1), p.chains.count());
    try std.testing.expectEqual(@as(i64, 1000), p.minInflight().?);

    // drain calm too → nothing inflight, cursor floor releases
    p.mutex.lockUncancelable(p.io);
    const calm = p.chains.get("did:plc:calm").?;
    while (p.takeItemLocked(calm)) |item| p.completeItemLocked(item);
    p.mutex.unlock(p.io);
    try std.testing.expect(p.minInflight() == null);
    try std.testing.expectEqual(@as(usize, 0), p.chains.count());
}

test "ready list: chains queue once, pop in FIFO order" {
    var threaded = Io.Threaded.init(std.testing.allocator, .{});
    var p = testPipeline(&threaded);
    defer {
        p.chains.deinit(std.testing.allocator);
        p.inflight.deinit(std.testing.allocator);
    }

    p.submit("did:plc:a", 1, "f");
    p.submit("did:plc:a", 2, "f"); // second submit must not re-queue the chain
    p.submit("did:plc:b", 3, "f");

    p.mutex.lockUncancelable(p.io);
    const first = p.popReady().?;
    const second = p.popReady().?;
    try std.testing.expectEqualStrings("did:plc:a", first.did());
    try std.testing.expectEqualStrings("did:plc:b", second.did());
    try std.testing.expect(p.popReady() == null);
    // clean up items so the test allocator is happy
    for ([_]*Chain{ first, second }) |chain| {
        while (p.takeItemLocked(chain)) |item| p.completeItemLocked(item);
    }
    p.mutex.unlock(p.io);
}
