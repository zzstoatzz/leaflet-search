//! commit verification — DID signing-key resolution + signature/MST check.
//!
//! simplified from zlay's validator (zat.dev/zlay src/internal/validator.zig).
//! since the pipeline restructure (docs/scale-300k-plan.md §1e) this runs on
//! the verify worker pool, NOT the firehose read thread: a slow PLC resolve
//! stalls one worker while other authors keep verifying. one shared Verifier
//! serves all workers — the key cache is lock-guarded and bounded (evict-one
//! on overflow, NEVER clear-all: a cache reset mid-burst multiplies PLC
//! traffic into a resolve stampede — stream's verify.zig learned this at
//! network scale), counters are atomic, and signature crypto runs outside
//! the cache lock so workers actually parallelize.

const std = @import("std");
const Io = std.Io;
const zat = @import("zat");
const logfire = @import("logfire");

/// decoded signing key, ready for verification (mirrors zlay's CachedKey).
const CachedKey = struct {
    key_type: zat.multicodec.KeyType,
    raw: [33]u8, // compressed public key (secp256k1 or p256)
    len: u8,
    /// PDS hosted on brid.gy. policy since 7e1f071: bridgy fed content is
    /// never indexed — non-canonical commits are rejected as
    /// a side effect of full MST verification; our sig_only verdict admitted
    /// them and let ~20k scraper docs/day flood the corpus (2026-06-10).
    bridged: bool,
};

/// how a commit passed (or failed) verification.
pub const Verdict = enum {
    /// signature + MST diff inversion both check out.
    full,
    /// signature checks out but the MST diff math doesn't — occasional
    /// legit-PDS bursts land here. authorship is still proven, so records
    /// are emitted.
    sig_only,
    /// repo is hosted on brid.gy — dropped before verification, never emitted.
    bridged,
    /// signature verification failed (or no key / no blocks) — never emitted.
    rejected,
};

/// how long a verify worker waits for a DID resolution before giving up on
/// the commit. zig 0.16's http client has NO timeouts, so a wedged PLC
/// connection would otherwise freeze the worker forever (pre-pipeline it
/// froze the whole read loop: 2026-06-09 22:33 UTC — ~6 min of zero
/// ingestion until a restart). now it costs one worker one deadline.
const RESOLVE_DEADLINE_MS: u64 = 10_000;
const RESOLVE_POLL_MS: u64 = 50;

/// key-cache bound. ~100k entries ≈ 15MB. an archive-backfill influx means
/// many new authors; the bound is generous enough that eviction is rare, and
/// overflow evicts ONE arbitrary entry (a re-resolve for one author) rather
/// than clearing (a stampede for every author).
const MAX_CACHED_KEYS: usize = 100_000;

const TASK_RUNNING: u8 = 0;
const TASK_DONE: u8 = 1;
const TASK_ABANDONED: u8 = 2;

/// one in-flight DID resolution, heap-owned so it can outlive the firehose
/// thread's patience: whoever loses the state race (resolver thread finishing
/// vs firehose thread abandoning) is responsible for freeing it.
const ResolveTask = struct {
    io: Io,
    allocator: std.mem.Allocator,
    did_buf: [512]u8 = undefined,
    did_len: usize = 0,
    state: std.atomic.Value(u8) = std.atomic.Value(u8).init(TASK_RUNNING),
    ok: bool = false,
    key: CachedKey = undefined,

    fn run(task: *ResolveTask) void {
        task.ok = resolveKey(task.io, task.allocator, task.did_buf[0..task.did_len], &task.key);
        if (task.state.swap(TASK_DONE, .acq_rel) == TASK_ABANDONED) {
            task.allocator.destroy(task);
        }
    }
};

fn isBridgyPds(endpoint: []const u8) bool {
    var host = endpoint;
    if (std.mem.indexOf(u8, host, "://")) |i| host = host[i + 3 ..];
    if (std.mem.indexOfScalar(u8, host, '/')) |i| host = host[0..i];
    if (std.mem.indexOfScalar(u8, host, ':')) |i| host = host[0..i];
    return std.mem.eql(u8, host, "brid.gy") or std.mem.endsWith(u8, host, ".brid.gy");
}

/// resolve DID -> decoded signing key. fresh resolver per call: a resolver is
/// not thread-safe and an abandoned (hung) task must not poison a shared one.
fn resolveKey(io: Io, allocator: std.mem.Allocator, did: []const u8, out: *CachedKey) bool {
    const parsed = zat.Did.parse(did) orelse return false;
    var resolver = zat.DidResolver.initWithOptions(io, allocator, .{ .keep_alive = false });
    defer resolver.deinit();
    var doc = resolver.resolve(parsed) catch |err| {
        logfire.warn("verifier: DID resolve failed for {s}: {s}", .{ did, @errorName(err) });
        return false;
    };
    defer doc.deinit();

    const vm = doc.signingKey() orelse return false;
    const key_bytes = zat.multibase.decode(allocator, vm.public_key_multibase) catch return false;
    defer allocator.free(key_bytes);
    const public_key = zat.multicodec.parsePublicKey(key_bytes) catch return false;
    if (public_key.raw.len > 33) return false;

    const bridged = if (doc.pdsEndpoint()) |pds| isBridgyPds(pds) else false;
    out.* = .{ .key_type = public_key.key_type, .raw = undefined, .len = @intCast(public_key.raw.len), .bridged = bridged };
    @memcpy(out.raw[0..public_key.raw.len], public_key.raw);
    return true;
}

pub const Verifier = struct {
    io: Io,
    allocator: std.mem.Allocator,
    cache: std.StringHashMapUnmanaged(CachedKey) = .empty,
    // spinlock guarding `cache` only — critical sections are map lookups and
    // inserts, never network or crypto (channel.zig's idiom; Io.Mutex works
    // here too but the sections are tiny and uncontended).
    cache_locked: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),
    verified: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),
    sig_only: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),
    bridged: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),
    rejected: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),
    unresolvable: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),
    // racy by design: @errorName returns static strings, a stale read is fine
    last_err: []const u8 = "none",

    pub fn init(io: Io, allocator: std.mem.Allocator) Verifier {
        return .{ .io = io, .allocator = allocator };
    }

    fn lockCache(self: *Verifier) void {
        while (self.cache_locked.swap(true, .acquire)) std.atomic.spinLoopHint();
    }
    fn unlockCache(self: *Verifier) void {
        self.cache_locked.store(false, .release);
    }

    pub fn deinit(self: *Verifier) void {
        var it = self.cache.iterator();
        while (it.next()) |entry| self.allocator.free(entry.key_ptr.*);
        self.cache.deinit(self.allocator);
    }

    /// verify a commit's CAR bytes against the repo's signing key. callers
    /// must not emit records from commits that return `.rejected`.
    pub fn verifyCommit(self: *Verifier, commit: zat.firehose.CommitEvent) Verdict {
        if (commit.blocks.len == 0) {
            // no CAR to verify (e.g. legacy tooBig frames strip blocks).
            _ = self.rejected.fetchAdd(1, .monotonic);
            logfire.warn("verifier: no blocks for did={s} seq={d} too_big={}", .{ commit.repo, commit.seq, commit.too_big });
            return .rejected;
        }

        const key = self.signingKey(commit.repo, false) orelse {
            _ = self.unresolvable.fetchAdd(1, .monotonic);
            return .rejected;
        };

        if (key.bridged) {
            _ = self.bridged.fetchAdd(1, .monotonic);
            return .bridged;
        }

        if (self.verifyWithKey(commit, key)) |verdict| return self.count(verdict);

        // signature failure can mean the key rotated since we cached it:
        // re-resolve once and retry before rejecting (sync spec guidance,
        // same as zlay's evict + re-resolve, but inline since we're low-volume).
        const fresh = self.signingKey(commit.repo, true) orelse {
            _ = self.unresolvable.fetchAdd(1, .monotonic);
            return .rejected;
        };
        if (fresh.bridged) {
            _ = self.bridged.fetchAdd(1, .monotonic);
            return .bridged;
        }
        if (self.verifyWithKey(commit, fresh)) |verdict| return self.count(verdict);

        _ = self.rejected.fetchAdd(1, .monotonic);
        logfire.warn("verifier: rejected commit did={s} seq={d} rev={s} err={s}", .{ commit.repo, commit.seq, commit.rev, self.last_err });
        return .rejected;
    }

    fn count(self: *Verifier, verdict: Verdict) Verdict {
        switch (verdict) {
            .full => _ = self.verified.fetchAdd(1, .monotonic),
            .sig_only => _ = self.sig_only.fetchAdd(1, .monotonic),
            .bridged, .rejected => unreachable,
        }
        return verdict;
    }

    /// null = signature failure (caller retries with a fresh key).
    fn verifyWithKey(self: *Verifier, commit: zat.firehose.CommitEvent, key: CachedKey) ?Verdict {
        const public_key = zat.multicodec.PublicKey{
            .key_type = key.key_type,
            .raw = key.raw[0..key.len],
        };
        var arena = std.heap.ArenaAllocator.init(self.allocator);
        defer arena.deinit();
        const alloc = arena.allocator();

        // firehose frames carry a *diff* CAR (commit block + changed MST nodes
        // only), so verifyCommitCar's full MST walk can never succeed here.
        // verifyCommitDiff is the relay-grade check: signature over the commit,
        // then invert the frame's ops against the partial MST and require the
        // resulting root to match prev_data (sync 1.1).
        const ops = commit.toMstOperations(alloc) catch return null;
        const prev_data: ?[]const u8 = if (commit.prev_data) |pd| pd.raw else null;
        _ = zat.verifyCommitDiff(alloc, commit.blocks, ops, prev_data, public_key, .{
            .expected_did = commit.repo,
            .skip_inversion = prev_data == null,
        }) catch |err| {
            self.last_err = @errorName(err);
            return switch (err) {
                // MST-math failures surface only after the signature already
                // verified (step 4 precedes MST load/inversion), so authorship
                // is proven — occasional legit-PDS bursts land here (bridgy's
                // non-canonical repos used to, but they're dropped earlier now).
                error.PrevDataMismatch,
                error.InversionMismatch,
                error.MstRootMismatch,
                error.PartialTree,
                error.DuplicatePath,
                error.InvalidMstNode,
                => .sig_only,
                // signature / structural failures: let the caller retry with
                // a freshly-resolved key, then reject.
                else => null,
            };
        };
        return .full;
    }

    /// cached signing key for a DID, resolving via PLC/did:web on miss.
    /// `force` re-resolves even on a cache hit (key rotation path).
    ///
    /// resolution runs on a detached thread and we wait at most
    /// RESOLVE_DEADLINE_MS — the firehose read loop must never block
    /// indefinitely on network I/O. on timeout the commit is dropped
    /// (unresolvable) and the orphaned thread frees itself whenever the
    /// kernel finally gives up on its socket.
    fn signingKey(self: *Verifier, did: []const u8, force: bool) ?CachedKey {
        if (!force) {
            self.lockCache();
            const hit = self.cache.get(did);
            self.unlockCache();
            if (hit) |cached| return cached;
        }
        if (did.len > 512) return null;

        const task = self.allocator.create(ResolveTask) catch return null;
        task.* = .{ .io = self.io, .allocator = self.allocator, .did_len = did.len };
        @memcpy(task.did_buf[0..did.len], did);

        const thread = std.Thread.spawn(.{}, ResolveTask.run, .{task}) catch {
            self.allocator.destroy(task);
            return null;
        };
        thread.detach();

        var waited: u64 = 0;
        while (task.state.load(.acquire) != TASK_DONE and waited < RESOLVE_DEADLINE_MS) {
            self.io.sleep(Io.Duration.fromMilliseconds(RESOLVE_POLL_MS), .awake) catch {};
            waited += RESOLVE_POLL_MS;
        }

        if (task.state.load(.acquire) != TASK_DONE) {
            // raced abandon: if the task finished between the check and the
            // swap, it's ours to consume after all.
            if (task.state.swap(TASK_ABANDONED, .acq_rel) != TASK_DONE) {
                logfire.warn("verifier: DID resolve timed out for {s} after {d}ms", .{ did, RESOLVE_DEADLINE_MS });
                return null;
            }
        }

        defer self.allocator.destroy(task);
        if (!task.ok) return null;
        const cached = task.key;

        self.lockCache();
        defer self.unlockCache();
        if (self.cache.count() >= MAX_CACHED_KEYS) {
            // evict ONE arbitrary entry; never clear-all (resolve stampede)
            var it = self.cache.iterator();
            if (it.next()) |victim| {
                const victim_key = victim.key_ptr.*;
                _ = self.cache.remove(victim_key);
                self.allocator.free(victim_key);
            }
        }
        const gop = self.cache.getOrPut(self.allocator, did) catch return cached;
        if (!gop.found_existing) {
            gop.key_ptr.* = self.allocator.dupe(u8, did) catch {
                _ = self.cache.remove(did);
                return cached;
            };
        }
        gop.value_ptr.* = cached;
        return cached;
    }

    /// drop a DID's cached key (on #identity events — key may have rotated).
    pub fn evict(self: *Verifier, did: []const u8) void {
        self.lockCache();
        defer self.unlockCache();
        if (self.cache.fetchRemove(did)) |kv| self.allocator.free(kv.key);
    }
};
