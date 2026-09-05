//! Corpus admission policy shared by every write path: the ingester drops
//! these at the firehose, the indexer drops them on insert (replays,
//! backfills), and the snapshot builder excludes them at build time —
//! turso may still hold historical rows for a banned DID until the paced
//! cleanup finishes, and a snapshot must never resurrect them.

const std = @import("std");

/// Bulk-archive repos that never enter the corpus. The list is the single
/// source of truth in `/banned-dids.txt` (repo root), wired in via build.zig
/// and parsed here at comptime. Registry of who/why/evidence: docs/exclusions.md.
pub const BANNED_ENTRIES = parseBannedEntries(@embedFile("banned_dids"));
pub const BANNED_DIDS = blk: {
    var list: []const []const u8 = &.{};
    for (BANNED_ENTRIES) |e| list = list ++ &[_][]const u8{e.did};
    break :blk list;
};

pub const BannedEntry = struct {
    did: []const u8,
    /// the inline '# comment' from banned-dids.txt (e.g. the site domain) — ""
    /// when absent. Used by the labeler seed so /labels can say who this is.
    note: []const u8,
};

/// Parse the shared banned-dids.txt: one DID per line, '#' starts a comment
/// (whole-line or inline; an inline comment is kept as the entry's note),
/// blank lines ignored.
fn parseBannedEntries(comptime data: []const u8) []const BannedEntry {
    comptime {
        @setEvalBranchQuota(100_000);
        var list: []const BannedEntry = &.{};
        var lines = std.mem.splitScalar(u8, data, '\n');
        while (lines.next()) |raw| {
            const hash = std.mem.indexOfScalar(u8, raw, '#');
            const code = if (hash) |h| raw[0..h] else raw;
            const did = std.mem.trim(u8, code, " \t\r");
            if (did.len == 0) continue;
            const note = if (hash) |h| std.mem.trim(u8, raw[h + 1 ..], " \t\r") else "";
            list = list ++ &[_]BannedEntry{.{ .did = did, .note = note }};
        }
        return list;
    }
}

/// Accounts pub-search deliberately KEEPS despite a true bulk-generated label
/// (taste is allowed in consumer policy, never in the label itself). The
/// hard-drop must skip these; /labels shows them as labeled · kept.
/// Source of truth: /kept-dids.txt (repo root).
pub const KEPT_ENTRIES = parseBannedEntries(@embedFile("kept_dids"));

pub fn isKept(did: []const u8) bool {
    for (KEPT_ENTRIES) |e| {
        if (std.mem.eql(u8, did, e.did)) return true;
    }
    return false;
}

pub fn isBanned(did: []const u8) bool {
    for (BANNED_DIDS) |banned| {
        if (std.mem.eql(u8, did, banned)) return true;
    }
    return false;
}

/// Comptime SQL fragment: `'did1','did2'` — for `did NOT IN (...)` clauses.
pub const banned_dids_sql = blk: {
    var out: []const u8 = "";
    for (BANNED_DIDS, 0..) |did, i| {
        out = out ++ (if (i == 0) "'" else ",'") ++ did ++ "'";
    }
    break :blk out;
};

test "isBanned matches only the banned list" {
    try std.testing.expect(isBanned("did:plc:oql6ds5vnff4ugar6rruliwd"));
    try std.testing.expect(isBanned("did:plc:2s32mlusc66sjb256aenynfc"));
    try std.testing.expect(isBanned("did:plc:llnmp5t7s3u4dzjqyhp76h62"));
    try std.testing.expect(isBanned("did:plc:6dis2hlap2lhwohabb2tniln"));
    try std.testing.expect(!isBanned("did:plc:ragtjsm2j2vknwkz3zp4oxrd"));
}

test "kept list: labeled-but-kept accounts, disjoint from banned" {
    try std.testing.expect(isKept("did:plc:4z33k5fjzw2ew3u373pg7ku5")); // thefestivusproject
    for (KEPT_ENTRIES) |e| try std.testing.expect(!isBanned(e.did)); // kept ∩ banned = ∅
}

test "banned entries carry their inline notes" {
    try std.testing.expect(BANNED_ENTRIES.len == BANNED_DIDS.len);
    try std.testing.expectEqualStrings("drivepatents.com patent bot", BANNED_ENTRIES[0].note);
}

test "banned_dids_sql is a quoted comma-separated list" {
    try std.testing.expectEqualStrings(
        "'did:plc:oql6ds5vnff4ugar6rruliwd','did:plc:2s32mlusc66sjb256aenynfc','did:plc:llnmp5t7s3u4dzjqyhp76h62','did:plc:6dis2hlap2lhwohabb2tniln'",
        banned_dids_sql,
    );
}
