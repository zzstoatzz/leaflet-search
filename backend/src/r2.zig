//! R2 object transport via rclone (shelled out), adapted from typeahead's
//! r2.zig. rclone is trusted ONLY for transport; correctness is enforced by
//! the manifest's sha256 gate on the consuming side.
//!
//! Config: we write a minimal rclone.conf defining remote `r2` from the
//! INDEX_R2_* secrets (same names as typeahead — one ops vocabulary) and pass
//! it via `--config` on every invocation. Env-var remotes don't work here:
//! Zig's process spawn uses the startup env snapshot, so runtime setenv never
//! reaches the child. A --config arg always propagates, and keeps secrets out
//! of argv/logs.

const std = @import("std");
const Io = std.Io;
const Allocator = std.mem.Allocator;
const logfire = @import("logfire");

const CONFIG_PATH = "/tmp/rclone-r2.conf";

pub const Error = error{ MissingR2Config, RcloneFailed };

pub const Config = struct {
    bucket: []const u8,
    config_path: []const u8,
};

/// Write the rclone config from INDEX_R2_* env. Idempotent.
pub fn configure(gpa: Allocator, io: Io) Error!Config {
    const endpoint = std.c.getenv("INDEX_R2_ENDPOINT") orelse return Error.MissingR2Config;
    const bucket = std.c.getenv("INDEX_R2_BUCKET") orelse return Error.MissingR2Config;
    const akid = std.c.getenv("INDEX_R2_ACCESS_KEY_ID") orelse return Error.MissingR2Config;
    const secret = std.c.getenv("INDEX_R2_SECRET_ACCESS_KEY") orelse return Error.MissingR2Config;

    const content = std.fmt.allocPrint(gpa,
        \\[r2]
        \\type = s3
        \\provider = Cloudflare
        \\acl = private
        \\no_check_bucket = true
        \\region = auto
        \\endpoint = {s}
        \\access_key_id = {s}
        \\secret_access_key = {s}
        \\
    , .{ std.mem.span(endpoint), std.mem.span(akid), std.mem.span(secret) }) catch return Error.MissingR2Config;
    defer gpa.free(content);

    writeConfig(io, content) catch |err| {
        logfire.err("r2: failed to write rclone config: {s}", .{@errorName(err)});
        return Error.RcloneFailed;
    };

    return .{ .bucket = std.mem.span(bucket), .config_path = CONFIG_PATH };
}

fn writeConfig(io: Io, content: []const u8) !void {
    const file = try Io.Dir.createFileAbsolute(io, CONFIG_PATH, .{ .truncate = true });
    defer file.close(io);
    var wbuf: [256]u8 = undefined;
    var fw = Io.File.Writer.init(file, io, &wbuf);
    try fw.interface.writeAll(content);
    try fw.interface.flush();
}

fn rcloneBin() []const u8 {
    return if (std.c.getenv("RCLONE")) |p| std.mem.span(p) else "rclone";
}

fn run(io: Io, argv: []const []const u8) Error!void {
    var child = std.process.spawn(io, .{
        .argv = argv,
        .stdout = .inherit,
        .stderr = .inherit,
    }) catch |err| {
        logfire.err("r2: rclone spawn failed: {s}", .{@errorName(err)});
        return Error.RcloneFailed;
    };
    const term = child.wait(io) catch |err| {
        logfire.err("r2: rclone wait failed: {s}", .{@errorName(err)});
        return Error.RcloneFailed;
    };
    switch (term) {
        .exited => |code| if (code != 0) {
            logfire.err("r2: rclone exited with code {d}", .{code});
            return Error.RcloneFailed;
        },
        else => return Error.RcloneFailed,
    }
}

/// Upload local_path → r2:<bucket>/<object_key>.
pub fn upload(gpa: Allocator, io: Io, cfg: Config, local_path: []const u8, object_key: []const u8) !void {
    const remote = try std.fmt.allocPrint(gpa, "r2:{s}/{s}", .{ cfg.bucket, object_key });
    defer gpa.free(remote);
    logfire.info("r2: upload {s} -> {s}", .{ local_path, remote });
    try run(io, &.{ rcloneBin(), "--config", cfg.config_path, "copyto", local_path, remote });
}

/// Download r2:<bucket>/<object_key> → local_path.
pub fn download(gpa: Allocator, io: Io, cfg: Config, object_key: []const u8, local_path: []const u8) !void {
    const remote = try std.fmt.allocPrint(gpa, "r2:{s}/{s}", .{ cfg.bucket, object_key });
    defer gpa.free(remote);
    logfire.info("r2: download {s} -> {s}", .{ remote, local_path });
    try run(io, &.{ rcloneBin(), "--config", cfg.config_path, "copyto", remote, local_path });
}

/// Bandwidth-capped download (rclone --bwlimit). For bulk pulls on a serving
/// box: an uncapped snapshot download saturates the single shared vCPU and
/// evicts the page cache, turning sub-ms FTS queries into tens of seconds
/// (2026-08-05). `bwlimit` uses rclone units, e.g. "12M" = 12 MiB/s.
pub fn downloadPaced(gpa: Allocator, io: Io, cfg: Config, object_key: []const u8, local_path: []const u8, bwlimit: []const u8) !void {
    const remote = try std.fmt.allocPrint(gpa, "r2:{s}/{s}", .{ cfg.bucket, object_key });
    defer gpa.free(remote);
    logfire.info("r2: download {s} -> {s} (bwlimit={s})", .{ remote, local_path, bwlimit });
    try run(io, &.{ rcloneBin(), "--config", cfg.config_path, "--bwlimit", bwlimit, "copyto", remote, local_path });
}
