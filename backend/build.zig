const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    const websocket = b.dependency("websocket", .{
        .target = target,
        .optimize = optimize,
    });

    const zql = b.dependency("zql", .{
        .target = target,
        .optimize = optimize,
    });

    const zat = b.dependency("zat", .{
        .target = target,
        .optimize = optimize,
    });

    const zqlite = b.dependency("zqlite", .{
        .target = target,
        .optimize = optimize,
        .sqlite3 = &[_][]const u8{ "-std=c99", "-DSQLITE_ENABLE_FTS5", "-DSQLITE_ENABLE_MATH_FUNCTIONS" },
    });

    const logfire = b.dependency("logfire", .{
        .target = target,
        .optimize = optimize,
    });

    const zug = b.dependency("zug", .{
        .target = target,
        .optimize = optimize,
    });

    const poolio = b.dependency("poolio", .{
        .target = target,
        .optimize = optimize,
    });

    const jetstream = b.dependency("jetstream", .{
        .target = target,
        .optimize = optimize,
    });

    const imports: []const std.Build.Module.Import = &.{
        .{ .name = "websocket", .module = websocket.module("websocket") },
        .{ .name = "jetstream", .module = jetstream.module("jetstream") },
        .{ .name = "zql", .module = zql.module("zql") },
        .{ .name = "zat", .module = zat.module("zat") },
        .{ .name = "zqlite", .module = zqlite.module("zqlite") },
        .{ .name = "logfire", .module = logfire.module("logfire") },
        .{ .name = "zug", .module = zug.module("zug") },
        .{ .name = "poolio", .module = poolio.module("poolio") },
    };

    const exe = b.addExecutable(.{
        .name = "pub-search",
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/main.zig"),
            .target = target,
            .optimize = optimize,
            .link_libc = true,
            .imports = imports,
        }),
    });

    // single source of truth for banned DIDs (repo root), embedded at comptime
    // by policy.zig. mirrored at runtime by scripts/purge-*. see banned-dids.txt.
    const banned_dids_mod = b.createModule(.{ .root_source_file = b.path("../banned-dids.txt") });
    exe.root_module.addImport("banned_dids", banned_dids_mod);
    const kept_dids_mod = b.createModule(.{ .root_source_file = b.path("../kept-dids.txt") });
    exe.root_module.addImport("kept_dids", kept_dids_mod);

    b.installArtifact(exe);

    const run_cmd = b.addRunArtifact(exe);
    run_cmd.step.dependOn(b.getInstallStep());
    if (b.args) |args| {
        run_cmd.addArgs(args);
    }

    const run_step = b.step("run", "Run the server");
    run_step.dependOn(&run_cmd.step);

    // tests — rooted at main.zig so all transitive imports are discovered
    const unit_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/main.zig"),
            .target = target,
            .optimize = optimize,
            .link_libc = true,
            .imports = imports,
        }),
    });

    unit_tests.root_module.addImport("banned_dids", banned_dids_mod);
    unit_tests.root_module.addImport("kept_dids", kept_dids_mod);

    const run_tests = b.addRunArtifact(unit_tests);
    const test_step = b.step("test", "Run unit tests");
    test_step.dependOn(&run_tests.step);
}
