//! Turso HTTP API client
//! https://docs.turso.tech/sdk/http/reference

const std = @import("std");
const Io = std.Io;
const http = std.http;
const json = std.json;
const mem = std.mem;
const Allocator = mem.Allocator;
const logfire = @import("logfire");

const result = @import("result.zig");
pub const Result = result.Result;
pub const Row = result.Row;
pub const BatchResult = result.BatchResult;

const Client = @This();

// Hrana protocol types (https://github.com/tursodatabase/libsql/blob/main/docs/HRANA_3_SPEC.md)
//
// `WireValue` is the JSON-serialized arg shape Turso expects on the wire. The
// `value` field is always a string even for integers — the `type` tag tells
// Turso how to coerce it. `null` values are emitted with no `value` field via
// the `emit_null_optional_fields = false` JSON option.
const WireValue = struct { type: []const u8 = "text", value: ?[]const u8 = null };
const Stmt = struct { sql: []const u8, args: ?[]const WireValue = null };
const ExecuteReq = struct { type: []const u8 = "execute", stmt: Stmt };
const CloseReq = struct { type: []const u8 = "close" };

/// A runtime-typed bind value for the migration / dynamic-SQL path.
///
/// The comptime `query`/`exec` API only ever sends `text` values (it accepts
/// `[]const u8` args), so it doesn't need this. zug-style migrations bind
/// `i64` directly for things like the `dirty INTEGER` column, so the runtime
/// path needs a typed shape.
pub const RuntimeValue = union(enum) {
    text: []const u8,
    int: i64,
    null_value,
};

const URL_BUF_SIZE = 512;
const AUTH_BUF_SIZE = 512;

/// Wall-clock second of the most recent turso request COMPLETION (success or
/// error — either proves the connection isn't hung). The builder's stall
/// watchdog reads this to tell "slow but progressing" from "hung forever":
/// zig's HTTP client has no read timeout, so a socket that dies after the
/// request write blocks doRequest indefinitely (2026-08-06: export sat silent
/// for 94min at 35k docs until the wall-clock watchdog killed the build).
pub var last_request_completed_s: std.atomic.Value(i64) = .init(0);

fn nowSeconds(io: Io) i64 {
    return @intCast(@divFloor(Io.Timestamp.now(io, .real).nanoseconds, std.time.ns_per_s));
}

allocator: Allocator,
url: []const u8,
token: []const u8,
io: Io,
// std.http.Client is internally thread-safe at the connection-pool level
// (see stdlib's http/Client.zig:3) so concurrent fetches share the pool but
// each acquires its own connection. nothing else on this struct is mutable
// after init, so no caller-side serialization is needed.
http_client: http.Client,

pub fn init(allocator_param: Allocator, io: Io) !Client {
    const url = if (std.c.getenv("TURSO_URL")) |p| std.mem.span(p) else {
        std.debug.print("TURSO_URL not set\n", .{});
        return error.MissingEnv;
    };
    const token = if (std.c.getenv("TURSO_TOKEN")) |p| std.mem.span(p) else {
        std.debug.print("TURSO_TOKEN not set\n", .{});
        return error.MissingEnv;
    };

    const libsql_prefix = "libsql://";
    const host = if (mem.startsWith(u8, url, libsql_prefix))
        url[libsql_prefix.len..]
    else
        url;

    std.debug.print("turso client initialized: {s}\n", .{host});

    return .{
        .allocator = allocator_param,
        .url = host,
        .token = token,
        .io = io,
        .http_client = .{ .allocator = allocator_param, .io = io },
    };
}

pub fn deinit(self: *Client) void {
    self.http_client.deinit();
}

pub fn query(self: *Client, comptime sql: []const u8, args: anytype) !Result {
    comptime validateArgs(sql, @TypeOf(args));
    const args_slice = try self.argsToSlice(args);
    defer self.allocator.free(args_slice);
    const response = try self.executeRaw(sql, args_slice);
    defer self.allocator.free(response);
    return Result.parse(self.allocator, response);
}

pub fn exec(self: *Client, comptime sql: []const u8, args: anytype) !void {
    comptime validateArgs(sql, @TypeOf(args));
    const args_slice = try self.argsToSlice(args);
    defer self.allocator.free(args_slice);
    const response = try self.executeRaw(sql, args_slice);
    self.allocator.free(response);
}

/// Execute a statement with a runtime-built SQL string and typed args.
///
/// Use this for code paths where the SQL or arg shape isn't known at compile
/// time — schema migrations, parameterized table-name queries, etc. Existing
/// handler code should keep using the comptime `exec` / `query`.
pub fn execRuntime(self: *Client, sql: []const u8, args: []const RuntimeValue) !void {
    const response = try self.executeRawTyped(sql, args);
    self.allocator.free(response);
}

/// Read rows with a runtime-built SQL string and typed args.
///
/// Same use-case as `execRuntime`. Caller deinits the returned `Result`.
pub fn queryRuntime(self: *Client, sql: []const u8, args: []const RuntimeValue) !Result {
    const response = try self.executeRawTyped(sql, args);
    defer self.allocator.free(response);
    return Result.parse(self.allocator, response);
}

pub const Statement = struct {
    sql: []const u8,
    args: []const []const u8 = &.{},
};

pub fn queryBatch(self: *Client, statements: []const Statement) !BatchResult {
    const response = try self.executeBatchRaw(statements);
    defer self.allocator.free(response);
    return BatchResult.parse(self.allocator, response, statements.len);
}

fn argsToSlice(self: *Client, args: anytype) ![]const []const u8 {
    const ArgsType = @TypeOf(args);
    const info = @typeInfo(ArgsType);

    if (info == .pointer) {
        const child = @typeInfo(info.pointer.child);
        if (child == .@"struct") {
            const fields = child.@"struct".fields;
            const slice = try self.allocator.alloc([]const u8, fields.len);
            inline for (fields, 0..) |field, i| {
                slice[i] = @field(args.*, field.name);
            }
            return slice;
        }
    }

    if (info == .@"struct") {
        const fields = info.@"struct".fields;
        const slice = try self.allocator.alloc([]const u8, fields.len);
        inline for (fields, 0..) |field, i| {
            slice[i] = @field(args, field.name);
        }
        return slice;
    }

    @compileError("args must be a tuple or pointer to tuple");
}

fn executeRaw(self: *Client, sql: []const u8, args: []const []const u8) ![]const u8 {
    const body = try self.buildRequestBody(sql, args);
    defer self.allocator.free(body);
    const span = logfire.span("db.query", .{
        .sql = truncateSql(sql),
        .args_count = @as(i64, @intCast(args.len)),
    });
    return self.doRequest(span, "db.query", sql, body);
}

fn executeBatchRaw(self: *Client, statements: []const Statement) ![]const u8 {
    const body = try self.buildBatchRequestBody(statements);
    defer self.allocator.free(body);
    const first_sql = if (statements.len > 0) statements[0].sql else "";
    // preserve the original batch span attribute names (`statement_count`,
    // `first_sql`) so any existing logfire dashboards/alerts keep working
    const span = logfire.span("db.batch", .{
        .statement_count = @as(i64, @intCast(statements.len)),
        .first_sql = truncateSql(first_sql),
    });
    return self.doRequest(span, "db.batch", first_sql, body);
}

fn executeRawTyped(self: *Client, sql: []const u8, args: []const RuntimeValue) ![]const u8 {
    const body = try buildTypedRequestBody(self.allocator, sql, args);
    defer self.allocator.free(body);
    const span = logfire.span("db.query", .{
        .sql = truncateSql(sql),
        .args_count = @as(i64, @intCast(args.len)),
    });
    return self.doRequest(span, "db.query", sql, body);
}

/// Shared HTTP path for all three execute variants.
///
/// The caller creates the span (with whatever attribute shape its dashboards
/// expect) and passes it in; `doRequest` is responsible for `end()`,
/// `recordError`, and decorating with the turso response details on failure.
/// `span_name` is used only for log-message prefixes. `sql_for_log` is the
/// already-untruncated SQL string used for log lines (truncated inside).
fn doRequest(self: *Client, span: logfire.Span, span_name: []const u8, sql_for_log: []const u8, body: []const u8) ![]const u8 {
    const truncated = truncateSql(sql_for_log);
    defer span.end();
    defer last_request_completed_s.store(nowSeconds(self.io), .monotonic);

    var url_buf: [URL_BUF_SIZE]u8 = undefined;
    const url = std.fmt.bufPrint(&url_buf, "https://{s}/v2/pipeline", .{self.url}) catch
        return error.UrlTooLong;

    var auth_buf: [AUTH_BUF_SIZE]u8 = undefined;
    const auth = std.fmt.bufPrint(&auth_buf, "Bearer {s}", .{self.token}) catch
        return error.AuthTooLong;

    var response_body: std.Io.Writer.Allocating = .init(self.allocator);
    errdefer response_body.deinit();

    const res = fetchEvictRetry(&self.http_client, self.io, .{
        .location = .{ .url = url },
        .method = .POST,
        .headers = .{
            .content_type = .{ .override = "application/json" },
            .authorization = .{ .override = auth },
        },
        .payload = body,
        .response_writer = &response_body.writer,
        // Batch builders favor bounded completion over connection reuse. Zig's
        // HTTP client has no read timeout, so a silently-dead pooled socket can
        // accept the request write and then wait forever for a response.
        .keep_alive = std.c.getenv("TURSO_DISABLE_KEEPALIVE") == null,
    }) catch |err| {
        logfire.err("{s} http failed: {s} | sql: {s}", .{ span_name, @errorName(err), truncated });
        span.recordError(error.HttpError);
        return error.HttpError;
    };

    if (res.status != .ok) {
        // `catch ""` is intentional: response_body is only consumed here for
        // the diagnostic log/span attribute. If OOM strikes during
        // toOwnedSlice we still want to surface the TursoError with whatever
        // context we have (empty preview is better than swallowing the
        // status code).
        const resp_text = response_body.toOwnedSlice() catch "";
        defer if (resp_text.len > 0) self.allocator.free(resp_text);
        const resp_preview = if (resp_text.len > 200) resp_text[0..200] else resp_text;
        logfire.err("{s} turso error: {} | sql: {s} | response: {s}", .{ span_name, res.status, truncated, resp_preview });
        span.recordError(error.TursoError);
        span.setAttribute("turso.status", @intFromEnum(res.status));
        // NEVER attach resp_preview to the span: it's freed by the defer
        // above before `defer span.end()` (declared earlier, runs later)
        // deep-copies attributes — use-after-free, segfault, process death.
        // The log line above carries the preview; the span gets the status.
        return error.TursoError;
    }

    return try response_body.toOwnedSlice();
}

/// `fetch` with a one-shot retry when a pooled keep-alive connection turns
/// out to be dead (turso / fly NAT close idle connections; the pool has no
/// liveness check). Works around ziglang/zig#21316: std.http.Client releases
/// a connection whose *send* failed back into the pool as reusable
/// (Request.deinit sees reader.state == .ready), so without eviction every
/// subsequent request pops the same dead connection and fails forever —
/// this wedged prod for 15min on 2026-07-06 (WriteFailed burst alert).
/// MRE: repros/zig-21316-pool-poisoning.zig.
///
/// Both retryable errors fire before any response bytes are written, so
/// retrying cannot duplicate output into `response_writer`. Retrying the
/// POST is safe for the same reason curl/Go do it: the server never began
/// processing a request it didn't finish reading.
fn fetchEvictRetry(http_client: *http.Client, io: Io, options: http.Client.FetchOptions) http.Client.FetchError!http.Client.FetchResult {
    return http_client.fetch(options) catch |err| switch (err) {
        error.WriteFailed, error.HttpConnectionClosing => {
            evictIdleConnections(http_client, io);
            return http_client.fetch(options);
        },
        else => err,
    };
}

/// Close and free every idle pooled connection. Mirrors what
/// ConnectionPool.deinit does for the free list (ConnectionPool.resize
/// doesn't compile in zig 0.16 — it predates the intrusive-list rework).
/// In-flight connections (`used` list) are untouched.
fn evictIdleConnections(http_client: *http.Client, io: Io) void {
    const pool = &http_client.connection_pool;
    pool.mutex.lockUncancelable(io);
    defer pool.mutex.unlock(io);
    while (pool.free.popFirst()) |node| {
        const connection: *http.Client.Connection = @alignCast(@fieldParentPtr("pool_node", node));
        connection.destroy(io);
    }
    pool.free_len = 0;
}

fn buildBatchRequestBody(self: *Client, statements: []const Statement) ![]const u8 {
    var body: std.Io.Writer.Allocating = .init(self.allocator);
    errdefer body.deinit();
    var jw: json.Stringify = .{ .writer = &body.writer, .options = .{ .emit_null_optional_fields = false } };

    try jw.beginObject();
    try jw.objectField("requests");
    try jw.beginArray();

    for (statements) |stmt| {
        const values = try self.toValues(stmt.args);
        defer self.allocator.free(values);
        try jw.write(ExecuteReq{
            .stmt = .{ .sql = stmt.sql, .args = if (values.len > 0) values else null },
        });
    }

    try jw.write(CloseReq{});
    try jw.endArray();
    try jw.endObject();

    return try body.toOwnedSlice();
}

fn buildRequestBody(self: *Client, sql: []const u8, args: []const []const u8) ![]const u8 {
    var body: std.Io.Writer.Allocating = .init(self.allocator);
    errdefer body.deinit();
    var jw: json.Stringify = .{ .writer = &body.writer, .options = .{ .emit_null_optional_fields = false } };

    const values = try self.toValues(args);
    defer self.allocator.free(values);

    try jw.beginObject();
    try jw.objectField("requests");
    try jw.beginArray();
    try jw.write(ExecuteReq{
        .stmt = .{ .sql = sql, .args = if (values.len > 0) values else null },
    });
    try jw.write(CloseReq{});
    try jw.endArray();
    try jw.endObject();

    return try body.toOwnedSlice();
}

/// Build a Hrana request body for a single statement with typed runtime args.
///
/// Differs from `buildRequestBody` only in that integer args are emitted with
/// `type: "integer"` (Turso requires the value to be a string-formatted decimal
/// even for integers — the `type` tag is what coerces it back).
fn buildTypedRequestBody(allocator: Allocator, sql: []const u8, args: []const RuntimeValue) ![]const u8 {
    var body: std.Io.Writer.Allocating = .init(allocator);
    errdefer body.deinit();
    var jw: json.Stringify = .{ .writer = &body.writer, .options = .{ .emit_null_optional_fields = false } };

    const values = try toTypedValues(allocator, args);
    defer freeTypedValues(allocator, values);

    try jw.beginObject();
    try jw.objectField("requests");
    try jw.beginArray();
    try jw.write(ExecuteReq{
        .stmt = .{ .sql = sql, .args = if (values.len > 0) values else null },
    });
    try jw.write(CloseReq{});
    try jw.endArray();
    try jw.endObject();

    return try body.toOwnedSlice();
}

fn toValues(self: *Client, args: []const []const u8) ![]const WireValue {
    if (args.len == 0) return &.{};
    const values = try self.allocator.alloc(WireValue, args.len);
    for (args, 0..) |arg, i| {
        values[i] = .{ .value = arg };
    }
    return values;
}

/// Convert typed `RuntimeValue` args into Hrana `WireValue` shapes.
///
/// Integer values are formatted into freshly-allocated decimal strings; the
/// caller frees them via `freeTypedValues` once the JSON body has been built.
fn toTypedValues(allocator: Allocator, args: []const RuntimeValue) ![]const WireValue {
    if (args.len == 0) return &.{};
    const values = try allocator.alloc(WireValue, args.len);
    errdefer allocator.free(values);

    var i: usize = 0;
    errdefer for (values[0..i]) |v| {
        if (std.mem.eql(u8, v.type, "integer")) if (v.value) |s| allocator.free(s);
    };

    while (i < args.len) : (i += 1) {
        values[i] = switch (args[i]) {
            .text => |s| .{ .type = "text", .value = s },
            .int => |n| blk: {
                const s = try std.fmt.allocPrint(allocator, "{d}", .{n});
                break :blk .{ .type = "integer", .value = s };
            },
            .null_value => .{ .type = "null", .value = null },
        };
    }
    return values;
}

fn freeTypedValues(allocator: Allocator, values: []const WireValue) void {
    for (values) |v| {
        if (std.mem.eql(u8, v.type, "integer")) if (v.value) |s| allocator.free(s);
    }
    allocator.free(values);
}

fn validateArgs(comptime sql: []const u8, comptime ArgsType: type) void {
    // Long upserts still need the compile-time placeholder/argument check;
    // allow the linear SQL scan to exceed Zig's small default branch quota.
    @setEvalBranchQuota(10_000);
    const expected = countPlaceholders(sql);
    const provided = countArgsType(ArgsType);
    if (expected != provided) {
        @compileError(std.fmt.comptimePrint(
            "SQL has {} placeholders but {} args provided",
            .{ expected, provided },
        ));
    }
}

fn countPlaceholders(comptime sql: []const u8) usize {
    var count: usize = 0;
    for (sql) |c| {
        if (c == '?') count += 1;
    }
    return count;
}

fn countArgsType(comptime ArgsType: type) usize {
    const info = @typeInfo(ArgsType);

    if (info == .pointer) {
        const child = @typeInfo(info.pointer.child);
        if (child == .@"struct") {
            return child.@"struct".fields.len;
        }
    }

    if (info == .@"struct") {
        return info.@"struct".fields.len;
    }

    return 0;
}

fn truncateSql(sql: []const u8) []const u8 {
    const max_len = 100;
    return if (sql.len > max_len) sql[0..max_len] else sql;
}

// --- keepalive ---

const KEEPALIVE_INTERVAL_NS: u64 = 3 * 60 * std.time.ns_per_s; // 3 minutes

pub fn startKeepalive(self: *Client) void {
    const thread = std.Thread.spawn(.{}, keepaliveLoop, .{self}) catch |err| {
        logfire.warn("turso: failed to start keepalive thread: {}", .{err});
        return;
    };
    thread.detach();
    logfire.info("turso: keepalive started (interval=3m)", .{});
}

fn keepaliveLoop(self: *Client) void {
    while (true) {
        self.io.sleep(Io.Duration.fromNanoseconds(KEEPALIVE_INTERVAL_NS), .awake) catch return;
        _ = self.exec("SELECT 1", .{}) catch |err| {
            logfire.debug("turso: keepalive ping failed: {}", .{err});
        };
    }
}

// --- tests ---

test "toTypedValues: empty args returns empty slice" {
    const values = try toTypedValues(std.testing.allocator, &.{});
    defer freeTypedValues(std.testing.allocator, values);
    try std.testing.expectEqual(@as(usize, 0), values.len);
}

test "toTypedValues: text values pass through" {
    const args = [_]RuntimeValue{ .{ .text = "hello" }, .{ .text = "world" } };
    const values = try toTypedValues(std.testing.allocator, &args);
    defer freeTypedValues(std.testing.allocator, values);

    try std.testing.expectEqual(@as(usize, 2), values.len);
    try std.testing.expectEqualStrings("text", values[0].type);
    try std.testing.expectEqualStrings("hello", values[0].value.?);
    try std.testing.expectEqualStrings("text", values[1].type);
    try std.testing.expectEqualStrings("world", values[1].value.?);
}

test "toTypedValues: int values become decimal strings tagged integer" {
    const args = [_]RuntimeValue{ .{ .int = 42 }, .{ .int = -17 }, .{ .int = 0 } };
    const values = try toTypedValues(std.testing.allocator, &args);
    defer freeTypedValues(std.testing.allocator, values);

    try std.testing.expectEqual(@as(usize, 3), values.len);
    try std.testing.expectEqualStrings("integer", values[0].type);
    try std.testing.expectEqualStrings("42", values[0].value.?);
    try std.testing.expectEqualStrings("integer", values[1].type);
    try std.testing.expectEqualStrings("-17", values[1].value.?);
    try std.testing.expectEqualStrings("integer", values[2].type);
    try std.testing.expectEqualStrings("0", values[2].value.?);
}

test "toTypedValues: null_value emits null type with no value" {
    const args = [_]RuntimeValue{.null_value};
    const values = try toTypedValues(std.testing.allocator, &args);
    defer freeTypedValues(std.testing.allocator, values);

    try std.testing.expectEqual(@as(usize, 1), values.len);
    try std.testing.expectEqualStrings("null", values[0].type);
    try std.testing.expect(values[0].value == null);
}

test "toTypedValues: mixed types (zug INSERT shape)" {
    // Mirrors zug's `insertMigration` call: 4 strings + 1 int.
    const args = [_]RuntimeValue{
        .{ .text = "001_initial" },
        .{ .text = "initial schema" },
        .{ .text = "abc123" },
        .{ .text = "startup" },
        .{ .int = 0 },
    };
    const values = try toTypedValues(std.testing.allocator, &args);
    defer freeTypedValues(std.testing.allocator, values);

    try std.testing.expectEqual(@as(usize, 5), values.len);
    try std.testing.expectEqualStrings("text", values[0].type);
    try std.testing.expectEqualStrings("text", values[3].type);
    try std.testing.expectEqualStrings("integer", values[4].type);
    try std.testing.expectEqualStrings("0", values[4].value.?);
}

test "buildTypedRequestBody: emits valid Hrana JSON with mixed types" {
    const args = [_]RuntimeValue{ .{ .text = "abc" }, .{ .int = 1 } };
    const body = try buildTypedRequestBody(std.testing.allocator, "INSERT INTO t VALUES (?, ?)", &args);
    defer std.testing.allocator.free(body);

    // parse it back to make sure it's valid JSON shaped as Hrana expects
    var parsed = try std.json.parseFromSlice(std.json.Value, std.testing.allocator, body, .{});
    defer parsed.deinit();

    const requests = parsed.value.object.get("requests").?.array;
    try std.testing.expectEqual(@as(usize, 2), requests.items.len); // execute + close

    const stmt = requests.items[0].object.get("stmt").?.object;
    try std.testing.expectEqualStrings("INSERT INTO t VALUES (?, ?)", stmt.get("sql").?.string);

    const wire_args = stmt.get("args").?.array;
    try std.testing.expectEqual(@as(usize, 2), wire_args.items.len);
    try std.testing.expectEqualStrings("text", wire_args.items[0].object.get("type").?.string);
    try std.testing.expectEqualStrings("abc", wire_args.items[0].object.get("value").?.string);
    try std.testing.expectEqualStrings("integer", wire_args.items[1].object.get("type").?.string);
    try std.testing.expectEqualStrings("1", wire_args.items[1].object.get("value").?.string);
}

test "buildTypedRequestBody: no args produces null args field" {
    const body = try buildTypedRequestBody(std.testing.allocator, "SELECT 1", &.{});
    defer std.testing.allocator.free(body);

    var parsed = try std.json.parseFromSlice(std.json.Value, std.testing.allocator, body, .{});
    defer parsed.deinit();

    // args field is omitted (emit_null_optional_fields = false)
    const stmt = parsed.value.object.get("requests").?.array.items[0].object.get("stmt").?.object;
    try std.testing.expect(stmt.get("args") == null);
}

// regression for the 2026-07-06 WriteFailed wedge (ziglang/zig#21316): a
// server that answers one keep-alive request per connection then RST-closes
// it. plain fetch poisons the pool and fails forever; fetchEvictRetry must
// recover on every request.
const StaleConnServer = struct {
    server: Io.net.Server,
    port: u16,
    thread: std.Thread,
    stopping: std.atomic.Value(bool),

    fn start(io: Io) !*StaleConnServer {
        const s = try std.testing.allocator.create(StaleConnServer);
        errdefer std.testing.allocator.destroy(s);
        var addr = try Io.net.IpAddress.parseIp4("127.0.0.1", 0);
        s.server = try addr.listen(io, .{});
        var bound: std.posix.sockaddr.in = undefined;
        var len: std.posix.socklen_t = @sizeOf(@TypeOf(bound));
        _ = std.c.getsockname(s.server.socket.handle, @ptrCast(&bound), &len);
        s.port = std.mem.bigToNative(u16, bound.port);
        s.stopping = .init(false);
        s.thread = try std.Thread.spawn(.{}, run, .{ s, io });
        return s;
    }

    fn run(s: *StaleConnServer, io: Io) void {
        while (true) {
            const stream = s.server.accept(io) catch return;
            if (s.stopping.load(.acquire)) {
                stream.close(io);
                return;
            }
            const fd = stream.socket.handle;
            var buf: [4096]u8 = undefined;
            _ = std.c.recv(fd, &buf, buf.len, 0);
            const resp = "HTTP/1.1 200 OK\r\ncontent-length: 0\r\n\r\n";
            _ = std.c.send(fd, resp, resp.len, 0);
            const linger = std.c.linger{ .onoff = 1, .linger = 0 };
            _ = std.c.setsockopt(fd, std.c.SOL.SOCKET, std.c.SO.LINGER, &linger, @sizeOf(std.c.linger));
            stream.close(io);
        }
    }

    fn stop(s: *StaleConnServer, io: Io) void {
        // Closing a listener from another thread does not reliably wake a
        // blocking accept on Linux. Connect once to wake it explicitly.
        s.stopping.store(true, .release);
        var address = Io.net.IpAddress{ .ip4 = .loopback(s.port) };
        const wake = address.connect(io, .{ .mode = .stream }) catch unreachable;
        wake.close(io);
        s.thread.join();
        s.server.deinit(io);
        std.testing.allocator.destroy(s);
    }
};

test "fetchEvictRetry recovers from a dead pooled connection" {
    var threaded: Io.Threaded = .init(std.testing.allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    const server = try StaleConnServer.start(io);
    defer server.stop(io);

    var url_buf: [64]u8 = undefined;
    const url = try std.fmt.bufPrint(&url_buf, "http://127.0.0.1:{d}/", .{server.port});

    var http_client: http.Client = .{ .allocator = std.testing.allocator, .io = io };
    defer http_client.deinit();

    for (0..3) |i| {
        // idle long enough for the server's RST close to land before reuse
        if (i > 0) try io.sleep(.fromMilliseconds(100), .awake);
        const res = try fetchEvictRetry(&http_client, io, .{
            .location = .{ .url = url },
            .method = .POST,
            .payload = "{}",
        });
        try std.testing.expectEqual(http.Status.ok, res.status);
    }
}
