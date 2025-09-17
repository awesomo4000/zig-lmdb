const std = @import("std");
const lmdb = @import("lmdb");

const ms: f64 = 1_000_000.0;
const test_size = 1_000_000; // 1 million entries

pub fn main() !void {
    var stdout_buffer: [4096]u8 = undefined;
    var stdout_writer = std.fs.File.stdout().writer(&stdout_buffer);
    const stdout = &stdout_writer.interface;

    try stdout.print("## Bulk Import Benchmarks (1M entries)\n\n", .{});
    try stdout.print("| Method | Time (ms) | Entries/sec |\n", .{});
    try stdout.print("|--------|-----------|-------------|\n", .{});

    // Test different bulk import strategies
    try benchmarkNormal(stdout);
    try benchmarkNoSync(stdout);
    try benchmarkAppendMode(stdout);
    try benchmarkBatchedTransactions(stdout);
    try benchmarkOptimizedBulk(stdout);

    try stdout.flush();
}

fn benchmarkNormal(writer: anytype) !void {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var path_buffer: [std.fs.max_path_bytes]u8 = undefined;
    const path = try tmp.dir.realpath(".", &path_buffer);
    path_buffer[path.len] = 0;

    const env = try lmdb.Environment.init(path_buffer[0..path.len :0], .{
        .map_size = 4 * 1024 * 1024 * 1024, // 4GB
    });
    defer env.deinit();

    var timer = try std.time.Timer.start();

    const txn = try env.transaction(.{ .mode = .ReadWrite });
    errdefer txn.abort();

    const db = try txn.database(null, .{});

    var key_buf: [8]u8 = undefined;
    var val_buf: [64]u8 = undefined;

    var i: u32 = 0;
    while (i < test_size) : (i += 1) {
        std.mem.writeInt(u64, &key_buf, i, .big);
        std.crypto.hash.Blake3.hash(&key_buf, &val_buf, .{});
        try db.set(&key_buf, &val_buf);
    }

    try txn.commit();
    try env.sync();

    const elapsed = @as(f64, @floatFromInt(timer.read())) / ms;
    const ops_per_sec = @as(f64, @floatFromInt(test_size)) * 1000.0 / elapsed;

    try writer.print("| Normal (single txn) | {d:.2} | {d:.0} |\n", .{ elapsed, ops_per_sec });
}

fn benchmarkNoSync(writer: anytype) !void {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var path_buffer: [std.fs.max_path_bytes]u8 = undefined;
    const path = try tmp.dir.realpath(".", &path_buffer);
    path_buffer[path.len] = 0;

    const env = try lmdb.Environment.init(path_buffer[0..path.len :0], .{
        .map_size = 4 * 1024 * 1024 * 1024,
        .no_sync = true,
    });
    defer env.deinit();

    var timer = try std.time.Timer.start();

    const txn = try env.transaction(.{ .mode = .ReadWrite });
    errdefer txn.abort();

    const db = try txn.database(null, .{});

    var key_buf: [8]u8 = undefined;
    var val_buf: [64]u8 = undefined;

    var i: u32 = 0;
    while (i < test_size) : (i += 1) {
        std.mem.writeInt(u64, &key_buf, i, .big);
        std.crypto.hash.Blake3.hash(&key_buf, &val_buf, .{});
        try db.set(&key_buf, &val_buf);
    }

    try txn.commit();
    try env.sync(); // Sync once at the end

    const elapsed = @as(f64, @floatFromInt(timer.read())) / ms;
    const ops_per_sec = @as(f64, @floatFromInt(test_size)) * 1000.0 / elapsed;

    try writer.print("| NoSync mode | {d:.2} | {d:.0} |\n", .{ elapsed, ops_per_sec });
}

fn benchmarkAppendMode(writer: anytype) !void {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var path_buffer: [std.fs.max_path_bytes]u8 = undefined;
    const path = try tmp.dir.realpath(".", &path_buffer);
    path_buffer[path.len] = 0;

    const env = try lmdb.Environment.init(path_buffer[0..path.len :0], .{
        .map_size = 4 * 1024 * 1024 * 1024,
        .no_sync = true,
    });
    defer env.deinit();

    var timer = try std.time.Timer.start();

    const txn = try env.transaction(.{ .mode = .ReadWrite });
    errdefer txn.abort();

    const db = try txn.database(null, .{});

    var key_buf: [8]u8 = undefined;
    var val_buf: [64]u8 = undefined;

    // Keys are already sorted when written sequentially with big-endian
    var i: u32 = 0;
    while (i < test_size) : (i += 1) {
        std.mem.writeInt(u64, &key_buf, i, .big);
        std.crypto.hash.Blake3.hash(&key_buf, &val_buf, .{});
        try db.append(&key_buf, &val_buf);
    }

    try txn.commit();
    try env.sync();

    const elapsed = @as(f64, @floatFromInt(timer.read())) / ms;
    const ops_per_sec = @as(f64, @floatFromInt(test_size)) * 1000.0 / elapsed;

    try writer.print("| MDB_APPEND (sorted) | {d:.2} | {d:.0} |\n", .{ elapsed, ops_per_sec });
}

fn benchmarkBatchedTransactions(writer: anytype) !void {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var path_buffer: [std.fs.max_path_bytes]u8 = undefined;
    const path = try tmp.dir.realpath(".", &path_buffer);
    path_buffer[path.len] = 0;

    const env = try lmdb.Environment.init(path_buffer[0..path.len :0], .{
        .map_size = 4 * 1024 * 1024 * 1024,
        .no_sync = true,
    });
    defer env.deinit();

    var timer = try std.time.Timer.start();

    const batch_size = 10000;
    var key_buf: [8]u8 = undefined;
    var val_buf: [64]u8 = undefined;

    var i: u32 = 0;
    while (i < test_size) : (i += batch_size) {
        const txn = try env.transaction(.{ .mode = .ReadWrite });
        errdefer txn.abort();

        const db = try txn.database(null, .{});

        const end = @min(i + batch_size, test_size);
        var j = i;
        while (j < end) : (j += 1) {
            std.mem.writeInt(u64, &key_buf, j, .big);
            std.crypto.hash.Blake3.hash(&key_buf, &val_buf, .{});
            try db.set(&key_buf, &val_buf);
        }

        try txn.commit();
    }

    try env.sync();

    const elapsed = @as(f64, @floatFromInt(timer.read())) / ms;
    const ops_per_sec = @as(f64, @floatFromInt(test_size)) * 1000.0 / elapsed;

    try writer.print("| Batched (10k/txn) | {d:.2} | {d:.0} |\n", .{ elapsed, ops_per_sec });
}

fn benchmarkOptimizedBulk(writer: anytype) !void {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var path_buffer: [std.fs.max_path_bytes]u8 = undefined;
    const path = try tmp.dir.realpath(".", &path_buffer);
    path_buffer[path.len] = 0;

    // Maximum optimization for bulk import
    const env = try lmdb.Environment.init(path_buffer[0..path.len :0], .{
        .map_size = 4 * 1024 * 1024 * 1024,
        .no_sync = true,
        .write_map = true,  // Memory-mapped writes
        .no_meta_sync = true, // Don't sync metadata
    });
    defer env.deinit();

    var timer = try std.time.Timer.start();

    const txn = try env.transaction(.{ .mode = .ReadWrite });
    errdefer txn.abort();

    const db = try txn.database(null, .{});

    var key_buf: [8]u8 = undefined;
    var val_buf: [64]u8 = undefined;

    var i: u32 = 0;
    while (i < test_size) : (i += 1) {
        std.mem.writeInt(u64, &key_buf, i, .big);
        std.crypto.hash.Blake3.hash(&key_buf, &val_buf, .{});
        try db.append(&key_buf, &val_buf); // Use append since keys are sorted
    }

    try txn.commit();
    try env.sync();

    const elapsed = @as(f64, @floatFromInt(timer.read())) / ms;
    const ops_per_sec = @as(f64, @floatFromInt(test_size)) * 1000.0 / elapsed;

    try writer.print("| Optimized bulk | {d:.2} | {d:.0} |\n", .{ elapsed, ops_per_sec });
}