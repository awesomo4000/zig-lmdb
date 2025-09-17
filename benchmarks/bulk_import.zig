const std = @import("std");
const lmdb = @import("lmdb");

const ms: f64 = 1_000_000.0;
const test_size = 10_000_000; // 10 million entries

pub fn main() !void {
    var stdout_buffer: [8192]u8 = undefined;
    var stdout_writer = std.fs.File.stdout().writer(&stdout_buffer);
    const stdout = &stdout_writer.interface;

    try stdout.print("## Bulk Import Benchmarks (10M entries)\n\n", .{});
    try stdout.print("| {s:<30} | {s:>12} | {s:>15} | {s:>10} |\n", .{ "Method", "Time (ms)", "Entries/sec", "Speedup" });
    try stdout.print("|{s:-<31}|{s:-<14}|{s:-<17}|{s:-<12}|\n", .{ "", "", "", "" });

    // Baseline benchmark
    const baseline_time = try benchmarkBaseline(stdout);

    // Test different strategies with various batch sizes
    const batch_sizes = [_]usize{ 1000, 10_000, 100_000, 1_000_000 };

    try stdout.print("\n### Standard Mode (sync after each txn)\n\n", .{});
    try stdout.print("| {s:<30} | {s:>12} | {s:>15} | {s:>10} |\n", .{ "Batch Size", "Time (ms)", "Entries/sec", "Speedup" });
    try stdout.print("|{s:-<31}|{s:-<14}|{s:-<17}|{s:-<12}|\n", .{ "", "", "", "" });

    for (batch_sizes) |batch_size| {
        try benchmarkBatched(stdout, batch_size, false, false, baseline_time);
    }

    try stdout.print("\n### NoSync Mode (sync once at end)\n\n", .{});
    try stdout.print("| {s:<30} | {s:>12} | {s:>15} | {s:>10} |\n", .{ "Batch Size", "Time (ms)", "Entries/sec", "Speedup" });
    try stdout.print("|{s:-<31}|{s:-<14}|{s:-<17}|{s:-<12}|\n", .{ "", "", "", "" });

    for (batch_sizes) |batch_size| {
        try benchmarkBatched(stdout, batch_size, true, false, baseline_time);
    }

    try stdout.print("\n### NoSync + MDB_APPEND (sorted keys)\n\n", .{});
    try stdout.print("| {s:<30} | {s:>12} | {s:>15} | {s:>10} |\n", .{ "Batch Size", "Time (ms)", "Entries/sec", "Speedup" });
    try stdout.print("|{s:-<31}|{s:-<14}|{s:-<17}|{s:-<12}|\n", .{ "", "", "", "" });

    for (batch_sizes) |batch_size| {
        try benchmarkBatched(stdout, batch_size, true, true, baseline_time);
    }

    try stdout.print("\n### Maximum Performance (NoSync + WriteMap + MDB_APPEND)\n\n", .{});
    try stdout.print("| {s:<30} | {s:>12} | {s:>15} | {s:>10} |\n", .{ "Configuration", "Time (ms)", "Entries/sec", "Speedup" });
    try stdout.print("|{s:-<31}|{s:-<14}|{s:-<17}|{s:-<12}|\n", .{ "", "", "", "" });

    try benchmarkMaxPerformance(stdout, baseline_time);

    try stdout.flush();
}

fn benchmarkBaseline(writer: anytype) !f64 {
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

    try writer.print("| {s:<30} | {d:>12.2} | {d:>15.0} | {s:>10} |\n",
        .{ "Baseline (single txn)", elapsed, ops_per_sec, "1.00x" });

    return elapsed;
}

fn benchmarkBatched(writer: anytype, batch_size: usize, no_sync: bool, use_append: bool, baseline: f64) !void {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var path_buffer: [std.fs.max_path_bytes]u8 = undefined;
    const path = try tmp.dir.realpath(".", &path_buffer);
    path_buffer[path.len] = 0;

    const env = try lmdb.Environment.init(path_buffer[0..path.len :0], .{
        .map_size = 4 * 1024 * 1024 * 1024,
        .no_sync = no_sync,
    });
    defer env.deinit();

    var timer = try std.time.Timer.start();

    var key_buf: [8]u8 = undefined;
    var val_buf: [64]u8 = undefined;

    var i: u32 = 0;
    while (i < test_size) : (i += @intCast(batch_size)) {
        const txn = try env.transaction(.{ .mode = .ReadWrite });
        errdefer txn.abort();

        const db = try txn.database(null, .{});

        const end = @min(i + batch_size, test_size);
        var j = i;
        while (j < end) : (j += 1) {
            std.mem.writeInt(u64, &key_buf, j, .big);
            std.crypto.hash.Blake3.hash(&key_buf, &val_buf, .{});

            if (use_append) {
                try db.append(&key_buf, &val_buf);
            } else {
                try db.set(&key_buf, &val_buf);
            }
        }

        try txn.commit();
        if (!no_sync) {
            try env.sync();
        }
    }

    if (no_sync) {
        try env.sync();
    }

    const elapsed = @as(f64, @floatFromInt(timer.read())) / ms;
    const ops_per_sec = @as(f64, @floatFromInt(test_size)) * 1000.0 / elapsed;
    const speedup = baseline / elapsed;

    const batch_label = if (batch_size >= 1_000_000)
        try std.fmt.allocPrint(std.heap.page_allocator, "{d}M entries/txn", .{batch_size / 1_000_000})
    else if (batch_size >= 1000)
        try std.fmt.allocPrint(std.heap.page_allocator, "{d}k entries/txn", .{batch_size / 1000})
    else
        try std.fmt.allocPrint(std.heap.page_allocator, "{d} entries/txn", .{batch_size});

    defer std.heap.page_allocator.free(batch_label);

    try writer.print("| {s:<30} | {d:>12.2} | {d:>15.0} | {d:>9.2}x |\n",
        .{ batch_label, elapsed, ops_per_sec, speedup });
}

fn benchmarkMaxPerformance(writer: anytype, baseline: f64) !void {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var path_buffer: [std.fs.max_path_bytes]u8 = undefined;
    const path = try tmp.dir.realpath(".", &path_buffer);
    path_buffer[path.len] = 0;

    // Test different configurations for maximum performance
    const configs = [_]struct {
        name: []const u8,
        write_map: bool,
        no_meta_sync: bool,
        batch_size: usize,
    }{
        .{ .name = "Single txn + WriteMap", .write_map = true, .no_meta_sync = false, .batch_size = test_size },
        .{ .name = "Single txn + NoMetaSync", .write_map = false, .no_meta_sync = true, .batch_size = test_size },
        .{ .name = "Single txn + Both", .write_map = true, .no_meta_sync = true, .batch_size = test_size },
        .{ .name = "100k batch + Both", .write_map = true, .no_meta_sync = true, .batch_size = 100_000 },
        .{ .name = "1M batch + Both", .write_map = true, .no_meta_sync = true, .batch_size = 1_000_000 },
    };

    for (configs) |config| {
        var tmp2 = std.testing.tmpDir(.{});
        defer tmp2.cleanup();

        var pb: [std.fs.max_path_bytes]u8 = undefined;
        const p = try tmp2.dir.realpath(".", &pb);
        pb[p.len] = 0;

        const env = try lmdb.Environment.init(pb[0..p.len :0], .{
            .map_size = 4 * 1024 * 1024 * 1024,
            .no_sync = true,
            .write_map = config.write_map,
            .no_meta_sync = config.no_meta_sync,
        });
        defer env.deinit();

        var timer = try std.time.Timer.start();

        var key_buf: [8]u8 = undefined;
        var val_buf: [64]u8 = undefined;

        if (config.batch_size >= test_size) {
            // Single transaction
            const txn = try env.transaction(.{ .mode = .ReadWrite });
            errdefer txn.abort();

            const db = try txn.database(null, .{});

            var i: u32 = 0;
            while (i < test_size) : (i += 1) {
                std.mem.writeInt(u64, &key_buf, i, .big);
                std.crypto.hash.Blake3.hash(&key_buf, &val_buf, .{});
                try db.append(&key_buf, &val_buf);
            }

            try txn.commit();
        } else {
            // Batched transactions
            var i: u32 = 0;
            while (i < test_size) : (i += @intCast(config.batch_size)) {
                const txn = try env.transaction(.{ .mode = .ReadWrite });
                errdefer txn.abort();

                const db = try txn.database(null, .{});

                const end = @min(i + config.batch_size, test_size);
                var j = i;
                while (j < end) : (j += 1) {
                    std.mem.writeInt(u64, &key_buf, j, .big);
                    std.crypto.hash.Blake3.hash(&key_buf, &val_buf, .{});
                    try db.append(&key_buf, &val_buf);
                }

                try txn.commit();
            }
        }

        try env.sync();

        const elapsed = @as(f64, @floatFromInt(timer.read())) / ms;
        const ops_per_sec = @as(f64, @floatFromInt(test_size)) * 1000.0 / elapsed;
        const speedup = baseline / elapsed;

        try writer.print("| {s:<30} | {d:>12.2} | {d:>15.0} | {d:>9.2}x |\n",
            .{ config.name, elapsed, ops_per_sec, speedup });
    }
}