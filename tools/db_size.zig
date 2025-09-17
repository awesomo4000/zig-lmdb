const std = @import("std");
const lmdb = @import("lmdb");

const test_size = 10_000_000; // 10 million entries

pub fn main() !void {
    var stdout_buffer: [4096]u8 = undefined;
    var stdout_writer = std.fs.File.stdout().writer(&stdout_buffer);
    const stdout = &stdout_writer.interface;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var path_buffer: [std.fs.max_path_bytes]u8 = undefined;
    const path = try tmp.dir.realpath(".", &path_buffer);
    path_buffer[path.len] = 0;

    const env = try lmdb.Environment.init(path_buffer[0..path.len :0], .{
        .map_size = 4 * 1024 * 1024 * 1024, // 4GB
        .no_sync = true,
    });
    defer env.deinit();

    // Write 10M entries
    {
        const txn = try env.transaction(.{ .mode = .ReadWrite });
        errdefer txn.abort();

        const db = try txn.database(null, .{});

        var key_buf: [8]u8 = undefined;
        var val_buf: [64]u8 = undefined;

        var i: u32 = 0;
        while (i < test_size) : (i += 1) {
            std.mem.writeInt(u64, &key_buf, i, .big);
            std.crypto.hash.Blake3.hash(&key_buf, &val_buf, .{});
            try db.append(&key_buf, &val_buf);
        }

        try txn.commit();
    }

    try env.sync();

    // Check the database file size
    const data_file = try tmp.dir.openFile("data.mdb", .{});
    defer data_file.close();

    const stat = try data_file.stat();
    const size_mb = @as(f64, @floatFromInt(stat.size)) / (1024.0 * 1024.0);
    const size_gb = size_mb / 1024.0;

    try stdout.print("\n## Database Size Analysis\n\n", .{});
    try stdout.print("Entries: {d:>15}\n", .{test_size});
    try stdout.print("Key size: {d:>14} bytes\n", .{8});
    try stdout.print("Value size: {d:>12} bytes\n", .{64});
    try stdout.print("Total data: {d:>12} bytes ({d:.1} MB)\n", .{test_size * (8 + 64), @as(f64, @floatFromInt(test_size * (8 + 64))) / (1024.0 * 1024.0)});
    try stdout.print("\nActual file size: {d:.2} MB ({d:.3} GB)\n", .{size_mb, size_gb});
    try stdout.print("Overhead: {d:.1}%\n", .{(@as(f64, @floatFromInt(stat.size)) - @as(f64, @floatFromInt(test_size * (8 + 64)))) * 100.0 / @as(f64, @floatFromInt(test_size * (8 + 64)))});
    try stdout.print("Bytes per entry: {d:.1}\n", .{@as(f64, @floatFromInt(stat.size)) / @as(f64, @floatFromInt(test_size))});

    // Get environment stats
    const env_stat = try env.stat();
    try stdout.print("\n## LMDB Statistics\n", .{});
    try stdout.print("Page size: {d} bytes\n", .{env_stat.psize});
    try stdout.print("B-tree depth: {d}\n", .{env_stat.depth});
    try stdout.print("Branch pages: {d}\n", .{env_stat.branch_pages});
    try stdout.print("Leaf pages: {d}\n", .{env_stat.leaf_pages});
    try stdout.print("Overflow pages: {d}\n", .{env_stat.overflow_pages});
    try stdout.print("Total pages: {d}\n", .{env_stat.branch_pages + env_stat.leaf_pages + env_stat.overflow_pages});
    try stdout.print("Total page bytes: {d:.2} MB\n", .{@as(f64, @floatFromInt((env_stat.branch_pages + env_stat.leaf_pages + env_stat.overflow_pages) * env_stat.psize)) / (1024.0 * 1024.0)});

    try stdout.flush();
}