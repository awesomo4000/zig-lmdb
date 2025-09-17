const std = @import("std");
const lmdb = @import("lmdb");

const allocator = std.testing.allocator;
const expect = std.testing.expect;
const expectEqual = std.testing.expectEqual;
const expectError = std.testing.expectError;

// Edge Case 1: Empty keys and values
test "empty keys and values" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var path_buffer: [std.fs.max_path_bytes]u8 = undefined;
    const path = try tmp.dir.realpath(".", &path_buffer);
    path_buffer[path.len] = 0;

    const env = try lmdb.Environment.init(path_buffer[0..path.len :0], .{});
    defer env.deinit();

    const txn = try env.transaction(.{ .mode = .ReadWrite });
    defer txn.abort();

    // Empty key should error with MDB_BAD_VALSIZE
    try expectError(error.MDB_BAD_VALSIZE, txn.set("", "value"));

    // Empty value should be OK
    try txn.set("key", "");
    const val = try txn.get("key");
    try expect(val != null);
    try expectEqual(@as(usize, 0), val.?.len);
}

// Edge Case 2: Maximum key/value sizes
test "maximum sizes" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var path_buffer: [std.fs.max_path_bytes]u8 = undefined;
    const path = try tmp.dir.realpath(".", &path_buffer);
    path_buffer[path.len] = 0;

    const env = try lmdb.Environment.init(path_buffer[0..path.len :0], .{});
    defer env.deinit();

    const txn = try env.transaction(.{ .mode = .ReadWrite });
    defer txn.abort();

    // LMDB max key size is 511 bytes by default
    const max_key = [_]u8{'K'} ** 511;
    const huge_key = [_]u8{'K'} ** 512;
    const big_value = [_]u8{'V'} ** (1024 * 1024); // 1MB value

    // Max key should work
    try txn.set(&max_key, "test");

    // Over max key should error
    try expectError(error.MDB_BAD_VALSIZE, txn.set(&huge_key, "test"));

    // Large value should work
    try txn.set("bigval", &big_value);
    const retrieved = try txn.get("bigval");
    try expectEqual(@as(usize, 1024 * 1024), retrieved.?.len);
}

// Edge Case 3: MVCC read consistency
test "MVCC read consistency" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var path_buffer: [std.fs.max_path_bytes]u8 = undefined;
    const path = try tmp.dir.realpath(".", &path_buffer);
    path_buffer[path.len] = 0;

    const env = try lmdb.Environment.init(path_buffer[0..path.len :0], .{
        .no_tls = true, // Allow multiple readers in same thread
    });
    defer env.deinit();

    // Write initial value
    {
        const txn = try env.transaction(.{ .mode = .ReadWrite });
        try txn.set("key", "value1");
        try txn.commit();
    }

    // Start read transaction
    const read_txn1 = try env.transaction(.{ .mode = .ReadOnly });
    defer read_txn1.abort();

    // Read initial value
    const val1 = try read_txn1.get("key");
    try expect(std.mem.eql(u8, val1.?, "value1"));

    // Write transaction changes value
    {
        const write_txn = try env.transaction(.{ .mode = .ReadWrite });
        try write_txn.set("key", "value2");
        try write_txn.commit();
    }

    // Old read transaction still sees old value (MVCC)
    const old_val = try read_txn1.get("key");
    try expect(std.mem.eql(u8, old_val.?, "value1"));

    // New read transaction sees new value
    const new_read_txn = try env.transaction(.{ .mode = .ReadOnly });
    defer new_read_txn.abort();
    const new_val = try new_read_txn.get("key");
    try expect(std.mem.eql(u8, new_val.?, "value2"));
}

// Edge Case 4: Database full scenarios
test "database full handling" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var path_buffer: [std.fs.max_path_bytes]u8 = undefined;
    const path = try tmp.dir.realpath(".", &path_buffer);
    path_buffer[path.len] = 0;

    // Create tiny database
    const env = try lmdb.Environment.init(path_buffer[0..path.len :0], .{
        .map_size = 1024 * 1024, // Only 1MB
    });
    defer env.deinit();

    const txn = try env.transaction(.{ .mode = .ReadWrite });
    defer txn.abort();

    // Try to fill it up
    var key_buf: [8]u8 = undefined;
    const value = [_]u8{'X'} ** 4096; // 4KB values

    var i: u32 = 0;
    while (i < 1000) : (i += 1) {
        std.mem.writeInt(u64, &key_buf, i, .big);
        txn.set(&key_buf, &value) catch |err| {
            // Should eventually hit MDB_MAP_FULL error
            try expectEqual(err, error.MDB_MAP_FULL);
            break;
        };
    }

    // Make sure we did hit the limit
    try expect(i < 1000);
}

// Edge Case 5: Cursor edge cases
test "cursor edge cases" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var path_buffer: [std.fs.max_path_bytes]u8 = undefined;
    const path = try tmp.dir.realpath(".", &path_buffer);
    path_buffer[path.len] = 0;

    const env = try lmdb.Environment.init(path_buffer[0..path.len :0], .{});
    defer env.deinit();

    // Test empty database cursor
    {
        const txn = try env.transaction(.{ .mode = .ReadOnly });
        defer txn.abort();

        const cursor = try txn.cursor();
        defer cursor.deinit();

        // Should return null on empty db
        const first = try cursor.goToFirst();
        try expect(first == null);

        const last = try cursor.goToLast();
        try expect(last == null);
    }

    // Add some data
    {
        const txn = try env.transaction(.{ .mode = .ReadWrite });
        try txn.set("key1", "val1");
        try txn.set("key2", "val2");
        try txn.set("key3", "val3");
        try txn.commit();
    }

    // Test cursor operations
    {
        const txn = try env.transaction(.{ .mode = .ReadOnly });
        defer txn.abort();

        const cursor = try txn.cursor();
        defer cursor.deinit();

        // Navigate to middle
        try cursor.goToKey("key2");

        // Test getting current after navigation
        const current = try cursor.getCurrentValue();
        try expect(std.mem.eql(u8, current, "val2"));

        // Navigate past end
        _ = try cursor.goToLast();
        const past_end = try cursor.goToNext();
        try expect(past_end == null);

        // Navigate before beginning
        _ = try cursor.goToFirst();
        const before_start = try cursor.goToPrevious();
        try expect(before_start == null);
    }
}

// Edge Case 6: Binary data with null bytes
test "binary data with null bytes" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var path_buffer: [std.fs.max_path_bytes]u8 = undefined;
    const path = try tmp.dir.realpath(".", &path_buffer);
    path_buffer[path.len] = 0;

    const env = try lmdb.Environment.init(path_buffer[0..path.len :0], .{});
    defer env.deinit();

    const txn = try env.transaction(.{ .mode = .ReadWrite });
    defer txn.abort();

    // Binary data with nulls
    const binary_key = [_]u8{ 0x00, 0xFF, 0x00, 0xFF };
    const binary_val = [_]u8{ 0xDE, 0xAD, 0x00, 0xBE, 0xEF };

    try txn.set(&binary_key, &binary_val);

    const retrieved = try txn.get(&binary_key);
    try expect(retrieved != null);
    try expect(std.mem.eql(u8, retrieved.?, &binary_val));
}

// Edge Case 7: Append mode violations
test "append mode violations" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var path_buffer: [std.fs.max_path_bytes]u8 = undefined;
    const path = try tmp.dir.realpath(".", &path_buffer);
    path_buffer[path.len] = 0;

    const env = try lmdb.Environment.init(path_buffer[0..path.len :0], .{});
    defer env.deinit();

    const txn = try env.transaction(.{ .mode = .ReadWrite });
    defer txn.abort();

    const db = try txn.database(null, .{});

    // Add keys in order
    try db.append("aaa", "1");
    try db.append("bbb", "2");

    // Try to append out of order - should error
    try expectError(error.MDB_KEYEXIST, db.append("aaa", "3"));
}

// Edge Case 8: Multiple databases
test "multiple named databases" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var path_buffer: [std.fs.max_path_bytes]u8 = undefined;
    const path = try tmp.dir.realpath(".", &path_buffer);
    path_buffer[path.len] = 0;

    const env = try lmdb.Environment.init(path_buffer[0..path.len :0], .{
        .max_dbs = 10,
    });
    defer env.deinit();

    const txn = try env.transaction(.{ .mode = .ReadWrite });
    defer txn.abort();

    // Create multiple named databases
    const db1 = try txn.database("users", .{ .create = true });
    const db2 = try txn.database("posts", .{ .create = true });

    // Same key in different databases
    try db1.set("id:1", "Alice");
    try db2.set("id:1", "First post");

    // Verify isolation
    const user = try db1.get("id:1");
    const post = try db2.get("id:1");

    try expect(std.mem.eql(u8, user.?, "Alice"));
    try expect(std.mem.eql(u8, post.?, "First post"));
}

// Edge Case 9: Rapid open/close cycles
test "rapid open close cycles" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var path_buffer: [std.fs.max_path_bytes]u8 = undefined;
    const path = try tmp.dir.realpath(".", &path_buffer);
    path_buffer[path.len] = 0;

    // Rapidly open and close environment
    var i: u32 = 0;
    while (i < 100) : (i += 1) {
        const env = try lmdb.Environment.init(path_buffer[0..path.len :0], .{});

        const txn = try env.transaction(.{ .mode = .ReadWrite });
        var key_buf: [4]u8 = undefined;
        std.mem.writeInt(u32, &key_buf, i, .big);
        try txn.set(&key_buf, "test");
        try txn.commit();

        env.deinit();
    }

    // Verify data persisted
    const env = try lmdb.Environment.init(path_buffer[0..path.len :0], .{});
    defer env.deinit();

    const txn = try env.transaction(.{ .mode = .ReadOnly });
    defer txn.abort();

    var key_buf: [4]u8 = undefined;
    std.mem.writeInt(u32, &key_buf, 99, .big);
    const val = try txn.get(&key_buf);
    try expect(val != null);
}

// Edge Case 10: Sorted vs unsorted bulk operations
test "sorted vs unsorted bulk operations" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var path_buffer: [std.fs.max_path_bytes]u8 = undefined;
    const path = try tmp.dir.realpath(".", &path_buffer);
    path_buffer[path.len] = 0;

    const env = try lmdb.Environment.init(path_buffer[0..path.len :0], .{});
    defer env.deinit();

    // Test BulkLoader with unsorted data
    {
        var loader = lmdb.BulkLoader.init(env, .{
            .batch_size = 100,
            .sorted = false,
        });
        defer loader.deinit();

        // Add in random order
        try loader.put("zzz", "last");
        try loader.put("aaa", "first");
        try loader.put("mmm", "middle");

        _ = try loader.finish();
    }

    // Verify data was stored correctly
    const txn = try env.transaction(.{ .mode = .ReadOnly });
    defer txn.abort();

    const cursor = try txn.cursor();
    defer cursor.deinit();

    // Should be in sorted order regardless of insert order
    const first = try cursor.goToFirst();
    try expect(std.mem.eql(u8, first.?, "aaa"));

    _ = try cursor.goToNext();
    const middle = try cursor.getCurrentKey();
    try expect(std.mem.eql(u8, middle, "mmm"));

    _ = try cursor.goToNext();
    const last = try cursor.getCurrentKey();
    try expect(std.mem.eql(u8, last, "zzz"));
}