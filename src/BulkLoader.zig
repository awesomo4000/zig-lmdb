const std = @import("std");
const c = @import("c.zig");
const errors = @import("errors.zig");
const throw = errors.throw;

const Environment = @import("Environment.zig");
const Transaction = @import("Transaction.zig");
const Database = @import("Database.zig");

const BulkLoader = @This();

pub const Options = struct {
    batch_size: usize = 10000,
    sorted: bool = false,
    no_overwrite: bool = false,
};

env: Environment,
options: Options,
current_txn: ?Transaction = null,
current_db: ?Database = null,
count: usize = 0,
batch_count: usize = 0,

pub fn init(env: Environment, options: Options) BulkLoader {
    return .{
        .env = env,
        .options = options,
    };
}

pub fn deinit(self: *BulkLoader) void {
    if (self.current_txn) |txn| {
        txn.abort();
        self.current_txn = null;
        self.current_db = null;
    }
}

fn ensureTransaction(self: *BulkLoader) !void {
    if (self.current_txn == null) {
        self.current_txn = try self.env.transaction(.{ .mode = .ReadWrite });
        self.current_db = try self.current_txn.?.database(null, .{});
        self.batch_count = 0;
    }
}

pub fn put(self: *BulkLoader, key: []const u8, value: []const u8) !void {
    try self.ensureTransaction();

    const db = self.current_db.?;

    if (self.options.sorted) {
        try db.append(key, value);
    } else if (self.options.no_overwrite) {
        _ = try db.setNoOverwrite(key, value);
    } else {
        try db.set(key, value);
    }

    self.count += 1;
    self.batch_count += 1;

    // Commit batch if we've reached the batch size
    if (self.batch_count >= self.options.batch_size) {
        try self.flush();
    }
}

pub fn flush(self: *BulkLoader) !void {
    if (self.current_txn) |txn| {
        try txn.commit();
        self.current_txn = null;
        self.current_db = null;
        self.batch_count = 0;
    }
}

pub fn finish(self: *BulkLoader) !usize {
    try self.flush();
    try self.env.sync();
    const total = self.count;
    self.count = 0;
    return total;
}

pub const SortedLoader = struct {
    loader: BulkLoader,
    last_key: ?[]const u8 = null,
    allocator: std.mem.Allocator,
    owned_key: ?[]u8 = null,

    pub fn init(env: Environment, allocator: std.mem.Allocator, batch_size: usize) SortedLoader {
        return .{
            .loader = BulkLoader.init(env, .{
                .batch_size = batch_size,
                .sorted = true,
            }),
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *SortedLoader) void {
        if (self.owned_key) |key| {
            self.allocator.free(key);
        }
        self.loader.deinit();
    }

    pub fn put(self: *SortedLoader, key: []const u8, value: []const u8) !void {
        // Verify keys are in sorted order
        if (self.last_key) |last| {
            if (std.mem.order(u8, key, last) != .gt) {
                return error.KeysNotSorted;
            }
        }

        // Store a copy of the key for next comparison
        if (self.owned_key) |old_key| {
            self.allocator.free(old_key);
        }
        self.owned_key = try self.allocator.dupe(u8, key);
        self.last_key = self.owned_key;

        try self.loader.put(key, value);
    }

    pub fn finish(self: *SortedLoader) !usize {
        return try self.loader.finish();
    }
};