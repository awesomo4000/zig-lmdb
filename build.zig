const std = @import("std");

pub fn build(b: *std.Build) void {
    const optimize = b.standardOptimizeOption(.{});
    const target = b.standardTargetOptions(.{});

    const lmdb = b.addModule("lmdb", .{
        .root_source_file = b.path("src/lib.zig"),
        .target = target,
        .optimize = optimize,
    });
    const lmdb_dep = b.dependency("lmdb", .{});

    lmdb.addIncludePath(lmdb_dep.path("libraries/liblmdb"));
    lmdb.addCSourceFiles(.{
        .root = lmdb_dep.path("libraries/liblmdb"),
        .flags = &.{},
        .files = &.{ "mdb.c", "midl.c" },
    });
    lmdb.link_libc = true;

    // Tests
    const tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("test/main.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });
    tests.root_module.addImport("lmdb", lmdb);
    const test_runner = b.addRunArtifact(tests);

    b.step("test", "Run LMDB tests").dependOn(&test_runner.step);

    // Benchmarks
    const bench = b.addExecutable(.{
        .name = "lmdb-benchmark",
        .root_module = b.createModule(.{
            .root_source_file = b.path("benchmarks/main.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });

    bench.root_module.addImport("lmdb", lmdb);

    // Install to zig-out/bin
    b.installArtifact(bench);

    const bench_runner = b.addRunArtifact(bench);
    b.step("bench", "Run LMDB benchmarks").dependOn(&bench_runner.step);

    // Bulk import benchmark
    const bulk_bench = b.addExecutable(.{
        .name = "lmdb-bulk-import",
        .root_module = b.createModule(.{
            .root_source_file = b.path("benchmarks/bulk_import.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });

    bulk_bench.root_module.addImport("lmdb", lmdb);

    // Install to zig-out/bin
    b.installArtifact(bulk_bench);

    const bulk_bench_runner = b.addRunArtifact(bulk_bench);
    b.step("bench-bulk", "Run bulk import benchmarks").dependOn(&bulk_bench_runner.step);

    // Database size check
    const size_check = b.addExecutable(.{
        .name = "check-db-size",
        .root_module = b.createModule(.{
            .root_source_file = b.path("tools/db_size.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });

    size_check.root_module.addImport("lmdb", lmdb);

    // Install to zig-out/bin
    b.installArtifact(size_check);

    const size_check_runner = b.addRunArtifact(size_check);
    b.step("check-size", "Check database file sizes").dependOn(&size_check_runner.step);

    // Build example executable
    const exe = b.addExecutable(.{
        .name = "lmdb-example",
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/lmdb-example.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });

    exe.root_module.addImport("lmdb", lmdb);

    // Install the executable to zig-out/bin
    b.installArtifact(exe);

    const exe_runner = b.addRunArtifact(exe);
    b.step("run", "Run example").dependOn(&exe_runner.step);
}
