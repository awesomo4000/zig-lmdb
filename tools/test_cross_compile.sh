#!/bin/bash

echo "====================================="
echo "LMDB Cross-Compilation Test Suite"
echo "====================================="
echo

# Define all target combinations
declare -a TARGETS=(
    "x86_64-linux"
    "x86_64-macos"
    "x86_64-windows"
    "x86_64-freebsd"
    "x86_64-netbsd"
    "aarch64-linux"
    "aarch64-macos"
    "aarch64-windows"
    "aarch64-freebsd"
    "aarch64-netbsd"
)

SUCCESS_COUNT=0
FAIL_COUNT=0
FAILED_TARGETS=""

echo "Testing ${#TARGETS[@]} target combinations..."
echo

for TARGET in "${TARGETS[@]}"
do
    printf "Building %-20s ... " "$TARGET"

    if zig build -Dtarget=$TARGET -Doptimize=ReleaseSmall > /dev/null 2>&1; then
        echo "✓ SUCCESS"
        ((SUCCESS_COUNT++))

        # Check binary size if it exists
        if [[ "$TARGET" == *"windows"* ]]; then
            BINARY="zig-out/bin/lmdb-example.exe"
        else
            BINARY="zig-out/bin/lmdb-example"
        fi

        if [ -f "$BINARY" ]; then
            SIZE=$(ls -lah "$BINARY" 2>/dev/null | awk '{print $5}')
            printf "                         Size: %s\n" "$SIZE"
        fi
    else
        echo "✗ FAILED"
        ((FAIL_COUNT++))
        FAILED_TARGETS="$FAILED_TARGETS $TARGET"
    fi
done

echo
echo "====================================="
echo "Results Summary"
echo "====================================="
echo "✓ Successful builds: $SUCCESS_COUNT"
echo "✗ Failed builds:     $FAIL_COUNT"

if [ $FAIL_COUNT -gt 0 ]; then
    echo
    echo "Failed targets: $FAILED_TARGETS"
    exit 1
else
    echo
    echo "🎉 All targets built successfully!"
fi

# Test that tests still work on native platform
echo
echo "====================================="
echo "Running Native Tests"
echo "====================================="
if zig build test > /dev/null 2>&1; then
    echo "✓ All tests pass on native platform"
else
    echo "✗ Tests failed on native platform"
    exit 1
fi