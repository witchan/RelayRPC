#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"

echo "=== RelayRPC Worker - Build All Variants ==="
echo ""

# Clean
make clean 2>/dev/null || true

# 1. Rootful (ios 13+)
echo "--- Building rootful (iphoneos-arm, iOS 13+) ---"
sed -i.bak 's/Architecture: iphoneos-arm64/Architecture: iphoneos-arm/' control 2>/dev/null || true
make clean 2>/dev/null || true
make package ROOTFUL=1 2>&1 | grep "building package"
mv control.bak control 2>/dev/null || true

# 2. Rootless (ios 15+)
echo "--- Building rootless (iphoneos-arm64, iOS 15+) ---"
make clean 2>/dev/null || true
make package 2>&1 | grep "building package"

# 3. Roothide (ios 15+)
echo "--- Building roothide (iphoneos-arm64, iOS 15+) ---"
make clean 2>/dev/null || true
make package ROOTHIDE=1 2>&1 | grep "building package"

echo ""
echo "=== All packages built ==="
ls -la packages/*.deb
