#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
CONFIG_FILE="${1:-$PROJECT_DIR/configs/config.yaml}"

if [ ! -f "$CONFIG_FILE" ]; then
    echo "Config file not found: $CONFIG_FILE"
    exit 1
fi

echo "=== RelayRPC Token Generator ==="
echo ""

read -rp "How many tokens to generate? " NUM

for i in $(seq 1 "$NUM"); do
    UUID=$(uuidgen | tr '[:upper:]' '[:lower:]')
    echo "  Generated: $UUID"

    # If tokens is empty list, replace it
    if grep -q "^tokens: \[\]" "$CONFIG_FILE"; then
        sed -i.bak 's/^tokens: \[\]/tokens:/' "$CONFIG_FILE"
        rm -f "${CONFIG_FILE}.bak"
    fi
    echo "  - \"$UUID\"" >> "$CONFIG_FILE"
done

echo ""
echo "Tokens appended to $CONFIG_FILE"
echo ""
echo "Usage:"
echo "  Server:   go run ./cmd/relayrpc-server"
echo "  Worker:   go run ./cmd/relayrpc-worker-sim --worker-id <name> --token <uuid>"
echo "  Consumer: curl -H 'Authorization: Bearer <uuid>' http://localhost:8080/api/v1/tasks"
