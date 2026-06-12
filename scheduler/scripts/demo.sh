#!/usr/bin/env bash
set -euo pipefail

BASE_URL="${BASE_URL:-http://localhost:8080}"
ADMIN_TOKEN="${ADMIN_TOKEN:-rr_adm_bootstrap_DEV_CHANGE_ME}"

echo "=== RelayRPC Demo ==="
echo ""

echo "1. Create consumer"
curl -sS -X POST "$BASE_URL/admin/v1/consumers" \
  -H "Authorization: Bearer $ADMIN_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"id":"consumer_a","name":"Consumer A"}' | python3 -m json.tool

echo ""
echo "2. Create worker"
curl -sS -X POST "$BASE_URL/admin/v1/workers" \
  -H "Authorization: Bearer $ADMIN_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"id":"ios-001","name":"iOS Worker 001","runtime_type":"ios","capabilities":["default"]}' | python3 -m json.tool

echo ""
echo "3. Create consumer token"
CONSUMER_TOKEN=$(curl -sS -X POST "$BASE_URL/admin/v1/tokens" \
  -H "Authorization: Bearer $ADMIN_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"token_type":"consumer","subject_id":"consumer_a","name":"consumer-a-main"}' | python3 -c "import sys,json;print(json.load(sys.stdin)['token'])")
echo "   CONSUMER_TOKEN=$CONSUMER_TOKEN"

echo ""
echo "4. Create worker token"
WORKER_TOKEN=$(curl -sS -X POST "$BASE_URL/admin/v1/tokens" \
  -H "Authorization: Bearer $ADMIN_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"token_type":"worker","subject_id":"ios-001","name":"ios-001-main"}' | python3 -c "import sys,json;print(json.load(sys.stdin)['token'])")
echo "   WORKER_TOKEN=$WORKER_TOKEN"

echo ""
echo "5. Start worker simulator (in another terminal):"
echo "   go run ./cmd/relayrpc-worker-sim --worker-id ios-001 --token \"$WORKER_TOKEN\" --success-rate 1.0 --min-delay 1s --max-delay 3s"
echo ""
read -r -p "Press Enter after worker simulator is connected..."

echo ""
echo "6. Create async task"
TASK_ID=$(curl -sS -X POST "$BASE_URL/api/v1/tasks/" \
  -H "Authorization: Bearer $CONSUMER_TOKEN" \
  -H "Idempotency-Key: demo-async-1" \
  -H "Content-Type: application/json" \
  -d '{"biz_id":"order_10001","payload":{"action":"demo","params":{"a":1}},"timeout_ms":120000,"deadline_ms":600000}' | python3 -c "import sys,json;print(json.load(sys.stdin)['task_id'])")
echo "   TASK_ID=$TASK_ID"

echo ""
echo "7. Wait for result"
curl -sS "$BASE_URL/api/v1/tasks/$TASK_ID/wait?timeout_ms=30000" \
  -H "Authorization: Bearer $CONSUMER_TOKEN" | python3 -m json.tool

echo ""
echo "8. Create sync task"
curl -sS -X POST "$BASE_URL/api/v1/tasks/sync" \
  -H "Authorization: Bearer $CONSUMER_TOKEN" \
  -H "Idempotency-Key: demo-sync-1" \
  -H "Content-Type: application/json" \
  -d '{"biz_id":"order_10002","payload":{"action":"demo","params":{}},"task_timeout_ms":120000,"wait_timeout_ms":60000}' | python3 -m json.tool

echo ""
echo "=== Demo Complete ==="
