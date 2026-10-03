#!/usr/bin/env bash
# Verifies the fix for issue #7 (rebalancer veto bug).
#
# Expected outcome with the fix applied:
#   - Both miner replicas have a non-zero claimed_count.
#   - Logs show "drain decision audit" with on_chain_result=staked but no
#     "DRAIN ABORTED" / "release vetoed by callback" messages.
#   - The supplier_drain_decision_total{drain_trigger="rebalance_release"}
#     metric is non-zero (audit still emitted).
#
# Pre-fix symptom (for reference): miner-2 stays at 0 claims and miner-1
# emits "rebalance: some releases vetoed (suppliers still staked)".
#
# Usage:
#   tilt up          # in another terminal, wait for both miners Ready
#   ./scripts/verify-rebalance-fix.sh

set -euo pipefail

ctx="kind-kind"
ns="default"

echo "=== Miner pods ==="
kubectl --context "$ctx" -n "$ns" get pods -l app=miner -o wide

echo
echo "=== Per-miner claimed supplier count (Redis source of truth) ==="
# Redis stores claim ownership under ha:miner:claim:<supplier> -> instance_id.
# Group by instance_id.
kubectl --context "$ctx" -n "$ns" exec redis-standalone-0 -c redis -- \
  redis-cli --no-raw <<'LUA' || true
EVAL "local keys = redis.call('keys', 'ha:miner:claim:*'); local agg = {}; for _, k in ipairs(keys) do local v = redis.call('get', k); agg[v] = (agg[v] or 0) + 1 end; local out = {}; for owner, n in pairs(agg) do table.insert(out, owner .. '=' .. n) end; return out" 0
LUA

echo
echo "=== Drain decision audit log (last 5 min, both miners) ==="
now_ns=$(date -d "now" +%s)000000000
start_ns=$(date -d "5 minutes ago" +%s)000000000
curl -sG 'http://localhost:3100/loki/api/v1/query_range' \
  --data-urlencode 'query={app="miner"} |= "drain decision audit"' \
  --data-urlencode "limit=50" \
  --data-urlencode "start=$start_ns" \
  --data-urlencode "end=$now_ns" \
  | jq -r '.data.result[].values[][1]' \
  | head -20

echo
echo "=== Veto / abort log check (should be EMPTY post-fix) ==="
hits=$(curl -sG 'http://localhost:3100/loki/api/v1/query_range' \
  --data-urlencode 'query={app="miner"} |~ "DRAIN ABORTED|release vetoed|rebalance: some releases vetoed"' \
  --data-urlencode "limit=20" \
  --data-urlencode "start=$start_ns" \
  --data-urlencode "end=$now_ns" \
  | jq -r '.data.result[].values[][1]')

if [[ -z "$hits" ]]; then
  echo "OK: no veto/abort log lines found."
else
  echo "FAIL: veto/abort log lines still present:"
  echo "$hits"
fi

echo
echo "=== Drain decision metric (miner-0 :6065 forwarded by Tilt) ==="
curl -sf http://localhost:6065/metrics 2>/dev/null \
  | grep -E 'supplier_drain_decision_total{drain_trigger="rebalance_release"' \
  || echo "(metric not found yet — wait one rebalance interval ~30s)"
