#!/usr/bin/env bash
# End-to-end smoke test against the kind cluster (Milestone 8 L7): ingest
# through the API edge, a query fanning out across the buffer fleet, a seal
# landing in MinIO through the Postgres catalog, and a buffer-node drain
# losing nothing.
set -euo pipefail

cd "$(dirname "$0")/.."

BASE=${BASE:-http://localhost:8080}
KEY=${SMOLQUERY_API_KEY:-kind-only-api-key}

CLUSTER=smolquery
KCTX="kind-${CLUSTER}"

# The repo-scoped kubeconfig kind-up.sh writes — works without direnv too.
export KUBECONFIG="$PWD/.kube/config"

# A fresh dataset per run: buffers (PVCs), the Postgres catalog, and MinIO all
# survive redeploys, so a fixed name would accumulate rows across runs.
DS="smoke_$(date +%s)"

# Pin every kubectl to the local kind context — never a stray ambient one.
kubectl() { command kubectl --context "$KCTX" "$@"; }

if ! command kubectl config get-contexts -o name 2>/dev/null | grep -qx "$KCTX"; then
  echo "ERROR: kube context '$KCTX' not found — run scripts/kind-up.sh first." >&2
  exit 1
fi

auth="authorization: Bearer $KEY"
json='content-type: application/json'

say() { printf '\n==> %s\n' "$*"; }

count_rows() {
  curl -fsS -H "$auth" -H "$json" \
    -d '{"query": "SELECT (SELECT count(*) FROM '$DS'.a) + (SELECT count(*) FROM '$DS'.b) AS n"}' \
    "$BASE/v1/queries" | jq -r '.rows[0].n | tostring'
}

expect_count() {
  local want=$1 got
  got=$(count_rows)
  if [ "$got" != "$want" ]; then
    echo "FAIL: expected $want rows across $DS.a + $DS.b, got $got" >&2
    exit 1
  fi
  echo "count across both tables: $got"
}

insert_rows() {
  local table=$1 n=$2 body
  body=$(jq -nc --argjson n "$n" '{rows: [range($n) | {id: ., v: ("r" + tostring)}]}')
  curl -fsS -H "$auth" -H "$json" -d "$body" \
    "$BASE/v1/datasets/$DS/tables/$table/insert" |
    jq -e '(.insertErrors // []) | length == 0' >/dev/null
}

say "waiting for the API"
for _ in $(seq 60); do
  curl -fsS "$BASE/healthz" >/dev/null 2>&1 && break
  sleep 2
done
curl -fsS "$BASE/healthz" >/dev/null

say "creating dataset + two tables"
curl -fsS -H "$auth" -H "$json" -d '{"id": "'$DS'"}' "$BASE/v1/datasets" >/dev/null
for t in a b; do
  curl -fsS -H "$auth" -H "$json" -d '{"id": "'"$t"'", "schema": [
        {"name": "id", "type": "INT64", "nullable": false},
        {"name": "v", "type": "STRING"}
      ]}' "$BASE/v1/datasets/$DS/tables" >/dev/null
done

say "inserting 200 rows into each table"
insert_rows a 200
insert_rows b 200

say "querying across both tables (planner fans out to every buffer node)"
expect_count 400

say "waiting for a seal to land in MinIO (seal_max_age is 60s)"
sealed=""
for _ in $(seq 60); do
  sealed=$(kubectl -n smolquery exec deploy/minio -- \
    sh -c 'for f in /data/smolquery-sealed/'"$DS"'/*/*.parquet; do [ -e "$f" ] && echo "$f" && break; done; :' || true)
  [ -n "$sealed" ] && break
  sleep 3
done
if [ -z "$sealed" ]; then
  echo "FAIL: no sealed segment appeared in MinIO within 180s" >&2
  exit 1
fi
echo "sealed object: $sealed"

say "re-querying after the seal (sealed tier reads back over s3://)"
expect_count 400

say "draining smolquery-buffer-0 (force-seal everything it owns, leave the ring)"
kubectl -n smolquery exec smolquery-buffer-0 -c smolquery -- /app/bin/smolquery rpc \
  ':ok = Smolquery.BufferService.Drain.drain(Smolquery.BufferService, timeout_ms: 120_000)'

say "inserting 100 more rows (must route to the remaining owners)"
insert_rows a 100
expect_count 500

say "restoring smolquery-buffer-0"
kubectl -n smolquery delete pod smolquery-buffer-0 --wait=false
kubectl -n smolquery rollout status statefulset/smolquery-buffer --timeout=180s
expect_count 500

say "smoke OK"
