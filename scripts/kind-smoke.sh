#!/usr/bin/env bash
# End-to-end smoke test against the kind cluster: the whole of Milestone 8's
# exit criterion (PL-11), on real distinct hosts.
#
#   L1/L2/L3  the fleet forms, ingests, and seals to MinIO through the Postgres
#             catalog, read back locked-down over s3://
#   L4        draining a buffer node loses no acked row, and new writes route to
#             the remaining owners
#   L5        a query whose tables are owned by *different* buffer nodes fans out
#             (asserted, not assumed — table names are chosen so the two tables
#             land on two nodes), and force-killing an owner makes queries over
#             its tables fail cleanly rather than hang or answer short (T-94)
#   L6        two StorageService replicas never double-merge: every row stays
#             unique across seals, which a table added to the catalog twice
#             would break
#
# The throughput half of the exit criterion is not here — see
# bench/cluster_ingest.exs, which measures it off a fleet of peer BEAMs rather
# than a 4 GB Docker VM.
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

fail() { echo "FAIL: $*" >&2; exit 1; }

rpc() { kubectl -n smolquery exec smolquery-api-0 -c smolquery -- /app/bin/smolquery rpc "$1"; }

# The pod behind a node name: smolquery@smolquery-buffer-1.<svc>… -> smolquery-buffer-1
pod_of_node() { printf '%s' "${1#*@}" | cut -d. -f1; }

owner_pod() {
  local table=$1 node
  node=$(rpc "IO.puts(Smolquery.BufferService.Client.owner(Smolquery.BufferService, {\"$DS\", \"$table\"}))" | tr -d '\r' | tail -n1)
  pod_of_node "$node"
}

count_rows() {
  curl -fsS --max-time 60 -H "$auth" -H "$json" \
    -d '{"query": "SELECT (SELECT count(*) FROM '"$DS.$T1"') + (SELECT count(*) FROM '"$DS.$T2"') AS n"}' \
    "$BASE/v1/queries" | jq -r '.rows[0].n | tostring'
}

expect_count() {
  local want=$1 got
  got=$(count_rows)
  [ "$got" = "$want" ] || fail "expected $want rows across $DS.$T1 + $DS.$T2, got $got"
  echo "count across both tables: $got"
}

expect_no_duplicates() {
  local table dup
  for table in "$T1" "$T2"; do
    dup=$(curl -fsS --max-time 60 -H "$auth" -H "$json" \
      -d '{"query": "SELECT count(*) - count(DISTINCT id) AS dup FROM '"$DS.$table"'"}' \
      "$BASE/v1/queries" | jq -r '.rows[0].dup | tostring')
    [ "$dup" = "0" ] || fail "$DS.$table has $dup duplicated ids — a segment was merged twice"
  done
  echo "no duplicated rows in either table"
}

insert_rows() {
  local table=$1 n=$2 from=$3 body
  body=$(jq -nc --argjson n "$n" --argjson from "$from" \
    '{rows: [range($from; $from + $n) | {id: ., v: ("r" + tostring)}]}')
  curl -fsS --max-time 120 -H "$auth" -H "$json" -d "$body" \
    "$BASE/v1/datasets/$DS/tables/$table/insert" |
    jq -e '(.insertErrors // []) | length == 0' >/dev/null
}

say "waiting for the API"
for _ in $(seq 60); do
  curl -fsS "$BASE/healthz" >/dev/null 2>&1 && break
  sleep 2
done
curl -fsS "$BASE/healthz" >/dev/null

say "choosing two tables the ring puts on two different buffer nodes"
pick=$(rpc "
  ds = \"$DS\"
  owner = fn i -> Smolquery.BufferService.Client.owner(Smolquery.BufferService, {ds, \"t\" <> Integer.to_string(i)}) end
  first = owner.(1)

  case Enum.find(2..500, fn i -> owner.(i) != first end) do
    nil -> IO.puts(\"NONE\")
    i -> IO.puts(\"t1 t\" <> Integer.to_string(i))
  end
" | tr -d '\r' | tail -n1)

[ "$pick" = "NONE" ] && fail "every candidate table hashed to one buffer node — is the ring a single node?"

T1=$(echo "$pick" | cut -d' ' -f1)
T2=$(echo "$pick" | cut -d' ' -f2)

say "creating dataset + tables $T1, $T2"
curl -fsS -H "$auth" -H "$json" -d '{"id": "'$DS'"}' "$BASE/v1/datasets" >/dev/null
for t in "$T1" "$T2"; do
  curl -fsS -H "$auth" -H "$json" -d '{"id": "'"$t"'", "schema": [
        {"name": "id", "type": "INT64", "nullable": false},
        {"name": "v", "type": "STRING"}
      ]}' "$BASE/v1/datasets/$DS/tables" >/dev/null
done

owner1=$(owner_pod "$T1")
owner2=$(owner_pod "$T2")
[ "$owner1" != "$owner2" ] || fail "$T1 and $T2 both landed on $owner1"
echo "$T1 -> $owner1, $T2 -> $owner2"

say "inserting 200 rows into each table"
insert_rows "$T1" 200 0
insert_rows "$T2" 200 0

say "querying across both tables (planner fans out to two owners)"
expect_count 400

say "waiting for a seal to land in MinIO (seal_max_age is 60s)"
sealed=""
for _ in $(seq 60); do
  sealed=$(kubectl -n smolquery exec deploy/minio -- \
    sh -c 'for f in /data/smolquery-sealed/'"$DS"'/*/*.parquet; do [ -e "$f" ] && echo "$f" && break; done; :' || true)
  [ -n "$sealed" ] && break
  sleep 3
done
[ -n "$sealed" ] || fail "no sealed segment appeared in MinIO within 180s"
echo "sealed object: $sealed"

say "re-querying after the seal (sealed tier reads back over s3://)"
expect_count 400

say "checking the seal was merged once, by two live StorageService replicas"
replicas=$(kubectl -n smolquery get pods -l app=smolquery-storage \
  --field-selector status.phase=Running -o name | wc -l | tr -d ' ')
[ "$replicas" -ge 2 ] || fail "expected 2 running storage replicas for the ownership gate, saw $replicas"
expect_no_duplicates

say "draining smolquery-buffer-0 (force-seal everything it owns, leave the ring)"
kubectl -n smolquery exec smolquery-buffer-0 -c smolquery -- /app/bin/smolquery rpc \
  ':ok = Smolquery.BufferService.Drain.drain(Smolquery.BufferService, timeout_ms: 120_000)'

say "inserting 100 more rows (must route to the remaining owners)"
insert_rows "$T1" 100 200
expect_count 500

say "restoring smolquery-buffer-0"
kubectl -n smolquery delete pod smolquery-buffer-0 --wait=false
kubectl -n smolquery rollout status statefulset/smolquery-buffer --timeout=180s
expect_count 500

say "force-killing $T1's owner — queries must fail cleanly, never answer short"
victim=$(owner_pod "$T1")
echo "victim: $victim ($T1 holds 300 acked rows)"
kubectl -n smolquery delete pod "$victim" --force --grace-period=0 --wait=false

# The deadline has to clear the planner's own: buffer_timeout_ms (30s) plus the
# 5s fetch_deadline slack, so a blackholed node's fan-out is a clean failure
# here rather than a timeout this script would misread as a hang.
clean_failures=0

for attempt in $(seq 10); do
  started=$(date +%s)

  if body=$(curl -sS --max-time 45 -w '\n%{http_code}' -H "$auth" -H "$json" \
    -d '{"query": "SELECT count(*) AS n FROM '"$DS.$T1"'"}' "$BASE/v1/queries" 2>&1); then
    code=$(printf '%s' "$body" | tail -n1)

    if [ "$code" = "200" ]; then
      n=$(printf '%s' "$body" | sed '$d' | jq -r '.rows[0].n | tostring')
      [ "$n" = "300" ] || fail "answered $n of 300 while $victim was down — acked rows went silently missing (T-94)"
      outcome="complete answer: 300"
    else
      clean_failures=$((clean_failures + 1))
      outcome="failed cleanly with HTTP $code after $(( $(date +%s) - started ))s"
    fi
  else
    status=$?
    [ "$status" -eq 28 ] && fail "query hung past the 45s deadline while $victim was down"
    clean_failures=$((clean_failures + 1))
    outcome="failed cleanly after $(( $(date +%s) - started ))s (curl $status)"
  fi

  echo "attempt $attempt: $outcome"
  sleep 2
done

if [ "$clean_failures" -eq 0 ]; then
  echo "NOTE: $victim recovered before any query saw it down — the T-94 window was not observed" >&2
else
  echo "observed $clean_failures clean failures while $victim was down"
fi

say "restoring $victim"
kubectl -n smolquery rollout status statefulset/smolquery-buffer --timeout=180s
for _ in $(seq 30); do
  [ "$(count_rows || true)" = "500" ] && break
  sleep 3
done
expect_count 500
expect_no_duplicates

say "smoke OK"
