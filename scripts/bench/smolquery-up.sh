#!/usr/bin/env bash
# Starts smolquery for a benchmark run, with the write path's knobs set.
#
#   FLUSH_WRITER=duckdb FLUSH_BYTES=32000000 ENCODE_CONCURRENCY=4 WRITE_POOL=4 \
#     ./scripts/bench/smolquery-up.sh
#
# Wipes the data directory first: measured on an M1 Pro, the third run of a
# series was ~20% slower than the first purely because the table had grown, so a
# run that does not start cold is measuring the previous run.
#
# Uses `mix run --no-start` and starts the app from `--eval` so the settings are
# in place before the supervision tree reads them. Application config would work
# too; this keeps a benchmark from editing tracked files.
set -uo pipefail

REPO="$(cd "$(dirname "$0")/../.." && pwd)"

WRITER="${FLUSH_WRITER:-polars}"
FLUSH_BYTES="${FLUSH_BYTES:-4500000}"
ENC="${ENCODE_CONCURRENCY:-1}"
POOL="${WRITE_POOL:-1}"
DATA_DIR="${SMOLQUERY_DATA_DIR:-/tmp/sq-bench}"
API_KEY="${SMOLQUERY_API_KEY:-benchkey}"
LOG="${BENCH_LOG:-/tmp/smolquery-bench.log}"

"$REPO/scripts/clickhouse/down.sh" >/dev/null 2>&1
pkill -f 'mix run' >/dev/null 2>&1
pkill -f 'erts-.*/beam.smp' >/dev/null 2>&1
sleep 3

rm -rf "$DATA_DIR"
mkdir -p "$DATA_DIR"

cd "$REPO" || exit 1

MIX_ENV=prod SMOLQUERY_DATA_DIR="$DATA_DIR" SMOLQUERY_API_KEY="$API_KEY" \
  nohup mix run --no-start --eval "
    opts = Application.get_env(:smolquery, Smolquery.BufferService, [])

    opts =
      Keyword.merge(opts,
        flush_writer: :$WRITER,
        flush_max_bytes: $FLUSH_BYTES,
        encode_concurrency: $ENC,
        write_pool_size: $POOL
      )

    Application.put_env(:smolquery, Smolquery.BufferService, opts)
    {:ok, _started} = Application.ensure_all_started(:smolquery)
    conf = Application.get_env(:smolquery, Smolquery.BufferService)

    IO.puts(
      \"ready flush_writer=#{inspect(conf[:flush_writer])}\" <>
        \" flush_max_bytes=#{conf[:flush_max_bytes]}\" <>
        \" encode_concurrency=#{conf[:encode_concurrency]}\" <>
        \" write_pool_size=#{conf[:write_pool_size]}\"
    )

    Process.sleep(:infinity)
  " >"$LOG" 2>&1 &

# Waits for the node's own line, not for the port. The endpoint accepts requests
# a moment before `ensure_all_started/1` returns, so polling /healthz would race
# the confirmation and report a healthy server as a failed start.
ready=""

for _ in $(seq 120); do
  ready="$(grep -m1 '^ready ' "$LOG" 2>/dev/null)"
  [ -n "$ready" ] && break
  sleep 1
done

if [ -z "$ready" ]; then
  echo "smolquery did not start; tail of $LOG:" >&2
  tail -20 "$LOG" >&2
  exit 1
fi

# Echoed from the running node rather than from this script's variables: a knob
# that failed to apply must not be reported as applied.
echo "$ready"

[ "$(curl -s -o /dev/null -w '%{http_code}' localhost:4000/healthz)" = "200" ] || {
  echo "smolquery started but /healthz is not answering" >&2
  exit 1
}

auth=(-H "authorization: Bearer $API_KEY" -H 'content-type: application/json')

curl -s -X POST localhost:4000/v1/datasets "${auth[@]}" -d '{"id":"logs"}' -o /dev/null
curl -s -X POST localhost:4000/v1/datasets/logs/tables "${auth[@]}" \
  --data-binary @scripts/k6/schema.json -o /dev/null
curl -s -X PATCH localhost:4000/v1/datasets/logs/tables/otel_logs "${auth[@]}" \
  -d '{"clustering":["project_id","timestamp"]}' -o /dev/null

echo "table logs.otel_logs ready, clustered on (project_id, timestamp)"
