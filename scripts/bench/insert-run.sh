#!/usr/bin/env bash
# One measured insert run: start the server, warm it, measure it, verify it.
#
#   ./scripts/bench/insert-run.sh <polars|duckdb> <label>
#
# Environment: VUS, FLUSH_BYTES, ENCODE_CONCURRENCY, WRITE_POOL, DURATION.
#
# The two writers do not take the same body and cannot be made to. The DuckDB
# path exists to take a body nobody parsed, and an unparsed body is NDJSON, which
# is row-major and 6.41 MiB where the columnar JSON is 3.28 MiB for the same
# rows. So the DuckDB arm moves twice the bytes over the socket. Stated here
# rather than left for a reader to discover.
set -uo pipefail

REPO="$(cd "$(dirname "$0")/../.." && pwd)"
WRITER="$1"
LABEL="$2"

VUS="${VUS:-16}"
DURATION="${DURATION:-60s}"
BODIES="${BODIES:-/tmp/smolquery-bodies}"
API_KEY="${SMOLQUERY_API_KEY:-benchkey}"
DATA_DIR="${SMOLQUERY_DATA_DIR:-/tmp/sq-bench}"
OUT="${OUT:-/tmp/smolquery-bench}/$LABEL"

case "$WRITER" in
  polars) BODY="$BODIES/columns.3062.json"; TYPE=application/json ;;
  duckdb) BODY="$BODIES/eachrow.3062.ndjson"; TYPE=application/x-ndjson ;;
  *) echo "usage: $0 <polars|duckdb> <label>" >&2; exit 2 ;;
esac

[ -f "$BODY" ] || { echo "missing $BODY — run generate.js first, see README" >&2; exit 1; }

mkdir -p "$OUT"

FLUSH_WRITER="$WRITER" "$REPO/scripts/bench/smolquery-up.sh" || exit 1

count() {
  curl -s -X POST localhost:4000/v1/queries \
    -H "authorization: Bearer $API_KEY" -H 'content-type: application/json' \
    -d '{"query":"SELECT count(*) AS n FROM logs.otel_logs"}' |
    sed -n 's/.*"rows":\[{"n":\([0-9]*\)}\].*/\1/p'
}

K6=(-e "URL=http://127.0.0.1:4000/v1/datasets/logs/tables/otel_logs/insert"
    -e "BODY=$BODY" -e ROWS=3062 -e "AUTH=Bearer $API_KEY"
    -e "CONTENT_TYPE=$TYPE" -e "VUS=$VUS")

# Discarded: schedulers, allocators and the page cache, not the number.
k6 run --quiet "${K6[@]}" -e DURATION=20s "$REPO/scripts/k6/insert.js" >/dev/null 2>&1
sleep 15

before="$(count)"

# The sampler outlives k6 by ten seconds: the drain after the clock, and the
# flush a buffer is still finishing, are work this run caused and must be counted.
watch_seconds=$(( ${DURATION%s} + 10 ))

go run "$REPO/scripts/k6/watch.go" -match 'beam\.smp' -match '^k6 run' \
  -duration "${watch_seconds}s" -label "$LABEL" -out "$OUT/watch.json" >"$OUT/watch.txt" 2>&1 &
watcher=$!

sleep 1
k6 run --quiet "${K6[@]}" -e "DURATION=$DURATION" "$REPO/scripts/k6/insert.js" >"$OUT/k6.txt" 2>&1
wait $watcher

after="$(count)"
accepted="$(grep -o '[0-9]* accepted' "$OUT/k6.txt" | grep -o '[0-9]*')"
landed=$((after - before))
claimed=$((accepted * 3062))

cat "$OUT/k6.txt"
cat "$OUT/watch.txt"

echo "  writer       $WRITER, body $(basename "$BODY")"
echo "  segments     $(find "$DATA_DIR" -name '*.parquet' | wc -l | tr -d ' ') parquet files on disk"

# A throughput number nobody checked against the stored rows is a count of round
# trips. The query goes through the real query path, so this also proves the rows
# are readable, not merely written.
if [ "$landed" = "$claimed" ]; then
  echo "  VERIFIED     $landed rows queryable == k6 accepted x 3062"
else
  echo "  MISMATCH     query says $landed, k6 says $claimed"
  exit 1
fi
