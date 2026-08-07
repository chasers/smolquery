#!/usr/bin/env bash
# Stops the repo-local ClickHouse server started by up.sh.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

DATA_ROOT="${CLICKHOUSE_DATA_ROOT:-${REPO_ROOT}/.cache/clickhouse-data}"
PID_FILE="${DATA_ROOT}/clickhouse.pid"

if [ ! -f "$PID_FILE" ]; then
  exit 0
fi

pid="$(cat "$PID_FILE" 2>/dev/null || true)"
rm -f "$PID_FILE"

if [ -z "${pid:-}" ] || ! kill -0 "$pid" 2>/dev/null; then
  exit 0
fi

echo "==> stopping clickhouse (pid ${pid})"
kill -TERM "$pid" 2>/dev/null || true

deadline=$((SECONDS + 30))
while kill -0 "$pid" 2>/dev/null; do
  if [ "$SECONDS" -ge "$deadline" ]; then
    echo "==> clickhouse did not exit after TERM — sending KILL"
    kill -KILL "$pid" 2>/dev/null || true
    break
  fi
  sleep 0.5
done

wait "$pid" 2>/dev/null || true
