#!/usr/bin/env bash
# Starts a repo-local ClickHouse server for the smolbench comparison arm.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

CLICKHOUSE_VERSION="25.8"
INSTALL_ROOT="${REPO_ROOT}/.cache/clickhouse/${CLICKHOUSE_VERSION}"
CLICKHOUSE_BIN="${INSTALL_ROOT}/clickhouse"

DATA_ROOT="${CLICKHOUSE_DATA_ROOT:-${REPO_ROOT}/.cache/clickhouse-data}"
PID_FILE="${DATA_ROOT}/clickhouse.pid"
CONFIG_TEMPLATE="${SCRIPT_DIR}/config.d/bench.xml"
CONFIG_RENDERED="${DATA_ROOT}/config.xml"
LOG_FILE="${DATA_ROOT}/logs/clickhouse-server.log"

RESET=0
for arg in "$@"; do
  case "$arg" in
    --reset) RESET=1 ;;
    -h|--help)
      echo "usage: $0 [--reset]" >&2
      exit 0
      ;;
    *)
      echo "unknown argument: $arg" >&2
      exit 1
      ;;
  esac
done

resolve_path() {
  local path="$1"
  if [ -d "$path" ]; then
    (cd "$path" && pwd -P)
  elif [ -f "$path" ]; then
    local dir
    dir="$(cd "$(dirname "$path")" && pwd -P)"
    echo "${dir}/$(basename "$path")"
  else
    local parent
    parent="$(dirname "$path")"
    if [ "$parent" = "." ]; then
      echo "$(pwd -P)/$(basename "$path")"
    else
      echo "$(cd "$parent" && pwd -P)/$(basename "$path")"
    fi
  fi
}

safe_under_repo() {
  local resolved="$1"
  local repo_resolved
  repo_resolved="$(resolve_path "$REPO_ROOT")"
  case "$resolved" in
    "${repo_resolved}"|"${repo_resolved}"/*) return 0 ;;
    *) return 1 ;;
  esac
}

logical_cpus() {
  case "$(uname -s)" in
    Darwin) sysctl -n hw.logicalcpu ;;
    Linux) nproc ;;
    *)
      echo "unsupported platform for core count: $(uname -s)" >&2
      exit 1
      ;;
  esac
}

if [ ! -x "$CLICKHOUSE_BIN" ]; then
  echo "clickhouse binary missing at ${CLICKHOUSE_BIN} — run scripts/clickhouse/install.sh first" >&2
  exit 1
fi

if [ "$RESET" -eq 1 ]; then
  resolved_data="$(resolve_path "$DATA_ROOT")"
  if [ "$resolved_data" = "/" ]; then
    echo "refusing --reset: data root resolves to /" >&2
    exit 1
  fi
  if ! safe_under_repo "$resolved_data"; then
    echo "refusing --reset: ${resolved_data} is not under repo root ${REPO_ROOT}" >&2
    exit 1
  fi
  if [ -e "$resolved_data" ]; then
    echo "==> resetting data root ${resolved_data}"
    rm -rf "$resolved_data"
  fi
fi

mkdir -p "${DATA_ROOT}/data" "${DATA_ROOT}/tmp" "${DATA_ROOT}/user_files" \
  "${DATA_ROOT}/format_schemas" "${DATA_ROOT}/logs"

if [ -f "$PID_FILE" ]; then
  pid="$(cat "$PID_FILE" 2>/dev/null || true)"
  if [ -n "${pid:-}" ] && kill -0 "$pid" 2>/dev/null; then
    echo "clickhouse already running (pid ${pid})"
    echo "http://127.0.0.1:8123"
    echo "data root: ${DATA_ROOT}"
    exit 0
  fi
  rm -f "$PID_FILE"
fi

max_threads="${CLICKHOUSE_MAX_THREADS:-$(logical_cpus)}"
max_memory="${CLICKHOUSE_MAX_MEMORY:-0}"

sed \
  -e "s|__DATA_ROOT__|${DATA_ROOT}|g" \
  -e "s|__MAX_THREADS__|${max_threads}|g" \
  -e "s|__MAX_SERVER_MEMORY_USAGE__|${max_memory}|g" \
  "$CONFIG_TEMPLATE" >"$CONFIG_RENDERED"

nohup "$CLICKHOUSE_BIN" server \
  --config-file="$CONFIG_RENDERED" \
  --pid-file="$PID_FILE" \
  >>"$LOG_FILE" 2>&1 &

disown

deadline=$((SECONDS + 60))
while [ "$SECONDS" -lt "$deadline" ]; do
  if [ -f "$PID_FILE" ]; then
    pid="$(cat "$PID_FILE")"
    if [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null; then
      response="$(curl -fsS "http://127.0.0.1:8123/?query=SELECT%201" 2>/dev/null || true)"
      if [ "$response" = "1" ]; then
        echo "clickhouse started (pid ${pid})"
        echo "http://127.0.0.1:8123"
        echo "data root: ${DATA_ROOT}"
        exit 0
      fi
    fi
  fi
  sleep 0.5
done

echo "clickhouse failed to become ready within 60s" >&2
if [ -f "$LOG_FILE" ]; then
  echo "==> tail of ${LOG_FILE}" >&2
  tail -n 50 "$LOG_FILE" >&2 || true
fi
exit 1
