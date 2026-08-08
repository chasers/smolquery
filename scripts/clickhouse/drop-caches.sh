#!/usr/bin/env bash
# Drops ClickHouse server caches and, when permitted, the OS page cache.
set -euo pipefail

CLICKHOUSE_HTTP="${CLICKHOUSE_HTTP:-http://127.0.0.1:8123}"

ch_query() {
  local query="$1"
  curl -fsS --data-binary "$query" "${CLICKHOUSE_HTTP}/" 2>/dev/null
}

drop_ch_cache() {
  local statement="$1"
  if ch_query "$statement" >/dev/null; then
    echo "==> ${statement}"
    return 0
  fi
  echo "warning: ${statement} failed or is unsupported on this server version" >&2
}

drop_ch_cache "SYSTEM DROP MARK CACHE"
drop_ch_cache "SYSTEM DROP UNCOMPRESSED CACHE"
drop_ch_cache "SYSTEM DROP QUERY CACHE"
drop_ch_cache "SYSTEM DROP COMPILED EXPRESSION CACHE"

page_cache_dropped=no

case "$(uname -s)" in
  Darwin)
    if command -v purge >/dev/null 2>&1; then
      purge
      page_cache_dropped=yes
      echo "==> purged OS page cache (darwin purge)"
    else
      echo "warning: purge not found — OS page cache was NOT dropped; numbers may be warm" >&2
    fi
    ;;
  Linux)
    if [ "$(id -u)" -eq 0 ] && [ -w /proc/sys/vm/drop_caches ]; then
      sync
      echo 3 >/proc/sys/vm/drop_caches
      page_cache_dropped=yes
      echo "==> dropped OS page cache (linux vm.drop_caches)"
    else
      echo "warning: linux page cache drop requires root write to /proc/sys/vm/drop_caches — OS page cache was NOT dropped; numbers may be warm" >&2
    fi
    ;;
  *)
    echo "warning: unsupported OS for page cache drop — OS page cache was NOT dropped; numbers may be warm" >&2
    ;;
esac

echo "PAGE_CACHE_DROPPED=${page_cache_dropped}"
