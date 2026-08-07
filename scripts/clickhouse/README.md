# ClickHouse bench lifecycle (native host process)

## Why native, not Docker

This benchmark compares read and write latency against smolquery on the same
machine. A container adds a network namespace hop and a storage driver between
the client and the server; at the millisecond scale this benchmark cares about,
that hop is a meaningful share of the number. ClickHouse therefore runs as a
plain host process from a pinned static binary, on a repo-local data directory,
with no root, no systemd, no package manager, and no Docker. Do not "simplify"
this into docker-compose — you would be measuring a different system.

## Pinned version

`scripts/clickhouse/install.sh` pins **`25.8`** in the `CLICKHOUSE_VERSION`
variable at the top of the file (and `up.sh` reads the same path). To bump the
pin, change that variable in `install.sh`, reinstall, and update this README.
Optionally set `CLICKHOUSE_SHA256` when downloading to verify the archive.

## Lifecycle

```sh
./scripts/clickhouse/install.sh   # once per pin / machine
./scripts/clickhouse/up.sh        # start detached; health-waits on SELECT 1
# … run bench/compare driver …
./scripts/clickhouse/down.sh      # TERM, then KILL after 30s if needed
```

`up.sh --reset` deletes the entire data root before starting. The path must
resolve under the repository root; `/` and paths outside the repo are refused.

Override the data directory with `CLICKHOUSE_DATA_ROOT` (default:
`.cache/clickhouse-data/` under the repo).

## Driver contract

| Item | Value |
|------|-------|
| PID file | `${CLICKHOUSE_DATA_ROOT:-.cache/clickhouse-data}/clickhouse.pid` — plain decimal pid, one line |
| HTTP endpoint | `http://127.0.0.1:8123` |
| Database | `smolbench` (created by the bench driver, not these scripts) |
| Page-cache marker | `drop-caches.sh` prints `PAGE_CACHE_DROPPED=yes` or `PAGE_CACHE_DROPPED=no` |

`up.sh` sets `max_threads` and `max_thread_pool_size` to the machine's logical
core count (`sysctl -n hw.logicalcpu` on macOS, `nproc` on Linux) so ClickHouse
matches the DuckDB thread count on the smolquery arm. Override with
`CLICKHOUSE_MAX_THREADS`.

`CLICKHOUSE_MAX_MEMORY` (default `0`) is substituted into
`max_server_memory_usage` in the rendered config (`0` = ClickHouse auto-detect).

## Deliberately not configured

- ClickHouse Keeper / ZooKeeper
- Replication
- Distributed tables or remote clusters

Single node, single shard.

## Verify before first run

The following could not be confirmed without network access or executing the
scripts:

1. **Tarball names** — `install.sh` constructs URLs under
   `https://packages.clickhouse.com/tgz/stable/` as
   `clickhouse-common-static-${VERSION}-amd64.tgz` /
   `clickhouse-common-static-${VERSION}-aarch64.tgz` on Linux and
   `clickhouse-macos-{aarch64,amd64}-${VERSION}.tgz` on macOS. The pin is
   `25.8` but stable archives may require a full patch build id (e.g.
   `25.8.2.25`) instead of the two-component tag.
2. **Tarball layout** — the installer locates the `clickhouse` binary with
   `find` after extraction. If the archive layout differs, adjust `install.sh`.
3. **`SYSTEM DROP …` statements** — `drop-caches.sh` tolerates failures for
   cache types your server version does not implement.
4. **Linux page cache** — dropping `/proc/sys/vm/drop_caches` requires root;
   without it the script warns and emits `PAGE_CACHE_DROPPED=no`.
