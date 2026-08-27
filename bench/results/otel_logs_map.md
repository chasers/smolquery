# OTel attribute bags: `MAP(STRING, STRING)` vs `VARIANT` vs flat columns (T-393, 2026-08-27)

Measured on the sandbox cluster (`main@43f1b30`) with the
`smolquery_bench` harness — ClickStack logs layout, `project` first in the
clustering key, three arms fed the same generator rows, 8 and 32 VUs, twelve
project-scoped and unscoped query cases over hot ∪ sealed and sealed. The
full write-up with every table and plan is
`smolquery_bench/results/2026-08-27-clickstack-map-vs-variant.md`.

## Recommendation: `MAP(STRING, STRING)`

- **VARIANT is not queryable at 7.9M rows once the bag holds per-row unique
  values.** Every query naming the variant column fails with DuckDB
  `Out of Memory Error: failed to allocate data of size 128.0 MiB (… / 953.6
  MiB used)` — `SMOLQUERY_MEMORY_LIMIT=1GB` — scoped to one project or not,
  scattered or single-engine. The column is JSON text on disk, ~1.3 KB per
  row decoded, and the Parquet reader's column chunks alone exceed the
  engine. The map answers the same key filter in 2.8 s over 6.0M rows; the
  63 flat columns in 1.5 s over 7.6M.
- With a body that repeats verbatim per request the variant *looked* faster
  than the map on filters (1.1 s vs 3.7 s): Parquet dictionary-encoded the
  3,062 distinct strings and DuckDB evaluated `variant_extract(...) = 'POST'`
  on dictionary entries. The group-by, which parses every row, showed the
  real cost: 2.2 µs per row against the map's 0.70 µs and flat's 0.08 µs.

## Ingest and seal

| arm | 32 VUs rows/s | MiB/s | p50 | seal, s per ~400k rows |
|---|---|---|---|---|
| variant | 148,028 | 357 | 653 ms | 6.8–7.7 |
| flat (63 columns) | 131,526 | 295 | 712 ms | 7.4 |
| map | 101,867 | 246 | 924 ms | 13.4–14.5 |

Zero refusals, zero restarts on every arm. The map ingest tax is the row
path's re-encode for a map schema (`Schema.value_from_json/2`); the map seal
costs 2× the flat seal for the nested Parquet MAP write.

## Size

Not comparable at this bench's repetition: values repeat across requests, so
every arm compressed to 2.4–7.5 B/row against 190 B/row for unique rows.
The one honest ratio: making each variant string unique took the variant
from 3.0 to 7.5 B/row; the map moved from 6.0 to 5.7, because a map
dictionary-encodes per key.

## `project` in the clustering key

Prunes row groups, not files. Every sealed file holds every project (each
request body carries 955 of 1,000), so `Total Files Read` is the whole table
on every case; inside a file the seal's `ORDER BY` gives the 100k-row groups
tight bounds and a scoped key filter runs ~6× cheaper than unscoped. File
pruning per project needs a partition key.

## Re-run when

T-394 lands (native shredded VARIANT), or `SMOLQUERY_MEMORY_LIMIT` changes
on the api pods. Harness: `mise run bench-attrs` in `smolquery_bench`.
