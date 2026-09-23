# VictoriaMetrics edge at cardinality — `bench/victoriametrics_cardinality.exs`

| | |
|---|---|
| Run | 2026-09-23 |
| Commit | `7d5e7bb` plus T-583 (the commit that adds this file) |
| Command | `TMPDIR=/home/dev/bench-tmp SERIES=<n> PROJECTS=<n / 100> REPS=3 LAKE_MEMORY=16GB mix run bench/victoriametrics_cardinality.exs 2>/dev/null` at 100,000, 10,000,000 and 100,000,000 series (defaults otherwise: `SAMPLES=1 HOURS=1 MAX_SERIES=1000000`) |
| Machine | aarch64, 8 cores, 31 GiB, Linux 6.12 (the dev box) |
| Runtime | Elixir 1.20.2 / OTP 29, 8 schedulers |

Every series has an `instance` of its own, one of ten `job` values and one of
`SERIES / 100` `project` values, and one sample in the last hour. The rows are
written as zstd Parquet by DuckDB `COPY`, ten million rows a file, and
registered as the table's sealed segments: the sealed tier as the query
service reads it, not the hot tier. A private node beside the application's
own, with the edge's series and sample ceilings raised to a million series
and a billion samples so a wide selector is measured rather than refused.

Each selector is `count(m{...})` over the hour at a 15 s step through
`/api/v1/query_range`. `fetch` is `SmolqueryVictoriaMetrics.Samples` reading
the matched samples by SQL, `sweep` the evaluator, and `in range` every
sample of `m` in the hour, which is what the scan reads: nothing prunes on a
label. The SQL rows are the edge's grouped query and a `count(*)` submitted
by hand through the query service with the predicate swapped, the MAP lookup
the edge writes against the integer fingerprints of the same series.

A billion-series tier (10,000,000 projects) was started and cancelled at 34 of
its 100 files: the decision below did not need it, and the scan's slope was
already clear at two orders of magnitude.

## 100,000 series, 1,000 projects

```
Generate — 100000 series x 1 samples, 1000 projects, over 1 h
  100000 rows in 0.4 s (279057 rows/s), 1 files, 1.1 MiB on disk

Label filters — 100000 series, 1000 projects, 1 h; 100000 samples of m in range; median of 3
query                               series   samples  wall ms  fetch ms  sweep ms  rest ms
m{instance="host-1"}                     1         1    136.1     135.2       0.3      0.6
m{project="project-1"}                 100       100    147.8     141.6       4.4      1.0
m{project=~"project-(1|22|333)"}       300       300    172.7     156.6      15.1      1.0
m{project=~"project-1.*"}            11100     11100   1184.9     409.1     786.8      1.0
m{job="job-1"}                       10000     10000    969.3     368.1     600.2      1.0
m{project!="project-1"}              99900     99900   8175.9    2565.9    5609.1      1.0

sql (same range, name = 'm')                          rows  wall ms
count(*), no label filter                                1    135.7
count(*) WHERE labels['instance'] = 'host-1'             1    145.1
count(*) WHERE series = <host-1>                         1    140.5
count(*) WHERE labels['project'] = 'project-1'           1    151.2
count(*) WHERE series IN (<100 of project-1>)            1    148.9
grouped WHERE labels['instance'] = 'host-1'              1    136.4
grouped WHERE series = <host-1>                          1    128.9
grouped WHERE labels['project'] = 'project-1'          100    149.1
grouped WHERE series IN (<100 of project-1>)           100    149.5

query                               series   samples  wall ms  fetch ms  sweep ms  rest ms
/api/v1/labels 1h                                       152.1
/api/v1/label/job/values 1h                             156.2
/api/v1/label/project/values 1h                         152.2
```


Two controls added after the sweep, run at 100,000 series only (a separate
run, same machine): the edge's grouped query without its `labels` column,
and a pushed-down `count(DISTINCT series)` per 15 s step for the `job`
selector, which is what T-568 would send instead of copying the series out.

```
sql (same range, name = 'm')                                    rows  wall ms
grouped, no labels column, WHERE series IN (<100 of project-1>)     100    134.6
count(DISTINCT series) by step WHERE labels['job'] = 'job-1'        240    163.9
```

Against the edge's 969 ms for `count(m{job="job-1"})` above (10,000 series
fetched and counted in Elixir), the pushed-down count is 164 ms, the job
floor plus the scan; the fetch without labels is at the floor.

## 10,000,000 series, 100,000 projects

```
Generate — 10000000 series x 1 samples, 100000 projects, over 1 h
  10000000 rows in 23.5 s (425159 rows/s), 1 files, 93.2 MiB on disk

Label filters — 10000000 series, 100000 projects, 1 h; 10000000 samples of m in range; median of 3
query                               series   samples  wall ms  fetch ms  sweep ms  rest ms
m{instance="host-1"}                     1         1    344.6     343.8       0.3      0.6
m{project="project-1"}                 100       100    399.3     391.3       7.0      0.9
m{project=~"project-(1|22|333)"}       300       300    466.7     448.1      16.6      1.0
m{project=~"project-1.*"}           skipped: 1111100 series expected
m{job="job-1"}                     1000000   1000000 124246.8   29011.1   94142.1      1.0
m{project!="project-1"}             skipped: 9999900 series expected

sql (same range, name = 'm')                          rows  wall ms
count(*), no label filter                                1    147.8
count(*) WHERE labels['instance'] = 'host-1'             1    343.8
count(*) WHERE series = <host-1>                         1    143.3
count(*) WHERE labels['project'] = 'project-1'           1    372.2
count(*) WHERE series IN (<100 of project-1>)            1    182.6
grouped WHERE labels['instance'] = 'host-1'              1    335.3
grouped WHERE series = <host-1>                          1    130.4
grouped WHERE labels['project'] = 'project-1'          100    361.1
grouped WHERE series IN (<100 of project-1>)           100    350.2

query                               series   samples  wall ms  fetch ms  sweep ms  rest ms
/api/v1/labels 1h                                       648.2
/api/v1/label/job/values 1h                             409.7
/api/v1/label/project/values 1h                         650.9
```

## 100,000,000 series, 1,000,000 projects

```
Generate — 100000000 series x 1 samples, 1000000 projects, over 1 h
  100000000 rows in 235.1 s (425300 rows/s), 10 files, 890.5 MiB on disk

Label filters — 100000000 series, 1000000 projects, 1 h; 100000000 samples of m in range; median of 3
query                               series   samples  wall ms  fetch ms  sweep ms  rest ms
m{instance="host-1"}                     1         1   2377.9    2377.0       0.4      0.6
m{project="project-1"}                 100       100   2434.4    2427.9       5.5      1.0
m{project=~"project-(1|22|333)"}       300       300   3214.5    3199.1      16.7      1.0
m{project=~"project-1.*"}           skipped: 11111100 series expected
m{job="job-1"}                      skipped: 10000000 series expected
m{project!="project-1"}             skipped: 99999900 series expected

sql (same range, name = 'm')                          rows  wall ms
count(*), no label filter                                1    445.6
count(*) WHERE labels['instance'] = 'host-1'             1   2525.6
count(*) WHERE series = <host-1>                         1    422.8
count(*) WHERE labels['project'] = 'project-1'           1   2526.9
count(*) WHERE series IN (<100 of project-1>)            1    527.1
grouped WHERE labels['instance'] = 'host-1'              1   2384.2
grouped WHERE series = <host-1>                          1    188.3
grouped WHERE labels['project'] = 'project-1'          100   2454.4
grouped WHERE series IN (<100 of project-1>)           100   2288.2

query                               series   samples  wall ms  fetch ms  sweep ms  rest ms
/api/v1/labels 1h                                      3584.0
/api/v1/label/job/values 1h                            2853.6
/api/v1/label/project/values 1h                        6489.1
```

## What this settles

- **A label matcher is a scan of the metric, at about 21 ns a row.** Past the
  job floor (`count(*)` with no label filter: 148 ms at ten million rows,
  446 ms at a hundred million), `labels['project'] = 'project-1'` costs
  224 ms per ten million rows and 2.08 s per hundred million, the same slope,
  whether it matches one series or a hundred. A regular expression on the
  same label is about a third more. Extrapolated, one selector over a
  billion samples of a metric is about 21 s of scan before anything is
  evaluated. Parquet keeps no statistics for a MAP value, so nothing prunes.
- **The cost is decoding the MAP column, not comparing it.** Filtering the
  same hundred series by their integer fingerprints costs 527 ms at a
  hundred million rows for `count(*)`, a fifth of the MAP lookup, and one
  fingerprint by `=` beats the unfiltered count, since DuckDB skips row
  groups by Parquet's bloom filter. But the edge's grouped query with the
  same `IN` list took 2,288 ms, as slow as the MAP filter, because it
  projects `any_value(labels)` and so decodes the map for every row group
  the filter touches. A series table (T-569) only pays off if the samples
  rows stop carrying `labels` at all: the labels come from the series
  table, and the samples scan reads `series`, `ts` and `value` only.
- **A wide selector is the evaluator, not the scan.** `count(m{job="job-1"})`
  at ten million series matched a million: 124 s in all, of which the scan
  was about 0.4 s, grouping a million series and copying their lists into
  the node 29 s, and counting them in Elixir 94 s. VictoriaMetrics never
  pays either: its index resolves the label to series ids without reading
  the rest, and vmselect aggregates blocks as they stream, in Go, under a
  default of 300,000 unique series a query. Here the database has already
  grouped the rows; the aggregate has to run there too. That is T-568,
  pushing `count`, `sum by`, and the common rollups into SQL so the node
  receives one row per output series per step, and it is the decision this
  sweep was run to make: two orders of magnitude before the layout matters.
- **The label routes scan the range, linearly.** 650 ms at ten million rows,
  3.6 s (`/api/v1/labels`) to 6.5 s (`/api/v1/label/project/values`, a
  million distinct values) at a hundred million. Grafana's label browser
  calls these on every panel edit. The series table (T-569) makes them reads
  of a table sized by series, not samples.
- **Storage is 9 bytes a sample** with three labels a row under zstd, and
  DuckDB writes it at 425,000 rows a second on this box; a billion series
  is about 9 GB.
