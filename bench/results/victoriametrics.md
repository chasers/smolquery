# VictoriaMetrics edge — `bench/victoriametrics.exs`

| | |
|---|---|
| Run | 2026-09-22 |
| Commit | `f597586` plus T-567 (the commit that adds this file) |
| Command | `mix run bench/victoriametrics.exs 2>/dev/null` (defaults: `SERIES=1000 HOURS=6 WRITERS=8 WRITE_REQUESTS=100 REPS=5`) |
| Machine | aarch64, 8 cores, 31 GiB, Linux 6.12 (the dev box) |
| Runtime | Elixir 1.20.2 / OTP 29, 8 schedulers |

A private node beside the application's own: DuckLake on SQLite, a buffer
with the default flush settings, an ingest service, a query service, and the
edge on a port the OS picked. No storage service, so nothing sealed: every
sample was in the hot tier when it was read. The driver runs on the same
machine.

## Remote write

```
Remote write — 10000 samples a request, 8 writers
─────────────────────────────────────────────────
depth (samples)     requests   samples/s   p50 ms   p99 ms   429s
0                        100    344636.0    204.3    525.4      0
Load — 1000 series of m over 6 h at 15 s
────────────────────────────────────────
  144 requests, 1440000 samples in 3883.8 ms, 0 retried after a 429
2440000                  100    362723.0    215.2    307.3      0
```

Each request is 1,000 series of 10 samples, snappy framed, 10,000 samples as
vmagent's default block. About 350,000 samples a second with eight writers,
and no slower with 2.4 million samples already in the hot tier. The p99 of
the first round is the table's first commits. No request was refused.

## Reads

```
Reads — 1000 series, 6 h at 15 s; median of 5
─────────────────────────────────────────────
query                                         series   samples  wall ms  fetch ms  sweep ms  rest ms
rate(m[5m]) 1h                                  1000    280000   2455.8     693.4     158.7   1610.5
rate(m[5m]) 6h                                  1000   1440000  11275.5    1181.7    1011.8   9050.1
sum by (job) (rate(m[5m])) 1h                   1000    280000    870.6     690.2     177.7      5.6
sum by (job) (rate(m[5m])) 6h                   1000   1440000   2440.1    1163.4    1248.8     41.1
m{instance="host-1"} 6h                            1      1440    774.6     771.0       0.4      3.2
/api/v1/labels 6h                                                 266.0
/api/v1/label/job/values 6h                                       449.9
```

All at a 15 s step: 241 points an hour, 1,441 over six. `fetch` is the SQL
that reads raw samples into the node (`fetch_us` of
`[:smolquery, :victoriametrics, :query]`), `sweep` the rollup sweep and
evaluation, `rest` rendering the JSON and sending it; the driver does not
decode the answer.

### After the review of T-564: one grouped query a selector (T-576)

Same command and machine, on the review commit: each selector is one
`GROUP BY series` job with its samples as two lists, instead of a series
query (an aggregate the query service scattered over four workers) and a
samples query.

```
query                                         series   samples  wall ms  fetch ms  sweep ms  rest ms
rate(m[5m]) 1h                                  1000    280000   2226.5     382.2     127.8   1712.0
rate(m[5m]) 6h                                  1000   1440000  11263.3     955.1    1036.2   9255.8
sum by (job) (rate(m[5m])) 1h                   1000    280000    827.1     625.8     198.4      6.8
sum by (job) (rate(m[5m])) 6h                   1000   1440000   2503.1    1080.1    1380.8     42.2
m{instance="host-1"} 6h                            1      1440    276.8     273.5       0.4      3.0
/api/v1/labels 6h                                                 246.8
/api/v1/label/job/values 6h                                       444.5
```

The one-series read fell from 771 ms of fetch to 274, and a thousand
series over an hour from 693 to 382. Measured apart, on the test stack
(`Smolquery.Test.VictoriaMetricsStack`, one series among 1,000, the median
of seven reads of `Samples.select/4`):

| hot micro-segments | series + samples queries | one grouped query |
|---|---|---|
| 50 | 591 ms | 181 ms |
| 200 | 792 ms | 301 ms |

## What this settles

- **Tier 1 is not the bottleneck at this size.** Reading 1.44 million samples
  into the node takes about 1.2 s, and sweeping them about 1 s. The design's
  first ceiling (PL-70: `max_samples` 20,000,000) was about 17 s of fetch at
  this rate and 1 GB or more held in one process; the review of T-564 lowered
  it to 5,000,000 a selector and 10,000,000 a query, about 0.25 and 0.5 GB,
  which is what `SMOLQUERY_VICTORIAMETRICS_MAX_SAMPLES` and
  `_MAX_SAMPLES_PER_QUERY` should be sized against.
- **A fetch had a floor of about 0.7 s**, one series or a thousand: the
  one-series `default_rollup` paid the same two query-service jobs (series,
  then samples) over the hot tier, the first scattered. One grouped job
  brought it to about 0.27 s (above); what is left is one job's planning
  and the hot tier's files, which label matchers cannot prune.
- **Rendering a wide matrix costs more than computing it.** 1,000 series of
  1,441 points is 1.44 million points of JSON: 9 s past the query, against
  2.2 s for the query. An aggregate that answers ten series renders in
  milliseconds. Grafana asks for about 1,000 points a panel, so the answer
  that matters is a few hundred series at most; a faster matrix renderer is
  a follow-up, not a blocker.
- **The label routes scan the range.** `/api/v1/labels` over six hours reads
  every sample's label map (T-569 is the series index that would not).
- **Remote write keeps up with vmagent.** A vmagent block lands in about
  200 ms at p50 under eight concurrent writers; one vmagent sends with
  `-remoteWrite.queues` workers (2 x cores by default), so a node takes
  about 350,000 samples a second from one.
