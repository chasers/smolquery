# VictoriaMetrics and Prometheus on smolquery

smolquery takes the metrics vmagent, Prometheus and the OpenTelemetry collector
push, and answers the MetricsQL that Grafana sends, on its own listener. The
`:victoriametrics` role starts the edge (`SmolqueryVictoriaMetrics`, PL-70). It
stands in for a single-node VictoriaMetrics: point `-remoteWrite.url` at it, and
point Grafana's Prometheus datasource at the same address. Every sample becomes
one row of an ordinary smolquery table, so it seals into Parquet and ages out by
retention like any other row.

```sh
vmagent-prod -promscrape.config=scrape.yml \
  -remoteWrite.url=http://127.0.0.1:8428/api/v1/write \
  -remoteWrite.bearerToken="$SMOLQUERY_API_KEY"

curl -sS -H "authorization: Bearer $SMOLQUERY_API_KEY" \
  'http://127.0.0.1:8428/api/v1/query?query=sum+by+(job)+(rate(vm_promscrape_scrapes_total[1m]))'
```

> **How this was checked.** Against VictoriaMetrics v1.152.0 and its own tests,
> not against a Grafana in a browser.
>
> 1. **The parser** carries all 838 cases of metricsql v0.87.4's `parser_test.go`
>    (`test/support/fixtures/victoriametrics/metricsql_corpus.jsonl`). Every case
>    VictoriaMetrics refuses is refused here; 115 it accepts are refused here (108
>    `WITH` templates, 7 argument counts it checks later than this parser does).
> 2. **The rollups** carry the cases of `rollup_test.go` for every rollup
>    function listed below.
> 3. **The evaluator** carries `exec_test.go`: 535 of its 610 success cases answer
>    VictoriaMetrics' series, in its order, to 1e-13. The other 75 use functions
>    listed below as not ported, and are asserted to answer 422. All 224 of its
>    error cases are refused.
> 4. **The write** decodes a request captured from vmagent v1.152.0 in each of its
>    two protocols (`test/support/fixtures/victoriametrics/`).
> 5. **A real vmagent v1.152.0** runs against the edge in
>    `test/smolquery_victoriametrics/vmagent_integration_test.exs` (`mix test
>    --only integration`). It scrapes itself every second and writes in its own
>    zstd protocol, then in Prometheus remote write 1.0. The test runs Grafana's
>    connect sequence (`buildinfo`, then `1+1`) and reads the samples back as
>    Grafana asks for them: `up`, `vm_app_version`, the label routes, `rate` and
>    a `query_range`. It also reads vmagent's own counters: every write was a
>    2xx, with no retry and no fall back to the other protocol.
>
> Grafana's UI **has not** been pointed at the edge: no container runs on the dev
> box. The routes and answers it needs are the ones Grafana's Prometheus
> datasource documents, answered as VictoriaMetrics answers them.

## The listener

- **Port.** `8428`, VictoriaMetrics' own (`18428` in dev). `SMOLQUERY_VICTORIAMETRICS_IP` and `SMOLQUERY_VICTORIAMETRICS_PORT` move it. It binds `127.0.0.1` by default.
- **Plain HTTP.** The edge does not terminate TLS. Bind it beyond the node only behind a TLS terminator.
- **Password.** The API key (`SMOLQUERY_API_KEY`), or `SMOLQUERY_VICTORIAMETRICS_PASSWORD` when set. A node with the `:victoriametrics` role and neither refuses to boot.
- **How a client sends the password.** As a `Bearer` token (`-remoteWrite.bearerToken`, Prometheus' `authorization:`), or with HTTP basic auth under any user name (`-remoteWrite.basicAuth.*`, Grafana's basic auth).
- **Health checks.** `GET /health`, `/-/healthy` and `/-/ready` answer `OK` without a password, as VictoriaMetrics' do.
- **Refused before the body.** A missing or wrong password is a 401 on every path, so a stranger cannot learn which paths exist. With the password, an unknown path is a 404. A write is then counted against the edge's own in-flight limit before its body is read.
- **Prefixes.** The write answers on `/api/v1/write`, `/prometheus/api/v1/write` and `/insert/<account>/prometheus/api/v1/write`. Every read route answers on `/api/v1/...`, `/prometheus/api/v1/...` and `/select/<account>/prometheus/api/v1/...`, which is where Grafana is pointed at a VictoriaMetrics behind a proxy or a cluster's `vmselect`. The account id is accepted and ignored: tenancy is not this edge's. As in a cluster, `/insert` only writes and `/select` only reads.
- **Errors.** Every refusal is the Prometheus API's JSON: `{"status":"error","errorType":"bad_data","error":"..."}`, with `retry-after` when sending again later can succeed.
- **Metrics.** On the node's `/metrics`: `smolquery_victoriametrics_requests_total` by status class; `smolquery_victoriametrics_request_microseconds_total` and `_bucket` by `kind` (`write`, `query`, `labels`, `health`, `other`), cumulative `le` counters at the ClickHouse edge's bounds; `smolquery_victoriametrics_samples_total{result}` for `written`, `nan`, `histogram`, `exemplar` and `refused`; `smolquery_victoriametrics_query_series_total` and `_query_samples_total`, what queries read into the node; and `smolquery_victoriametrics_query_microseconds_total{phase}`, where `fetch` is each selector's grouped query and copying its samples into the node, and `evaluate` parsing, the rollups and the rest of the expression; rendering the JSON answer is in neither.

## The write

Prometheus remote write: a protobuf `WriteRequest`, compressed, on `POST /api/v1/write`.

**vmagent.** One URL and one credential:

```sh
vmagent-prod -promscrape.config=scrape.yml \
  -remoteWrite.url=http://smolquery:8428/api/v1/write \
  -remoteWrite.bearerToken="$SMOLQUERY_VICTORIAMETRICS_PASSWORD"
```

or `-remoteWrite.basicAuth.username=vmagent -remoteWrite.basicAuth.password=...`
in place of the token.
`-remoteWrite.maxBlockSize` is not needed: its default, 8 MiB before
compression, is a quarter of the edge's decoded bound
(`SMOLQUERY_VICTORIAMETRICS_MAX_DECODED_BYTES`, 32 MiB), and a compressed block
is far below the 8,000,000 bytes a body may be as sent. That matters, because
vmagent drops a block only on a 400, 409 or 415 and retries every other refusal
forever, with its queue stuck behind the block.

**Prometheus.**

```yaml
remote_write:
  - url: http://smolquery:8428/api/v1/write
    authorization:
      credentials: <the password>
```

**The OpenTelemetry collector.**

```yaml
exporters:
  prometheusremotewrite:
    endpoint: http://smolquery:8428/api/v1/write
    headers:
      Authorization: Bearer ${env:SMOLQUERY_VICTORIAMETRICS_PASSWORD}
```

What the edge does with a request:

- **Protocols.** VictoriaMetrics' own, which vmagent sends by default: `Content-Encoding: zstd`. Prometheus remote write 1.0: `Content-Encoding: snappy`, what Prometheus and the collector send. A body with no encoding is read as it is. Both are decoded in the node with no new dependency.
- **Refused protocols.** Remote write 2.0 (`Content-Type: application/x-protobuf;proto=io.prometheus.write.v2.Request`) is a 415, so a 2.0 sender falls back to 1.0 as the specification says it must. So is any other `proto` than none or `prometheus.WriteRequest`, any content type but `application/x-protobuf`, and any encoding but `zstd`, `snappy` or none.
- **Body limits.** The body as sent is held to `SMOLQUERY_INSERT_MAX_NDJSON_BYTES` (8,000,000), the API's insert limit, and what it inflates to to `SMOLQUERY_VICTORIAMETRICS_MAX_DECODED_BYTES` (32 MiB), four times vmagent's largest default block. The declared size is checked before anything is decompressed, so a small body cannot expand into a large one. A block past the decoded bound is a 400 that names it, as VictoriaMetrics answers its `-maxInsertRequestSize`, so vmagent drops it rather than stalling on it; the refusal is logged at warning with both sizes.
- **In-flight bytes.** The edge keeps its own in-flight counter, sized as the API's is by `SMOLQUERY_INSERT_MAX_IN_FLIGHT_BYTES`. A write is counted at its size as sent before its body is read, then at that plus what it declares it inflates to before it is decompressed (the whole bound when a zstd frame declares no size). Decoded rows still take several times the protobuf, so leave the limit headroom.
- **Labels.** A label with an empty value is dropped, as VictoriaMetrics drops it and PromQL reads it: absent. A series left with no `__name__`, or with a label name more than once (`__name__` included), is a 400 that names the series, as Prometheus refuses it.
- **All or nothing.** A block is written whole or not at all.
- **Once.** The block's SHA-256 is its batch id, so a vmagent retry after a lost answer is answered from the first commit, not written twice. The `smolquery_victoriametrics_samples_total` counts are of what each request carried, so such a retry is counted again.
- **NaN is dropped, ±Inf is clamped.** The row path cannot carry IEEE specials. A NaN sample, Prometheus' staleness marker included, is left out and counted (`result="nan"`). `+Inf` and `-Inf` are stored as the largest finite double and its negation, and answered as `+Inf` and `-Inf`.
- **Not stored.** Native histograms, exemplars and metadata are counted and dropped.

The answers, and what each client does with them. vmagent's behavior is its
v1.152.0 source (`app/vmagent/remotewrite/client.go`): it drops a block only on a
400, 409 or 415 and retries every other status forever, so a block no retry can
fix is answered 400 or 415:

| status | when | vmagent | Prometheus, the collector |
|---|---|---|---|
| 204 | written | next block | next block |
| 400 | the body does not decode, it inflates past `SMOLQUERY_VICTORIAMETRICS_MAX_DECODED_BYTES`, a series has no `__name__` or repeats a label name, or a timestamp is out of range | a zstd block is re-sent as snappy and vmagent switches to remote write 1.0 for good; a snappy block is dropped | dropped |
| 401 | no or wrong password | retried with backoff | dropped |
| 413 | the body as sent is past `SMOLQUERY_INSERT_MAX_NDJSON_BYTES`; a compressed vmagent block never is | **retried with backoff, forever** | dropped |
| 415 | a content type, `proto`, encoding or remote write 2.0 the edge does not read | as 400 | dropped |
| 429 | the edge's in-flight bytes are taken, or the buffer is full, overloaded or at its backlog ceiling; `retry-after` | retried after `retry-after` | retried |
| 500 | the table refused a row; nothing was written | retried | retried |
| 503 | the ingest or buffer service is unreachable, ownership is moving, or the catalog did not answer; `retry-after` | retried after `retry-after` | retried |

## The table

Every sample is one row of `metrics.samples` (`SMOLQUERY_VICTORIAMETRICS_TABLE=dataset.table` moves it):

| column | type | |
|---|---|---|
| `name` | `STRING NOT NULL` | the `__name__` label |
| `series` | `INT64 NOT NULL` | the first 8 bytes of the SHA-256 of the name and the other labels, sorted, each length-framed; what a rollup groups by |
| `labels` | `MAP(STRING, STRING)` | every label but `__name__` |
| `ts` | `TIMESTAMP NOT NULL` | the sample's millisecond timestamp |
| `value` | `FLOAT64 NOT NULL` | |

- **Clustering.** `name, ts`. A query names its metric and a time range, and the planner reads both off the generated SQL, so the files of other metrics and other hours are not opened.
- **Created on the first write.** vmagent has no way to create a table, so the edge creates the dataset and the table, with that clustering, the first time a write finds them missing. A table that already exists is written as it is.
- **Or created first.** On a node that runs queries while the first write arrives, the create can meet a query job holding the SQLite catalog (`database is locked`, T-574). Creating the table through the API before vmagent starts avoids it:

  ```sh
  auth='authorization: Bearer '$SMOLQUERY_API_KEY
  json='content-type: application/json'
  api=http://127.0.0.1:4000

  curl -H "$auth" -H "$json" -d '{"id": "metrics"}' $api/v1/datasets
  curl -H "$auth" -H "$json" -d '{"id": "samples", "schema": [
        {"name": "name", "type": "STRING", "nullable": false},
        {"name": "series", "type": "INT64", "nullable": false},
        {"name": "labels", "type": "MAP(STRING, STRING)"},
        {"name": "ts", "type": "TIMESTAMP", "nullable": false},
        {"name": "value", "type": "FLOAT64", "nullable": false}
      ]}' $api/v1/datasets/metrics/tables
  curl -X PATCH -H "$auth" -H "$json" -d '{"clustering": ["name", "ts"]}' \
       $api/v1/datasets/metrics/tables/samples
  ```

- **Retention.** The table's own, set through the API, not something the edge owns. Thirty days: `PATCH /v1/datasets/metrics/tables/samples` with `{"retention": {"column": "ts", "ttlMs": 2592000000}}` ([api.md](api.md)). It is segment-grained, as on any table.
- **Partitions.** The deployment default, `SMOLQUERY_WRITE_PARTITIONS`, as for any table; `PATCH {"partitions": N}` raises it.

## The reads

**Grafana.** Add a **Prometheus** datasource. The URL is `http://smolquery:8428`
(or with `/prometheus`). Turn on basic auth with any user and the password, or
add an `Authorization: Bearer <password>` header. "Save & test" asks
`/api/v1/status/buildinfo` and then `query=1+1`, and both answer. The
**VictoriaMetrics** datasource plugin takes the same URL and credential.

The routes, each on `GET` and on a form-encoded `POST`, as Grafana sends them:

- **`/api/v1/query`.** `query` is required. `time` is now by default, and `step` is 5 minutes, the least a bare selector looks back.
- **`/api/v1/query_range`.** `start` is 5 minutes ago by default, `end` now, `step` 5 minutes. An `end` before `start` is `start` plus 5 minutes. A grid of 50 points or more is aligned to the step, as VictoriaMetrics' `AdjustStartEnd` does, unless `nocache=1`.
- **Query length.** `query`, and each `match[]`, is held to `SMOLQUERY_VICTORIAMETRICS_MAX_QUERY_BYTES` (16,384), VictoriaMetrics' `-search.maxQueryLen`. A longer one, or one that is not UTF-8, is a 400 before it is parsed. Brackets nest at most 1,000 deep.
- **Times and durations.** Unix seconds, integer or fractional, or RFC 3339. A duration is seconds or MetricsQL's (`15s`, `1h30m`). `timeout` is one deadline for the whole request, held to `SMOLQUERY_VICTORIAMETRICS_MAX_QUERY_DURATION_MS` (30 s, VictoriaMetrics' `-search.maxQueryDuration`), which is also its default: every job gets what is left of it, and a request past it is cancelled and answers 503 `timeout`.
- **`/api/v1/labels`, `/api/v1/label/<name>/values`, `/api/v1/series`.** `start` is 5 minutes before `end`, which is now, when missing or 0. `match[]` repeats, and `match` is read too; a series matching any is kept. `limit` is 0 when missing. The label routes cap it at 100,000 and use that cap when it is not positive. `/api/v1/series` needs `match[]` (400 without) and is held to `max_series`. A label name in the path is percent-decoded and may be any UTF-8 text (`/api/v1/label/http.method/values`), as VictoriaMetrics and Prometheus 3 take it; names beginning `U__` are unescaped as VictoriaMetrics does. One that is not UTF-8 is a 400.
- **Fixed answers**, byte for byte as VictoriaMetrics writes them: `/api/v1/status/buildinfo` (`{"version":"2.24.0"}`), `/api/v1/metadata` (`{}`), and empty `/api/v1/rules`, `/api/v1/alerts`, `/api/v1/notifiers` and `/api/v1/query_exemplars`.

**MetricsQL.** VictoriaMetrics' rules, not Prometheus', wherever they differ,
ported from v1.152.0's `app/vmselect/promql`:

- **Rollups.** `rate` and `increase` do not extrapolate: they take the last sample in the window against the sample before the window, which counts. Counter resets are removed from the whole series first.
- **An omitted window.** `rate(m)` takes the step, widened by the series' scrape interval for the functions VictoriaMetrics widens (`getScrapeInterval`, `getMaxPrevInterval`).
- **The lookback.** A bare selector is `default_rollup`: the last sample in `max(step, lookback)`. The lookback is `SMOLQUERY_VICTORIAMETRICS_LOOKBACK_MS`, 5 minutes by default. Inside a subquery, `default_rollup` over the inner points takes the window written (none when none is) and widens it past the step only to the gap it expects between two points (`maxPrevInterval`), as VictoriaMetrics does; the lookback plays no part there.
- **`keep_metric_names`.** Rollups and transforms drop `__name__`, as VictoriaMetrics does, unless the call says `keep_metric_names`. An aggregate keeps only its `by` labels.
- **Values.** A value is a string, as Go's `FormatFloat(v, 'f', -1, 64)` writes it. An instant `m[5m]` answers the raw samples. A scalar is a series with no labels.
- **Selectors.** `name = ` prunes to that metric's files. A selector with no non-empty matcher, `{}` included, is refused before any SQL runs. `{a="1" or b="2"}` filter sets are read. A metric the table does not have, or a table not created yet, answers empty.

**What evaluates.** Everything the parser accepts, except as listed:

- **Aggregates**, 36 of 37: `sum avg min max count group stddev stdvar sum2 geomean distinct mode median mad quantile quantiles count_values any share zscore limitk topk bottomk`, the `topk_*` and `bottomk_*` family, `outliers_iqr outliers_mad outliersk`; with `by`, `without` and `limit N`.
- **Operators.** Arithmetic, comparisons with and without `bool`, `and or unless`, MetricsQL's `default if ifnot`, `on`/`ignoring`, `group_left`/`group_right` with label lists, `(*)` and a prefix, `fill`, `fill_left`, `fill_right`.
- **Transforms**, 107 of 111: math and trigonometry, `round clamp* sgn`, `bitmap_*`, the calendar functions (in UTC), `time start end step pi now scalar vector absent union`, the `sort*` family, `limit_offset`, `drop_empty_series`, `drop_common_labels`, `running_*`, `range_*`, `keep_last_value keep_next_value interpolate remove_resets smooth_exponential`, every `label_*` function, `histogram_quantile(s)` and the other `histogram_*` functions, `prometheus_buckets`, `buckets_limit`, and `vmrange` buckets.
- **Rollups**, 39 of 80: `default_rollup rate increase increase_pure irate delta idelta deriv deriv_fast ideriv changes resets lag lifetime scrape_interval rate_over_sum timestamp tmin_over_time tmax_over_time present_over_time absent_over_time`, and `avg count distinct first geomean last max min quantile range stddev stdvar sum sum2` `_over_time`, `count_eq count_ne count_gt count_le` `_over_time`.
- **Subqueries**, `[5m:1m]` and `[5m:]`, and `offset` (either sign) and `@` on any expression. The built-in templates `alias`, `range_median`, `ru` and `ttf` expand.
- **Answering 422, naming the function:** the aggregate `histogram`; the transforms `rand rand_exponential rand_normal timezone_offset`; the rollups `aggr_over_time ascent_over_time changes_prometheus count_values_over_time decreases_over_time delta_prometheus descent_over_time duration_over_time histogram_over_time hoeffding_bound_lower hoeffding_bound_upper holt_winters increase_prometheus increases_over_time integrate mad_over_time median_over_time mode_over_time outlier_iqr_over_time predict_linear quantiles_over_time rate_prometheus rollup rollup_candlestick rollup_delta rollup_deriv rollup_increase rollup_rate rollup_scrape_interval share_eq_over_time share_gt_over_time share_le_over_time stale_samples_over_time sum_eq_over_time sum_gt_over_time sum_le_over_time tfirst_over_time timestamp_with_name tlast_change_over_time tlast_over_time zscore_over_time`; and `WITH` templates.

**Limits.** A query past one is a 422 that names the variable, not a slow answer:

| variable | default | what |
|---|---|---|
| `SMOLQUERY_VICTORIAMETRICS_MAX_SERIES` | `10000` | series one selector, or one `/api/v1/series`, may match |
| `SMOLQUERY_VICTORIAMETRICS_MAX_SAMPLES` | `5000000` | raw samples one selector may read into the node, about 50 bytes each while the query runs |
| `SMOLQUERY_VICTORIAMETRICS_MAX_SAMPLES_PER_QUERY` | `10000000` | raw samples all of one query's selectors may read between them (`a / b + c` is three) |
| `SMOLQUERY_VICTORIAMETRICS_MAX_POINTS_PER_SERIES` | `30000` | points in a `query_range` grid (`-search.maxPointsPerTimeseries`); Grafana asks for about 1,000 |

Size them from `smolquery_victoriametrics_query_series_total` and
`smolquery_victoriametrics_query_samples_total`, divided by the `query` kind's
count in `smolquery_victoriametrics_request_microseconds_bucket{le="+Inf"}`.

**Where it differs from VictoriaMetrics:**

- **Scalars.** An instant query of a scalar expression answers `resultType` `scalar`. VictoriaMetrics answers a one-point vector; Grafana's connection test reads either.
- **Errors.** A query that does not parse is a 422 here, a 400 in VictoriaMetrics. `errorType` is Prometheus' word (`bad_data`, `execution`, `timeout`, `unavailable`) where VictoriaMetrics writes the status code. A job that failed for the node's reasons, not the query's (an engine that died or ran out of memory, a disk or connection error, an unreachable worker or buffer node), is a 503 `unavailable` with `retry-after`, which Grafana and vmalert retry.
- **Regular expressions.** Selectors match in DuckDB, which is RE2, as VictoriaMetrics is. The `label_*` functions match in the node, which is PCRE; the common syntax is the same.
- **Not read:** `WITH` templates; remote write 2.0; native histograms and exemplars (dropped and counted); staleness markers, so a series ends when the lookback passes it, not at the marker; the `extra_label` and `extra_filters[]` query arguments; `/api/v1/status/tsdb`, which would be a day-long scan here; `/api/v1/import` and `/api/v1/export`.

## What it costs

- **The label routes scan the range.** `/api/v1/labels`, the label values and `/api/v1/series` read every sample between `start` and `end` that the selectors allow. A selector with `name =` prunes to that metric; none prunes by time only. Keep Grafana's label browser to a short range. A series index is T-569.
- **Tier 1 moves samples into the BEAM.** Each selector is one query job, grouped by series with its samples as two lists, and the rollups run in the node over what they read, about 50 bytes a sample. That is why the ceilings above exist. Pushing the common rollups into SQL is T-568.
- **Measured** by `bench/victoriametrics.exs` on the aarch64 dev box (8 cores), all in the hot tier ([results](../bench/results/victoriametrics.md)):
  - Remote write, 10,000-sample blocks from eight writers: about 345,000 samples a second into an empty table and 363,000 with 2.4 million samples already there; p50 about 210 ms, p99 under 530 ms. The bench frames its blocks as snappy with one literal chunk, so they decode with no copying back-references: a real vmagent's zstd or compressed snappy block costs more to decode than this measures.
  - 1,000 series, six hours at 15 s: `sum by (job) (rate(m[5m]))` over the six hours reads 1.44 million samples in 1.2 s and evaluates in 1.2 s. Over one hour it answers in 0.9 s.
  - A fetch's floor is one job's planning and the hot tier's files, not two jobs and a scatter (T-576): `m{instance="host-1"}` over six hours answers in 0.28 s (0.8 s before), and one series among 1,000 over 50 / 200 hot micro-segments reads in 181 / 301 ms, against 591 / 792 ms when a selector was a series query and a samples query.
  - A wide matrix costs more to send than to compute: `rate(m[5m])` over six hours for 1,000 series is 1.44 million points of JSON, 9 s past its 2.2 s of query.
  - `/api/v1/labels` over six hours: 0.27 s. `/api/v1/label/job/values`: 0.45 s.
