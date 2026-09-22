# vmagent remote write bodies

Two request bodies exactly as vmagent sent them, captured on 2026-09-22 for
PL-70 / T-561. Each file is one `POST /api/v1/write` body; `headers.json`
records the method, path and request headers that came with it.

| file | protocol | Content-Encoding | series | samples |
|---|---|---|---|---|
| `write_zstd.bin` | VictoriaMetrics remote write (vmagent's default) | `zstd` | 835 | 835 |
| `write_snappy.bin` | Prometheus remote write 1.0 (`-remoteWrite.forcePromProto=true`) | `snappy` | 835 | 835 |

Every series is one of vmagent's own metrics from one scrape of itself, with
labels `job="vmagent"` and `instance="127.0.0.1:8429"`.

## Recapturing

- vmagent `v1.152.0`, the non-enterprise `vmutils-linux-arm64-v1.152.0.tar.gz`
  from the GitHub release, binary `vmagent-prod`
  (`vmagent-20260911-125957-tags-v1.152.0-0-g540b91da03`).
- A throwaway Bandit plug on `127.0.0.1:18499` that wrote each request body and
  its headers to files and answered `204`.
- `prom.yml`:

  ```yaml
  global:
    scrape_interval: 1s
  scrape_configs:
    - job_name: vmagent
      static_configs:
        - targets: ["127.0.0.1:8429"]
  ```

- zstd, the default protocol:

  ```sh
  vmagent-prod -httpListenAddr=127.0.0.1:8429 -promscrape.config=prom.yml \
    -remoteWrite.url=http://127.0.0.1:18499/api/v1/write \
    -remoteWrite.tmpDataPath=vmdata1
  ```

- snappy, the same plus `-remoteWrite.forcePromProto=true` and a fresh
  `-remoteWrite.tmpDataPath`.

Each run was stopped after about six seconds, and the first request of each
was kept. A recapture will differ in timestamps and values, and possibly in
which metrics vmagent exposes; the tests assert on shape and on metric names
vmagent has long exposed (`vm_app_version`, `process_cpu_cores_available`).

# MetricsQL parser corpus

`metricsql_corpus.jsonl` is every case of `parser_test.go` in
`github.com/VictoriaMetrics/metricsql` v0.87.4, the version VictoriaMetrics
v1.152.0 builds with (its `go.mod`), in file order: 838 cases, one JSON object
per line, read by `test/smolquery_victoriametrics/metricsql_test.exs` for
PL-70 / T-563.

| field | meaning |
|---|---|
| `section` | the comment above the case in `parser_test.go` |
| `input` | the query |
| `output` | the query parses, and this is how it prints |
| `error` | the query is refused with `{:error, {kind, _}}`, `kind` one of `syntax`, `unsupported`, `unknown_function`, `arity` |
| `vm` | present where the answer here differs from VictoriaMetrics': what `parser_test.go` expects instead |
| `note` | why it differs |

The notes:

- `folding`: VictoriaMetrics evaluates constant subexpressions while parsing
  (`1 + 2` prints `3`, `-1` prints `-1`); here the tree keeps them
  (`1 + 2`, `0 - 1`) and the evaluator folds them.
- `or_branch`: VictoriaMetrics prints `{__name__="a",b="1" or __name__="a"}` as
  `a{b="1"}`, which drops the second branch; here every name is printed so the
  text parses back to the same selector.
- `with`: `WITH` templates are not supported yet; the query is refused with
  `{:unsupported, "WITH templates"}`.
- `arity`: VictoriaMetrics accepts `sum()` or `rate(a, b, c, d)` when parsing
  and refuses it when evaluating; here the argument count is checked when
  parsing.

Cases VictoriaMetrics refuses carry no `vm` or `note`: every one of them is
refused here too.

## Regenerating

The inputs and VictoriaMetrics' expectations come from the `same(...)`,
`another(...)` and `f(...)` calls of
`https://raw.githubusercontent.com/VictoriaMetrics/metricsql/v0.87.4/parser_test.go`,
with Go string literals unquoted; `output` and `error` are this parser's
answers, reviewed case by case against `vm`. A new metricsql version means
re-extracting, re-running and reviewing every line whose answer changed.

# MetricsQL evaluator corpus

`exec_corpus.jsonl` and `exec_errors.jsonl` are every case of
`TestExecSuccess` and `TestExecError` in VictoriaMetrics v1.152.0's
`app/vmselect/promql/exec_test.go`, in file order, read by
`test/smolquery_victoriametrics/eval_exec_corpus_test.exs` for PL-70 / T-565.

`exec_corpus.jsonl`, 610 cases, one per `t.Run` of `TestExecSuccess`:

| field | meaning |
|---|---|
| `name` | the `t.Run` name (names repeat in the Go file; order does not) |
| `query` | the query, the `q := ...` raw string |
| `result` | VictoriaMetrics' expected series, in order: `labels` (`__name__` from `MetricGroup`, then `Tags`/`testAddLabels`) and `values` at the six points 1000s..2000s by 200s, `null` for NaN and `"+Inf"`/`"-Inf"` for the infinities; `null` for the two `timezone_offset` cases whose expectation Go computes at run time |
| `skip` | present on the 75 cases not evaluated here: the `{:unsupported, what}` the evaluator answers, which the test asserts |

The skipped cases, by reason: 52 use a rollup function the rollup engine of
T-564 does not compute (`median_over_time`, `rollup_*`, `histogram_over_time`,
`aggr_over_time`, ...); 17 use `rand`, `rand_normal` or `rand_exponential`,
whose values are Go's `math/rand` sequence; 3 use `timezone_offset`, which
needs a time zone database; 2 use the `histogram` aggregate; 1 uses `WITH`.

`exec_errors.jsonl`, 224 cases, one per `f(...)` of `TestExecError`, each
refused here too: `stage` is `parse` (with `error` the parse error's kind:
`syntax`, `arity`, `unknown_function`) or `eval` (with `error` the
evaluation error's kind: `invalid_argument`, `duplicate_series`, or
`unsupported` with the `skip` it answers, for the 5 cases whose rollup
function is not computed here).

## Regenerating

The cases were extracted from
`https://raw.githubusercontent.com/VictoriaMetrics/VictoriaMetrics/v1.152.0/app/vmselect/promql/exec_test.go`
by an Elixir script, not kept, that splits the body of `TestExecSuccess` on
`\n\tt.Run(`, takes each block's `q := `...`` string, each
`rN := netstorage.Result{...}` with its `Values: []float64{...}` (`nan` is
`null`, `inf` is `"+Inf"` unless the block says `inf := math.Inf(-1)`,
`1.23 * (1 << 20)` is evaluated), `rN.MetricName.MetricGroup = []byte(...)`,
`rN.MetricName.Tags = []storage.Tag{...}` and `testAddLabels(t, &rN.MetricName, ...)`
(Go string literals unquoted, raw strings taken as is), and the order of
`resultExpected := []netstorage.Result{r1, r2}` (`f(q, nil)` and `{}` are no
series). `TestExecError`'s queries are every `f(...)` argument. `skip`,
`stage` and `error` are this evaluator's answers, reviewed against the
source: a case is skipped only when it needs a function listed above. A new
VictoriaMetrics version means re-extracting and reviewing every case whose
answer changed.
