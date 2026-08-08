# k6 insert load

A load generator that knows nothing about what it is loading.

`insert.js` takes a URL, a file, and some headers, and posts the file at the URL
until the clock runs out. It does not import the application, does not start it,
does not read its config, and does not parse the body. That is the whole point:

* **it does not compete with the server for CPU.** The `.exs` scripts beside this
  directory boot smolquery's supervision tree inside the driver's own BEAM, so
  the driver's JSON encoding and garbage collection run on the same schedulers as
  the thing being measured. Past a few concurrent writers it stops being possible
  to tell a saturated server from a busy driver. Everything measured that way is
  a latency number wearing a throughput costume.
* **it drives more than one back end.** smolquery and ClickHouse differ here only
  by URL, headers, and which file holds the body.

Those `.exs` scripts are not this and should not be replaced by it. They are
profilers: they call internal functions directly, subtract stages from each
other, and set runtime knobs no HTTP client can reach. None of that is visible
over the wire, and none of it is load generation. The two answer different
questions and the repository wants both.

## Running, end to end

Four steps, and the server is a server in its own OS process — not something the
benchmark starts inside itself.

**1. Generate the bodies.** Once, by k6 itself — this directory needs no second
runtime. Writes all three shapes from the same rows, so whatever two back ends
differ by, it is not the data.

```bash
mkdir -p /tmp/smolquery-bodies
```

```bash
k6 run -e ROWS=3062 -e PROJECTS=1000 -e OUT=/tmp/smolquery-bodies bench/k6/generate.js
```

**2. Start the server**, separately, with its own data directory.

```bash
SMOLQUERY_DATA_DIR=/tmp/sq-k6 SMOLQUERY_API_KEY=k6key mix run --no-halt
```

**3. Create the table** by posting `schema.json` — the same file the generator
reads its column list from, so the table and the rows cannot disagree.

```bash
curl -X POST localhost:4000/v1/datasets -H 'authorization: Bearer k6key' -H 'content-type: application/json' -d '{"id":"logs"}'
```

```bash
curl -X POST localhost:4000/v1/datasets/logs/tables -H 'authorization: Bearer k6key' -H 'content-type: application/json' --data-binary @bench/k6/schema.json
```

Then set the clustering key, which `POST /tables` does not take. It has to match
`ORDER BY` in `clickhouse.sql`: a sorted table against an unsorted one measures
the sort, not the engine.

```bash
curl -X PATCH localhost:4000/v1/datasets/logs/tables/otel_logs -H 'authorization: Bearer k6key' -H 'content-type: application/json' -d '{"clustering":["project_id","timestamp"]}'
```

**4. Load it.** Columnar body, which is the default shape:

```bash
k6 run -e URL=http://127.0.0.1:4000/v1/datasets/logs/tables/otel_logs/insert -e BODY=/tmp/smolquery-bodies/columns.3062.json -e ROWS=3062 -e AUTH="Bearer k6key" -e VUS=4 -e DURATION=60s bench/k6/insert.js
```

Row-major is the same run with one file swapped, which is the whole comparison:

```bash
k6 run -e URL=http://127.0.0.1:4000/v1/datasets/logs/tables/otel_logs/insert -e BODY=/tmp/smolquery-bodies/rows.3062.json -e ROWS=3062 -e AUTH="Bearer k6key" -e VUS=4 -e DURATION=60s bench/k6/insert.js
```

Output:

```
  mode         4 VUs closed loop, 60s
  body         3.28 MiB, 3062 rows
  window       60.21s measured

  rows/s       63612
  requests     1251 accepted, 0 refused, 0 dropped
  latency ms   p50 187.7   p95 243.7   p99 271.2   max 305.7
```

## ClickHouse

Same script, same rows, a different URL and file. This repository ships no
ClickHouse and starts none: `insert.js` posts at whatever HTTP endpoint you give
it, so bring your own server and record its version next to the numbers. Pin that
version — a comparison against "whatever was installed" is not repeatable.

Create the table from the DDL next to this file, which mirrors `schema.json`
column for column:

```bash
clickhouse client --queries-file bench/k6/clickhouse.sql
```

```bash
k6 run -e URL='http://127.0.0.1:8123/?query=INSERT%20INTO%20bench.otel_logs%20FORMAT%20JSONEachRow' \
       -e BODY=/tmp/smolquery-bodies/eachrow.3062.ndjson \
       -e ROWS=3062 \
       -e CONTENT_TYPE=text/plain \
       -e VUS=4 -e DURATION=60s \
       bench/k6/insert.js
```

Three settings decide what this run means, so all three are left at their
defaults and none should be changed without saying so in the results:

* **`async_insert` stays off.** With `async_insert=1, wait_for_async_insert=0`
  ClickHouse acks before the rows are anywhere durable, which is not what
  smolquery's 200 means. Nothing stops you appending it to the query string —
  the comparison just stops being one.
* **`fsync_after_insert` is off by default, and that is an asymmetry.**
  smolquery fsyncs its buffer manifest before it answers 200; ClickHouse's
  default insert commits the part to the page cache and returns. To compare the
  two acks rather than two policies, turn it on — but note that it is a
  **MergeTree table setting, not a query setting**. In the query string it
  returns `404 UNKNOWN_SETTING`, and the run then reports tens of thousands of
  sub-millisecond "requests" that are all refusals:

  ```bash
  clickhouse client --query "ALTER TABLE bench.otel_logs MODIFY SETTING fsync_after_insert = 1, fsync_part_directory = 1"
  ```
* **`date_time_input_format` stays at `basic`.** `generate.js` writes ClickHouse
  timestamps as `2026-08-01 10:00:00.000`, which the default parser takes; the
  ISO form with a `Z` would need `best_effort`, the slowest parser it has, on two
  columns of every row of every request.

Neither engine gets compression here — the file goes on the wire verbatim. Real
ClickHouse ingestion is usually compressed, so this is a floor for it.

## Parameters

| variable | meaning |
|---|---|
| `URL` | required, where to post |
| `BODY` | required, path to the file posted verbatim |
| `ROWS` | required, how many rows that file holds — see below |
| `MODE` | `vus` (default) or `rate` |
| `VUS` | concurrent clients in `vus` mode, default 4 |
| `RATE` | requests per second in `rate` mode |
| `DURATION` | default `30s` |
| `AUTH` | value of the `Authorization` header |
| `CONTENT_TYPE` | default `application/json` |
| `HEADERS` | JSON object merged into the headers |
| `EXPECT_STATUS` | status treated as success, default `200` |
| `JSON_OUT` | write the full k6 summary here |
| `MAX_VUS` | ceiling in `rate` mode, default `2 × RATE` — see below |
| `GRACEFUL_STOP` | how long in-flight requests get after the clock, default `5s` |

`ROWS` is passed rather than counted. Counting would mean parsing the body on
every iteration — more work per request than the request itself, in a process
whose only job is to not be the bottleneck — and it would have to understand
every wire format the script is meant to stay ignorant of. As a guard, the row
count is in the file name and the script refuses a `ROWS` that disagrees with it.

`MAX_VUS` is a memory budget as much as a concurrency one: every VU holds its own
copy of the body, so 800 VUs on a 6.4 MiB body is gigabytes on the same machine
as the server. Raise it deliberately, and if a run reports dropped iterations,
decide whether that was the server or this ceiling before reading the number.

## `vus` or `rate`

They answer different questions and are not interchangeable.

**`vus` is a closed loop.** Each client posts, waits for the reply, posts again,
so the server never sees more than `VUS` requests in flight. Raising `VUS` raises
throughput until it does not, and the turning point is a property of the loop as
much as of the server. Use it to ask *what does a client with N connections get*.

**`rate` is an open loop.** Requests start on a schedule regardless of whether
earlier ones have finished. Push the arrival rate past what the server absorbs
and the backlog grows instead of the generator quietly slowing down to match.
Use it to ask *where does this server stop keeping up*, and read the answer off
the latency percentiles and the refusal count, not off rows/s alone.

A closed loop cannot find a saturation point. If a `vus` sweep shows throughput
falling at high concurrency, that is a knee, and it may belong to the client, the
server, or the machine — an open-loop run at fixed arrival rates is what tells
them apart.

## Bodies

Generated separately and once, never by `insert.js`. A body is a file; where it
came from is not the load generator's business, and keeping it that way is what
lets the same file be replayed byte for byte across back ends and across runs.

`generate.js` writes all three from one set of rows:

| file | shape | size at 3062 rows |
|---|---|---|
| `columns.N.json` | smolquery columnar, the default — `{"rowCount": N, "columns": {"col": [...], ...}}` | 3.28 MiB |
| `rows.N.json` | smolquery row-major — `{"rows": [{"project_id": ..., ...}, ...]}`, every record carrying every column | 6.42 MiB |
| `eachrow.N.ndjson` | ClickHouse `JSONEachRow` — one JSON object per line, no wrapper | 6.41 MiB |

`ROWS`, `PROJECTS` and `SEED` control them. The seed is what makes a run from
today comparable with a run from last week: same seed, same bytes.

Same rows in all three, with one deliberate exception: the ClickHouse file writes
timestamps as `2026-08-01 10:00:00.000` and the smolquery files as ISO 8601 with
a `Z`, because those are the two engines' own fast paths. Same instants either
way; handing one engine a format it has to adapt to is not a measurement of it.

Project ids are drawn log-uniformly, so a few tenants are large and the tail is
long. Uniform ids would hide exactly what a clustering key exists for — one
tenant's rows landing in the same place.

The columnar body is half the bytes of the other two, for identical rows. That is
a real advantage of the format, but it means a columnar-vs-`JSONEachRow` run
compares two endpoints and two wire sizes, not two ingestion engines. Say which
question the result answers.

## Measuring the machine

`watch.go` samples CPU and resident memory of the server **and of k6 itself**,
for the length of a run. Start it just before k6, give it a little more time than
the run needs, and it prints both:

```bash
go run bench/k6/watch.go -match beam.smp -match '^k6 run' -duration 70s -out /tmp/k6-watch.json
```

```
  match              pid  avg %CPU peak %CPU   mean RSS   peak RSS
  beam.smp         48221     412.6     530.1    1204 MiB    1387 MiB
  k6 run           48539     298.9     308.9     117 MiB     130 MiB

  %CPU is per core; 1000 is all ten cores of an M1 Pro.
```

Use `-match clickhouse-server` for the other arm. Nothing needs to be installed:
`go run` on the single file, standard library only.

**Anchor the k6 pattern.** A pattern is a regexp over the whole command line, so
an unanchored `k6 run` also matches the shell that started the sampler — that
shell's own arguments contain the text. It then reports a process that is idle by
construction, and the generator's real cost reads as zero. Check the `pid` column
against the k6 you actually started.

The k6 line is not decoration. Posting a 6.4 MiB body a few hundred times a
second is real work, and it happens on the same cores as the server. If k6 and
the server together approach the core count, the run measured the machine, and
the throughput number belongs to neither of them.

CPU is consumed CPU time between samples over wall time — not `ps %cpu`, which on
Linux averages over the process's entire lifetime and would answer a question
about an hour ago rather than about the run.

## Rules for this directory

JavaScript, or Go where JavaScript cannot reach — `watch.go` needs the process
table, which k6's sandbox does not expose. Nothing here may need the application,
a build tool, or a second language runtime installed: the load has to stay
independent of what it is loading, and a generator that imports the system under
test can only ever measure that one system.

## Reading the output

`rows/s` counts only accepted requests, over the `window` the summary prints.
A refused request moved no rows, and counting it would make an overloaded server
look fast — so a run that reports refusals is reporting what got through, not
capacity, and says so.

Refused requests are kept out of the latency percentiles too. A connection
refused returns in about a millisecond and a timeout in 120 seconds; blended into
one Trend, the first makes an overloaded server look quick and the second inflates
p99 with time nobody spent waiting for data.

`window` is wall time including the drain after the clock expires. The drain runs
at falling concurrency, so it is not steady state; the summary says so when it is
more than 5% of the run, and that number is understated by roughly its share.

`dropped` is iterations `rate` mode could not start on schedule. It means the
offered load was **below** the `RATE` printed above it — the run is not the open
loop it claims to be until you know whether the ceiling was `MAX_VUS` or the
server.

## Comparison runs

The number is only a comparison if the two arms differ in one thing. In practice:

* **Identical `VUS`/`RATE`, `DURATION` and `MODE` on both arms.** Not the 15s/30s
  the older examples used.
* **A cold table for every run, on both arms.** Measured here: the third run of
  a series was ~20% slower than the first purely because the table had grown
  under the two before it. Wipe the data directory, start the server, warm up at
  the run's own settings, then measure. Otherwise the result depends on the
  order the runs happened to be in.
* **Long enough for steady state.** A short run against a buffering server
  measures its ack rate, not its ingestion rate: smolquery can absorb a brief run
  into its buffer and defer the flush past the measurement window, while
  ClickHouse pays part creation on every request. 60s is a floor; discard a
  warmup run before the one you record.
* **One server at a time.** Both engines on the same machine as k6 is three
  processes competing for ten cores, and neither number would mean anything.
* **Record CPU and RSS for k6 and for the server**, per run, with `watch.go`
  below. k6 copies megabytes per request through loopback and is not cheap;
  without its CPU share on record, a plateau in a `vus` sweep cannot be pinned on
  the server rather than on the machine.
* **State what a 200 means on each side.** They do not mean the same thing —
  durability and read visibility differ, and that belongs next to the throughput
  number, not in a footnote.

## Results

Recorded in `bench/results/`, the same place every other script in `bench/`
records to, under the header that README prescribes — date, commit, command,
machine, runtime. This directory ships the harness and no numbers: a result is
only meaningful against the commit it ran on, so it belongs with the change that
produced it rather than with the tool.

What a k6 run must carry beyond that header: throughput, latency percentiles, and
CPU and RSS **for the server and for k6**, plus the asymmetries that were not
controlled for. A result without the generator's own CPU on the page is not a
result — it cannot distinguish a saturated server from a saturated machine. That
is the whole reason `watch.go` exists.
