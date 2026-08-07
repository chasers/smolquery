Code.require_file("support.exs", __DIR__)
Code.require_file("otel_support.exs", __DIR__)
Code.require_file("compare_support.exs", __DIR__)
Code.require_file("compare_backends.exs", __DIR__)

defmodule Bench.Compare do
  @moduledoc """
  smolquery against ClickHouse: same rows, same sort key, same machine.

  Every other script in `bench/` measures smolquery against itself, which can
  only ever say whether a change helped. This one asks the question a reader
  outside the project actually has — *how far off a real column store are we,
  and on which axis* — and it is built so the answer cannot be flattered by
  accident. The things a comparison usually gets wrong are handled explicitly
  rather than hoped away.

  ## The write path is mostly not the storage engine

  Measured on this repo (`bench/results/otel_logs.md`, Phase 0), one 2,000-row
  batch on one core is 26% JSON decode, 39% validate-and-coerce, and only 35%
  actual storage work. So **65% of smolquery's write path is client-format
  handling**, and a JSON-fed write comparison largely measures Elixir's JSON
  parser and validator against ClickHouse's C++ `JSONEachRow` parser. That
  number would be true and misleading at the same time. The write phase
  therefore reports three numbers and never one:

    * **`:http`** — `POST /v1/…/insert`, what a client actually experiences.
    * **`:storage`** — `Smolquery.BufferService.Client.write_batch/3` on the
      node under test, bypassing Phoenix, the JSON decode, and the validator;
      the path `bench/clustering.exs` already uses. What the storage engine
      costs.
    * **`:default` / `:durable` / `:async` / `:durable_async`** — ClickHouse's
      four durability modes, the comparison target.

  `:storage` is *not* a like-for-like number either: ClickHouse's `JSONEachRow`
  parser validates types too, so `:storage` is the lower bound on our side. The
  honest read is the pair, never one of them alone.

  ## The other three biases

    * **The durability promise.** smolquery's `200` means Parquet in the store,
      manifest entry appended and fsynced, amortized across a `TableBuffer`
      group commit. ClickHouse's MergeTree does not fsync on insert by default,
      so all four of its modes are reported. `:durable_async` (async insert with
      wait *plus* table-level fsync) is the like-for-like number — amortized
      fsync before ack, the same shape as our group commit. `:durable` alone
      fsyncs every INSERT; `:async` alone batches without fsync; both are
      context beside the headline.
    * **Client cost inside the measured window.** Every arm goes through the
      same `encode_rows/1` seam *before* the timer starts — identity on
      smolquery, whose API already takes that shape, a decode-and-re-encode to
      `JSONEachRow` on ClickHouse. The `:storage` mode's validation is prepared
      outside the timer for the same reason. Leaving either inside would charge
      one arm for driver-side work the other never pays.
    * **Whose CPU is being counted.** Every resource figure comes from
      `Bench.CompareSupport.sample_start/2` polling the *server's* OS pid from
      outside, identically on both arms. When a backend cannot name a pid the
      resource columns print `—`; they never print zeros, because a zero reads
      as a measurement and an em dash does not.

  ## Phases

  Each arm is set up, driven through every phase, and torn down before the next
  arm starts, so the two servers never compete for the machine.

    * **Phase W — write throughput.** Bounded `BURST_SECONDS` bursts at each
      `WRITERS` count, in every mode, `REPS` times. Runs against a **scratch
      table** that phases D and R never read, so a cell can be as long as it
      needs to be for a meaningful p99 without the arms having to accumulate
      equal amounts of data. Reports achieved rows/s (median repetition with the
      spread), batch latency p50/p95/p99/max, and errors **broken out by
      reason** — a 429 and a 500 are different events, and collapsing them hides
      backpressure. Nothing is ever retried: a retried 429 turns an
      unsustainable offered rate into a good-looking number.
    * **Phase L — corpus load.** Exactly `ROWS` rows into the real table,
      identically on both arms, one mode. This is the bulk-load number *and* it
      is what makes phases D and R compare the same corpus.
    * **Phase D — disk.** `settle/1` first — seal plus compaction on smolquery,
      `OPTIMIZE TABLE … FINAL` on ClickHouse — then total bytes and bytes per
      row. A settle that fails marks the arm's disk figure invalid, because a
      size measured mid-merge is not a size.
    * **Phase R — read.** The frozen ten-query set, `drop_caches/1` before the
      first repetition of each query, then `QUERY_REPS` runs reported as
      `cold / hot-min / hot-median` — three numbers, never averaged into one.

  ## How to read the read numbers

  Per-tenant queries are bound to `Bench.Otel.project_ref(0)` — rank 0, the
  *heaviest* tenant under the fixture's log-uniform skew. Those numbers are a
  **ceiling** on per-tenant latency, not a typical case.

  Q2, Q3 and Q5 additionally run against the emptiest tenant, because the
  interesting failure mode of a sorted layout is what it does for a tenant that
  is almost all absence. That tenant is **discovered from the corpus**, not
  assumed: at `PROJECTS=100_000` the log-uniform draw leaves the last rank with
  no rows at all, so `project_ref(projects - 1)` would silently measure an empty
  result and read as a spectacular win. Both arms are bound to the same
  discovered ref, and both verify against their own data that it is populated
  before any latency is recorded.

  Both arms must return the **same row count** for every query. When they do
  not, the run prints a mismatch block rather than a ratio: two systems that
  disagree about how many rows a query returns are not being compared.

      mix run bench/compare.exs 2>/dev/null
      ARMS=smolquery ROWS=1000000 POOL=100000 mix run bench/compare.exs
      ROWS=100000000 PROJECTS=100000 POOL=100000 CH_VARIANT=tuned mix run bench/compare.exs

  `ARMS` (default `smolquery,clickhouse`) selects which systems run.
  `ARMS=smolquery` is a complete, self-contained run for a contributor with no
  ClickHouse on the machine: the comparison tables are skipped with a note
  rather than printed half empty. `CH_VARIANT` is `identical` (plain `String`,
  LZ4) or `tuned` (`LowCardinality`, ZSTD) — one variant per run, so a document
  never claims to have covered both. `ROWS`, `PROJECTS`, `BATCH`, `WRITERS`,
  `REPS` and `QUERY_REPS` come from `Bench.CompareSupport.workload/0`;
  `BURST_SECONDS` (default 10) is Phase W's per-cell duration.

  `POOL` is load-bearing and is printed in the header for that reason. The
  fixture's 256 default templates compress far harder than real log traffic,
  which makes bytes-on-disk fantasy and flatters read latency on both arms but
  not necessarily by the same factor. Below 10,000 the driver says on stdout
  that the run is a smoke test rather than a measurement.

  The pre-run estimate is derived from the 42,500 wide rows/s this repo last
  measured through the HTTP API (`bench/results/otel_logs.md`, 2026-08-04) —
  a measured rate from this fixture, not a guess.
  """

  import Bench.Support, except: [table: 0, schema: 0]

  alias Bench.CompareSupport
  alias Bench.CompareSupport.Backend.ClickHouse, as: ClickHouseArm
  alias Bench.CompareSupport.Backend.Smolquery, as: SmolqueryArm
  alias Bench.Otel
  alias Smolquery.BufferService.Client
  alias Smolquery.IngestService.Validator

  @arms %{"smolquery" => SmolqueryArm, "clickhouse" => ClickHouseArm}

  @variants %{
    "identical" => [low_cardinality: false, codec: :lz4],
    "tuned" => [low_cardinality: true, codec: :zstd]
  }

  @clickhouse_modes [:default, :durable, :async, :durable_async]
  @smolquery_modes [:http, :storage]
  @mode_width 15
  @tail_tenant_queries ~w(Q2 Q3 Q5)
  @scratch_suffix "_w"
  @measured_rows_per_second 42_500
  @sample_interval_ms 200
  @pool_floor 10_000
  @tenant_sample 400_000
  @writer_stride 100_000_000_000
  @erpc_timeout_ms 600_000
  @pool_key {__MODULE__, :pool}
  @results_path "bench/results/compare.md"
  @long_run_seconds 3_600

  @doc """
  Runs every selected arm end to end and writes `bench/results/compare.md`.

  Each arm's phases run inside a `try/after` whose `after` clause is
  `teardown/1`. That is not tidiness: a phase that raises half way through
  leaves a peer BEAM holding a port and a scratch directory, or a ClickHouse
  table holding a partial load, and the next arm — or the next run — then
  measures a machine that is already busy and a table that already has rows in
  it. The teardown has to happen on the failure path or the failure silently
  poisons everything after it.

  Arms are threaded through `Enum.map_reduce/3` rather than `Enum.map/2` because
  the emptiest tenant is discovered once, on whichever arm loads its corpus
  first, and then reused verbatim by every later arm. Discovering it per arm
  would let the two arms measure two different tenants under one row label.
  """
  @spec main() :: :ok
  def main do
    Logger.configure(level: :warning)

    workload = normalise(CompareSupport.workload())
    arms = arms()
    variant = variant()

    put_pool()

    try do
      preflight(workload, arms, variant)

      {measured, _tail} =
        Enum.map_reduce(arms, nil, fn arm, tail ->
          result = run_arm(arm, workload, variant, tail)

          {result, result.tail}
        end)

      compare(measured, workload)
      emit_results(measured, workload, variant)
    after
      drop_pool()
    end
  end

  # The fixture pool is a `POOL`-element tuple of 62-key maps, and every writer
  # needs it. Handing it to `Task.async_stream/3` through a closure copies it into
  # each spawned task: measured here at `POOL=100_000`, 292.4 MiB of task heap and
  # 2.1 s to spawn eight writers — 9.1 GiB at `WRITERS=32`, beside a peer BEAM and
  # the node `mix run` already booted, and 2.1 s of that spend falls inside a
  # 10-second burst. A persistent term is read in constant time and is not copied
  # into the reader's heap: the same measurement is 0.008 MiB and 0 ms.
  #
  # It is erased in `main/0`'s `after` clause. Erasing forces a global scan for
  # references, which is why the pool is written exactly once per run rather than
  # per phase.
  defp put_pool do
    :persistent_term.put(@pool_key, Otel.pool())

    :ok
  end

  defp pool, do: :persistent_term.get(@pool_key)

  defp drop_pool do
    :persistent_term.erase(@pool_key)

    :ok
  end

  defp normalise(workload) do
    workload
    |> Map.put(:reps, max(workload.reps, 1))
    |> Map.put(:query_reps, max(workload.query_reps, 1))
    |> Map.put(:batch, max(workload.batch, 1))
    |> Map.put(:writers, writers(workload.writers))
    |> Map.put(:burst_seconds, max(env("BURST_SECONDS", 10), 1))
    |> Map.put(:pool, Otel.pool_size())
  end

  defp writers([]), do: [1]
  defp writers(counts), do: Enum.map(counts, &max(&1, 1))

  # ── arm and variant selection ─────────────────────────────────────────

  defp arms do
    "ARMS"
    |> System.get_env("smolquery,clickhouse")
    |> String.split(",", trim: true)
    |> Enum.map(&resolve_arm!(String.trim(&1)))
    |> case do
      [] -> raise "ARMS selected no arms — valid arms are: #{valid_arms()}"
      arms -> arms
    end
  end

  defp resolve_arm!(name) do
    case Map.fetch(@arms, name) do
      {:ok, module} -> %{name: name, module: module}
      :error -> raise "unknown arm #{inspect(name)} in ARMS — valid arms are: #{valid_arms()}"
    end
  end

  defp valid_arms, do: Enum.join(Enum.sort(Map.keys(@arms)), ", ")

  defp variant do
    name = String.trim(System.get_env("CH_VARIANT", "identical"))

    case Map.fetch(@variants, name) do
      {:ok, opts} ->
        %{name: name, opts: opts}

      :error ->
        raise "unknown CH_VARIANT #{inspect(name)} — valid values are: " <>
                Enum.join(Enum.sort(Map.keys(@variants)), ", ")
    end
  end

  defp modes(%{module: ClickHouseArm}), do: @clickhouse_modes
  defp modes(_arm), do: @smolquery_modes

  defp load_mode(%{module: ClickHouseArm}), do: :durable_async
  defp load_mode(_arm), do: :http

  defp dialect(%{module: ClickHouseArm}), do: :clickhouse
  defp dialect(_arm), do: :smolquery

  defp setup_opts(%{module: ClickHouseArm}, workload, variant) do
    [workload: workload] ++ variant.opts
  end

  defp setup_opts(_arm, workload, _variant), do: [workload: workload]

  # ── preflight ─────────────────────────────────────────────────────────

  defp preflight(workload, arms, variant) do
    schedulers()

    section("Workload")

    say("  ROWS=#{workload.rows} PROJECTS=#{workload.projects} BATCH=#{workload.batch}")

    say(
      "  WRITERS=#{Enum.join(workload.writers, ",")} REPS=#{workload.reps} " <>
        "BURST_SECONDS=#{workload.burst_seconds} QUERY_REPS=#{workload.query_reps}"
    )

    say("  POOL=#{workload.pool} row templates")
    say("  arms: #{Enum.map_join(arms, ", ", & &1.name)}")
    say("  CH_VARIANT=#{variant.name} (#{variant_description(variant)})")
    say("  sort key: (#{Enum.join(CompareSupport.clustering_key(), ", ")}) on both arms")
    say("  commit: #{git_sha()}")

    pool_guard(workload)
    estimate(workload, arms)
    legend()
  end

  defp variant_description(%{name: "tuned"}),
    do: "LowCardinality on the allowlisted strings, ZSTD"

  defp variant_description(_variant), do: "plain String, ClickHouse's stock LZ4"

  defp pool_guard(%{pool: pool}) when pool >= @pool_floor, do: :ok

  defp pool_guard(%{pool: pool}) do
    say("")
    say("  ** POOL=#{pool} IS A SMOKE TEST, NOT A MEASUREMENT.")
    say("  ** #{pool} row templates compress far harder than real log traffic, so")
    say("  ** bytes-on-disk is fantasy and read latency is flattered. That happens on")
    say("  ** both arms, but not necessarily by the same factor, which is worse than a")
    say("  ** shared bias. Re-run with POOL=100000 or higher before quoting any number.")
    say("")
  end

  defp estimate(workload, arms) do
    per_row = wire_bytes_per_row(workload)
    load_seconds = workload.rows / @measured_rows_per_second
    burst_seconds = Enum.sum(Enum.map(arms, &burst_budget(&1, workload)))
    total = burst_seconds + load_seconds * length(arms)

    say("")
    say("  fixture is #{per_row} B/row on the wire → #{mib(workload.rows * per_row)} MiB per arm")

    say(
      "  Phase W ~#{minutes(burst_seconds)} min of bursts across all arms; Phase L " <>
        "~#{minutes(load_seconds)} min per arm at #{@measured_rows_per_second} rows/s " <>
        "(bench/results/otel_logs.md, 2026-08-04)"
    )

    say("  settle and the query phase are on top of that and are not estimated here")

    if total > @long_run_seconds do
      say("")
      say("  ** ~#{minutes(total)} min of write time alone across #{length(arms)} arm(s).")
      say("  ** Interrupt now if that is not what ROWS and BURST_SECONDS were meant to buy.")
    end
  end

  defp burst_budget(arm, workload) do
    length(modes(arm)) * length(workload.writers) * workload.reps * workload.burst_seconds
  end

  defp wire_bytes_per_row(workload) do
    sample = min(workload.batch, 200)
    body = Otel.body(pool(), sample, 0)

    div(byte_size(body), sample)
  end

  defp legend do
    section("Query set")

    say("  " <> label("id", 5) <> label("query", 48) <> "hypothesis")

    for query <- CompareSupport.queries() do
      say("  " <> label(query.id, 5) <> label(query.label, 48) <> query.hypothesis)
    end

    say("")
    say("  heavy tenant = project_ref(0), the most frequent under the log-uniform skew:")
    say("  a ceiling on per-tenant latency, not a typical tenant.")

    say(
      "  #{Enum.join(@tail_tenant_queries, ", ")} also run against the emptiest tenant, " <>
        "chosen from the corpus rather than assumed."
    )
  end

  # ── one arm, all phases ───────────────────────────────────────────────

  defp run_arm(arm, workload, variant, tail) do
    section("Arm #{arm.name} — setup")

    state = setup!(arm, workload, variant)

    try do
      :ok = arm.module.create_table(state, Otel.columns(), CompareSupport.clustering_key())

      say(
        "  corpus table created: #{length(Otel.columns())} columns, sorted by " <>
          "(#{Enum.join(CompareSupport.clustering_key(), ", ")})"
      )

      write = phase_write(arm, state, workload)
      load = phase_load(arm, state, workload)
      disk = phase_disk(arm, state, load)
      tail = tenants(arm, state, workload, tail)
      read = phase_read(arm, state, workload, tail)

      %{arm: arm, write: write, load: load, disk: disk, read: read, tail: tail}
    after
      arm.module.teardown(state)
    end
  end

  defp setup!(arm, workload, variant) do
    case arm.module.setup(setup_opts(arm, workload, variant)) do
      {:ok, state} ->
        say("  #{arm.module.name()} up; server pid #{inspect(arm.module.os_pid(state))}")
        state

      {:error, reason} ->
        raise "#{arm.name} setup failed: #{inspect(reason)}"
    end
  end

  # The frozen `Bench.CompareSupport.Backend` behaviour has no seam for a second
  # table: `create_table/3` takes no name and there is no drop. Phase W needs one
  # anyway, because its bursts must not land in the corpus that phases D and R
  # read. So this reaches past the abstraction and rewrites the table name in the
  # backend's own state — deliberately, and only here.
  #
  # It raises rather than falling back when the state has no `:table` key. A
  # silent fallback would send every burst into the corpus table, and the run
  # would then report a disk figure and query latencies for a table holding an
  # unknown number of extra rows, with nothing in the output saying so.
  defp scratch_state(%{table: table} = state, suffix) do
    scratch = table <> suffix

    state
    |> Map.put(:table, scratch)
    |> rewrite_ddl_table(scratch)
  end

  defp scratch_state(_state, _suffix) do
    raise "the backend state has no :table key, so Phase W cannot be given a scratch " <>
            "table — its bursts would land in the corpus that Phase D and Phase R read. " <>
            "Refusing to continue."
  end

  defp rewrite_ddl_table(%{ddl_opts: opts} = state, scratch) when is_list(opts) do
    %{state | ddl_opts: Keyword.put(opts, :table, scratch)}
  end

  defp rewrite_ddl_table(state, _scratch), do: state

  # ── phase W: throughput, on a scratch table ───────────────────────────

  defp phase_write(arm, state, workload) do
    scratch = scratch_state(state, @scratch_suffix)
    :ok = arm.module.create_table(scratch, Otel.columns(), CompareSupport.clustering_key())

    section("Phase W — #{arm.name}: write throughput")

    say(
      "  #{workload.burst_seconds}s bursts on scratch table `#{scratch.table}`, " <>
        "#{workload.batch} rows/batch, #{workload.reps} rep(s) per cell"
    )

    say("  nothing written here is read by Phase D or Phase R")

    cells =
      for mode <- modes(arm), writers <- workload.writers do
        %{mode: mode, writers: writers}
      end

    results =
      cells
      |> Enum.with_index()
      |> Enum.map(fn {cell, index} -> write_cell(arm, scratch, workload, cell, index) end)

    %{cells: results, table: scratch.table}
  end

  defp write_cell(arm, state, workload, cell, index) do
    say("  running #{cell.writers} writers, mode #{cell.mode} …")

    sampler = start_sampler(arm, state)

    reps =
      for rep <- 0..(workload.reps - 1) do
        stride = @writer_stride * (Enum.max(workload.writers) + 1)

        burst(arm, state, workload, cell, workload.rows + (index * workload.reps + rep) * stride)
      end

    sample = stop_sampler(sampler)
    rates = Enum.sort(Enum.map(reps, & &1.rows_per_second))
    slice = Enum.reduce(Enum.map(reps, & &1.slice), &merge_slices/2)
    latency = percentiles(slice.latencies)

    result = %{
      mode: cell.mode,
      writers: cell.writers,
      rates: rates,
      rate_median: Enum.at(rates, div(length(rates), 2)),
      rate_min: hd(rates),
      rate_max: List.last(rates),
      latency: latency,
      acked: slice.acked,
      offered: slice.offered,
      errors: slice.errors,
      sample: sample
    }

    say(
      "    #{result.rate_median} rows/s median (#{result.rate_min}–#{result.rate_max}), " <>
        "p95 #{format(latency.p95)} ms, #{error_count(result)} error(s), " <>
        "driver took #{driver_share(reps)}% of the burst"
    )

    result
  end

  # How much of a burst's wall clock went on this driver rather than on the server.
  # Printed per cell because it is the size of the bias W1 would have carried if the
  # rate were taken over wall clock, and it differs per arm by construction.
  defp driver_share(reps) do
    wall = Enum.sum(Enum.map(reps, & &1.seconds))
    measured = Enum.sum(Enum.map(reps, & &1.measured_seconds))

    if wall > 0, do: round((wall - measured) / wall * 100), else: 0
  end

  defp burst(arm, state, workload, cell, base) do
    started = System.monotonic_time(:microsecond)
    deadline = started + workload.burst_seconds * 1_000_000

    slices =
      0..(cell.writers - 1)
      |> Task.async_stream(
        fn writer ->
          burst_writer(arm, state, workload, cell.mode, base + writer * @writer_stride, deadline)
        end,
        max_concurrency: cell.writers,
        timeout: :infinity,
        ordered: false
      )
      |> Enum.map(fn {:ok, slice} -> slice end)

    wall = (System.monotonic_time(:microsecond) - started) / 1_000_000

    tally(slices, wall)
  end

  # Rows acked over the *measured* window, not over burst wall clock.
  #
  # Wall clock also contains `prepare/4`, and that cost is arm-specific: smolquery
  # `:http` pays row generation plus one `JSON.encode!`; ClickHouse pays that plus
  # a `JSON.decode!` plus a `JSON.encode_to_iodata!` per row; `:storage` pays
  # generation plus `Validator.validate/2`. The contract's own stage profile prices
  # one such decode at 25.7 ms against a 100.4 ms insert path, so a wall-clock rate
  # would understate ClickHouse by tens of percent *in our favour*, on exactly the
  # `:durable_async` against `:http` pair this driver calls the headline. It would also
  # leave `:storage` — the mode whose entire purpose is to remove the validator —
  # with the validator in its denominator.
  #
  # The denominator is the longest single writer's time inside the timer rather
  # than the sum across writers: the writers run concurrently against one server,
  # so summing would divide the aggregate rate by the writer count.
  #
  # It is an estimator, and it errs in a known direction. A writer's timed
  # intervals are disjoint and all fall inside the burst, so the longest writer's
  # total is a *lower* bound on the union of every writer's timed intervals — the
  # true concurrent measured window. Dividing by a lower bound yields an *upper*
  # bound on the rate: this never understates, and it can overstate. The gap
  # widens with the driver work sitting between a writer's batches, which is
  # larger on the ClickHouse arm, so what residual bias survives runs *against*
  # smolquery. That is the safe direction for a benchmark whose author has a
  # stake in the outcome, but it is a bias and the ledger says so.
  #
  # `wall` is kept beside it, because the gap between them is how much of the
  # burst the driver spent on itself.
  defp tally(slices, wall) do
    slice = Enum.reduce(slices, &merge_slices/2)
    measured = Enum.max(Enum.map(slices, & &1.measured_us)) / 1_000_000

    %{
      slice: slice,
      seconds: wall,
      measured_seconds: measured,
      rows_per_second: round(slice.acked / max(measured, 1.0e-6))
    }
  end

  defp burst_writer(arm, state, workload, mode, base, deadline) do
    if System.monotonic_time(:microsecond) >= deadline do
      blank_slice()
    else
      acc = one_batch(arm, state, workload, mode, base, blank_slice())

      merge_slices(
        acc,
        burst_writer(arm, state, workload, mode, base + workload.batch, deadline)
      )
    end
  end

  defp one_batch(arm, state, workload, mode, offset, acc) do
    prepared = prepare(arm, mode, workload.batch, offset)
    {us, result} = submit(arm, state, mode, prepared)

    record(acc, us, workload.batch, result)
  end

  # ── phase L: the corpus both arms read ────────────────────────────────

  defp phase_load(arm, state, workload) do
    mode = load_mode(arm)
    writers = Enum.max(workload.writers)

    section("Phase L — #{arm.name}: corpus load")

    say(
      "  #{workload.rows} rows into `#{Map.get(state, :table, "?")}` via #{writers} writers, " <>
        "mode #{mode} — this is the table Phase D and Phase R measure"
    )

    per_writer = max(div(workload.rows, writers), 1)
    sampler = start_sampler(arm, state)
    started = System.monotonic_time(:microsecond)

    slices =
      0..(writers - 1)
      |> Task.async_stream(
        fn writer ->
          load_writer(arm, state, workload, mode, writer * per_writer, per_writer)
        end,
        max_concurrency: writers,
        timeout: :infinity,
        ordered: false
      )
      |> Enum.map(fn {:ok, slice} -> slice end)

    wall = (System.monotonic_time(:microsecond) - started) / 1_000_000
    sample = stop_sampler(sampler)
    result = load_result(mode, writers, tally(slices, wall), sample)

    say(
      "  loaded #{result.acked} rows in #{result.seconds}s wall — " <>
        "#{result.rows_per_second} rows/s over the measured window, " <>
        "#{error_count(result)} error(s)"
    )

    result
  end

  defp load_result(mode, writers, tallied, sample) do
    %{
      mode: mode,
      writers: writers,
      seconds: Float.round(tallied.seconds, 1),
      measured_seconds: Float.round(tallied.measured_seconds, 1),
      rows_per_second: tallied.rows_per_second,
      latency: percentiles(tallied.slice.latencies),
      acked: tallied.slice.acked,
      offered: tallied.slice.offered,
      errors: tallied.slice.errors,
      sample: sample
    }
  end

  defp load_writer(arm, state, workload, mode, base, count) do
    Enum.reduce(batches(base, count, workload.batch), blank_slice(), fn batch, acc ->
      prepared = prepare(arm, mode, batch.count, batch.offset)
      {us, result} = submit(arm, state, mode, prepared)

      record(acc, us, batch.count, result)
    end)
  end

  defp batches(base, count, size) when count > 0 do
    taken = min(count, size)

    [%{offset: base, count: taken} | batches(base + taken, count - taken, size)]
  end

  defp batches(_base, _count, _size), do: []

  # ── the two write paths ───────────────────────────────────────────────

  # Both branches build a batch's payload outside the measured window. `:storage`
  # prepares validated, coerced Elixir rows, because the whole point of that mode
  # is to price the storage engine without the validator in front of it; every
  # other mode goes through the arm's own `encode_rows/1` seam. Both are
  # driver-side work only one arm would otherwise pay for, and neither may sit
  # inside the timer.
  defp prepare(_arm, :storage, count, offset) do
    {valid, []} = Validator.validate(storage_schema(), Otel.rows(pool(), count, offset))

    valid
  end

  defp prepare(arm, _mode, count, offset) do
    arm.module.encode_rows(Otel.body(pool(), count, offset))
  end

  # `:storage` deliberately bypasses the `Backend` abstraction. That abstraction
  # exists to hide exactly the difference this mode measures — the transport and
  # the validator in front of the storage engine — so going through it would make
  # the measurement impossible by construction. The behaviour stays frozen and the
  # exception lives here, in the driver, where it is visible.
  #
  # The timing call is `:timer.tc/3` *on the peer*, not around the `:erpc` on this
  # side: arguments are decoded before the call is applied, so the batch crossing
  # Erlang distribution is charged to nobody and the reported window is the
  # peer-local `write_batch/3` alone. Timing it from the driver would put a
  # multi-megabyte term copy inside a number labelled "storage".
  defp submit(_arm, state, :storage, rows), do: storage_write(state, rows)

  defp submit(arm, state, mode, payload) do
    :timer.tc(fn -> arm.module.insert(state, payload, mode: mode) end)
  end

  defp storage_write(%{node: node, dataset: dataset, table: table}, rows) do
    batch = %{schema: storage_schema(), rows: rows}
    args = [Client, :write_batch, [Smolquery.BufferService, {dataset, table}, batch]]

    case :erpc.call(node, :timer, :tc, args, @erpc_timeout_ms) do
      {us, {:ok, _ack}} -> {us, {:ok, %{rows: length(rows)}}}
      {us, {:error, reason}} -> {us, {:error, reason}}
    end
  catch
    kind, reason -> {0, {:error, {:storage_write_failed, kind, clip(inspect(reason))}}}
  end

  defp storage_write(_state, _rows) do
    raise "the :storage write mode needs the smolquery peer's node name, dataset and " <>
            "table, and this arm's state carries none of them. Only the smolquery arm " <>
            "has a storage-only path; ClickHouse's parser validates types too."
  end

  defp storage_schema do
    case Process.get(:compare_storage_schema) do
      nil ->
        schema = %{Otel.table_schema() | clustering: CompareSupport.clustering_key()}
        Process.put(:compare_storage_schema, schema)

        schema

      schema ->
        schema
    end
  end

  # ── slices ────────────────────────────────────────────────────────────

  defp blank_slice,
    do: %{acked: 0, offered: 0, requests: 0, measured_us: 0, latencies: [], errors: %{}}

  defp record(acc, us, offered, {:ok, %{rows: rows}}) do
    %{
      acc
      | acked: acc.acked + rows,
        offered: acc.offered + offered,
        requests: acc.requests + 1,
        measured_us: acc.measured_us + us,
        latencies: [us | acc.latencies]
    }
  end

  defp record(acc, us, offered, {:error, error}) do
    %{
      acc
      | offered: acc.offered + offered,
        requests: acc.requests + 1,
        measured_us: acc.measured_us + us,
        errors: Map.update(acc.errors, reason(error), 1, &(&1 + 1))
    }
  end

  defp merge_slices(a, b) do
    %{
      acked: a.acked + b.acked,
      offered: a.offered + b.offered,
      requests: a.requests + b.requests,
      measured_us: a.measured_us + b.measured_us,
      latencies: a.latencies ++ b.latencies,
      errors: Map.merge(a.errors, b.errors, fn _reason, x, y -> x + y end)
    }
  end

  defp reason({:http, status, _body}) when is_integer(status), do: "HTTP #{status}"
  defp reason({:http, status}) when is_integer(status), do: "HTTP #{status}"
  defp reason({:status, status}) when is_integer(status), do: "HTTP #{status}"
  defp reason(tag) when is_atom(tag), do: to_string(tag)

  defp reason(term) when is_tuple(term) and tuple_size(term) > 0 do
    case elem(term, 0) do
      tag when is_atom(tag) -> to_string(tag)
      _other -> clip(inspect(term))
    end
  end

  defp reason(term), do: clip(inspect(term))

  defp clip(text) when byte_size(text) <= 40, do: text
  defp clip(text), do: binary_part(text, 0, 40) <> "…"

  defp error_count(cell), do: Enum.sum(Map.values(cell.errors))

  # ── phase D ───────────────────────────────────────────────────────────

  defp phase_disk(arm, state, load) do
    section("Phase D — #{arm.name}: settle, then bytes on disk")
    say("  settling (seal + compaction / OPTIMIZE TABLE FINAL) — this can take a while …")

    sampler = start_sampler(arm, state)
    settled = arm.module.settle(state)
    sample = stop_sampler(sampler)

    valid? = report_settle(settled)
    bytes = measure_disk(arm, state)

    say("  #{format(bytes)} bytes over #{load.acked} acked rows")

    %{bytes: bytes, valid?: valid?, settle: settled, rows: load.acked, sample: sample}
  end

  defp report_settle(:ok) do
    say("  settled")
    true
  end

  defp report_settle({:error, {:settle_timeout, detail}}) do
    say("  SETTLE TIMED OUT: #{inspect(detail)}")
    say("  the disk figure below was measured mid-merge and is INVALID — do not quote it")
    false
  end

  defp report_settle({:error, reason}) do
    say("  SETTLE FAILED: #{inspect(reason)}")
    say("  the disk figure below is INVALID — background work did not finish")
    false
  end

  defp measure_disk(arm, state) do
    case arm.module.disk_bytes(state) do
      {:ok, bytes} ->
        bytes

      {:error, reason} ->
        say("  disk_bytes unavailable: #{inspect(reason)}")
        nil
    end
  end

  # ── tenant selection ──────────────────────────────────────────────────

  # The heavy tenant is `Bench.Otel.project_ref(0)`, which the log-uniform draw
  # guarantees is the most frequent. The empty one cannot be named by formula: at
  # `PROJECTS=100_000` the last rank draws no rows at all, and binding to it would
  # return instantly with nothing and read as a spectacular result. So it comes
  # from the corpus — `project_for/1` is a pure function of the row index and
  # Phase L wrote exactly `0..ROWS-1`, so folding the generator over that range is
  # the true tenant histogram of the data on disk, and the least frequent
  # *populated* tenant falls out of it. It is chosen once and reused by every arm,
  # so both measure the same tenant under one row label.
  #
  # The histogram is then checked against the arm's own data, because a computed
  # answer nothing verified is a guess with extra steps. The obvious
  # `GROUP BY project_id ORDER BY count() ASC LIMIT 1` is unavailable: the frozen
  # `query/2` callback returns row counts and timings, never values, so the driver
  # cannot read a project ref back out of either database. The probe asks the one
  # thing a row count *can* answer — does this tenant have rows — and raises when
  # it does not.
  defp tenants(arm, state, workload, tail) do
    section("Tenants — #{arm.name}")

    head = Otel.project_ref(0)
    tail = tail || least_populated(workload)

    verify_tenant!(arm, state, head, "heavy")
    verify_tenant!(arm, state, tail, "empty")

    say("  heavy tenant #{head}, empty tenant #{tail} — both verified populated on this arm")

    tail
  end

  # Strided rather than exhaustive, and that is not an approximation in the way
  # that matters. Every index sampled is an index Phase L actually wrote, so every
  # tenant this histogram sees has at least one row in the corpus by construction
  # — which is the property the choice depends on. The sample's least frequent
  # tenant is drawn from the sparse tail and is guaranteed populated, where the
  # true global minimum would cost a full fold over `ROWS` (≈2 min at 10M, ≈20 at
  # 100M) to find a tenant that is sparse in exactly the same way.
  defp least_populated(workload) do
    stride = max(div(workload.rows, @tenant_sample), 1)

    {ref, count} =
      0..(workload.rows - 1)//stride
      |> Enum.reduce(%{}, fn index, acc ->
        Map.update(acc, Otel.project_for(index), 1, &(&1 + 1))
      end)
      |> Enum.min_by(fn {_ref, count} -> count end)

    say(
      "  sampled every #{stride}th of #{workload.rows} row indices; emptiest tenant seen " <>
        "#{count} time(s) in the sample"
    )

    ref
  end

  defp verify_tenant!(arm, state, ref, kind) do
    sql = "SELECT project_id FROM #{corpus_ref(arm)} WHERE project_id = '#{ref}' LIMIT 1"

    case arm.module.query(state, sql) do
      {:ok, %{rows: rows}} when rows > 0 ->
        :ok

      {:ok, %{rows: 0}} ->
        raise "the #{kind} tenant #{ref} has no rows on the #{arm.name} arm. Every " <>
                "per-tenant query would return instantly with nothing and read as an " <>
                "excellent result. Refusing to measure it."

      {:error, reason} ->
        raise "could not verify the #{kind} tenant #{ref} on the #{arm.name} arm: " <>
                inspect(reason)
    end
  end

  # Lifted out of Q1's SQL rather than re-declared here, so the probe cannot end up
  # asking about a different table than the ten measured queries read.
  defp corpus_ref(arm) do
    sql =
      CompareSupport.queries()
      |> Enum.find(&(&1.id == "Q1"))
      |> Map.fetch!(:sql)
      |> Map.fetch!(dialect(arm))

    case Regex.run(~r/\bFROM\s+(\S+)/i, sql) do
      [_match, ref] -> ref
      nil -> raise "cannot find the corpus table in Q1's SQL for #{arm.name}: #{inspect(sql)}"
    end
  end

  # ── phase R ───────────────────────────────────────────────────────────

  defp phase_read(arm, state, workload, tail) do
    section("Phase R — #{arm.name}: read")

    say(
      "  #{length(CompareSupport.queries())} queries, cold + " <>
        "#{max(workload.query_reps - 1, 0)} hot, caches dropped before each cold run"
    )

    sampler = start_sampler(arm, state)

    entries =
      for query <- CompareSupport.queries(),
          target <- targets(query, tail) do
        measure_query(arm, state, query, target, workload)
      end

    sample = stop_sampler(sampler)
    cache = entries |> Enum.flat_map(& &1.cache_note) |> Enum.uniq()

    report_cache(cache)

    %{entries: entries, sample: sample, cache: cache}
  end

  defp targets(%{needs_project: false}, _tail), do: [{:all, nil}]

  defp targets(query, tail) do
    head = {:heavy, Otel.project_ref(0)}

    if query.id in @tail_tenant_queries, do: [head, {:empty, tail}], else: [head]
  end

  defp measure_query(arm, state, query, {tenant, project}, workload) do
    say("  #{query.id}/#{tenant} …")

    sql = sql_for(arm, query, project)
    dropped = arm.module.drop_caches(state)
    runs = for _rep <- 1..workload.query_reps, do: arm.module.query(state, sql)

    query
    |> summarise_runs(tenant, runs)
    |> Map.put(:cache_note, cache_note(dropped))
  end

  defp sql_for(arm, query, nil), do: Map.fetch!(query.sql, dialect(arm))

  defp sql_for(arm, query, project) do
    CompareSupport.bind_project(Map.fetch!(query.sql, dialect(arm)), project)
  end

  defp summarise_runs(query, tenant, [{:ok, cold} | rest]) do
    hot = for {:ok, run} <- rest, do: run.ms

    %{
      id: query.id,
      tenant: tenant,
      cold: cold.ms,
      hot_min: Enum.min(hot, fn -> nil end),
      hot_med: median(hot),
      rows: cold.rows,
      rows_read: cold.rows_read,
      bytes_read: cold.bytes_read,
      error: failure(rest)
    }
  end

  defp summarise_runs(query, tenant, [{:error, reason} | _rest]) do
    say("    #{query.id} FAILED: #{inspect(reason)}")

    %{
      id: query.id,
      tenant: tenant,
      cold: nil,
      hot_min: nil,
      hot_med: nil,
      rows: nil,
      rows_read: nil,
      bytes_read: nil,
      error: reason(reason)
    }
  end

  defp failure(runs) do
    case Enum.find(runs, &match?({:error, _reason}, &1)) do
      {:error, term} -> reason(term)
      nil -> nil
    end
  end

  defp cache_note(:ok), do: []
  defp cache_note({:error, reason}), do: [clip(inspect(reason))]

  defp report_cache([]), do: :ok

  defp report_cache(notes) do
    say("  caches were NOT fully dropped: #{Enum.join(notes, "; ")}")
    say("  cold numbers on this arm are warmer than the label claims")
  end

  defp median([]), do: nil

  defp median(values) do
    sorted = Enum.sort(values)

    Enum.at(sorted, div(length(sorted), 2))
  end

  # ── resource sampling ─────────────────────────────────────────────────

  defp start_sampler(arm, state) do
    case arm.module.os_pid(state) do
      pid when is_integer(pid) and pid > 0 ->
        CompareSupport.sample_start(pid, interval_ms: @sample_interval_ms)

      _none ->
        nil
    end
  end

  defp stop_sampler(nil), do: nil
  defp stop_sampler(sampler), do: CompareSupport.sample_stop(sampler)

  defp combine_samples(samples) do
    case Enum.reject(samples, &is_nil/1) do
      [] -> nil
      present -> combined(present)
    end
  end

  defp combined(present) do
    cpu = Enum.sum(Enum.map(present, & &1.cpu_seconds))
    wall = Enum.sum(Enum.map(present, & &1.wall_seconds))
    means = Enum.map(present, & &1.rss_mean_mib)

    %{
      rss_peak_mib: Enum.max(Enum.map(present, & &1.rss_peak_mib)),
      rss_p95_mib: Enum.max(Enum.map(present, & &1.rss_p95_mib)),
      rss_mean_mib: Float.round(Enum.sum(means) / length(means), 1),
      rss_end_mib: List.last(present).rss_end_mib,
      cores_mean: ratio(cpu, wall),
      cores_peak: Enum.max(Enum.map(present, & &1.cores_peak)),
      cpu_seconds: Float.round(cpu, 3),
      wall_seconds: Float.round(wall, 3),
      samples: Enum.sum(Enum.map(present, & &1.samples))
    }
  end

  # ── comparison tables ─────────────────────────────────────────────────

  defp compare(measured, workload) do
    throughput_table(measured, workload)
    latency_table(measured)
    error_table(measured)
    load_table(measured)
    resource_table(measured)
    disk_table(measured)
    read_tables(measured)
    ratio_table(measured)
  end

  defp throughput_table(measured, workload) do
    section("W1 — write throughput, rows/s (median repetition)")

    say(
      "  " <>
        label("arm", 12) <>
        label("mode", @mode_width) <> Enum.map_join(workload.writers, "", &pad("w=#{&1}", 12))
    )

    for result <- measured, mode <- modes(result.arm) do
      cells = for cell <- result.write.cells, cell.mode == mode, do: cell

      say(
        "  " <>
          label(result.arm.name, 12) <>
          label(mode, @mode_width) <>
          Enum.map_join(workload.writers, "", fn writers -> pad(rate_at(cells, writers), 12) end)
      )
    end

    say("  rows acked ÷ the longest writer's time inside the timer — the same window W2's")
    say("  percentiles describe. Not burst wall clock: that also contains this driver's own")
    say("  row generation and format conversion, which costs a different amount per arm and")
    say("  would understate ClickHouse on the headline pair.")
    say("  That denominator is a lower bound on the true concurrent window, so these rates")
    say("  are upper bounds — never understated, possibly overstated, and overstated more")
    say("  on the arm whose writers do more driver work between batches, which is")
    say("  ClickHouse. The residual bias therefore runs against smolquery, not for it.")
    say("  smolquery `:http` is what a client experiences; `:storage` is the same engine")
    say("  with Phoenix, the JSON decode and the validator taken out — 65% of the path by")
    say("  the stage profile in bench/results/otel_logs.md. ClickHouse has no `:storage`")
    say("  counterpart: its JSONEachRow parser validates types too, so `:storage` is our")
    say("  lower bound, not a like-for-like number. The honest read is the pair.")
    say("  The like-for-like ClickHouse row against smolquery `:http` is `:durable_async`:")
    say("  async_insert with wait plus table-level fsync. Our ack amortizes — one")
    say("  TableBuffer group commit covers many client batches and pays one segment fsync")
    say("  plus one manifest-log fsync for the whole commit — and `:durable_async` is the")
    say("  ClickHouse shape that does the same. `:durable` alone fsyncs every INSERT;")
    say("  `:async` alone batches without fsync; both are context, not the headline.")
  end

  defp rate_at(cells, writers) do
    case Enum.find(cells, &(&1.writers == writers)) do
      nil -> "—"
      cell -> cell.rate_median
    end
  end

  defp latency_table(measured) do
    section("W2 — write latency per batch, ms (all repetitions pooled)")
    say("  " <> latency_header())

    for result <- measured, cell <- result.write.cells do
      say("  " <> latency_row(result.arm.name, cell.mode, cell.writers, cell))
    end

    say("  latencies cover acked batches only — a shed request returns in microseconds")
    say("  and would pull p50 down while the shedding it signals disappeared from view.")
  end

  defp latency_header do
    label("arm", 12) <>
      label("mode", @mode_width) <>
      pad("writers", 9) <>
      pad("rows/s", 10) <>
      pad("spread", 9) <>
      pad("p50", 10) <>
      pad("p95", 10) <>
      pad("p99", 10) <> pad("max", 10) <> pad("acked", 12) <> pad("errors", 9)
  end

  defp latency_row(arm, mode, writers, cell) do
    label(arm, 12) <>
      label(mode, @mode_width) <>
      pad(writers, 9) <>
      pad(cell_rate(cell), 10) <>
      pad(spread(cell), 9) <>
      pad(format(cell.latency.p50), 10) <>
      pad(format(cell.latency.p95), 10) <>
      pad(format(cell.latency.p99), 10) <>
      pad(format(cell.latency.max), 10) <> pad(cell.acked, 12) <> pad(error_count(cell), 9)
  end

  defp cell_rate(%{rate_median: rate}), do: rate
  defp cell_rate(%{rows_per_second: rate}), do: rate

  defp spread(%{rate_min: min, rate_max: max, rate_median: median}) when median > 0 do
    "#{round((max - min) / median * 100)}%"
  end

  defp spread(_cell), do: "—"

  defp error_table(measured) do
    rows =
      for result <- measured,
          cell <- result.write.cells ++ [result.load],
          {reason, count} <- cell.errors do
        {result.arm.name, cell.mode, Map.get(cell, :writers), reason, count}
      end

    section("W3 — write errors by reason (Phase W and Phase L)")
    report_errors(rows)
  end

  defp report_errors([]) do
    say("  none on any arm — nothing was shed, refused, or failed, and nothing was retried")
  end

  defp report_errors(rows) do
    say(
      "  " <>
        label("arm", 12) <>
        label("mode", @mode_width) <>
        pad("writers", 9) <> "  " <> label("reason", 44) <> pad("count", 9)
    )

    for {arm, mode, writers, reason, count} <- rows do
      say(
        "  " <>
          label(arm, 12) <>
          label(mode, @mode_width) <>
          pad(writers, 9) <> "  " <> label(reason, 44) <> pad(count, 9)
      )
    end

    say("  no request was ever retried: a retried 429 is an unsustainable rate wearing a")
    say("  good number, and the whole point of this column is to see the rate give way.")
  end

  defp load_table(measured) do
    section("L — corpus load: the same ROWS into the table Phase D and Phase R measure")
    say("  " <> latency_header() <> pad("seconds", 10))

    for result <- measured do
      load = result.load

      say(
        "  " <>
          latency_row(result.arm.name, load.mode, load.writers, load) <> pad(load.seconds, 10)
      )
    end

    say("  one mode per arm, at the highest writer count in WRITERS: this phase exists to")
    say("  put identical data on both arms, and the bulk-load rate is what that cost.")
    say("  rows/s is over the measured window, as in W1; `seconds` is wall clock, so the")
    say("  gap between them is what the driver spent generating and encoding rows.")
  end

  defp resource_table(measured) do
    section("W4 — server process resources, per arm per phase")

    say(
      "  " <>
        label("arm", 12) <>
        label("phase", 8) <>
        pad("RSS peak", 11) <>
        pad("RSS p95", 11) <>
        pad("RSS mean", 11) <>
        pad("cores mean", 12) <> pad("cores peak", 12) <> pad("samples", 9)
    )

    for result <- measured, {phase, sample} <- phase_samples(result) do
      say(
        "  " <>
          label(result.arm.name, 12) <>
          label(phase, 8) <>
          pad(field(sample, :rss_peak_mib), 11) <>
          pad(field(sample, :rss_p95_mib), 11) <>
          pad(field(sample, :rss_mean_mib), 11) <>
          pad(field(sample, :cores_mean), 12) <>
          pad(field(sample, :cores_peak), 12) <> pad(field(sample, :samples), 9)
      )
    end

    say("  RSS in MiB, sampled every #{@sample_interval_ms} ms from the server's OS pid only.")
    say("  Phase W aggregates the per-cell windows: cores mean is Σcpu ÷ Σwall over the")
    say("  measured windows, so idle time between cells is excluded rather than averaged in.")
    say("  `—` means the arm could not name a pid; it is an absence, not a zero.")
  end

  defp phase_samples(result) do
    [
      {"W", combine_samples(Enum.map(result.write.cells, & &1.sample))},
      {"L", result.load.sample},
      {"D", result.disk.sample},
      {"R", result.read.sample}
    ]
  end

  defp field(nil, _key), do: "—"
  defp field(map, key), do: format(Map.fetch!(map, key))

  defp disk_table(measured) do
    section("D — bytes on disk after settle")

    say(
      "  " <>
        label("arm", 12) <>
        pad("rows", 14) <>
        pad("bytes", 16) <> pad("MiB", 11) <> pad("B/row", 10) <> "  " <> label("status", 30)
    )

    for result <- measured do
      say(
        "  " <>
          label(result.arm.name, 12) <>
          pad(result.disk.rows, 14) <>
          pad(format(result.disk.bytes), 16) <>
          pad(disk_mib(result.disk), 11) <>
          pad(bytes_per_row(result.disk), 10) <> "  " <> label(disk_status(result.disk), 30)
      )
    end

    scratch_residue_note()
  end

  # Phase W's scratch table is never dropped — the frozen behaviour has no drop
  # seam — and that is contained for *bytes* but not for time. `Store.Local.list/2`
  # globs a directory rather than matching a string prefix, so `<table>_w` provably
  # cannot enter `disk_bytes/1`, and ClickHouse's `system.parts` filter is by exact
  # table name. Latency and resources are a different matter, and the two arms are
  # contaminated differently, which is worse than a shared bias.
  defp scratch_residue_note do
    say("")
    say("  Phase W's scratch table is still on the machine during Phase D and Phase R.")
    say("  Bytes are unaffected: both arms scope disk_bytes/1 to the corpus table by name.")
    say("  Time is not. ClickHouse scopes settle/1 to the corpus, so scratch merges are")
    say("  neither triggered nor waited for and run through D and R on the server's CPU.")
    say("  smolquery is asymmetric the other way: Compactor.sweep/2 and GC.sweep/2 are")
    say("  global, so scratch compaction lands inside D's sampler window, while")
    say("  force_seal/2 is corpus-scoped, so scratch micro-segments stay resident in the")
    say("  buffer and inflate RSS through R. Treat D's and R's cores and RSS as upper")
    say("  bounds, differently inflated per arm.")
  end

  defp disk_mib(%{bytes: nil}), do: "—"
  defp disk_mib(%{bytes: bytes}), do: mib(bytes)

  defp bytes_per_row(%{bytes: nil}), do: "—"
  defp bytes_per_row(%{rows: rows}) when rows <= 0, do: "—"
  defp bytes_per_row(%{bytes: bytes, rows: rows}), do: Float.round(bytes / rows, 1)

  defp disk_status(%{valid?: true}), do: "measured after settle"
  defp disk_status(%{valid?: false}), do: "INVALID — settle did not complete"

  defp read_tables(measured) do
    latencies(measured)
    scans(measured)
    mismatches(measured)
  end

  defp latencies(measured) do
    section("R1 — query latency, ms (cold / hot-min / hot-median)")

    say(
      "  " <>
        label("id", 5) <>
        label("tenant", 8) <>
        Enum.map_join(measured, "", fn result ->
          pad("#{short(result.arm)}:cold", 12) <>
            pad("#{short(result.arm)}:hmin", 12) <> pad("#{short(result.arm)}:hmed", 12)
        end)
    )

    for key <- read_keys(measured) do
      say(
        "  " <>
          label(elem(key, 0), 5) <>
          label(elem(key, 1), 8) <>
          Enum.map_join(measured, "", fn result ->
            entry = entry_at(result, key)

            pad(field(entry, :cold), 12) <>
              pad(field(entry, :hot_min), 12) <> pad(field(entry, :hot_med), 12)
          end)
      )
    end

    say("  cold is the run right after drop_caches; hot excludes it. They are never averaged")
    say("  together — the average of a cold and a hot run describes neither. `heavy` is the")
    say("  most populated tenant and a ceiling; `empty` is the least populated one that")
    say("  still has rows, chosen from the corpus rather than assumed.")
    say("  Phase W's scratch table is still resident — see the note under D. On ClickHouse")
    say("  its merges may still be running here; on smolquery its micro-segments are still")
    say("  in the buffer. Both inflate these latencies, and not equally.")
  end

  defp scans(measured) do
    section("R2 — what each query touched")

    say(
      "  " <>
        label("id", 5) <>
        label("tenant", 8) <>
        Enum.map_join(measured, "", fn result ->
          pad("#{short(result.arm)}:rows", 12) <>
            pad("#{short(result.arm)}:read", 14) <> pad("#{short(result.arm)}:MiB", 12)
        end)
    )

    for key <- read_keys(measured) do
      say(
        "  " <>
          label(elem(key, 0), 5) <>
          label(elem(key, 1), 8) <>
          Enum.map_join(measured, "", fn result ->
            entry = entry_at(result, key)

            pad(field(entry, :rows), 12) <>
              pad(field(entry, :rows_read), 14) <> pad(scanned_mib(entry), 12)
          end)
      )
    end

    say("  `read` is rows scanned, `MiB` bytes scanned, both as the arm reported them;")
    say("  `—` means that arm does not expose the figure, not that it read nothing.")
  end

  defp scanned_mib(nil), do: "—"
  defp scanned_mib(%{bytes_read: nil}), do: "—"
  defp scanned_mib(%{bytes_read: bytes}), do: mib(bytes)

  defp read_keys(measured) do
    measured
    |> Enum.flat_map(fn result -> Enum.map(result.read.entries, &{&1.id, &1.tenant}) end)
    |> Enum.uniq()
  end

  defp entry_at(result, {id, tenant}) do
    Enum.find(result.read.entries, &(&1.id == id and &1.tenant == tenant))
  end

  defp short(%{name: name}), do: String.slice(name, 0, 2)

  defp mismatches(measured) do
    rows =
      measured
      |> read_keys()
      |> Enum.map(fn key -> {key, row_counts(measured, key)} end)
      |> Enum.filter(&disagrees?/1)

    section("R3 — row-count agreement between arms")
    report_mismatches(measured, rows)
  end

  defp disagrees?({_key, counts}) do
    distinct = counts |> Enum.map(&elem(&1, 1)) |> Enum.uniq()

    match?([_first, _second | _rest], distinct)
  end

  defp row_counts(measured, key) do
    for result <- measured, entry = entry_at(result, key), do: {result.arm.name, entry.rows}
  end

  defp report_mismatches([_only], _rows) do
    say("  only one arm ran — there is nothing to agree or disagree with.")
  end

  defp report_mismatches(_measured, []) do
    say("  every query returned the same row count on every arm.")
  end

  defp report_mismatches(_measured, rows) do
    say("  ROW COUNT MISMATCH — THE ARMS ARE NOT BEING COMPARED, THEY ARE BEING MISMEASURED.")
    say("  A query that returns different row counts on two systems is running against")
    say("  different data, and every latency beside it is meaningless.")
    say("")

    for {{id, tenant}, counts} <- rows do
      say(
        "    #{id}/#{tenant}: " <> Enum.map_join(counts, ", ", fn {arm, n} -> "#{arm} #{n}" end)
      )
    end
  end

  defp ratio_table(measured) do
    section("R4 — ClickHouse ÷ smolquery (below 1.0 means ClickHouse is faster)")

    case {find_arm(measured, "clickhouse"), find_arm(measured, "smolquery")} do
      {nil, _sq} ->
        say("  skipped: the clickhouse arm did not run (ARMS=#{arm_names(measured)}).")

      {_ch, nil} ->
        say("  skipped: the smolquery arm did not run (ARMS=#{arm_names(measured)}).")

      {ch, sq} ->
        ratios(measured, ch, sq)
    end
  end

  defp ratios(measured, ch, sq) do
    say("  " <> label("id", 5) <> label("tenant", 8) <> pad("cold ×", 11) <> pad("hot-med ×", 11))

    for key <- read_keys(measured) do
      a = entry_at(ch, key)
      b = entry_at(sq, key)

      say(
        "  " <>
          label(elem(key, 0), 5) <>
          label(elem(key, 1), 8) <>
          pad(entry_ratio(a, b, :cold), 11) <> pad(entry_ratio(a, b, :hot_med), 11)
      )
    end
  end

  defp entry_ratio(nil, _b, _key), do: "—"
  defp entry_ratio(_a, nil, _key), do: "—"

  defp entry_ratio(a, b, key) do
    case {Map.fetch!(a, key), Map.fetch!(b, key)} do
      {left, right} when is_number(left) and is_number(right) and right > 0 ->
        Float.round(left / right, 2)

      _missing ->
        "—"
    end
  end

  defp find_arm(measured, name), do: Enum.find(measured, &(&1.arm.name == name))

  defp arm_names(measured), do: Enum.map_join(measured, ",", & &1.arm.name)

  # ── the results document ──────────────────────────────────────────────

  defp emit_results(measured, workload, variant) do
    File.mkdir_p!(Path.dirname(@results_path))
    File.write!(@results_path, document(measured, workload, variant))

    IO.puts("\nwrote #{@results_path}")
  end

  defp document(measured, workload, variant) do
    """
    # `bench/compare.exs` — smolquery vs ClickHouse

    | | |
    |---|---|
    | Run | #{Date.utc_today()} |
    | Commit | `#{git_sha()}` |
    | Command | `#{command(workload, variant)}` |
    | Machine | #{machine()} |
    | Runtime | #{runtime()} |
    | ClickHouse | #{clickhouse_version()} · config hash `#{clickhouse_config_hash()}` |
    | ClickHouse variant | `#{variant.name}` — #{variant_description(variant)} |
    | Fixture | POOL=#{workload.pool} row templates, PROJECTS=#{workload.projects} tenants |
    | Arms | #{arm_names(measured)} |

    Same rows, same sort key `(#{Enum.join(CompareSupport.clustering_key(), ", ")})`, same
    machine, one arm at a time. The driver shares the machine with whichever server it
    is measuring, on both arms.
    #{pool_caveat(workload)}
    ## Reading the write phase

    Phase W reports three kinds of number and they answer different questions.
    smolquery `:http` is what a client experiences. smolquery `:storage` is the same
    storage engine with Phoenix, the JSON decode and the per-row validator removed —
    65% of the write path by the stage profile in `bench/results/otel_logs.md`.
    **`:storage` is not like-for-like either**: ClickHouse's `JSONEachRow` parser
    validates types as it reads, so `:storage` is the lower bound on our side and
    `:http` the upper. The honest read is the pair, never one alone.

    Phase L is a separate number again — one mode, highest writer count, the bulk
    load that puts identical data on both arms so phases D and R compare the same
    corpus.

    #{render_sections()}
    #{fairness_ledger(measured, variant, workload)}
    #{what_this_settles(measured)}
    """
  end

  defp pool_caveat(%{pool: pool}) when pool >= @pool_floor, do: ""

  defp pool_caveat(%{pool: pool}) do
    """

    > **This run is a smoke test, not a measurement.** `POOL=#{pool}` row templates
    > compress far harder than real log traffic, so bytes-on-disk is fantasy and read
    > latency is flattered — on both arms, but not necessarily by the same factor.
    > Re-run at `POOL=100000` or higher before quoting anything below.
    """
  end

  defp render_sections do
    sections()
    |> Enum.reject(&(&1.lines == []))
    |> Enum.map_join("\n", fn section ->
      "## #{section.title}\n\n```\n#{Enum.join(section.lines, "\n")}\n```\n"
    end)
  end

  defp fairness_ledger(measured, variant, workload) do
    rows =
      Enum.map_join(ledger_rows(measured, variant, workload), "\n", fn {what, whom, why} ->
        "| #{what} | #{whom} | #{why} |"
      end)

    """
    ## Fairness ledger

    Everything this benchmark did *not* equalise, who it helps, and why it was left
    unequal. A comparison without this table is an advertisement.

    | what differs | which arm it favours | why it was not equalised |
    |---|---|---|
    #{rows}
    """
  end

  defp ledger_rows(measured, variant, workload) do
    [
      {"fsync on ack — ClickHouse `:default` does not fsync, smolquery always does", "ClickHouse",
       "It is what ClickHouse is out of the box. `:durable_async` is reported beside it as " <>
         "the like-for-like amortized promise, with `:durable` and `:async` as context, so " <>
         "the reader gets the spectrum rather than a choice already made for them."},
      {"client-format work — 65% of smolquery's `:http` path is JSON decode and " <>
         "per-row validation, done in Elixir; ClickHouse parses JSONEachRow in C++", "ClickHouse",
       "Not equalised because it is real: it is what a client pays. `:storage` is " <>
         "reported beside it as our lower bound, and it is *not* like-for-like either " <>
         "— ClickHouse's parser validates types as it reads, so no single number on " <>
         "either side is the answer."},
      {"`LowCardinality` column encodings", low_cardinality_favours(variant),
       "This run used `CH_VARIANT=#{variant.name}` — " <>
         "`low_cardinality: #{Keyword.fetch!(variant.opts, :low_cardinality)}`. " <>
         "A single run measures one variant; the side-by-side the contract wants " <>
         "needs one run per variant, so nothing here should be read as having " <>
         "covered both."},
      {"compression codec — smolquery writes Parquet zstd", codec_favours(variant),
       "This run used `codec: #{inspect(Keyword.fetch!(variant.opts, :codec))}`. LZ4 " <>
         "writes faster and reads bigger, zstd the reverse, so the sign of this row " <>
         "flips with the setting — and again, one run is one variant, not both."},
      {"smolquery reads its own hot tier over HTTP even on one machine", "ClickHouse",
       "Architecture rather than a benchmark artifact: the hot tier is a service so a " <>
         "query node can read a buffer node it shares no memory with. Removing the hop " <>
         "here would measure a system that does not exist."},
      {"smolquery pins a snapshot and applies a membership rule on every read", "ClickHouse",
       "It is what we pay for read consistency across seal and compaction. ClickHouse " <>
         "does not offer that guarantee, so there is nothing to equalise against — only " <>
         "a cost to name."},
      {"process model — ClickHouse is one process; smolquery is a BEAM plus a DuckDB " <>
         "thread pool plus a store", "neither, but RSS is not naively comparable",
       "The RSS columns measure the single OS process each arm names. On smolquery that " <>
         "is one of several cooperating pieces, so the figure is a floor for the " <>
         "deployment, not the whole of it."},
      {"the driver shares the machine with the server", "neither",
       "True on both arms, which is what makes it tolerable. It caps both throughput " <>
         "figures; the resource columns exclude the driver so the server's own cost " <>
         "stays readable."},
      {"the per-tenant queries use the most and least populated tenants, not a median one",
       "neither, but both are extremes",
       "`heavy` is a ceiling on per-tenant latency and `empty` a floor. Neither is a " <>
         "typical tenant, and the log-uniform skew means most tenants sit far closer to " <>
         "`empty` than to `heavy`."},
      {"the ingest format is JSON — nobody ships OTel logs as 2.1 KB of JSON per record " <>
         "at volume; real pipelines send OTLP over protobuf",
       "neither arm, but it flatters neither either",
       "Both arms are measured on a format neither is optimised for, so the write " <>
         "numbers are a floor for both. It was not equalised because smolquery's ingest " <>
         "API takes JSON and adding an OTLP path to compare against ClickHouse's " <>
         "protobuf input would be building the thing under test. It matters most to us: " <>
         "the stage profile puts 26% of our write path in the JSON decode alone, and " <>
         "ClickHouse parses its JSON in C++."},
      {"fixture cardinality is bounded by `POOL` — this run used " <>
         "#{workload.pool} row templates", "unknown, and that is the problem",
       pool_ledger_why(workload)},
      {"both arms run on local disk; smolquery's production sealed tier is an " <>
         "S3-compatible object store (MinIO)",
       "smolquery — production pays object-store " <>
         "latency this run does not measure",
       "Putting both on MinIO would make object-store round-trip latency dominate both " <>
         "arms and turn the result into a measurement of MinIO rather than of either " <>
         "engine. The object-store shape is a separate experiment with its own table."},
      {"Phase W's scratch table is still on the machine during Phase D and Phase R",
       "neither consistently — the two arms are contaminated differently",
       "There is no drop seam in the frozen `Backend` behaviour. Bytes are unaffected: " <>
         "both arms scope `disk_bytes/1` to the corpus table by name. Time is not. " <>
         "ClickHouse scopes `settle/1` to the corpus, so scratch merges run through D " <>
         "and R unwaited; smolquery's compaction sweep is global and lands inside D's " <>
         "sampler, while its seal is corpus-scoped so scratch micro-segments inflate RSS " <>
         "through R. Different contamination per arm is worse than a shared bias, and " <>
         "D's and R's cores and RSS should be read as upper bounds."},
      {"W1 and L divide by the longest writer's timed total, a lower bound on the true " <>
         "concurrent window", "ClickHouse — the residual runs against smolquery",
       "Dividing by a lower bound makes every rate an upper bound: never understated, " <>
         "possibly overstated. The gap grows with the driver work between a writer's " <>
         "batches, which is larger on the ClickHouse arm, so ClickHouse's rate is " <>
         "overstated by more than smolquery's. It was not equalised because closing it " <>
         "needs the union of every writer's timed intervals, and only durations are " <>
         "recorded, not start times. Left deliberately in this direction: an estimator " <>
         "that flatters the arm this benchmark's author does not own is the safe one."}
    ] ++ page_cache_row(measured)
  end

  defp pool_ledger_why(%{pool: pool}) when pool >= @pool_floor do
    "#{pool} distinct templates is enough that compression sees realistic entropy, but " <>
      "it is still a bounded corpus: real log traffic has effectively unbounded body " <>
      "cardinality. Bytes-on-disk and read latency are both better than a production " <>
      "corpus would give, on both arms — and not necessarily by the same factor, since " <>
      "Parquet and MergeTree do not compress repetition identically."
  end

  defp pool_ledger_why(%{pool: pool}) do
    "**#{pool} templates is below the #{@pool_floor} floor: this run is a smoke test.** " <>
      "Each template repeats often enough that bytes-on-disk is fantasy and read latency " <>
      "is flattered, on both arms but not by the same factor. Nothing in the D or R " <>
      "tables should be quoted."
  end

  defp low_cardinality_favours(%{name: "tuned"}), do: "ClickHouse"
  defp low_cardinality_favours(_variant), do: "neither — identical model"

  defp codec_favours(%{name: "tuned"}), do: "neither — both zstd"
  defp codec_favours(_variant), do: "depends: ClickHouse on write, smolquery on size"

  defp page_cache_row(measured) do
    case Enum.flat_map(measured, & &1.read.cache) do
      [] ->
        []

      notes ->
        [
          {"OS page cache could not be dropped",
           "whichever arm was warmer when its cold run started",
           "`drop_caches/1` reported: #{Enum.join(notes, "; ")}. Dropping the page cache " <>
             "needs root on Linux, which this bench does not take. The `cold` column is " <>
             "therefore an upper bound on how cold a cold run actually was."}
        ]
    end
  end

  defp what_this_settles(measured) do
    """
    ## What this settles

    - **Write throughput at the like-for-like durability promise.** Compare
      smolquery `:http` against ClickHouse `:durable_async`, not `:default` or
      unamortized `:durable`, in W1.
      _(fill in: the ratio, and whether the gap is a serialization ceiling or a
      capacity one — W4's cores columns are the tell.)_
    - **How much of that gap is ours to fix cheaply.** The `:http` to `:storage`
      distance is client-format work, not storage. _(fill in: the ratio, and
      whether closing it would change the headline at all.)_
    - **What the sort key buys per tenant.** Q2/Q3/Q5 run against the heaviest
      and the emptiest populated tenant. _(fill in: whether the empty tenant is
      cheap on both arms; if it is not cheap on ours, the clustering key is not
      pruning.)_
    - **Where we lose outright.** Q1 is metadata-only in ClickHouse and I/O for
      us; Q4 is the tenant fan-out the whole exercise exists for.
      _(fill in: the two ratios, and whether Q4 justifies the rest.)_
    - **What a row costs on disk.** _(fill in: the D table's B/row on each arm,
      and whether the difference is the codec or the layout.)_
    - **What this run cannot say.** #{unsaid(measured)}
    """
  end

  defp unsaid([only]) do
    "Only one arm ran (#{only.arm.name}), so nothing here is a comparison — these are " <>
      "one system's numbers on the comparison harness."
  end

  defp unsaid(_measured) do
    "Every row of the fairness ledger above is a claim this run did not test, and the " <>
      "ClickHouse variant row names the one model this run actually built."
  end

  # ── run header facts ──────────────────────────────────────────────────

  defp command(workload, variant) do
    knobs =
      for {name, value} <- [
            {"ARMS", System.get_env("ARMS")},
            {"CH_VARIANT", variant.name},
            {"ROWS", workload.rows},
            {"PROJECTS", workload.projects},
            {"POOL", workload.pool},
            {"BATCH", workload.batch},
            {"WRITERS", Enum.join(workload.writers, ",")},
            {"REPS", workload.reps},
            {"BURST_SECONDS", workload.burst_seconds},
            {"QUERY_REPS", workload.query_reps}
          ],
          value != nil,
          do: "#{name}=#{value}"

    Enum.join(knobs, " ") <> " mix run bench/compare.exs 2>/dev/null"
  end

  defp git_sha do
    case cmd("git", ["rev-parse", "--short", "HEAD"]) do
      nil -> "unknown"
      sha -> sha <> dirty_suffix()
    end
  end

  defp dirty_suffix do
    case cmd("git", ["status", "--porcelain"]) do
      nil -> ""
      "" -> ""
      _changes -> " (dirty working tree)"
    end
  end

  defp machine do
    cpu =
      cmd("sysctl", ["-n", "machdep.cpu.brand_string"]) || cmd("uname", ["-m"]) || "unknown CPU"

    os = cmd("uname", ["-sr"]) || "unknown OS"

    "#{cpu} · #{:erlang.system_info(:logical_processors_available)} cores · #{memory()} · #{os}"
  end

  defp memory do
    case cmd("sysctl", ["-n", "hw.memsize"]) do
      nil -> "unknown RAM"
      bytes -> gib(bytes)
    end
  end

  defp gib(bytes) do
    case Integer.parse(bytes) do
      {value, _rest} -> "#{Float.round(value / 1_073_741_824, 1)} GiB"
      :error -> "unknown RAM"
    end
  end

  defp runtime do
    "Elixir #{System.version()} / OTP #{:erlang.system_info(:otp_release)} · " <>
      "#{:erlang.system_info(:schedulers_online)} schedulers online"
  end

  defp clickhouse_version do
    System.get_env("CLICKHOUSE_VERSION") || pinned_clickhouse_version() || "unknown version"
  end

  defp pinned_clickhouse_version do
    with {:ok, text} <- File.read("scripts/clickhouse/install.sh"),
         [_line, version] <- Regex.run(~r/^CLICKHOUSE_VERSION="([^"]+)"/m, text) do
      version
    else
      _missing -> nil
    end
  end

  defp clickhouse_config_hash do
    case Enum.sort(Path.wildcard("scripts/clickhouse/config.d/**/*.xml")) do
      [] -> "no config.d found"
      files -> digest(Enum.map_join(files, "\n", &File.read!/1))
    end
  end

  defp digest(text) do
    hash = :crypto.hash(:sha256, text)

    binary_part(Base.encode16(hash, case: :lower), 0, 12)
  end

  defp cmd(executable, args) do
    case System.cmd(executable, args, stderr_to_stdout: true) do
      {output, 0} -> String.trim(output)
      _failed -> nil
    end
  rescue
    ErlangError -> nil
  end

  # ── transcript ────────────────────────────────────────────────────────
  #
  # The results document has to contain the tables verbatim. Re-deriving them
  # from the collected data would give the console and the document two
  # independent renderers, and they would drift.

  defp section(title) do
    heading(title)

    Process.put(:compare_sections, [
      %{title: title, lines: []} | Process.get(:compare_sections, [])
    ])

    :ok
  end

  defp say(line) do
    IO.puts(line)

    case Process.get(:compare_sections, []) do
      [current | rest] ->
        Process.put(:compare_sections, [%{current | lines: [line | current.lines]} | rest])

      [] ->
        :ok
    end

    :ok
  end

  defp sections do
    :compare_sections
    |> Process.get([])
    |> Enum.reverse()
    |> Enum.map(fn section -> %{section | lines: Enum.reverse(section.lines)} end)
  end

  # ── formatting ────────────────────────────────────────────────────────

  defp format(nil), do: "—"
  defp format(value), do: value

  defp ratio(_cpu, wall) when wall <= 0.0, do: 0.0
  defp ratio(cpu, wall), do: Float.round(cpu / wall, 2)

  defp minutes(seconds), do: Float.round(seconds / 60, 1)
end

Bench.Compare.main()
