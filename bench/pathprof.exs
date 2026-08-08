Code.require_file("otel_support.exs", __DIR__)

defmodule Bench.PathProf do
  @moduledoc """
  Where a batch's milliseconds go, from the socket to the fsynced Parquet file.

  Coarse on purpose: five groups, not a flame graph. The point is to find which
  group is worth opening, not to explain any one of them.

  ## The unit is a flush, not a request

  Profiling one insert measures the wrong thing. `TableBuffer` accumulates until
  `flush_max_rows` or `flush_max_bytes` trips, so the per-batch costs (decode,
  validate, two process copies) are paid N times per flush while the per-flush
  costs (frame build, Parquet encode, fsync, manifest append) are paid once.
  Any ratio between them is meaningless until both are measured over the same
  span of rows.

  So the batch is sized to the flush trigger: `size_batch/2` finds the row count
  whose validator byte estimate first reaches `flush_max_bytes`, which is what
  the buffer actually admits against. Every group below is then reported per
  that one flush, and they add up to something comparable.

  ## Groups are measured in isolation and reconciled against the whole

  Each group runs on its own, on the same rows, rather than being read off a
  single instrumented run — instrumenting the real path would need probes in
  five modules and would perturb the thing being measured. The check on that is
  `end-to-end`: one real HTTP POST of the same batch against the booted node.
  Sum-of-parts and end-to-end should land within noise of each other; when they
  do not, the residual is named and reported rather than hidden.

  Two groups cannot be isolated by calling a public function, so they are
  measured by difference:

    * frame build is `Writer.write/3` with the Parquet encode subtracted, using
      a copy of `build_frame/2`'s body — the private function it mirrors is
      pinned by `writer_frame_matches?/0`, which fails loudly if it drifts
    * manifest append is the buffer's whole flush minus everything above it

  ## Running

      mix run bench/pathprof.exs

  `REPS` (default 7) sets the reps per group; the median is reported. `PROJECTS`
  and `POOL` are `Bench.Otel`'s.
  """

  alias Explorer.DataFrame
  alias Explorer.Series
  alias Smolquery.BufferService
  alias Smolquery.IngestService
  alias Smolquery.Schema
  alias Smolquery.Segments.Store
  alias Smolquery.Segments.Writer

  @dir Path.join(System.tmp_dir!(), "smolquery-pathprof")

  def main do
    reps = env_int("REPS", 7)
    clustering = clustering()
    schema = %{Bench.Otel.table_schema() | clustering: clustering}
    pool = Bench.Otel.pool()

    IO.puts("\n#{IO.ANSI.bright()}write path profile#{IO.ANSI.reset()}")

    {batch, rows_json, body} = size_batch(pool, schema, limits())

    banner(limits(), batch, body)

    # A debug line per request is not part of the write path, and at one request
    # per group it would land inside the measurement.
    Logger.configure(level: :warning)

    req = Bench.Otel.boot!(@dir)
    Bench.Otel.create_tables!(req, 1)
    cluster_table!(req, clustering)

    try do
      groups = measure(reps, schema, pool, batch, rows_json, body, req)

      report(groups, batch)
    after
      Bench.Otel.teardown!(@dir)
    end
  end

  # The sort is part of the encode's cost, so the profile runs with the same key
  # the ClickHouse comparison writes under rather than with none — an unclustered
  # table would understate the frame group. `CLUSTERING=` measures that case.
  defp clustering do
    case System.get_env("CLUSTERING", "project_id,timestamp") do
      "" -> []
      value -> String.split(value, ",", trim: true)
    end
  end

  defp cluster_table!(_req, []), do: :ok

  defp cluster_table!(req, clustering) do
    %{status: 200} =
      Req.patch!(req,
        url: "/v1/datasets/#{Bench.Otel.dataset()}/tables/#{Bench.Otel.table()}",
        json: %{"clustering" => clustering}
      )

    :ok
  end

  # ── sizing ────────────────────────────────────────────────────────────────

  defp limits do
    config = Application.get_env(:smolquery, BufferService, [])

    %{
      max_rows: Keyword.fetch!(config, :flush_max_rows),
      max_bytes: Keyword.fetch!(config, :flush_max_bytes),
      interval_ms: Keyword.fetch!(config, :flush_interval_ms)
    }
  end

  # The buffer admits against the validator's byte estimate, so the batch is
  # sized against the same number rather than against the JSON body's size or
  # the rows' `:erlang.external_size/1`. Doubling then bisecting costs a dozen
  # validate passes and lands exactly on the trigger.
  #
  # `ROWS` pins the count instead, for comparing two versions of the write path
  # whose byte estimates differ — the column-major batch prices out roughly half
  # of what the row-major one did, so letting each find its own trigger would
  # compare different batches. Pinning the rows moves the trigger down to match,
  # which keeps every group measuring the same work.
  defp size_batch(pool, schema, limits) do
    count =
      case env_int("ROWS", nil) do
        nil -> grow(pool, schema, limits, 1_000)
        pinned -> pin(pool, schema, pinned)
      end

    rows = Bench.Otel.rows(pool, count, 0)

    {count, rows, JSON.encode!(%{"rows" => rows})}
  end

  defp pin(pool, schema, count) do
    merge_env(Smolquery.BufferService, flush_max_bytes: bytes(pool, schema, count))

    count
  end

  defp merge_env(key, opts) do
    Application.put_env(
      :smolquery,
      key,
      Keyword.merge(Application.get_env(:smolquery, key, []), opts)
    )
  end

  defp grow(pool, schema, limits, count) do
    cond do
      count >= limits.max_rows ->
        limits.max_rows

      bytes(pool, schema, count) >= limits.max_bytes ->
        bisect(pool, schema, limits, div(count, 2), count)

      true ->
        grow(pool, schema, limits, count * 2)
    end
  end

  defp bisect(_pool, _schema, _limits, low, high) when high - low <= 16, do: high

  defp bisect(pool, schema, limits, low, high) do
    mid = div(low + high, 2)

    if bytes(pool, schema, mid) >= limits.max_bytes do
      bisect(pool, schema, limits, low, mid)
    else
      bisect(pool, schema, limits, mid, high)
    end
  end

  defp bytes(pool, schema, count) do
    {batch, []} = IngestService.Validator.validate(schema, Bench.Otel.rows(pool, count, 0))

    batch.byte_size
  end

  # ── the groups ────────────────────────────────────────────────────────────

  defp measure(reps, schema, pool, _batch, rows_json, body, req) do
    check_frame_copy!(schema, pool)

    # Same byte count as `body` — "rowz" is as long as "rows" — so the edge arm
    # reads and decodes exactly what the real one does.
    edge_body = String.replace(body, ~s("rows":), ~s("rowz":), global: false)
    ^body = String.replace(edge_body, ~s("rowz":), ~s("rows":), global: false)

    decode = time(reps, fn -> JSON.decode!(body) end)

    {accepted, []} = IngestService.Validator.validate(schema, rows_json)

    validate = time(reps, fn -> IngestService.Validator.validate(schema, rows_json) end)

    copy = time(reps, fn -> round_trip(accepted.columns) end)

    frame = time(reps, fn -> build_frame(accepted.columns, schema) end)

    store = Store.Local.new(dir: Path.join(@dir, "prof"), fsync: true)
    File.mkdir_p!(Path.join(@dir, "prof"))

    write =
      time(reps, fn ->
        {:ok, _segment} = Writer.write({:columns, accepted.columns}, schema, store: store)
      end)

    flush = time(reps, fn -> write_batch(schema, accepted) end)

    # The same client, same parsers, a body of the same size and shape — but with
    # the array under a key the controller does not accept, so `rows/1` answers
    # 400 before anything downstream of it runs. That is the HTTP edge plus the
    # decode and nothing else, measured rather than inferred.
    #
    # Routing at a *missing table* was the obvious way to do this and it was
    # wrong: `SchemaCache` caches successes only, so an unknown table misses on
    # every rep and each one paid a catalog query. The edge came out overstated
    # and the residual understated by the same amount.
    edge = time(reps, fn -> post(req, edge_body, Bench.Otel.table(), 400) end)

    # The same three server-side steps, in a process that is born for them and
    # dies after — which is what a request is. The groups above run in this
    # long-lived driver, whose heap is already grown and already collected, so
    # they cannot see what a fresh 6 MiB heap costs to grow and collect.
    fresh = time(reps, fn -> in_fresh_process(fn -> pipeline(schema, body) end) end)

    e2e = time(reps, fn -> post(req, body, Bench.Otel.table(), 200) end)

    %{
      http: edge - decode,
      heap: fresh - decode - validate - flush,
      decode: decode,
      validate: validate,
      copy: copy,
      frame: frame,
      # `Writer.write/3` is frame build plus the Parquet encode and the store's
      # fsync-and-rename; the frame build is the only part of it measured above.
      encode: write - frame,
      # One `write_batch` is the two process copies, the frame build, the encode,
      # and the manifest append plus its own fsync. Everything but the last is
      # already accounted for, so the manifest is what is left.
      manifest: flush - copy - write,
      total: e2e,
      # What the groups do not explain. `decode` is in both `edge` and `fresh`,
      # so it is added back once. Every term here was measured on its own, so
      # this is a real residual and not an identity — a large one means a group
      # is wrong, or the isolated runs are not paying what the real one does.
      residual: e2e - edge - fresh + decode
    }
  end

  defp pipeline(schema, body) do
    {accepted, _errors} = IngestService.Validator.validate(schema, JSON.decode!(body)["rows"])

    write_batch(schema, accepted)
  end

  defp in_fresh_process(fun) do
    caller = self()

    spawn(fn ->
      fun.()

      send(caller, :done)
    end)

    receive do: (:done -> :ok)
  end

  # The write path deep-copies the batch twice: the ingest process sends it to
  # the table's buffer, and the buffer hands it to the committer. Both legs are
  # a full copy of the term into another heap, so both are walked here.
  defp round_trip(rows) do
    caller = self()

    second = spawn(fn -> receive do: (term -> send(caller, {:done, length(term)})) end)
    first = spawn(fn -> receive do: (term -> send(second, term)) end)

    send(first, rows)

    receive do: ({:done, _count} -> :ok)
  end

  defp write_batch(schema, accepted) do
    {:ok, _ack} =
      BufferService.Client.write_batch(
        BufferService,
        {Bench.Otel.dataset(), Bench.Otel.table()},
        Map.put(accepted, :schema, schema)
      )
  end

  defp post(req, body, table, status) do
    %{status: ^status} =
      Req.post!(req,
        url: "/v1/datasets/#{Bench.Otel.dataset()}/tables/#{table}/insert",
        headers: [{"content-type", "application/json"}],
        body: body,
        decode_body: false
      )
  end

  # ── the copy of build_frame/2, and its guard ──────────────────────────────

  defp build_frame(values, schema) do
    {:ok, dtypes} = Schema.explorer_dtypes(schema)

    columns =
      dtypes
      |> Enum.zip(values)
      |> Enum.map(fn {{name, dtype}, column} ->
        {name, Series.from_list(column, dtype: dtype)}
      end)

    case Schema.clustering_columns(schema) do
      [] ->
        DataFrame.new(columns)

      key ->
        DataFrame.sort_with(DataFrame.new(columns), fn lf -> Enum.map(key, &lf[&1]) end,
          stable: true,
          nils: :last
        )
    end
  end

  # The copy above duplicates a private function. If `Writer` changes how a
  # frame is built, the subtraction that isolates the Parquet encode stops
  # meaning anything — so prove the two agree on a small batch before trusting
  # it on a large one.
  defp check_frame_copy!(schema, pool) do
    {accepted, []} = IngestService.Validator.validate(schema, Bench.Otel.rows(pool, 64, 0))

    dir = Path.join(@dir, "guard")
    File.mkdir_p!(dir)

    {:ok, segment} =
      Writer.write({:columns, accepted.columns}, schema,
        store: Store.Local.new(dir: dir, fsync: false)
      )

    frame = build_frame(accepted.columns, schema)
    key = Schema.clustering_columns(schema)

    checks = [
      {"row count", DataFrame.n_rows(frame), segment.row_count},
      {"columns", DataFrame.names(frame), Schema.names(schema)},
      {"timestamp bounds", bounds(frame, "timestamp"), segment_bounds(segment, "timestamp")},
      {"sorted on the clustering key", sorted_on(frame, key), true}
    ]

    for {what, got, want} <- checks, got != want do
      raise """
      build_frame/2's copy no longer matches Writer.write/3 — #{what}
        copy   #{inspect(got, limit: 8)}
        writer #{inspect(want, limit: 8)}
      Fix the copy in this file; the encode group is measured by subtracting it.
      """
    end

    :ok
  end

  # Only orderable columns carry bounds — `Writer.stats/2` leaves strings `nil` —
  # so the check runs on the timestamp, which the fixture always has.
  defp bounds(frame, column),
    do: {Series.min(frame[column]), Series.max(frame[column])}

  defp segment_bounds(segment, column),
    do: {segment.stats[column].min, segment.stats[column].max}

  defp sorted_on(_frame, []), do: true

  defp sorted_on(frame, [column | _rest]) do
    values = Series.to_list(frame[column])

    values == Enum.sort(values)
  end

  # ── measurement and output ────────────────────────────────────────────────

  defp time(reps, fun) do
    fun.()

    1..reps
    |> Enum.map(fn _rep -> elem(:timer.tc(fun), 0) / 1_000 end)
    |> Enum.sort()
    |> Enum.at(div(reps, 2))
  end

  defp banner(limits, batch, body) do
    IO.puts("""

      flush trigger  #{limits.max_rows} rows or #{fmt_bytes(limits.max_bytes)} or #{limits.interval_ms} ms
      batch          #{batch} rows — what #{fmt_bytes(limits.max_bytes)} of validator bytes buys
      wire body      #{fmt_bytes(byte_size(body))} of JSON
    """)
  end

  defp report(groups, batch) do
    rows = [
      {"HTTP edge (socket, Bandit, Plug)", groups.http},
      {"JSON decode", groups.decode},
      {"validate + coerce", groups.validate},
      {"process copies (ingest→buffer→committer)", groups.copy},
      {"frame build (maps → Arrow, sort)", groups.frame},
      {"Parquet encode + store fsync", groups.encode},
      {"hot manifest append + fsync", groups.manifest},
      {"fresh-process heap growth + GC", groups.heap},
      {"unexplained residual", groups.residual}
    ]

    IO.puts("  #{IO.ANSI.bright()}per flush of #{batch} rows#{IO.ANSI.reset()}\n")

    for {label, ms} <- rows do
      IO.puts(
        "    #{pad(label)}  #{bar(ms, groups.total)}  #{fmt_ms(ms)}  #{pct(ms, groups.total)}"
      )
    end

    IO.puts("""

      #{pad("end-to-end (one real POST)")}  #{fmt_ms(groups.total)}
      #{pad("rows/s, one writer, this batch")}  #{round(batch / (groups.total / 1_000))}

    The HTTP edge includes this driver's own `Req` encode and send of the body;
    the server pays less than the number shown. Every other group is server-side.
    """)
  end

  defp bar(ms, total) when total > 0 do
    filled = ms |> Kernel./(total) |> Kernel.*(28) |> round() |> max(0) |> min(28)

    String.duplicate("█", filled) <> String.duplicate("░", 28 - filled)
  end

  defp bar(_ms, _total), do: String.duplicate("░", 28)

  defp pad(label), do: String.pad_trailing(label, 42)

  defp pct(ms, total) when total > 0,
    do: "#{:erlang.float_to_binary(ms / total * 100, decimals: 1)}%"

  defp pct(_ms, _total), do: "-"

  defp fmt_ms(ms), do: String.pad_leading(:erlang.float_to_binary(ms, decimals: 1) <> " ms", 9)

  defp fmt_bytes(bytes) when bytes >= 1_048_576,
    do: "#{:erlang.float_to_binary(bytes / 1_048_576, decimals: 1)} MiB"

  defp fmt_bytes(bytes), do: "#{:erlang.float_to_binary(bytes / 1024, decimals: 1)} KiB"

  defp env_int(name, default) do
    case System.get_env(name) do
      nil -> default
      value -> String.to_integer(value)
    end
  end
end

Bench.PathProf.main()
