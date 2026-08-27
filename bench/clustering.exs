Code.require_file("support.exs", __DIR__)

defmodule Bench.Clustering do
  @moduledoc """
  Does the ORDER BY analog work, and what does it cost.

  Three questions against the same row stream on two tables — `clustered`
  (clustering `[project_id, ts]`) vs `plain` (clustering `[]`):

    * **A. Correctness + read effect** — flushed and sealed segments are
      (project_id, ts) nondecreasing with nulls last; hot and sealed scans
      under `WHERE project_id = ?` report latency and whatever EXPLAIN ANALYZE
      (or parquet row-group metadata) can honestly say about skipped work.
    * **B. Write-path cost** — flush rows/s and p50/p95 through the buffer
      commit path, then seal merge ms / rows/s / sealed bytes, both arms.
    * **C. RAM cost** — BEAM heap delta on the flush sort (`Writer.write`, the
      process that actually sorts), and OS RSS of the whole BEAM around the
      seal merge (DuckDB's ORDER BY lives off-heap).

      mix run bench/clustering.exs
      P=1000 R=500000 MICRO=16 BATCH=10000 mix run bench/clustering.exs

  Numbers are on OTP 28.3 when run with the supervisor's ASDF pins; they are
  not comparable to published OTP 29 results.
  """

  import Bench.Support

  alias Explorer.DataFrame
  alias Explorer.Series
  alias Smolquery.BufferService
  alias Smolquery.BufferService.Client
  alias Smolquery.BufferService.HotManifest
  alias Smolquery.BufferService.HotServer
  alias Smolquery.Catalog
  alias Smolquery.Engine
  alias Smolquery.Schema
  alias Smolquery.Segments.Id
  alias Smolquery.Segments.Store
  alias Smolquery.Segments.Writer
  alias Smolquery.Test.SegmentFixture
  alias Smolquery.StorageService
  alias Smolquery.StorageService.HotTier
  alias Smolquery.StorageService.Merge
  alias Smolquery.StorageService.Runtime

  @dataset "analytics"
  @clustered {@dataset, "clustered"}
  @plain {@dataset, "plain"}

  def run do
    projects = env("P", 1_000)
    rows = env("R", 500_000)
    micro = env("MICRO", 16)
    batch = env("BATCH", 10_000)
    reps = env("REPS", 7)

    schedulers()

    IO.puts("\n  fixture: P=#{projects} projects (zipf-ish), R=#{rows} rows,")
    IO.puts("  MICRO=#{micro} segments, BATCH=#{batch}, payload ~200B/row")
    IO.puts("  memory_limit=#{engine_memory_limit()}")

    with_tmp_dir("clustering", fn dir ->
      stream = row_stream(projects, rows)
      target = pick_project(stream)
      per_segment = max(div(length(stream), micro), 1)

      correctness(dir, stream)
      heap = flush_heap(dir, stream, max(batch, 50_000))
      filled = fill_and_flush_cost(dir, stream, micro, per_segment)
      hot = hot_pruning(filled, target, reps)
      sealed = seal_and_costs(filled, target, reps)
      stop_stack(filled.stack)

      summarize(heap, filled.flush, hot, sealed)
    end)
  end

  defp correctness(dir, stream) do
    heading("A1. correctness: flushed + sealed rows are (project_id, ts) sorted")

    stack = start_stack(dir, "correctness", flush_max_rows: 250)
    schema = clustered_schema()
    :ok = Catalog.put_clustering(stack.catalog, @clustered, ["project_id", "ts"])

    sample =
      stream
      |> Enum.take(200)
      |> Kernel.++([
        %{"project_id" => nil, "ts" => ~N[2026-06-01 00:00:00], "payload" => null_payload()},
        %{"project_id" => 1, "ts" => nil, "payload" => null_payload()},
        %{"project_id" => 1, "ts" => ~N[2026-02-01 00:00:00], "payload" => null_payload()},
        %{"project_id" => 1, "ts" => ~N[2026-01-31 23:59:59.500000], "payload" => null_payload()},
        %{"project_id" => 1, "ts" => ~N[2026-01-31 23:59:59.000001], "payload" => null_payload()}
      ])
      |> Enum.shuffle()

    {:ok, ack} = Client.write_batch(stack.buffer, @clustered, %{schema: schema, rows: sample})
    :ok = Client.flush(stack.buffer, @clustered)
    {:ok, [entry]} = Client.hot_manifest(stack.buffer, @clustered)

    micro_path = Store.location(stack.buffer_runtime.store, entry.key)
    assert_sorted!(micro_path, "flushed micro-segment #{ack.segment_id}")

    claim = freeze(stack, @clustered, [ack.segment_id])
    {:ok, segment} = merge(stack.runtime, @clustered, claim)
    sealed_path = Store.location(stack.runtime.store, segment.key)
    assert_sorted!(sealed_path, "sealed segment #{segment.id}")

    IO.puts("  PASS  flushed micro-segment is (project_id, ts) nondecreasing, nulls last")
    IO.puts("  PASS  sealed segment is (project_id, ts) nondecreasing, nulls last")

    stop_stack(stack)
    :ok
  end

  defp flush_heap(dir, stream, batch) do
    heading("C. RAM — BEAM heap delta on Writer.write (the flush sort)")

    store_dir = Path.join(dir, "heap-store")
    File.mkdir_p!(store_dir)
    store = Store.Local.new(dir: store_dir)
    rows = Enum.take(stream, batch)

    plain = measure_writer_heap(rows, plain_schema(), store)
    clustered = measure_writer_heap(rows, clustered_schema(), store)

    IO.puts("\n  arm            rows   peak heap Δ (MiB)")

    IO.puts(
      "  #{label("plain", 14)} #{pad(batch, 6)}  #{pad(Float.round(plain / 1_048_576, 2), 16)}"
    )

    IO.puts(
      "  #{label("clustered", 14)} #{pad(batch, 6)}  #{pad(Float.round(clustered / 1_048_576, 2), 16)}"
    )

    delta_pct =
      if plain > 0 do
        Float.round((clustered - plain) / plain * 100, 1)
      else
        :undefined
      end

    IO.puts("\n  clustered heap over plain: #{format_pct(delta_pct)}")
    IO.puts("  (measured in the calling process — Writer sorts here, not in DuckDB)")

    %{plain: plain, clustered: clustered, delta_pct: delta_pct, batch: batch}
  end

  defp fill_and_flush_cost(dir, stream, micro, per_segment) do
    heading("B. write-path cost — flush through buffer commit")

    stack = start_stack(dir, "main", flush_max_rows: per_segment)
    :ok = Catalog.put_clustering(stack.catalog, @clustered, ["project_id", "ts"])

    chunks = segment_chunks(stream, micro, per_segment)
    IO.puts("\n  #{micro} micro-segments × #{per_segment} rows (one write_batch = one flush)")

    plain = flush_arm(stack, @plain, plain_schema(), chunks)
    clustered = flush_arm(stack, @clustered, clustered_schema(), chunks)

    print_flush_table(plain, clustered)

    %{stack: stack, chunks: chunks, flush: %{plain: plain, clustered: clustered}}
  end

  defp hot_pruning(%{stack: stack}, target, reps) do
    heading("A2. pruning effect — hot tier (N unsealed micro-segments)")

    plain = hot_arm(stack, @plain, target, reps)
    clustered = hot_arm(stack, @clustered, target, reps)

    IO.puts("\n  predicate: WHERE project_id = #{target}")
    IO.puts("  arm         files   match RG   rows ret   p50 ms")

    IO.puts(
      "  #{label("plain", 10)}  #{pad(plain.files, 5)}  #{pad(plain.match_rg, 8)}  " <>
        "#{pad(plain.rows, 8)}  #{pad(plain.p50, 7)}"
    )

    IO.puts(
      "  #{label("clustered", 10)}  #{pad(clustered.files, 5)}  #{pad(clustered.match_rg, 8)}  " <>
        "#{pad(clustered.rows, 8)}  #{pad(clustered.p50, 7)}"
    )

    note_explain(plain, clustered)
    %{plain: plain, clustered: clustered, target: target}
  end

  defp seal_and_costs(%{stack: stack, flush: flush}, target, reps) do
    heading("B+C. seal merge cost + OS RSS; A3 sealed pruning")

    plain_ids = flush.plain.ids
    clustered_ids = flush.clustered.ids

    plain_seal = seal_arm(stack, @plain, plain_ids)
    clustered_seal = seal_arm(stack, @clustered, clustered_ids)

    IO.puts("\n  arm         merge ms     rows/s   sealed MiB   RSS Δ MiB   RSS peak MiB")

    IO.puts(
      "  #{label("plain", 10)}  #{pad(plain_seal.merge_ms, 8)}  " <>
        "#{pad(plain_seal.rows_s, 10)}  #{pad(mib(plain_seal.bytes), 10)}  " <>
        "#{pad(plain_seal.rss_delta_mib, 10)}  #{pad(plain_seal.rss_peak_mib, 12)}"
    )

    IO.puts(
      "  #{label("clustered", 10)}  #{pad(clustered_seal.merge_ms, 8)}  " <>
        "#{pad(clustered_seal.rows_s, 10)}  #{pad(mib(clustered_seal.bytes), 10)}  " <>
        "#{pad(clustered_seal.rss_delta_mib, 10)}  #{pad(clustered_seal.rss_peak_mib, 12)}"
    )

    byte_ratio =
      Float.round(clustered_seal.bytes / max(plain_seal.bytes, 1), 3)

    IO.puts("\n  sealed bytes clustered/plain = #{byte_ratio}")
    IO.puts("  memory_limit in effect: #{engine_memory_limit()}")

    plain_q = sealed_queries(stack, plain_seal.path, target, reps, plain_seal.row_groups)

    clustered_q =
      sealed_queries(stack, clustered_seal.path, target, reps, clustered_seal.row_groups)

    IO.puts("\n  sealed WHERE project_id = #{target} (select payload — forces page reads)")
    IO.puts("  arm         row groups   match RG   explain rows   p50 ms   last100 p50")

    IO.puts(
      "  #{label("plain", 10)}  #{pad(plain_q.row_groups, 10)}  #{pad(plain_q.match_rg, 8)}  " <>
        "#{pad(plain_q.explain_rows, 12)}  #{pad(plain_q.p50, 7)}  #{pad(plain_q.last100_p50, 11)}"
    )

    IO.puts(
      "  #{label("clustered", 10)}  #{pad(clustered_q.row_groups, 10)}  #{pad(clustered_q.match_rg, 8)}  " <>
        "#{pad(clustered_q.explain_rows, 12)}  #{pad(clustered_q.p50, 7)}  #{pad(clustered_q.last100_p50, 11)}"
    )

    IO.puts("\n  NOTE: EXPLAIN ANALYZE does not expose rows-scanned-before-prune;")
    IO.puts("  'explain rows' is the TABLE_SCAN output cardinality (post-filter).")
    IO.puts("  match RG is from parquet_metadata min/max (pruning potential).")

    read_win = read_win(plain_q, clustered_q)

    %{
      plain_seal: plain_seal,
      clustered_seal: clustered_seal,
      byte_ratio: byte_ratio,
      plain_q: plain_q,
      clustered_q: clustered_q,
      read_win: read_win,
      target: target
    }
  end

  defp summarize(heap, flush, hot, sealed) do
    heading("headline numbers")

    write_pct =
      if flush.plain.rows_s > 0 do
        Float.round((flush.plain.rows_s - flush.clustered.rows_s) / flush.plain.rows_s * 100, 1)
      else
        0.0
      end

    correctness = "PASS"

    rss_peak_delta =
      Float.round(sealed.clustered_seal.rss_peak_mib - sealed.plain_seal.rss_peak_mib, 1)

    IO.puts("  correctness:          #{correctness}")
    IO.puts("  write cost:           #{write_pct}% slower flush rows/s (clustered vs plain)")

    IO.puts(
      "  RAM BEAM:             #{format_pct(heap.delta_pct)} over plain " <>
        "(#{Float.round(heap.clustered / 1_048_576, 2)} vs #{Float.round(heap.plain / 1_048_576, 2)} MiB)"
    )

    IO.puts(
      "  RAM RSS Δ:            clustered peak #{sealed.clustered_seal.rss_peak_mib} MiB vs plain #{sealed.plain_seal.rss_peak_mib} MiB (Δ #{rss_peak_delta} MiB)"
    )

    IO.puts("  read win:             #{sealed.read_win}× (sealed filter p50, plain/clustered)")

    %{
      correctness: correctness,
      write_pct: write_pct,
      beam_pct: heap.delta_pct,
      rss_delta_mib: rss_peak_delta,
      read_win: sealed.read_win,
      hot: hot,
      sealed: sealed,
      flush: flush,
      heap: heap
    }
  end

  # --- arms -----------------------------------------------------------------

  defp flush_arm(stack, table, schema, chunks) do
    {total_us, ids, samples} =
      Enum.reduce(chunks, {0, [], []}, fn rows, {acc_us, ids, samples} ->
        {us, {:ok, ack}} =
          :timer.tc(fn ->
            Client.write_batch(stack.buffer, table, %{schema: schema, rows: rows})
          end)

        {acc_us + us, [ack.segment_id | ids], [us | samples]}
      end)

    :ok = Client.flush(stack.buffer, table)
    ids = Enum.reverse(ids)
    total_rows = Enum.sum_by(chunks, &length/1)
    pct = percentiles(samples)

    %{
      ids: ids,
      total_rows: total_rows,
      total_us: total_us,
      rows_s: rows_per_second(total_rows, total_us),
      p50: pct.p50,
      p95: pct.p95,
      flushes: length(ids)
    }
  end

  defp hot_arm(stack, table, target, reps) do
    {:ok, entries} = HotTier.manifest(stack.runtime, table)
    urls = Enum.map(entries, & &1["url"])
    engine = Runtime.engine(stack.storage)

    sql = """
    SELECT count(*) FROM read_parquet([#{placeholders(length(urls))}], union_by_name := true)
    WHERE project_id = $#{length(urls) + 1}
    """

    params = urls ++ [target]
    warm_query(engine, sql, params)

    times =
      for _ <- 1..reps do
        {us, {:ok, result}} = :timer.tc(fn -> Engine.query(engine, sql, params) end)
        {us, result}
      end

    rows = times |> hd() |> elem(1) |> then(&hd(hd(&1.rows)))
    p50 = times |> Enum.map(&elem(&1, 0)) |> Enum.sort() |> then(&ms(Enum.at(&1, div(reps, 2))))

    files = explain_files(engine, sql, params)
    explain_rows = explain_scan_rows(engine, sql, params)
    match_rg = matching_row_groups_over_urls(engine, urls, target)

    %{
      files: files,
      match_rg: match_rg,
      rows: rows,
      p50: p50,
      explain_rows: explain_rows,
      urls: length(urls)
    }
  end

  defp seal_arm(stack, table, ids) do
    claim = freeze(stack, table, ids)
    id_set = MapSet.new(ids)

    {:ok, entries} = Client.hot_manifest(stack.buffer, table)

    total_rows =
      entries |> Enum.filter(&MapSet.member?(id_set, &1.id)) |> Enum.sum_by(& &1.row_count)

    {us, {:ok, segment}, rss} =
      with_rss_peak(fn -> merge(stack.runtime, table, claim) end)

    path = Store.location(stack.runtime.store, segment.key)
    row_groups = row_group_count(stack, path)

    %{
      merge_ms: ms(us),
      rows_s: rows_per_second(total_rows, us),
      bytes: segment.byte_size,
      row_count: segment.row_count,
      path: path,
      row_groups: row_groups,
      rss_delta_mib: rss.delta_mib,
      rss_peak_mib: rss.peak_mib,
      total_rows: total_rows
    }
  end

  defp sealed_queries(stack, path, target, reps, row_groups) do
    engine = Runtime.engine(stack.storage)

    filter_sql = """
    SELECT project_id, ts, payload FROM read_parquet($1)
    WHERE project_id = $2
    """

    last100_sql = """
    SELECT project_id, ts, payload FROM read_parquet($1)
    WHERE project_id = $2
    ORDER BY ts DESC LIMIT 100
    """

    warm_query(engine, filter_sql, [path, target])
    warm_query(engine, last100_sql, [path, target])

    filter_times =
      for _ <- 1..reps do
        {us, _} = :timer.tc(fn -> Engine.query(engine, filter_sql, [path, target]) end)
        us
      end

    last_times =
      for _ <- 1..reps do
        {us, _} = :timer.tc(fn -> Engine.query(engine, last100_sql, [path, target]) end)
        us
      end

    explain_rows = explain_scan_rows(engine, filter_sql, [path, target])
    match_rg = matching_row_groups(engine, path, target)

    %{
      row_groups: row_groups,
      match_rg: match_rg,
      explain_rows: explain_rows,
      p50: ms(Enum.at(Enum.sort(filter_times), div(reps, 2))),
      last100_p50: ms(Enum.at(Enum.sort(last_times), div(reps, 2)))
    }
  end

  # --- helpers --------------------------------------------------------------

  defp assert_sorted!(path, label) do
    frame = DataFrame.from_parquet!(path)
    projects = Series.to_list(frame["project_id"])
    stamps = Series.to_list(frame["ts"])

    pairs = Enum.zip(projects, stamps)

    ordered? =
      pairs
      |> Enum.chunk_every(2, 1, :discard)
      |> Enum.all?(fn [{p1, t1}, {p2, t2}] ->
        cluster_le({p1, t1}, {p2, t2})
      end)

    unless ordered? do
      raise "CLUSTERING BENCH FAIL: #{label} is not (project_id, ts) sorted with nulls last"
    end

    :ok
  end

  defp cluster_le({p1, t1}, {p2, t2}) do
    cond do
      p1 == p2 -> ts_le(t1, t2)
      true -> cluster_key(p1) <= cluster_key(p2)
    end
  end

  defp cluster_key(nil), do: {1, nil}
  defp cluster_key(value), do: {0, value}

  defp ts_le(_earlier, nil), do: true
  defp ts_le(nil, _later), do: false
  defp ts_le(t1, t2), do: NaiveDateTime.compare(t1, t2) != :gt

  defp measure_writer_heap(rows, schema, store) do
    shuffled = Enum.shuffle(rows)
    :erlang.garbage_collect()
    parent = self()
    before = process_memory_of(parent)
    sampler = spawn_link(fn -> beam_sample_loop(parent, before, before) end)
    {:ok, _segment} = SegmentFixture.write(shuffled, schema, store: store)
    send(sampler, :stop)

    peak =
      receive do
        {:beam_peak, bytes} -> bytes
      after
        2_000 -> process_memory_of(parent)
      end

    max(peak - before, 0)
  end

  defp beam_sample_loop(target, _baseline, peak) do
    receive do
      :stop -> send(target, {:beam_peak, peak})
    after
      1 ->
        now = process_memory_of(target)
        beam_sample_loop(target, now, max(peak, now))
    end
  end

  defp process_memory_of(pid) do
    case Process.info(pid, :memory) do
      {:memory, bytes} -> bytes
      nil -> 0
    end
  end

  defp merge(runtime, table_ref, claim) do
    {:ok, entries} = HotTier.manifest(runtime, table_ref, nil, ids: claim.ids, stats: false)

    Merge.run(runtime, table_ref, claim, entries)
  end

  defp with_rss_peak(fun) do
    baseline = rss_kib()
    parent = self()
    sampler = spawn_link(fn -> rss_sample_loop(parent, baseline, baseline) end)

    {us, result} = :timer.tc(fun)
    send(sampler, :stop)

    peak =
      receive do
        {:rss_peak, kib} -> kib
      after
        2_000 -> max(rss_kib(), baseline)
      end

    {us, result,
     %{
       baseline_mib: Float.round(baseline / 1024, 1),
       peak_mib: Float.round(peak / 1024, 1),
       delta_mib: Float.round((peak - baseline) / 1024, 1)
     }}
  end

  defp rss_sample_loop(parent, baseline, peak) do
    receive do
      :stop -> send(parent, {:rss_peak, peak})
    after
      5 ->
        now = rss_kib()
        rss_sample_loop(parent, baseline, max(peak, now))
    end
  end

  defp rss_kib do
    {output, 0} = System.cmd("ps", ["-o", "rss=", "-p", System.pid()])
    output |> String.trim() |> String.to_integer()
  end

  defp row_stream(projects, rows) do
    seed = env("SEED", 42)
    :rand.seed(:exsss, {seed, seed * 2 + 1, seed * 3 + 2})

    for i <- 1..rows do
      u = :rand.uniform()
      project_id = min(projects - 1, trunc(:math.pow(u, 2) * projects))

      %{
        "project_id" => project_id,
        "ts" => NaiveDateTime.add(~N[2026-01-31 22:00:00], i * 1_000_001, :microsecond),
        "payload" => payload(i)
      }
    end
  end

  defp payload(i) do
    # ~180 printable bytes + small suffix ≈ 200B/row with column overhead
    String.duplicate("x", 180) <> Integer.to_string(rem(i, 10_000))
  end

  defp null_payload, do: String.duplicate("n", 180)

  defp segment_chunks(stream, micro, per_segment) do
    stream
    |> Enum.take(per_segment * micro)
    |> Enum.chunk_every(per_segment)
    |> Enum.take(micro)
    |> Enum.map(&Enum.shuffle/1)
  end

  defp pick_project(stream) do
    freqs =
      Enum.frequencies_by(Enum.take(stream, min(length(stream), 50_000)), & &1["project_id"])

    {project, _n} = Enum.max_by(freqs, fn {_k, v} -> v end)
    project
  end

  defp print_flush_table(plain, clustered) do
    IO.puts("\n  arm         flushes     rows      rows/s    p50 ms    p95 ms")

    IO.puts(
      "  #{label("plain", 10)}  #{pad(plain.flushes, 7)}  #{pad(plain.total_rows, 8)}  " <>
        "#{pad(plain.rows_s, 10)}  #{pad(plain.p50, 8)}  #{pad(plain.p95, 8)}"
    )

    IO.puts(
      "  #{label("clustered", 10)}  #{pad(clustered.flushes, 7)}  #{pad(clustered.total_rows, 8)}  " <>
        "#{pad(clustered.rows_s, 10)}  #{pad(clustered.p50, 8)}  #{pad(clustered.p95, 8)}"
    )

    if plain.rows_s > 0 do
      pct = Float.round((plain.rows_s - clustered.rows_s) / plain.rows_s * 100, 1)
      IO.puts("\n  flush rows/s delta (clustered vs plain): #{pct}%")
    end
  end

  defp note_explain(plain, clustered) do
    IO.puts("\n  files = EXPLAIN ANALYZE 'Total Files Read' (file-level; same data ⇒ same")
    IO.puts("  min/max per micro-segment, so both arms usually read every file).")
    IO.puts("  match RG = row groups whose project_id stats could contain the predicate.")

    if plain.files == clustered.files do
      IO.puts("  Hot file counts match — sorting inside a segment does not tighten file stats.")
    end
  end

  defp read_win(plain_q, clustered_q) do
    cond do
      clustered_q.p50 > 0 -> Float.round(plain_q.p50 / clustered_q.p50, 2)
      true -> 1.0
    end
  end

  defp explain_files(engine, sql, params) do
    case files_read(engine, sql, params) do
      n when is_integer(n) -> n
      _ -> :na
    end
  end

  defp explain_scan_rows(engine, sql, params) do
    case Engine.query(engine, "EXPLAIN ANALYZE " <> sql, params) do
      {:ok, result} ->
        text = result.rows |> List.flatten() |> Enum.map_join("\n", &to_string/1)

        case Regex.scan(~r/([\d,]+)\s+rows/, text) do
          matches when matches != [] ->
            matches
            |> List.last()
            |> Enum.at(1)
            |> String.replace(",", "")
            |> String.to_integer()

          [] ->
            :na
        end

      {:error, _} ->
        :na
    end
  end

  defp matching_row_groups(engine, path, target) do
    sql = """
    SELECT count(*) FROM parquet_metadata($1)
    WHERE path_in_schema = 'project_id'
      AND CAST(stats_min_value AS BIGINT) <= $2
      AND CAST(stats_max_value AS BIGINT) >= $2
    """

    case Engine.query(engine, sql, [path, target]) do
      {:ok, result} -> hd(hd(result.rows))
      {:error, _} -> :na
    end
  end

  defp matching_row_groups_over_urls(engine, urls, target) do
    Enum.reduce(urls, 0, fn url, acc ->
      case matching_row_groups(engine, url, target) do
        n when is_integer(n) -> acc + n
        _ -> acc
      end
    end)
  end

  defp row_group_count(stack, path) do
    engine = Runtime.engine(stack.storage)

    case Engine.query(
           engine,
           "SELECT count(DISTINCT row_group_id) FROM parquet_metadata($1)",
           [path]
         ) do
      {:ok, result} -> hd(hd(result.rows))
      {:error, _} -> :na
    end
  end

  defp warm_query(engine, sql, params) do
    {:ok, _} = Engine.query(engine, sql, params)
    :ok
  end

  defp placeholders(n), do: Enum.map_join(1..n, ", ", &"$#{&1}")

  defp fields do
    [
      {"project_id", :int64},
      {"ts", :timestamp},
      {"payload", :string}
    ]
  end

  defp plain_schema, do: Schema.new!(fields())

  defp clustered_schema,
    do: %{Schema.new!(fields()) | clustering: ["project_id", "ts"]}

  defp start_stack(dir, label, opts) do
    unique = System.unique_integer([:positive])
    buffer = Module.concat(__MODULE__, "Buffer#{unique}")
    storage = Module.concat(__MODULE__, "Storage#{unique}")
    root = Path.join(dir, label)
    File.mkdir_p!(root)

    flush_max_rows = Keyword.get(opts, :flush_max_rows, env("BATCH", 10_000))

    {:ok, buffer_pid} =
      BufferService.Supervisor.start_link(
        name: buffer,
        dir: Path.join(root, "buffer"),
        hot_server_port: 0,
        flush_interval_ms: 25,
        flush_max_rows: flush_max_rows,
        flush_max_bytes: 256_000_000,
        seal_max_files: 1_000_000,
        seal_max_bytes: 1_000_000_000,
        seal_max_age_ms: 600_000,
        retire_grace_ms: 600_000,
        write_timeout_ms: 120_000,
        control_timeout_ms: 120_000
      )

    catalog =
      start_lake!(Runtime.catalog_engine(storage), root,
        extensions: [:httpfs],
        settings: [memory_limit: engine_memory_limit()]
      )

    :ok = Catalog.create_table(catalog, @clustered, plain_schema())
    :ok = Catalog.create_table(catalog, @plain, plain_schema())

    storage_opts = [
      name: storage,
      dir: Path.join(root, "sealed"),
      buffer_name: buffer,
      buffer_base_url: HotServer.base_url(buffer),
      catalog: catalog,
      engine_extensions: [:httpfs],
      seal_row_group_size: 16_384
    ]

    {:ok, storage_pid} = StorageService.Supervisor.start_link(storage_opts)
    {:ok, buffer_runtime} = BufferService.Runtime.fetch(buffer)

    %{
      buffer: buffer,
      buffer_pid: buffer_pid,
      storage: storage,
      storage_pid: storage_pid,
      catalog: catalog,
      runtime: Runtime.new(storage_opts),
      buffer_runtime: buffer_runtime
    }
  end

  defp stop_stack(stack) do
    Supervisor.stop(stack.storage_pid)
    Supervisor.stop(stack.buffer_pid)
    Runtime.delete(stack.storage)
    BufferService.Runtime.delete(stack.buffer)
  end

  defp freeze(stack, table, ids) do
    {:ok, key} = sealed_key(table)
    {:ok, claim} = HotManifest.claim(stack.buffer_runtime.manifest, table, ids, [key])
    claim
  end

  defp sealed_key(table) do
    {:ok, prefix} = Store.prefix(table)
    Store.key(prefix, Id.generate())
  end

  defp rows_per_second(rows, us) when us > 0, do: Float.round(rows / (us / 1_000_000), 1)
  defp rows_per_second(_rows, _us), do: 0.0

  defp engine_memory_limit do
    :smolquery
    |> Application.get_env(Smolquery.Engine, [])
    |> Keyword.get(:memory_limit, "2GB")
  end

  defp format_pct(:undefined), do: "n/a"
  defp format_pct(pct) when is_number(pct), do: "#{pct}%"
end

Bench.Clustering.run()
