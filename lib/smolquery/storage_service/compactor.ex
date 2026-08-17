defmodule Smolquery.StorageService.Compactor do
  @moduledoc """
  Re-merges undersized sealed segments so a quiet table stops accreting files.

  Eager seals and age-cap seals are the right call on the write path — they
  bound the hot tier — and their cost lands here: a table trickling writes
  seals small, and small sealed files cost every query a footer read forever.
  This process finds runs of undersized segments and replaces each run with
  one merged segment.

  Compaction is smolquery's own, never DuckLake's:
  `ducklake_merge_adjacent_files` crashes DuckDB fatally over
  externally-registered files (PL-2 findings 9/14), so the swap is built from
  this side of the catalog seam — merge through
  `Smolquery.StorageService.Merge.compact/4`, then
  `Smolquery.Catalog.replace_segments/4`, one transaction, one snapshot.

  ## Level-triggered by looking, not by signals

  The sealer is told when a table wants sealing because only the buffer knows.
  Undersized sealed segments are entirely the catalog's knowledge, so nothing
  signals a compactor — it sweeps on an interval and finds work by reading
  `segments/3`. A failed or crashed compaction needs no bookkeeping for the
  same reason: the undersized run is still there next sweep, and the sweep is
  the retry.

  This is exactly the "polling for a compaction-due signal" case Milestone 8
  L6 (PL-11 D6) calls out: `Catalog.tables/1` returns the same catalog-wide
  list to every storage node's compactor, so without a gate every node would
  plan and race to compact the same undersized run.

  ## Ownership is per time bucket, not per table

  The gate used to check `Routing.own?(table_ref)`, which bounded compaction
  parallelism by table count: one node owned a hot table's whole backlog,
  carried every merge's memory pressure alone, and OOMed while its peers
  idled (T-269). The unit of ownership is now `{table_ref, bucket}`, where a
  segment's bucket is its ULID timestamp divided by `compact_bucket_ms`.
  Disjointness holds only while every storage node runs the same
  `compact_bucket_ms`: divergent values during a rolling config change give
  two nodes overlapping owned sets until the rollout completes, so change
  the value fleet-wide, not node by node. The overlap is survivable (below)
  but wasteful while it lasts.

  Every node lists every table's segments each sweep — the listing is what
  makes owned buckets knowable, so fleet-wide listing load is N x T catalog
  queries per interval where the per-table gate cost T. The listing is
  metadata-only, but its cost scales with segment count; sizing and merging
  stay owned-only, so footer I/O and merge work spread across the fleet.

  Ownership is snapshotted once per table, before sizing: sizing a large
  backlog takes chunked engine calls and can run for minutes, so a ring
  change mid-scan re-opens the two-owner overlap for later buckets — the
  same window the per-table gate had across a sweep. What makes the overlap
  survivable is the catalog's registration diff being re-derived inside
  every commit retry (`Smolquery.Catalog.DuckLake`), so the losing node's
  retry re-reads what the winner committed instead of replaying a stale
  swap.

  A group crosses a bucket boundary in exactly one case: a bucket that
  cannot meet `compact_min_inputs` alone rolls its candidates forward into
  this node's next owned bucket, oldest first, so a quiet table that seals
  one small file per bucket still compacts instead of accreting forever.
  Cross-node disjointness holds — a carry only ever holds this node's own
  candidates — and everything else stays bucket-local, so merged output
  stays as time-local as its inputs and min/max pruning does not erode. A
  path whose basename is not a ULID has no bucket, so it silently stops
  being a compaction candidate (it could never derive an output key anyway).
  The merged segment's id derives from its newest input's timestamp, so a
  still-undersized output lands back in its own bucket and the same node
  keeps converging it.

  ## The policy is deliberately boring

  Per table, per sweep: segments under `compact_below_bytes` (sizes summed
  from the Parquet footers, never a data read), oldest first — segment ids are
  ULIDs, so name order is time order — greedily grouped until adding the next
  would pass `compact_max_bytes`, compacted only if at least
  `compact_min_inputs` made the cut, all within one owned bucket, oldest
  owned bucket first. One group per table per sweep per node; a table with
  more work keeps its place in line rather than monopolizing the sweep, and
  a fleet of N owners advances up to N of its buckets per sweep.

  Bytes bound the group (T-248), with one safety valve. A small input-count
  cap made a small-segment backlog converge across sweeps, with each group
  re-ingesting the previous sweep's still-undersized output. That is
  quadratic write amplification for the exact case compaction exists to
  clean up. A large group is safe because the merge bounds its own engine
  calls: `Merge.compact/5` reads inputs in chunks of `merge_inputs_per_call`.
  The valve protects the sweep's time, not the engine: a group also stops at
  64 staging chunks of files (768 at the default cap), so one table of very
  small files cannot hold the sweep for hours. Re-ingestion at that width is
  negligible — a backlog past the valve converges hundreds of files per
  sweep, not twelve.

  Rows bound the group too (T-260). Compressed bytes alone do not predict
  merge cost: on ~100x-compressible data a byte-bounded group held ~25M rows,
  and the staging inserts plus the clustered `ORDER BY` on the final `COPY`
  scale with rows, so the group blew the merge's five-minute budget and
  re-planned identically every sweep. `compact_max_rows` caps the group by
  the footers' summed `num_rows`, which sizing already reads. A head file no
  neighbor fits beside under the row cap cannot wedge the table: a group
  smaller than `compact_min_inputs` drops its head and regroups from the next
  candidate, so the small files behind a row-heavy head still merge. The byte
  cap never needs this — two files under `compact_below_bytes` always fit —
  but a single small file can carry more rows than the cap admits twice.

  The row cap also adapts per table: a merge OOM halves the table's cap, and
  evidence at the tightened cap earns it back, because no static
  bytes-per-row constant fits every workload — see `adjusted_row_caps/3`
  (T-262).

  The sizing query chunks the same way, oldest first, and reads each file's
  `num_rows` in the same call. Sizing stops once the undersized bytes found
  reach `compact_max_bytes`, the rows found reach `compact_max_rows`, or the
  file count reaches the valve. That halt is
  only a work bound — `group/2`'s fold owns the caps, and it alone decides
  the group. The sweep passes the group's summed row count to the merge,
  which then skips its own footer pass, and it narrows the merge's staging
  chunk when the group's average input is large, so one chunk never moves an
  unbounded number of bytes. An output still under `compact_below_bytes`
  stays a candidate and merges with future arrivals. It never re-merges
  alone; `compact_min_inputs` gates that.

  ## Compaction runs on its own engine, and recycles it after a call exit

  Sizing and merging go through `Runtime.compact_engine/1`, never the seal
  merge engine (T-259). An `Adbc.Connection` serializes its queries and a
  timed-out statement keeps running — adbc exposes no cancel — so one
  abandoned compaction merge on the shared connection starved every seal and
  every later sizing call, and each sweep stacked another abandoned query on
  top. T-251 rightly refused to kill that connection: healthy in-flight seals
  run there. A dedicated engine removes the conflict, so when a failure
  carries a `Smolquery.Engine.CallExited`, this module kills the engine's
  database process — `rest_for_one` rebuilds database and connection — waits
  briefly for a replacement connection to register (the old registration is
  not the signal: the supervisor tears it down asynchronously, so only a new
  pid proves the rebuild), and moves to the next table. The
  abandoned statement's DuckDB instance burns until it completes, but nothing
  queues behind it anymore, and a killed merge is free to retry: the output
  key is derived, so next sweep's attempt overwrites its orphan.

  ## The output key is derived, so a retry overwrites instead of duplicating

  The merged segment's id comes from the sorted input ids
  (`Smolquery.Segments.Id.derive/2`), the same identity rule the sealer's
  claims use. A compaction that crashed after writing but before the swap
  re-plans the same group next sweep, derives the same key, and overwrites its
  own orphan — which GC would otherwise sweep, since nothing ever registered
  it. Old files are never deleted here: earlier snapshots still read them, and
  physical reclaim is GC's job once no snapshot does.

  ## The swap is verified, because the failure it guards against is silent

  `drop_segments` retires a file only because the lake is attached with
  `DATA_INLINING_ROW_LIMIT 0` (PL-2 finding 13). Attached without it,
  compaction still deletes the right rows while the planner keeps listing the
  dead segments — queries get slower and nothing errors. So after every swap
  the compactor re-reads `segments/3` and fails the table loudly if a dropped
  path survived, turning a broken invariant into a logged error instead of a
  slow mystery.
  """

  alias Smolquery.Catalog
  alias Smolquery.Engine
  alias Smolquery.Engine.CallExited
  alias Smolquery.Segments.Id
  alias Smolquery.Segments.Store
  alias Smolquery.StorageService.Merge
  alias Smolquery.StorageService.Routing
  alias Smolquery.StorageService.Runtime

  @enforce_keys [:runtime]
  defstruct [:runtime, row_caps: %{}]

  @group_max_staging_chunks 64
  @stage_chunk_target_bytes 67_108_864
  @engine_recycle_wait_ms 5_000

  use Smolquery.StorageService.Sweeper, interval: :compact_interval_ms

  require Logger

  @doc """
  Starts the compactor for a runtime.
  """
  @spec start_link(Runtime.t()) :: GenServer.on_start()
  def start_link(%Runtime{} = runtime) do
    runtime = Runtime.with_compact_max_rows(runtime)
    GenServer.start_link(__MODULE__, runtime, name: Runtime.compactor(runtime.name))
  end

  @doc """
  Sweeps now, without waiting for the interval.

  Reports what was compacted and what failed, per table — the observable form
  of the policy above, and what tests assert on.
  """
  @spec sweep(atom(), timeout()) :: {:ok, map()} | {:error, term()}
  def sweep(name, timeout \\ 60_000), do: GenServer.call(Runtime.compactor(name), :sweep, timeout)

  defp run(state) do
    runtime = state.runtime

    with {:ok, tables} <- Catalog.tables(runtime.catalog) do
      outcomes = Enum.map(tables, &compact_table(runtime, state.row_caps, &1))

      report = %{
        compacted: for({:ok, swap} <- outcomes, do: swap),
        failed: for({:failed, failure} <- outcomes, do: failure)
      }

      row_caps = adjusted_row_caps(state.row_caps, outcomes, runtime.compact_max_rows)

      {:ok, report, %{state | row_caps: row_caps}}
    end
  end

  @row_cap_floor 65_536
  @relax_patience_start 2
  @relax_patience_max 64

  @doc """
  The per-table row caps after a sweep — the workload-adaptive half of the
  row bound (T-262).

  No static bytes-per-row constant predicts a workload's pin rate: wide,
  repetitive text fields pin kilobytes per row while compressing well enough
  to pass every static cap, and narrow rows pin almost nothing. So the
  runtime's cap is only a start, and the caps here answer the workload
  instead of predicting it. A merge that OOMs halves the
  table's cap, never below #{@row_cap_floor} rows. Only tables that OOMed
  carry an entry, so the map stays empty on healthy deployments. The caps
  live in the compactor's state: a restart forgets them, and the first OOM
  after the restart re-learns them.

  Raising a cap needs evidence, because a raise re-probes the level that
  OOMed and a failed probe burns a multi-minute merge. A sweep counts toward
  the raise when the table's group compacts at more than half its cap — a
  two-file success proves nothing about a cap-sized group — or when the cap
  itself makes every plan skip, since a cap wedged at the floor produces no
  successes to learn from. The cap doubles after `patience` such sweeps, and
  sheds the override when it reaches the runtime's. Each OOM doubles the
  table's patience, up to #{@relax_patience_max} sweeps, so a table sitting
  on its true limit probes it rarely instead of every other sweep.

  The log lines classify each OOM, because a probe finding its limit is the
  design working while an OOM under a tightened cap is not (T-283). A raise
  marks the entry as a probe; the next outcome resolves it. An OOM at a
  probed cap logs at info as expected. A table's first OOM logs at info as
  calibration. An OOM at a cap that was not probing — a level that
  previously held — logs at warning, because it means memory pressure
  changed, not that the compactor is learning.

  Cap state is per node and in-memory. Under bucket-sharded ownership
  (T-269) each node calibrates a table independently, so a fleet of N pays
  up to N calibration OOMs per table, and a restart or a bucket moving to a
  fresh node re-enters calibration — a regression arriving at that moment
  logs at info, not warning. The calibration line says so, and the warning
  fires on the recurrence.
  """
  @spec adjusted_row_caps(
          %{Catalog.table_ref() => map()},
          [term()],
          pos_integer()
        ) :: %{Catalog.table_ref() => map()}
  def adjusted_row_caps(row_caps, outcomes, resolved) do
    Enum.reduce(outcomes, row_caps, fn
      {:ok, %{table: table, rows: rows}}, caps ->
        relax(caps, table, rows, resolved)

      {:skip, table}, caps ->
        case caps do
          %{^table => entry} -> advance(caps, table, entry, resolved)
          _no_override -> caps
        end

      {:failed, %{table: table, reason: reason} = failure}, caps ->
        if merge_oom?(reason),
          do: tighten(caps, table, resolved, Map.get(failure, :rows)),
          else: caps

      _not_owned, caps ->
        caps
    end)
  end

  defp tighten(caps, table, resolved, rows) do
    {attempted, patience, kind} =
      case caps do
        %{^table => %{cap: cap, patience: patience, probe: probe}} ->
          {min(cap, resolved), min(patience * 2, @relax_patience_max),
           if(probe, do: :probe, else: :regression)}

        _no_override ->
          {resolved, @relax_patience_start, :calibration}
      end

    cap = max(div(attempted, 2), @row_cap_floor)
    log_tighten(kind, table, oom_phrase(rows, attempted), cap)

    Map.put(caps, table, %{cap: cap, streak: 0, patience: patience, probe: false})
  end

  defp oom_phrase(nil, attempted), do: "a merge OOM under the #{attempted}-row cap"

  defp oom_phrase(rows, attempted),
    do: "a merge of #{rows} rows OOMed under the #{attempted}-row cap"

  defp log_tighten(:probe, table, oom, cap) do
    Logger.info(
      "compaction row-cap probe of #{inspect(table)} found its limit: #{oom} " <>
        "(expected while probing); the cap returns to #{cap}"
    )
  end

  defp log_tighten(:calibration, table, oom, cap) do
    Logger.info(
      "compaction row cap of #{inspect(table)} is calibrating: first merge OOM " <>
        "on this node (#{oom}); the cap tightens to #{cap}. A restart or a ring " <>
        "change also lands here, so recurrence at this cap logs as a warning"
    )
  end

  defp log_tighten(:regression, table, oom, cap) do
    Logger.warning(
      "unexpected compaction merge OOM under the already-tightened row cap of " <>
        "#{inspect(table)}: #{oom}; the cap tightens to #{cap} — " <>
        "the compaction engine memory limit may be too small"
    )
  end

  defp relax(caps, table, rows, resolved) do
    case caps do
      %{^table => %{cap: cap} = entry} when rows * 2 >= cap ->
        advance(caps, table, entry, resolved)

      _small_group_or_no_override ->
        caps
    end
  end

  defp advance(caps, table, entry, resolved) do
    cond do
      entry.streak + 1 < entry.patience ->
        Map.put(caps, table, %{entry | streak: entry.streak + 1, probe: false})

      entry.cap * 2 >= resolved ->
        Logger.info(
          "compaction row cap of #{inspect(table)} earned back the resolved " <>
            "#{resolved} rows"
        )

        Map.delete(caps, table)

      true ->
        Logger.info(
          "compaction row cap of #{inspect(table)} probes #{entry.cap * 2} rows; " <>
            "a merge OOM at this level is expected and re-tightens the cap"
        )

        Map.put(caps, table, %{entry | cap: entry.cap * 2, streak: 0, probe: true})
    end
  end

  defp merge_oom?({:put_failed, _key, reason}), do: merge_oom?(reason)

  defp merge_oom?({:merge_failed, %Adbc.Error{message: message}}),
    do: message =~ "Out of Memory"

  defp merge_oom?(_reason), do: false

  defp compact_table(runtime, row_caps, table_ref) do
    runtime = table_capped(runtime, row_caps, table_ref)
    compact_table(runtime, table_ref)
  end

  defp table_capped(runtime, row_caps, table_ref) do
    cap =
      case row_caps do
        %{^table_ref => %{cap: cap}} -> min(cap, runtime.compact_max_rows)
        _no_override -> runtime.compact_max_rows
      end

    %{runtime | compact_max_rows: cap}
  end

  defp compact_table(runtime, table_ref) do
    started_at = System.monotonic_time(:microsecond)
    routing = Routing.resolve(runtime.name)

    with {:ok, paths} <- Catalog.segments(runtime.catalog, table_ref, :current),
         [_ | _] = owned <- owned_paths(runtime, routing, table_ref, paths),
         {:ok, group} <- plan(runtime, owned) do
      swap(runtime, table_ref, group, started_at)
    else
      [] -> :skip
      :skip -> {:skip, table_ref}
      {:error, reason} -> failed(runtime, table_ref, reason, started_at)
    end
  end

  defp owned_paths(runtime, routing, table_ref, paths) do
    paths
    |> Enum.sort_by(&Path.basename/1)
    |> Enum.filter(fn path ->
      case bucket(path, runtime.compact_bucket_ms) do
        :error -> false
        bucket -> Routing.own?(routing, {table_ref, bucket})
      end
    end)
  end

  defp bucket(path, bucket_ms) do
    case path |> Path.basename(".parquet") |> Id.timestamp() do
      {:ok, timestamp} -> div(timestamp, bucket_ms)
      :error -> :error
    end
  end

  defp plan(runtime, owned) do
    if length(owned) < runtime.compact_min_inputs do
      :skip
    else
      plan_undersized(runtime, owned)
    end
  end

  defp plan_undersized(runtime, owned) do
    with {:ok, undersized} <- undersized(runtime, owned) do
      undersized
      |> Enum.sort_by(fn {path, _bytes, _rows} -> Path.basename(path) end)
      |> Enum.chunk_by(fn {path, _bytes, _rows} -> bucket(path, runtime.compact_bucket_ms) end)
      |> carried_group(runtime, [])
    end
  end

  defp carried_group([], _runtime, _carry), do: :skip

  defp carried_group([bucket_entries | rest], runtime, carry) do
    candidates = carry ++ bucket_entries

    if length(candidates) < runtime.compact_min_inputs do
      carried_group(rest, runtime, candidates)
    else
      case group(runtime, candidates) do
        :skip -> carried_group(rest, runtime, candidates)
        {:ok, group} -> {:ok, group}
      end
    end
  end

  defp undersized(runtime, paths) do
    paths
    |> Enum.chunk_every(runtime.merge_inputs_per_call)
    |> Enum.reduce_while(%{chunks: [], bytes: 0, rows: 0, count: 0}, &size_chunk(runtime, &1, &2))
    |> case do
      {:error, reason} -> {:error, reason}
      %{chunks: chunks} -> {:ok, chunks |> Enum.reverse() |> List.flatten()}
    end
  end

  defp size_chunk(runtime, chunk, acc) do
    case sizes_chunk(runtime, chunk) do
      {:ok, sizes} ->
        found =
          Enum.filter(sizes, fn {_path, size, _rows} -> size < runtime.compact_below_bytes end)

        group_filled(
          %{
            chunks: [found | acc.chunks],
            bytes: acc.bytes + Enum.sum_by(found, fn {_path, size, _rows} -> size end),
            rows: acc.rows + Enum.sum_by(found, fn {_path, _size, rows} -> rows end),
            count: acc.count + length(found)
          },
          runtime
        )

      {:error, reason} ->
        {:halt, {:error, reason}}
    end
  end

  defp group_filled(%{bytes: bytes, rows: rows, count: count} = acc, runtime)
       when bytes >= runtime.compact_max_bytes or rows >= runtime.compact_max_rows or
              count >= @group_max_staging_chunks * runtime.merge_inputs_per_call,
       do: {:halt, acc}

  defp group_filled(acc, _runtime), do: {:cont, acc}

  defp sizes_chunk(runtime, paths) do
    count = length(paths)

    sql =
      "SELECT sizes.file_name, sizes.bytes, files.num_rows::BIGINT " <>
        "FROM (SELECT file_name, sum(total_compressed_size)::BIGINT AS bytes " <>
        "FROM parquet_metadata([#{placeholders(paths)}]) GROUP BY file_name) sizes " <>
        "JOIN parquet_file_metadata([#{placeholders(paths, count)}]) files USING (file_name)"

    case Engine.try_query(Runtime.compact_engine(runtime.name), sql, paths ++ paths) do
      {:ok, result} ->
        {:ok, Enum.map(result.rows, fn [path, bytes, rows] -> {path, bytes, rows} end)}

      {:error, error} ->
        {:error, {:sizing_failed, error}}
    end
  end

  defp group(runtime, undersized) do
    ceiling = @group_max_staging_chunks * runtime.merge_inputs_per_call

    undersized
    |> Enum.sort_by(fn {path, _bytes, _rows} -> Path.basename(path) end)
    |> Enum.take(ceiling)
    |> grouped(runtime)
  end

  defp grouped([], _runtime), do: :skip

  defp grouped([_head | rest] = candidates, runtime) do
    {group, total, row_count} =
      Enum.reduce_while(candidates, {[], 0, 0}, fn {path, bytes, rows},
                                                   {group, total, group_rows} ->
        if (total + bytes > runtime.compact_max_bytes or
              group_rows + rows > runtime.compact_max_rows) and group != [] do
          {:halt, {group, total, group_rows}}
        else
          {:cont, {[path | group], total + bytes, group_rows + rows}}
        end
      end)

    if length(group) >= runtime.compact_min_inputs do
      {:ok, %{paths: Enum.reverse(group), row_count: row_count, bytes: total}}
    else
      grouped(rest, runtime)
    end
  end

  defp swap(runtime, table_ref, %{paths: paths, row_count: row_count} = group, started_at) do
    with {:ok, key} <- output_key(table_ref, paths),
         {:ok, segment} <-
           Merge.compact(
             %{runtime | merge_engine: Runtime.compact_engine(runtime.name)},
             table_ref,
             key,
             paths,
             row_count: row_count,
             inputs_per_call: staging_per_call(runtime, group)
           ),
         {:ok, snapshot} <-
           Catalog.replace_segments(runtime.catalog, table_ref, [segment], paths),
         :ok <- verify_retired(runtime, table_ref, paths) do
      Logger.info(fn ->
        "compacted #{length(paths)} segment(s) of #{inspect(table_ref)} " <>
          "into #{key} at snapshot #{snapshot}"
      end)

      :telemetry.execute(
        [:smolquery, :compact, :swap],
        %{replaced: length(paths), duration_us: elapsed_us(started_at)},
        %{result: :ok, table_ref: table_ref}
      )

      {:ok,
       %{table: table_ref, key: key, replaced: length(paths), rows: row_count, snapshot: snapshot}}
    else
      {:error, reason} -> failed(runtime, table_ref, reason, started_at, row_count)
    end
  end

  defp output_key({dataset, table} = table_ref, paths) do
    with {:ok, ids} <- input_ids(paths),
         {:ok, prefix} <- Store.prefix(table_ref) do
      sorted = Enum.sort(ids)
      {:ok, timestamp} = sorted |> List.last() |> Id.timestamp()

      Store.key(prefix, Id.derive(timestamp, [dataset, 0, table, 0, Enum.intersperse(sorted, 0)]))
    end
  end

  defp input_ids(paths) do
    Enum.reduce_while(paths, {:ok, []}, fn path, {:ok, ids} ->
      id = Path.basename(path, ".parquet")

      if Id.valid?(id) do
        {:cont, {:ok, [id | ids]}}
      else
        {:halt, {:error, {:not_a_segment_path, path}}}
      end
    end)
  end

  defp verify_retired(runtime, table_ref, dropped) do
    with {:ok, current} <- Catalog.segments(runtime.catalog, table_ref, :current) do
      listed = MapSet.new(current)

      case Enum.filter(dropped, &MapSet.member?(listed, &1)) do
        [] -> :ok
        survivors -> {:error, {:inputs_survived_swap, survivors}}
      end
    end
  end

  defp failed(runtime, table_ref, reason, started_at, rows \\ nil) do
    Logger.warning("compaction of #{inspect(table_ref)} failed: #{inspect(reason)}")

    :telemetry.execute(
      [:smolquery, :compact, :swap],
      %{replaced: 0, duration_us: elapsed_us(started_at)},
      %{result: :error, table_ref: table_ref}
    )

    recycle_on_exit(runtime, reason)

    failure = %{table: table_ref, reason: reason}

    {:failed, if(rows, do: Map.put(failure, :rows, rows), else: failure)}
  end

  @doc """
  Whether a compaction failure carries a `Smolquery.Engine.CallExited` — what
  `failed/4` recycles the compaction engine on. A final `COPY`'s exit arrives
  wrapped by the store as `{:put_failed, key, {:merge_failed, exit}}`; sizing
  and staging exits arrive bare.
  """
  @spec engine_call_exited?(term()) :: boolean()
  def engine_call_exited?({:put_failed, _key, reason}), do: engine_call_exited?(reason)

  def engine_call_exited?({step, %CallExited{}}) when step in [:sizing_failed, :merge_failed],
    do: true

  def engine_call_exited?(_reason), do: false

  defp recycle_on_exit(runtime, reason) do
    if engine_call_exited?(reason) do
      engine = Runtime.compact_engine(runtime.name)
      stale_connection = Process.whereis(Engine.connection_name(engine))

      case Process.whereis(Engine.database_name(engine)) do
        nil ->
          await_engine(engine, stale_connection, @engine_recycle_wait_ms)

        database ->
          Logger.warning("recycling the compaction engine after an engine call exit")
          Process.exit(database, :kill)
          await_engine(engine, stale_connection, @engine_recycle_wait_ms)
      end
    else
      :ok
    end
  end

  defp await_engine(_engine, _stale_connection, remaining_ms) when remaining_ms <= 0, do: :ok

  defp await_engine(engine, stale_connection, remaining_ms) do
    connection = Process.whereis(Engine.connection_name(engine))

    if is_pid(connection) and connection != stale_connection do
      :ok
    else
      Process.sleep(100)
      await_engine(engine, stale_connection, remaining_ms - 100)
    end
  end

  defp staging_per_call(runtime, %{paths: paths, bytes: bytes}) do
    average = max(div(bytes, length(paths)), 1)

    runtime.merge_inputs_per_call
    |> min(div(@stage_chunk_target_bytes, average))
    |> max(1)
  end

  defp elapsed_us(started_at), do: System.monotonic_time(:microsecond) - started_at

  defp placeholders(paths, offset \\ 0),
    do: Enum.map_join(1..length(paths), ", ", &"$#{&1 + offset}")
end
