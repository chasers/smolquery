defmodule Smolquery.StorageService.Scheduler.Planner do
  @moduledoc """
  Which files of a table this node merges next, and how many.

  Undersized sealed segments are entirely the catalog's knowledge, so the
  plan is read off `Smolquery.Catalog.segment_files/3` each sweep and sized
  from the Parquet footers, never a data read.

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
  bytes-per-row constant fits every workload — see `Smolquery.StorageService.Scheduler.Caps.adjusted_row_caps/3`
  (T-262).

  The catalog screens the candidates before any footer is opened (T-463).
  `Catalog.segment_files/3` carries the whole-file size DuckLake recorded at
  registration, and a file at or above `compact_below_bytes` by that measure
  is never a candidate — its compressed-data sum is smaller still, so the
  screen only ever tightens the threshold by a footer's width. Without it,
  every owned segment's footer was read on every sweep, and once the
  engines stopped caching file reads (T-461) a table with nothing to
  compact cost two store requests per file per sweep, forever. A size the
  catalog does not know is recorded as `0`, which is under any threshold,
  so the footer decides for that file. Footers are still read for the
  candidates: sizing is where a corrupt file first fails, and the
  quarantine keys on that.

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

  ## Files settle at a target size, two levels, as soon as they can (T-592)

  The policy above tops out at `compact_max_bytes` inside an hour bucket, so
  on a table that seals small files often it tops out much lower: at about
  6 MB, the 1 h bucket held `metrics.samples` to 168 files a week, and a
  backlog of 16k never converged (T-588). Every query pays per file.

  So compaction runs at two levels, split by age. The **hour level** is the
  policy above, over the current bucket and the one before it. Every file
  older than one more bucket is **settled**, and the **span level** merges
  settled files under half of `compact_target_bytes` toward the target:
  grouped within one `compact_span_ms` (a day), never carried across spans,
  capped by the target's bytes and by no row count, because the merge
  spills (T-591) and a row cap would hold a day of small rows to a fraction
  of the target. A file therefore reaches the target about two buckets after
  its data arrived, not once its day closes, at the cost of rewriting a day
  file still under half the target about once a bucket until it gets there:
  for a table writing less than that in a day, up to about 24 rewrites of a
  growing file, around 12 times the day's bytes. A table that seals at most
  one file in a span keeps one file per span, since no merge crosses a span.

  The bucket between the levels belongs to neither, so a merge the hour level
  planned has usually finished before the span level can plan any of its
  files. Usually is not enough when rows would count twice, so correctness
  does not rest on it: a swap whose inputs another swap already retired
  refuses in the catalog and commits nothing (`Smolquery.Catalog.DuckLake`),
  the next sweep replans from the current files, and that refusal never backs
  the table off.

  ## A span merge is sized by what it decodes to, and pauses when the disk is short

  Bytes on disk do not bound a merge. The `bench.clickstack_*` tables keep
  attribute bags as `MAP` or `VARIANT`: about 4 bytes a row on disk and about
  3 KiB in a merge. A 1 GiB span group of them was hundreds of gigabytes of
  sort, so every span merge spilled to DuckDB's 32 GiB temp cap and filled
  the storage nodes' disks within ten minutes of the roll (T-601).

  So once a span group forms, its rows are also capped at
  `compact_span_decoded_bytes` over an estimated decoded row width: the mean
  text width of up to 1,024 rows of the group's first file, read only then.
  A group over that cap is cut again under it. Text is a proxy for what
  DuckDB holds, not a measure of it; the span cap halving below remains the
  correction when it guesses low. A sample that fails is
  `{:width_sample_failed, error}`, which names no file, so it never counts
  toward quarantining a file that is fine.

  A failing span group does not re-run as it was. Its merge is 1 GiB with no
  row cap, so the lever is bytes: an OOM, an engine call exit or a swap timeout
  halves the table's span cap, never below `compact_max_bytes`, and the table
  does not back off while the cap can still shrink; see
  `Smolquery.StorageService.Scheduler.Caps.adjusted_span_caps/3`. A span failure leaves the hour level's row cap
  alone. Span-level work is owned per
  `{table_ref, {:span, span}}`, the way the hour level is per bucket, so a
  backlog of days spreads across the fleet. Only when the span level has
  nothing to do does the sweep turn to the hour level, still one group per
  table per sweep. A `compact_target_bytes` of `nil` turns the span level
  off and leaves every file at the hour level, as before.
  """

  alias Smolquery.Catalog
  alias Smolquery.Engine
  alias Smolquery.Segments.Id
  alias Smolquery.StorageService.Routing
  alias Smolquery.StorageService.Runtime
  alias Smolquery.StorageService.Scheduler.Failure
  alias Smolquery.StorageService.Scheduler.Quarantine

  @group_max_staging_chunks 64
  @span_max_rows 9_223_372_036_854_775_807
  @width_sample_rows 1024

  defp reject_quarantined(quarantined_groups, owned, listed) do
    active = Quarantine.active_quarantined_paths(quarantined_groups, listed)

    Enum.reject(owned, &MapSet.member?(active, &1))
  end

  @doc """
  The one group this node compacts next for `table_ref`, from the table's
  current `files`: at the span level when it has a group, else at the hour
  level. `planning` carries the ring (`:routing`) and this node's
  quarantined groups (`:quarantined_groups`); `span_cap` is the table's
  learned span cap. Answers `:not_owned` when this node owns none of the
  table's files, `:skip` when it owns some and nothing is worth merging, and
  an error, with the paths that failed when it knows them, when sizing
  failed.
  """
  @spec plan(Runtime.t(), map(), Catalog.table_ref(), pos_integer() | nil, integer()) ::
          {:ok, map()} | :skip | :not_owned | {:error, term()} | {:error, term(), [String.t()]}
  def plan(runtime, planning, table_ref, span_cap, now_ms) do
    case by_level(runtime, Enum.map(planning.files, & &1.path), now_ms) do
      {[], recent} ->
        plan_level(runtime, :hour, planning, table_ref, recent)

      {settled, recent} ->
        span = span_level(runtime, span_cap)

        case plan_level(span, :span, planning, {table_ref, :span}, settled) do
          skipped when skipped in [:not_owned, :skip] ->
            runtime
            |> plan_level(:hour, planning, table_ref, recent)
            |> hour_or_skip(skipped)

          planned ->
            planned
        end
    end
  end

  defp hour_or_skip(:not_owned, :skip), do: :skip
  defp hour_or_skip(hour, _span), do: hour

  defp plan_level(runtime, level, planning, owner, paths) do
    listed = Enum.map(planning.files, & &1.path)

    with [_ | _] = owned <- owned_paths(runtime, planning.routing, owner, paths),
         [_ | _] = plannable <- reject_quarantined(planning.quarantined_groups, owned, listed),
         {:ok, group} <- plan_files(runtime, level, listed_among(planning.files, plannable)) do
      {:ok, Map.merge(group, %{level: level, span_cap: runtime.compact_max_bytes})}
    else
      [] -> :not_owned
      other -> other
    end
  end

  defp by_level(%Runtime{compact_target_bytes: nil}, paths, _now_ms), do: {[], paths}

  defp by_level(runtime, paths, now_ms) do
    grace = runtime.compact_bucket_ms

    Enum.reduce(paths, {[], []}, fn path, {settled, recent} ->
      case bucket(path, grace) do
        :error -> {settled, recent}
        hour when (hour + 1) * grace + 2 * grace <= now_ms -> {[path | settled], recent}
        hour when (hour + 1) * grace + grace > now_ms -> {settled, [path | recent]}
        _between -> {settled, recent}
      end
    end)
  end

  defp span_level(runtime, span_cap) do
    %{
      runtime
      | compact_below_bytes: div(runtime.compact_target_bytes, 2),
        compact_max_bytes: span_cap,
        compact_max_rows: @span_max_rows,
        compact_bucket_ms: runtime.compact_span_ms
    }
  end

  defp owned_paths(runtime, routing, owner, paths) do
    paths
    |> Enum.sort_by(&Path.basename/1)
    |> Enum.filter(fn path ->
      case bucket(path, runtime.compact_bucket_ms) do
        :error -> false
        bucket -> Routing.own?(routing, owner_key(owner, bucket))
      end
    end)
  end

  defp owner_key({table_ref, :span}, bucket), do: {table_ref, {:span, bucket}}
  defp owner_key(table_ref, bucket), do: {table_ref, bucket}

  defp bucket(path, bucket_ms) do
    case path |> Path.basename(".parquet") |> Id.timestamp() do
      {:ok, timestamp} -> div(timestamp, bucket_ms)
      :error -> :error
    end
  end

  defp listed_among(files, paths) do
    plannable = MapSet.new(paths)

    Enum.filter(files, &MapSet.member?(plannable, &1.path))
  end

  defp plan_files(runtime, level, files) do
    candidates =
      for %{path: path, bytes: bytes} <- files, bytes < runtime.compact_below_bytes, do: path

    if length(candidates) < runtime.compact_min_inputs do
      :skip
    else
      plan_undersized(runtime, level, candidates)
    end
  end

  defp decoded_capped(runtime, entries, %{paths: [sample | _rest], row_count: rows} = group) do
    case row_width(runtime, sample) do
      {:ok, width} ->
        cap = max(div(runtime.compact_span_decoded_bytes, width), 1)

        if rows <= cap, do: {:ok, group}, else: group(%{runtime | compact_max_rows: cap}, entries)

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp row_width(runtime, path) do
    sql =
      "SELECT CAST(coalesce(avg(strlen(CAST(sampled AS VARCHAR))), 1) AS BIGINT) " <>
        "FROM (SELECT * FROM read_parquet($1) LIMIT #{@width_sample_rows}) AS sampled"

    case Engine.try_query(Runtime.compact_engine(runtime.name), sql, [path]) do
      {:ok, %{rows: [[width]]}} -> {:ok, max(width, 1)}
      {:error, error} -> {:error, {:width_sample_failed, error}}
    end
  end

  defp plan_undersized(runtime, level, owned) do
    with {:ok, undersized} <- undersized(runtime, owned) do
      undersized
      |> Enum.sort_by(fn {path, _bytes, _rows} -> Path.basename(path) end)
      |> Enum.chunk_by(fn {path, _bytes, _rows} -> bucket(path, runtime.compact_bucket_ms) end)
      |> carried_group(runtime, level, [])
    end
  end

  defp carried_group([], _runtime, _carry, _carried), do: :skip

  defp carried_group([bucket_entries | rest], runtime, :span, []) do
    with {:ok, group} <- group(runtime, bucket_entries),
         {:ok, group} <- decoded_capped(runtime, bucket_entries, group) do
      {:ok, group}
    else
      :skip -> carried_group(rest, runtime, :span, [])
      {:error, _reason} = failed -> failed
    end
  end

  defp carried_group([bucket_entries | rest], runtime, :hour, carried) do
    candidates = carried ++ bucket_entries

    if length(candidates) < runtime.compact_min_inputs do
      carried_group(rest, runtime, :hour, candidates)
    else
      case group(runtime, candidates) do
        :skip -> carried_group(rest, runtime, :hour, candidates)
        {:ok, group} -> {:ok, group}
      end
    end
  end

  defp undersized(runtime, paths) do
    paths
    |> Enum.chunk_every(runtime.merge_inputs_per_call)
    |> Enum.reduce_while(%{chunks: [], bytes: 0, rows: 0, count: 0}, &size_chunk(runtime, &1, &2))
    |> case do
      {:error, reason, chunk} -> {:error, reason, chunk}
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
        {:halt, {:error, reason, attribute_sizing_failure(runtime, reason, chunk)}}
    end
  end

  defp attribute_sizing_failure(_runtime, _reason, [_single] = chunk), do: chunk

  defp attribute_sizing_failure(runtime, reason, chunk) do
    if Failure.engine_call_exited?(reason) do
      chunk
    else
      Enum.filter(chunk, &match?({:error, _}, sizes_chunk(runtime, [&1])))
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

  defp placeholders(paths, offset \\ 0),
    do: Enum.map_join(1..length(paths), ", ", &"$#{&1 + offset}")
end
