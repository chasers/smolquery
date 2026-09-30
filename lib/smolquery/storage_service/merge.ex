defmodule Smolquery.StorageService.Merge do
  @moduledoc """
  Turns a claim's micro-segments into one sealed segment.

  The merge runs inside DuckDB and writes straight to the store's staging path:

      COPY (SELECT projection FROM read_parquet([urls], union_by_name := true)) TO staged

  So no segment's bytes ever become an Elixir term. That matters more here than
  anywhere else in the system — a sealed segment is the largest object smolquery
  writes — and it is the same reason `Smolquery.Segments.Writer` hands DuckDB a
  path rather than building a binary.

  `union_by_name` is what makes additive schema evolution work at the file level:
  micro-segments written before and after a column was added merge into one segment
  carrying the union, which is what the buffer's flush-on-schema-change already set
  up.

  ## Every engine call is bounded, so no claim is too large to seal

  Per-input cost is what kills a merge, not total bytes. Each `read_parquet`
  input costs footer round trips over `httpfs`, and enough inputs outrun the
  engine's 30 s call timeout — T-244 measured ≥ ~830 ms per input on the
  sandbox. An input list within `merge_inputs_per_call` merges in the single
  `COPY` above. The merge reads a larger list in capped chunks into a session
  temp table: one `DESCRIBE` and one `CREATE`/`INSERT ... SELECT` per chunk
  (T-246, T-247). Each chunk projects onto the catalog's declared schema, so
  every chunk emits identical columns. One final `COPY` writes the segment
  from local data, and the merge then drops the table.

  The temp table's name derives from the output key's id. Concurrent merges
  on the shared connection therefore cannot collide, and a retry's
  `CREATE OR REPLACE` clears a crashed predecessor's leftover. A retry
  re-stages the whole claim — there is no partial-stage resume — and the
  buffer's claim valves (bytes and input count) are what bound that cost. The staging
  hop's cost is memory: the claim's rows live in the engine, spilling to the
  connection's `temp_directory` past its limit, until the final `COPY`.

  The final `COPY` gets `merge_copy_timeout_ms` — five minutes by default —
  rather than the engine's 30 s default, on both paths (T-261). The staged variant reads local data, so its duration
  scales with the backlog's bytes, not with per-input `httpfs` latency; a
  30 s ceiling would decide how large a backlog may seal — the T-244 trap
  with the bound moved. The direct variant earns the same budget the other
  way around: its inputs are count-capped, not byte- or row-capped, and a
  row-capped compaction group of twelve inputs sorts millions of rows in one
  read-sort-write `COPY`. On the default budget those groups died at ~30 s
  and re-planned identically every sweep — while a larger group of the same
  data succeeded, purely because its input count pushed it onto the chunked
  path whose budgets were real. A staging chunk
  gets `merge_staging_timeout_ms` — two minutes by default — for a similar
  reason: the count cap bounds its inputs, not
  its bytes, and twelve compaction inputs near `compact_below_bytes` move
  hundreds of megabytes on a slow link. The schema `DESCRIBE` that precedes
  a projection gets `merge_describe_timeout_ms`, the same two minutes rather
  than the 30 s default (T-288): its
  own work is bounded — it reads the footers the projected read is about to
  read anyway — but its budget is spent on a serialized connection, where
  every other merge's staging call is this call's queue time. On the 30 s
  default, three tables sealing backlogs concurrently starved each other's
  first `DESCRIBE` behind their own two-minute staging calls, and each
  timeout re-staged its whole claim, so no merge ever finished. A budget in
  line with the calls it queues behind is what lets a merge wait its turn
  instead of failing before its statement ever runs. An `after` block drops the staging
  table, so an error or an exit on the way out does not strand the staged
  rows on the shared connection. The drop's own call gets five seconds, not
  the 30 s default: on a wedged connection the drop queues behind the
  abandoned statement anyway — waiting longer only delays the failure
  reaching the compactor's engine recycle — and the statement still executes
  once the connection drains, so the cleanup is not lost with the wait. A
  drop that itself fails logs a warning, and a seal retry's
  `CREATE OR REPLACE` clears what the drop could not.

  All three budgets are `Smolquery.StorageService.Runtime` fields, so a
  deployment whose claims outgrow them can say so (T-335). A timed-out merge
  re-stages its whole claim, so a budget the claim cannot fit inside is a
  merge that never finishes rather than one that finishes late.

  Every engine call here goes through `Smolquery.Engine.try_query/4`, so a
  call that exits — timed out against a connection wedged by an OOM, or busy
  behind another merge's `COPY` — is a merge failure, not a caller crash
  (T-251). The distinction matters most for the compactor, which merges every
  table in one sweep process: before this, the cleanup drop's timeout exit
  blew through the `after` block and killed the Compactor mid-sweep, once per
  sweep, starving every table behind the failing group.

  A failure carries the exception itself, `{:merge_failed, exception}`, not
  its rendered message: the compactor recycles its engine when the exception
  is a `Smolquery.Engine.CallExited` (T-259), and that decision has to be a
  pattern match, not a string match. Calls run on
  `Smolquery.StorageService.Runtime.merge_engine/1` — the seal merge engine
  unless the caller overrode it, which is how the compactor keeps its merges
  off the connection the sealer is using.

  ## The union of the inputs is not the schema the catalog declares

  Unioning the inputs is necessary and not sufficient. A claim whose inputs *all*
  predate an added column unions to the schema as it was before the column existed,
  and registration rejects the file it produces:

      Invalid Input Error: Column "name" exists in table "events"
      but was not found in file "…/01K….parquet"

  That is not a failure a retry can clear — the claim's input set is frozen, so
  every attempt merges the same narrow files — so the seal never retires, the
  buffer re-signals every `seal_retry_ms`, and the table's tail is stuck in the hot
  tier for good. The merge therefore projects onto the schema the catalog declares
  rather than onto whatever the inputs happen to carry: each declared column is
  selected in the catalog's order and cast to the catalog's type, and one the
  inputs do not carry is a typed `NULL`. The sealed file matches the table by
  construction, which is the invariant registration needs.

  Registration also takes `allow_missing => true, ignore_extra_columns => true`
  (T-430), and the two are not redundant. The projection settles column order
  and type, and fixes what this module writes when its inputs are narrow. The
  registration options cover the race the projection cannot see: a column
  change that lands *between* this module reading the schema and the handoff
  registering the file. The output key is write-once, so a refusal there would
  meet the same bytes on every retry — the stuck tail again, reached from the
  output side — where the tolerant registration takes the file as it is, and
  the column the change touched reads `NULL` or is ignored, exactly as a file
  written a moment earlier would.

  The reverse — a column the inputs carry and the catalog does not — is projected
  away, and this is where a dropped column's data leaves the system. After
  `DROP COLUMN` (T-430) every micro-segment written before the drop still carries
  the column; the catalog has said that data is no longer wanted, the query view
  already stops showing it, and the seal is what stops storing it. This used to be
  refused as `{:error, {:undeclared_columns, names}}`, on the argument that
  projecting away a column of acked rows is worse than a stuck claim. With a way
  to drop a column, that refusal inverted its own purpose: the claim's input set
  is frozen, so every retry met the same column, the seal never retired, and the
  table's tail stayed in the hot tier for good — the T-54 failure, reached from
  the other side. Nothing but a drop can put an undeclared column in an input:
  registration validates every name it accepts, and the buffer writes only the
  schema it was handed.

  ## The output is already named

  The key comes from the claim, not from this module. It was derived from the
  claim's inputs when the claim was frozen, so a retry writes the same key —
  the store commits it once and reports every later identical put as a no-op
  success (T-308), rather than creating a second segment. That is what makes
  a crashed merge free to retry, and it is why this module never generates an
  id.

  ## A materialized column is recomputed, not copied

  Both final `COPY`s — the direct one over the projected inputs and the
  staged one over the session temp table — render through
  `Smolquery.Schema.computed_select/2` (PL-61 L5): a regular column by
  name, a materialized one from its expression over the projected inputs. So a claim whose micro-segments
  predate the column seals with the value computed, and a compaction of
  files that carried it recomputes it identically — the definition-time
  determinism gate is what makes the two the same value. The inner
  projection still sources the column from an input that has it, but the
  outer select overrides it, so nothing stored can disagree with the
  expression.

  ## What it does not do

  No catalog commit and no retirement: this produces a `Smolquery.Segments.Segment`
  and stops. Composing it into the full handoff is
  `Smolquery.StorageService.Handoff`'s job, which is where the ordering that makes
  the handoff exactly-once lives.

  ## Clustering keys sort at seal so row-group stats prune like ClickHouse

  A table's `clustering` columns are an `ORDER BY` on the `COPY` that writes
  the sealed file. DuckDB's Parquet row groups carry min/max per column; sorted
  data makes those bounds tight, so scans with predicates on the clustering key
  skip row groups the way ClickHouse's sparse index does — without a separate
  index structure. `NULLS LAST` and stable ordering match the writer's flush sort.
  An empty clustering key omits `ORDER BY` entirely, so tables without one seal
  exactly as before.

  The columns are `Smolquery.Schema.clustering_columns/1`, not the schema's raw
  `:clustering` field, and here that matters more than at flush: the `ORDER BY`
  composes with the projection above, whose output columns are exactly the ones
  the catalog declares. A key naming a column the table no longer has would be
  an unresolvable identifier, so the `COPY` would fail — identically on every
  retry, since a claim's inputs are frozen — and the claim would never retire.
  Intersecting first turns a stranded table back into a slightly worse sort.

  `ROW_GROUP_SIZE` is set on every seal `COPY`, clustered or not (T-280). A
  sealed-tier scan over `httpfs` pays roughly one range request per row group,
  so the row-group count is the scan's request count, and the default of
  1_048_576 rows cuts it ~8x against DuckDB's 122_880-row default. The knob
  used to be gated on `Schema.clustering_columns/1`, when its default was
  16_384: groups that small buy clustered-key pruning but cost metadata and
  compression — measured at +25% sealed size on an unclustered table in
  `bench/sealer.exs` (64 micro-segments × 10k rows). At 1M rows that penalty
  runs the other way, so the gate is gone. The value is configured once at
  boot as `seal_row_group_size` on `Smolquery.StorageService.Runtime`.

  ## A large span merge sorts window by window, so its memory is bounded (T-607)

  A compaction's inputs are already sorted by the clustering key, and one
  `COPY ... ORDER BY` over all of them sorts every row again, in a sort whose
  memory and spill grow with the group: at 2,000 `metrics.samples` seals the
  sort spilled 2.5 GiB for 251 MiB of inputs, and 4x the data cost 9x the
  spill (T-605, `bench/merge_order.exs`). So a compaction whose caller passes
  the group's estimated row width (`width:`, which the span planner knows)
  and whose rows exceed one window, `compact_window_decoded_bytes` over that
  width, merges as an external sort instead:

    1. **Localize.** Each chunk of `merge_inputs_per_call` inputs is copied,
       projected, to a local run file, without a sort. The per-call input cap
       holds as on the other paths, and no later step reads the store: a
       remote input's footer round trips are paid once.
    2. **Bound.** The window boundaries are quantiles of the leading
       clustering column, sampled from the runs into a session temp table
       named for the output key, dropped with the merge.
    3. **Partition.** One `COPY ... PARTITION_BY` per run writes its rows
       into their windows' directories, `NULL`s into the last window to match
       `NULLS LAST`, and deletes the run. A partitioned write buffers rows
       per window, so partitioning a run at a time bounds that buffer by one
       chunk, not by the group.
    4. **Sort each window.** One `COPY ... ORDER BY` per window, lowest key
       range first, over about a window of rows.
    5. **Concatenate.** One `COPY` of the window parts in key order with
       `PRESERVE_ORDER true` writes the segment. The compaction session runs
       with `preserve_insertion_order = false`, under which a plain
       concatenation reorders row groups; the per-statement option overrides
       it for this statement only, without touching the shared connection.

  A window is sized to sort in memory, but the width it is sized from is an
  estimate, low by as much as 20x on some tables (T-603). A window whose sort
  spills to the temp cap fails the merge the way a group's sort does, and the
  table learns a wider width from it: `sorted_rows/3` gives the failure one
  window's rows rather than the group's, so the learned width is the temp
  cap over what one sort held, and the next plan's windows shrink.

  Scratch files are zstd Parquet under
  `Smolquery.StorageService.Runtime.spill_root/0`, disk the spill floor
  already watches, and each step deletes what the one before it wrote. They
  live in a directory per attempt, under a root per runtime that each
  windowed merge and each scheduler start clear first: merges run one at a
  time in the scheduler, so nothing under the root is live then, and a
  statement a timed-out attempt abandoned never writes into its retry's
  directory. Every file operation returns an error rather than raising,
  because the merge runs in the scheduler's process.

  The windows are only as fine as the leading column: a column with few
  distinct values gives few windows, and a window holding one heavy value is
  sorted whole, spilling as the single sort did. Without `width:`, without a
  clustering key, or within one window, a merge takes the paths above.

  ## Compression has to match the writer's, or sealing inflates the data

  `COPY`'s default codec is snappy while `Smolquery.Segments.Writer` writes
  micro-segments with zstd, so taking DuckDB's default made a sealed segment
  *2.85 times larger* than the micro-segments it replaced — measured in
  `bench/sealer.exs`. The sealed tier is where bytes live longest and where object
  storage is billed, so the codec is explicit here and defaults to zstd, matching
  the tier it merges from. The configured value is validated once at boot by
  `Smolquery.StorageService.Runtime.new/1` — never per attempt, where a bad codec
  would crash every re-signalled seal forever.

  ## Row counts come from the manifest, not a read-back

  The merged segment's `row_count` is the sum of its inputs', which the buffer
  already vouched for when it acked them. Counting the output instead would mean
  another round trip to say the same thing, and a disagreement between the two
  would mean the merge silently dropped rows — which `COPY` cannot do.
  """

  require Logger

  alias Smolquery.BufferService.SealConsumer
  alias Smolquery.Catalog
  alias Smolquery.Engine
  alias Smolquery.Identifier
  alias Smolquery.Schema
  alias Smolquery.Segments.FieldIds
  alias Smolquery.Segments.Segment
  alias Smolquery.Segments.Store
  alias Smolquery.StorageService.HotTier
  alias Smolquery.StorageService.Runtime

  @drop_staging_timeout_ms 5_000
  @window_sample_rows 100_000
  @window_column "__merge_window"
  @scratch_options "FORMAT PARQUET, COMPRESSION ZSTD"

  @doc """
  Merges `claim`'s micro-segments into the sealed segment its key names.

  Inputs the manifest no longer lists are skipped rather than fatal: a
  micro-segment whose file vanished is unreadable, so its rows are gone either way,
  and refusing to seal the rest would strand a table's whole tail on one lost file.
  A claim with none of its inputs left is `{:error, :no_inputs}` — there is nothing
  to seal, and writing an empty segment would register emptiness in the catalog as
  though it were the data.

  A claim whose key is not a well-formed segment key, and a manifest entry missing
  its `"url"` or carrying a `"row_count"` that is not a count, are both errors
  before any byte moves: a bad key would fail after the merge already ran, and a
  defaulted row count would be committed to the catalog as though it were true.
  So is an input column the catalog does not declare — see the moduledoc.

  `entries` are the claim's manifest entries, read by the caller rather than
  here. The handoff reads them to check the claim is still live before it merges,
  and reading them a second time made every seal attempt cost the buffer node two
  passes over its whole backlog (T-316).
  """
  @spec run(Runtime.t(), Store.table_ref(), SealConsumer.claim(), [HotTier.entry()]) ::
          {:ok, Segment.t()} | {:error, term()}
  def run(%Runtime{} = runtime, table_ref, claim, entries) do
    with {:ok, key} <- output_key(claim),
         {:ok, inputs} <- inputs(entries, claim) do
      merge(runtime, table_ref, key, inputs)
    end
  end

  @doc """
  Merges already-sealed segments at `urls` into the segment `key` names.

  The compactor's entry point: same projection onto the catalog's declared
  schema, same codec, same write-once put of a deterministic key — only the
  inputs differ. They come from the catalog rather than a hot
  manifest, so their row counts are read from the Parquet footers instead of
  vouched for by a buffer. That is a metadata read, not the read-back the
  moduledoc rules out: a footer's `num_rows` is the file's row count by
  definition, and no data page moves to answer it.

  An empty `urls` is `{:error, :no_inputs}` for the same reason an emptied
  claim is: registering emptiness as though it were data is the one thing a
  merge must never do.

  A caller that already read the inputs' footers passes `row_count:` and
  skips the metadata pass here — the compactor's sizing query reads
  `num_rows` in the same call as the sizes, so re-reading every footer for a
  number the sweep already holds would triple the group's metadata I/O.

  `inputs_per_call:` narrows the staging chunk below the runtime's cap — it
  never widens it. The count cap prices footer round trips, not data volume,
  and the compactor knows its inputs' sizes, so it shrinks the chunk when the
  inputs are large and one full-width chunk would move too many bytes in one
  call.

  `width:` is the group's estimated decoded bytes a row, which the span
  planner already knows. With it, a clustered group of more rows than
  `compact_window_decoded_bytes` holds at that width merges window by
  window instead of in one sort; see the moduledoc.
  """
  @spec compact(Runtime.t(), Store.table_ref(), Store.key(), [String.t()], keyword()) ::
          {:ok, Segment.t()} | {:error, term()}
  def compact(runtime, table_ref, key, urls, opts \\ [])

  def compact(%Runtime{} = _runtime, _table_ref, _key, [], _opts), do: {:error, :no_inputs}

  def compact(%Runtime{} = runtime, table_ref, key, urls, opts) when is_list(urls) do
    runtime = narrow_per_call(runtime, Keyword.get(opts, :inputs_per_call))

    with {:ok, key} <- valid_key(key),
         {:ok, row_count} <- compact_row_count(runtime, urls, Keyword.get(opts, :row_count)),
         {:ok, sources} <- sealed_sources(runtime, table_ref, urls) do
      inputs = with_urls(%{sources: sources, row_count: row_count})
      merge(runtime, table_ref, key, inputs, window_rows(runtime, Keyword.get(opts, :width)))
    end
  end

  defp window_rows(_runtime, nil), do: nil

  defp window_rows(runtime, width) when is_integer(width) and width > 0,
    do: max(div(runtime.compact_window_decoded_bytes, width), 1)

  defp sealed_sources(runtime, table_ref, urls) do
    with {:ok, field_ids} <- file_field_ids(runtime, urls),
         {:ok, snapshots} <- registration_snapshots(runtime, table_ref, urls, field_ids) do
      {:ok,
       Enum.map(
         urls,
         &%{
           "url" => &1,
           "field_ids" => Map.get(field_ids, &1),
           "snapshot" => Map.get(snapshots, &1)
         }
       )}
    end
  end

  defp file_field_ids(runtime, urls) do
    urls
    |> Enum.chunk_every(runtime.merge_inputs_per_call)
    |> Enum.reduce_while({:ok, %{}}, fn chunk, {:ok, acc} ->
      case query(runtime, FieldIds.sql(length(chunk)), chunk, runtime.merge_describe_timeout_ms) do
        {:ok, result} -> {:cont, {:ok, Map.merge(acc, FieldIds.ids_by_file(result.rows))}}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
  end

  defp registration_snapshots(runtime, table_ref, urls, field_ids) do
    if Enum.any?(urls, &is_nil(Map.get(field_ids, &1))) do
      with {:ok, files} <-
             Catalog.segment_files(
               runtime.catalog,
               Smolquery.Partitions.parent(table_ref),
               :current
             ) do
        {:ok, Map.new(files, &{&1.path, &1.snapshot})}
      end
    else
      {:ok, %{}}
    end
  end

  defp narrow_per_call(runtime, nil), do: runtime

  defp narrow_per_call(runtime, per_call) when is_integer(per_call) and per_call > 0,
    do: %{runtime | merge_inputs_per_call: min(per_call, runtime.merge_inputs_per_call)}

  defp compact_row_count(_runtime, _urls, count) when is_integer(count) and count >= 0,
    do: {:ok, count}

  defp compact_row_count(runtime, urls, nil), do: footer_row_count(runtime, urls)

  defp merge(runtime, table_ref, key, inputs, window_rows \\ nil) do
    with {:ok, schema} <-
           Catalog.table_schema(runtime.catalog, Smolquery.Partitions.parent(table_ref)) do
      cond do
        windowed?(schema, inputs, window_rows) ->
          merge_windowed(runtime, key, schema, inputs, window_rows)

        length(inputs.urls) <= runtime.merge_inputs_per_call ->
          merge_direct(runtime, key, schema, inputs)

        true ->
          merge_chunked(runtime, key, schema, inputs)
      end
    end
  end

  defp windowed?(_schema, _inputs, nil), do: false

  defp windowed?(schema, inputs, window_rows),
    do: inputs.row_count > window_rows and Schema.clustering_columns(schema) != []

  defp merge_windowed(runtime, key, schema, inputs, window_rows) do
    dir =
      Path.join(scratch_root(runtime), "#{staging_id(key)}-#{System.unique_integer([:positive])}")

    bounds = Identifier.quote_name!("merge_bounds_#{staging_id(key)}")
    windows = div(inputs.row_count + window_rows - 1, window_rows)

    try do
      with :ok <- fresh_scratch(runtime, dir),
           {:ok, runs} <- localize(runtime, schema, dir, inputs.sources),
           :ok <- window_bounds(runtime, schema, bounds, runs, windows),
           :ok <- partition(runtime, schema, dir, bounds, runs),
           {:ok, parts} <- sort_windows(runtime, schema, dir),
           {:ok, put} <- Store.put(runtime.store, key, &concatenate(runtime, schema, parts, &1)) do
        {:ok, segment(key, put, inputs.row_count)}
      end
    after
      drop_staging(runtime, bounds)
      File.rm_rf(dir)
    end
  end

  @doc """
  The rows one sort of a compaction holds: the group's `row_count`, or one
  window's worth when `width:` would have the merge sort window by window. A
  failure's learned row width is the temp cap over these rows, not over the
  group's, since a windowed merge's spill comes from one window's sort.
  """
  @spec sorted_rows(Runtime.t(), non_neg_integer(), pos_integer() | nil) :: non_neg_integer()
  def sorted_rows(runtime, row_count, width) do
    case window_rows(runtime, width) do
      nil -> row_count
      window_rows -> min(row_count, window_rows)
    end
  end

  @doc """
  Removes every windowed merge's scratch directory under this runtime, left
  by a merge that died without its cleanup running. Merges run one at a time
  in the scheduler, so nothing under the root is live when it is called.
  """
  @spec clear_scratch(Runtime.t()) :: :ok
  def clear_scratch(runtime) do
    File.rm_rf(scratch_root(runtime))
    :ok
  end

  defp scratch_root(runtime),
    do: Path.expand(Path.join([Runtime.spill_root(), "merge_windows", "#{runtime.name}"]))

  defp staging_id(key) do
    {:ok, id} = Store.id(key)
    id
  end

  defp fresh_scratch(runtime, dir) do
    :ok = clear_scratch(runtime)

    case File.mkdir_p(Path.join(dir, "windows")) do
      :ok -> :ok
      {:error, reason} -> {:error, {:merge_scratch_failed, reason, dir}}
    end
  end

  defp localize(runtime, schema, dir, sources) do
    sources
    |> Enum.chunk_every(runtime.merge_inputs_per_call)
    |> Enum.with_index(&{&1, Path.join(dir, "run_#{&2}.parquet")})
    |> collect(fn {chunk, run} ->
      with :ok <- localize_chunk(runtime, schema, chunk, run), do: {:ok, run}
    end)
  end

  defp localize_chunk(runtime, schema, sources, run) do
    with {:ok, select, urls} <- scan_select(runtime, schema, sources) do
      sql =
        "COPY (#{Schema.computed_select(schema, "(#{select})")}) " <>
          "TO $#{length(urls) + 1} (#{@scratch_options})"

      with {:ok, _result} <- query(runtime, sql, urls ++ [run], runtime.merge_staging_timeout_ms),
           do: :ok
    end
  end

  defp window_bounds(runtime, schema, bounds, runs, windows) do
    lead = lead_column(schema)
    fractions = Enum.map_join(1..max(windows - 1, 1), ", ", &"#{&1 / windows}")

    sql = """
    CREATE OR REPLACE TEMPORARY TABLE #{bounds} AS
    SELECT coalesce(list_sort(list_distinct(quantile_disc(#{lead}, [#{fractions}]))), []) AS b
    FROM (SELECT #{lead} FROM #{local_scan(runs)} USING SAMPLE #{@window_sample_rows} ROWS)
    """

    with {:ok, _result} <- query(runtime, sql, runs, runtime.merge_staging_timeout_ms), do: :ok
  end

  defp partition(runtime, schema, dir, bounds, runs) do
    lead = lead_column(schema)

    sql = """
    COPY (
      SELECT rows.*,
             CASE WHEN rows.#{lead} IS NULL THEN len(bounds.b)
                  ELSE len(list_filter(bounds.b, bound -> bound < rows.#{lead})) END
               AS #{@window_column}
      FROM read_parquet($1) AS rows, #{bounds} AS bounds
    ) TO $2 (#{@scratch_options}, PARTITION_BY (#{@window_column}))
    """

    runs
    |> Enum.with_index()
    |> collect(fn {run, index} ->
      params = [run, Path.join([dir, "windows", "run_#{index}"])]

      with {:ok, _result} <- query(runtime, sql, params, runtime.merge_staging_timeout_ms) do
        File.rm(run)
        {:ok, run}
      end
    end)
    |> case do
      {:ok, _runs} -> :ok
      {:error, reason} -> {:error, reason}
    end
  end

  defp lead_column(schema),
    do: schema |> Schema.clustering_columns() |> hd() |> Identifier.quote_name!()

  defp sort_windows(runtime, schema, dir) do
    sql =
      "COPY (SELECT * FROM read_parquet($1, hive_partitioning = false)#{order_by(schema)}) " <>
        "TO $2 (#{@scratch_options})"

    [dir, "windows", "*", "#{@window_column}=*"]
    |> Path.join()
    |> Path.wildcard()
    |> Enum.flat_map(&window_index/1)
    |> Enum.uniq()
    |> Enum.sort()
    |> collect(&sort_window(runtime, sql, dir, &1))
  end

  defp sort_window(runtime, sql, dir, index) do
    window = Path.join([dir, "windows", "*", "#{@window_column}=#{index}"])
    part = Path.join(dir, "part_#{index}.parquet")

    with {:ok, _result} <-
           query(
             runtime,
             sql,
             [Path.join(window, "*.parquet"), part],
             runtime.merge_copy_timeout_ms
           ) do
      window |> Path.wildcard() |> Enum.each(&File.rm_rf/1)
      {:ok, part}
    end
  end

  defp window_index(path) do
    with @window_column <> "=" <> digits <- Path.basename(path),
         {index, ""} <- Integer.parse(digits) do
      [index]
    else
      _other -> []
    end
  end

  defp collect(items, fun, done \\ [])

  defp collect([], _fun, done), do: {:ok, Enum.reverse(done)}

  defp collect([item | rest], fun, done) do
    with {:ok, value} <- fun.(item), do: collect(rest, fun, [value | done])
  end

  defp concatenate(runtime, schema, parts, staged) do
    sql = """
    COPY (SELECT * FROM #{local_scan(parts)})
    TO $#{length(parts) + 1} (#{parquet_options(runtime, schema)}, PRESERVE_ORDER true)
    """

    with {:ok, _result} <- query(runtime, sql, parts ++ [staged], runtime.merge_copy_timeout_ms),
         do: :ok
  end

  defp local_scan(paths), do: "read_parquet([#{placeholders(paths)}])"

  defp merge_direct(runtime, key, schema, inputs) do
    with {:ok, select, urls} <- scan_select(runtime, schema, inputs.sources),
         {:ok, put} <- Store.put(runtime.store, key, &copy(runtime, schema, select, urls, &1)) do
      {:ok, segment(key, put, inputs.row_count)}
    end
  end

  defp merge_chunked(runtime, key, schema, inputs) do
    table = staging_table(key)
    chunks = Enum.chunk_every(inputs.sources, runtime.merge_inputs_per_call)

    try do
      with :ok <- stage_chunks(runtime, schema, table, chunks),
           {:ok, put} <-
             Store.put(runtime.store, key, &copy_staged(runtime, schema, table, &1)) do
        {:ok, segment(key, put, inputs.row_count)}
      end
    after
      drop_staging(runtime, table)
    end
  end

  defp stage_chunks(runtime, schema, table, chunks) do
    chunks
    |> Enum.with_index()
    |> Enum.reduce_while(:ok, fn {chunk, index}, :ok ->
      case stage_chunk(runtime, schema, table, chunk, index) do
        :ok -> {:cont, :ok}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
  end

  defp stage_chunk(runtime, schema, table, sources, index) do
    with {:ok, select, urls} <- scan_select(runtime, schema, sources) do
      sql =
        case index do
          0 -> "CREATE OR REPLACE TEMPORARY TABLE #{table} AS #{select}"
          _ -> "INSERT INTO #{table} #{select}"
        end

      with {:ok, _result} <- query(runtime, sql, urls, runtime.merge_staging_timeout_ms),
           do: :ok
    end
  end

  defp copy_staged(runtime, schema, table, staged) do
    sql = """
    COPY (#{Schema.computed_select(schema, table)}#{order_by(schema)})
    TO $1 (#{parquet_options(runtime, schema)})
    """

    with {:ok, _result} <- query(runtime, sql, [staged], runtime.merge_copy_timeout_ms),
         do: :ok
  end

  defp drop_staging(runtime, table) do
    case query(runtime, "DROP TABLE IF EXISTS #{table}", [], @drop_staging_timeout_ms) do
      {:ok, _result} ->
        :ok

      {:error, reason} ->
        Logger.warning("dropping merge staging table #{table} failed: #{inspect(reason)}")

        :ok
    end
  end

  defp staging_table(key) do
    {:ok, id} = Store.id(key)

    Identifier.quote_name!("merge_#{id}")
  end

  defp output_key(%{keys: [key]}) do
    case valid_key(key) do
      {:ok, key} -> {:ok, key}
      {:error, {:invalid_segment_key, key}} -> {:error, {:invalid_claim_key, key}}
    end
  end

  defp output_key(%{keys: keys}), do: {:error, {:unsupported_claim_keys, keys}}
  defp output_key(claim), do: {:error, {:invalid_claim, claim}}

  defp valid_key(key) do
    case Store.id(key) do
      {:ok, _id} -> {:ok, key}
      :error -> {:error, {:invalid_segment_key, key}}
    end
  end

  defp footer_row_count(runtime, urls) do
    urls
    |> Enum.chunk_every(runtime.merge_inputs_per_call)
    |> Enum.reduce_while({:ok, 0}, fn chunk, {:ok, total} ->
      case footer_row_count_chunk(runtime, chunk) do
        {:ok, count} -> {:cont, {:ok, total + count}}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
  end

  defp footer_row_count_chunk(runtime, urls) do
    sql = "SELECT sum(num_rows)::BIGINT FROM parquet_file_metadata([#{placeholders(urls)}])"

    with {:ok, result} <- query(runtime, sql, urls) do
      case result.rows do
        [[count]] when is_integer(count) -> {:ok, count}
        rows -> {:error, {:unexpected_row_count_result, rows}}
      end
    end
  end

  defp inputs(entries, %{ids: ids}) do
    claimed = MapSet.new(ids)

    entries
    |> Enum.filter(&MapSet.member?(claimed, &1["id"]))
    |> Enum.reduce_while({:ok, %{sources: [], row_count: 0}}, fn entry, {:ok, acc} ->
      case input(entry) do
        {:ok, source, row_count} ->
          {:cont,
           {:ok, %{acc | sources: [source | acc.sources], row_count: acc.row_count + row_count}}}

        {:error, reason} ->
          {:halt, {:error, reason}}
      end
    end)
    |> case do
      {:ok, %{sources: []}} -> {:error, :no_inputs}
      {:ok, inputs} -> {:ok, with_urls(%{inputs | sources: Enum.reverse(inputs.sources)})}
      {:error, reason} -> {:error, reason}
    end
  end

  defp input(%{"url" => url, "row_count" => row_count} = entry)
       when is_binary(url) and is_integer(row_count) and row_count >= 0,
       do: {:ok, %{"url" => url, "field_ids" => Map.get(entry, "field_ids")}, row_count}

  defp input(entry), do: {:error, {:invalid_manifest_entry, entry}}

  defp with_urls(%{sources: sources} = inputs),
    do: Map.put(inputs, :urls, Enum.map(sources, & &1["url"]))

  defp scan_select(runtime, schema, sources) do
    sources
    |> Enum.group_by(&group_key/1, & &1["url"])
    |> Enum.sort_by(fn {{field_ids, snapshot}, _urls} ->
      {field_ids && Enum.sort(field_ids), snapshot}
    end)
    |> Enum.reduce_while({:ok, [], [], 0}, fn {key, urls}, {:ok, selects, groups, offset} ->
      case group_projection(runtime, schema, key, urls) do
        {:ok, projection} ->
          select = "SELECT #{projection} FROM #{scan(urls, offset)}"

          {:cont, {:ok, [select | selects], [urls | groups], offset + length(urls)}}

        {:error, reason} ->
          {:halt, {:error, reason}}
      end
    end)
    |> case do
      {:ok, selects, groups, _offset} ->
        {:ok, selects |> Enum.reverse() |> Enum.join(" UNION ALL BY NAME "),
         groups |> Enum.reverse() |> List.flatten()}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp group_key(source) do
    case Map.get(source, "field_ids") do
      nil -> {nil, Map.get(source, "snapshot")}
      field_ids -> {field_ids, nil}
    end
  end

  defp group_projection(runtime, schema, {nil, snapshot}, urls) do
    with {:ok, columns} <- input_columns(runtime, urls) do
      if is_integer(snapshot),
        do: Schema.projection_as_of(schema, columns, snapshot),
        else: Schema.projection(schema, columns)
    end
  end

  defp group_projection(_runtime, schema, {field_ids, _snapshot}, _urls),
    do: Schema.projection_by_id(schema, field_ids)

  defp input_columns(runtime, urls) do
    sql = "DESCRIBE SELECT * FROM #{scan(urls)}"

    with {:ok, result} <- query(runtime, sql, urls, runtime.merge_describe_timeout_ms) do
      {:ok, Enum.map(result.rows, &hd/1)}
    end
  end

  defp copy(runtime, schema, select, urls, staged) do
    sql = """
    COPY (#{Schema.computed_select(schema, "(#{select})")}#{order_by(schema)})
    TO $#{length(urls) + 1} (#{parquet_options(runtime, schema)})
    """

    with {:ok, _result} <- query(runtime, sql, urls ++ [staged], runtime.merge_copy_timeout_ms),
         do: :ok
  end

  defp parquet_options(runtime, %Schema{} = schema) do
    field_ids =
      case Schema.parquet_field_ids(schema) do
        nil -> ""
        literal -> ", FIELD_IDS #{literal}"
      end

    "FORMAT PARQUET, COMPRESSION #{codec(runtime.compression)}, " <>
      "ROW_GROUP_SIZE #{runtime.seal_row_group_size}#{field_ids}"
  end

  defp order_by(%Schema{} = schema) do
    case Schema.clustering_columns(schema) do
      [] ->
        ""

      columns ->
        " ORDER BY " <>
          Enum.map_join(columns, ", ", fn column ->
            "#{Identifier.quote_name!(column)} ASC NULLS LAST"
          end)
    end
  end

  defp scan(urls, offset \\ 0),
    do: "read_parquet([#{placeholders(urls, offset)}], union_by_name := true)"

  defp placeholders(urls, offset \\ 0),
    do: Enum.map_join((offset + 1)..(offset + length(urls)), ", ", &"$#{&1}")

  defp query(runtime, sql, params, timeout \\ 30_000) do
    case Engine.try_query(Runtime.merge_engine(runtime), sql, params, timeout) do
      {:ok, result} -> {:ok, result}
      {:error, error} -> {:error, {:merge_failed, error}}
    end
  end

  defp codec(:zstd), do: "ZSTD"
  defp codec(:snappy), do: "SNAPPY"
  defp codec(:gzip), do: "GZIP"
  defp codec(:uncompressed), do: "UNCOMPRESSED"

  defp segment(key, put, row_count) do
    {:ok, id} = Store.id(key)

    %Segment{
      id: id,
      key: key,
      path: put.location,
      row_count: row_count,
      byte_size: put.byte_size
    }
  end
end
