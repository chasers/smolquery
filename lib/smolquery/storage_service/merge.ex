defmodule Smolquery.StorageService.Merge do
  @moduledoc """
  Turns a claim's micro-segments into one sealed segment.

  The merge runs inside DuckDB and writes straight to the store's staging path:

      COPY (SELECT projection FROM read_parquet([urls], union_by_name := true)) TO staged

  So no segment's bytes ever become an Elixir term. That matters more here than
  anywhere else in the system — a sealed segment is the largest object smolquery
  writes — and it is the same reason `Smolquery.Segments.Writer` hands Polars a
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
  buffer's claim byte valve is what bounds that cost. The staging
  hop's cost is memory: the claim's rows live in the engine, spilling to the
  connection's `temp_directory` past its limit, until the final `COPY`.

  The final `COPY` gets five minutes rather than the engine's 30 s default —
  on both paths (T-261). The staged variant reads local data, so its duration
  scales with the backlog's bytes, not with per-input `httpfs` latency; a
  30 s ceiling would decide how large a backlog may seal — the T-244 trap
  with the bound moved. The direct variant earns the same budget the other
  way around: its inputs are count-capped, not byte- or row-capped, and a
  row-capped compaction group of twelve inputs sorts millions of rows in one
  read-sort-write `COPY`. On the default budget those groups died at ~30 s
  and re-planned identically every sweep — while a larger group of the same
  data succeeded, purely because its input count pushed it onto the chunked
  path whose budgets were real. A staging chunk
  gets two minutes for a similar reason: the count cap bounds its inputs, not
  its bytes, and twelve compaction inputs near `compact_below_bytes` move
  hundreds of megabytes on a slow link. An `after` block drops the staging
  table, so an error or an exit on the way out does not strand the staged
  rows on the shared connection. The drop's own call gets five seconds, not
  the 30 s default: on a wedged connection the drop queues behind the
  abandoned statement anyway — waiting longer only delays the failure
  reaching the compactor's engine recycle — and the statement still executes
  once the connection drains, so the cleanup is not lost with the wait. A
  drop that itself fails logs a warning, and a seal retry's
  `CREATE OR REPLACE` clears what the drop could not.

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

  DuckDB offers `allow_missing => true` on registration instead, which would take
  the narrow file as it is. That relaxes the check for every file the catalog ever
  accepts, to fix a file this module is the one writing — the projection is the
  narrower fix, and it settles column order and type at the same time rather than
  only presence.

  The reverse — a column the inputs carry and the catalog does not, because the
  buffer accepted a wider batch before the table was altered — is rejected here as
  `{:error, {:undeclared_columns, names}}`, before any byte moves. Registration
  would reject it too (`Column "…" exists in file … but was not found in table`),
  and projecting it away instead would drop a column of acked rows silently, which
  is the one outcome worse than a stuck claim. Widening the table is the catalog's
  call to make, not the sealer's — the same reason a table the catalog does not
  hold is an error here rather than something to create.

  ## The output is already named

  The key comes from the claim, not from this module. It was derived from the
  claim's inputs when the claim was frozen, so a retry writes the same key — an
  idempotent overwrite of identical rows rather than a second segment. That is
  what makes a crashed merge free to retry, and it is why this module never
  generates an id.

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

  `ROW_GROUP_SIZE` is set only when clustering columns are non-empty — the same
  `Schema.clustering_columns/1` gate as `ORDER BY`. Row groups buy pruning only
  when the seal sorts: tight min/max bounds on the clustering key let scans skip
  groups. Without that sort the knob changes nothing about what a scan can prune
  and still splits the file into smaller groups, which cost more metadata and
  compress worse — measured at +25% sealed size on an unclustered table in
  `bench/sealer.exs` (64 micro-segments × 10k rows). Unclustered tables therefore
  keep DuckDB's default row group. The value is configured once at boot as
  `seal_row_group_size` on `Smolquery.StorageService.Runtime`.

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
  alias Smolquery.Segments.Segment
  alias Smolquery.Segments.Store
  alias Smolquery.StorageService.HotTier
  alias Smolquery.StorageService.Runtime

  @staged_copy_timeout_ms 300_000
  @stage_chunk_timeout_ms 120_000
  @drop_staging_timeout_ms 5_000

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
  """
  @spec run(Runtime.t(), Store.table_ref(), SealConsumer.claim()) ::
          {:ok, Segment.t()} | {:error, term()}
  def run(%Runtime{} = runtime, table_ref, claim) do
    with {:ok, key} <- output_key(claim),
         {:ok, entries} <- HotTier.manifest(runtime, table_ref, claim[:origin]),
         {:ok, inputs} <- inputs(entries, claim) do
      merge(runtime, table_ref, key, inputs)
    end
  end

  @doc """
  Merges already-sealed segments at `urls` into the segment `key` names.

  The compactor's entry point: same projection onto the catalog's declared
  schema, same codec, same idempotent overwrite of a deterministic key —
  only the inputs differ. They come from the catalog rather than a hot
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
  """
  @spec compact(Runtime.t(), Store.table_ref(), Store.key(), [String.t()], keyword()) ::
          {:ok, Segment.t()} | {:error, term()}
  def compact(runtime, table_ref, key, urls, opts \\ [])

  def compact(%Runtime{} = _runtime, _table_ref, _key, [], _opts), do: {:error, :no_inputs}

  def compact(%Runtime{} = runtime, table_ref, key, urls, opts) when is_list(urls) do
    runtime = narrow_per_call(runtime, Keyword.get(opts, :inputs_per_call))

    with {:ok, key} <- valid_key(key),
         {:ok, row_count} <- compact_row_count(runtime, urls, Keyword.get(opts, :row_count)) do
      merge(runtime, table_ref, key, %{urls: urls, row_count: row_count})
    end
  end

  defp narrow_per_call(runtime, nil), do: runtime

  defp narrow_per_call(runtime, per_call) when is_integer(per_call) and per_call > 0,
    do: %{runtime | merge_inputs_per_call: min(per_call, runtime.merge_inputs_per_call)}

  defp compact_row_count(_runtime, _urls, count) when is_integer(count) and count >= 0,
    do: {:ok, count}

  defp compact_row_count(runtime, urls, nil), do: footer_row_count(runtime, urls)

  defp merge(runtime, table_ref, key, inputs) do
    with {:ok, schema} <-
           Catalog.table_schema(runtime.catalog, Smolquery.Partitions.parent(table_ref)) do
      if length(inputs.urls) <= runtime.merge_inputs_per_call do
        merge_direct(runtime, key, schema, inputs)
      else
        merge_chunked(runtime, key, schema, inputs)
      end
    end
  end

  defp merge_direct(runtime, key, schema, inputs) do
    with {:ok, projection} <- projection(runtime, schema, inputs.urls),
         {:ok, put} <-
           Store.put(runtime.store, key, &copy(runtime, schema, projection, inputs.urls, &1)) do
      {:ok, segment(key, put, inputs.row_count)}
    end
  end

  defp merge_chunked(runtime, key, schema, inputs) do
    table = staging_table(key)
    chunks = Enum.chunk_every(inputs.urls, runtime.merge_inputs_per_call)

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

  defp stage_chunk(runtime, schema, table, urls, index) do
    with {:ok, projection} <- projection(runtime, schema, urls) do
      sql =
        case index do
          0 ->
            "CREATE OR REPLACE TEMPORARY TABLE #{table} AS " <>
              "SELECT #{projection} FROM #{scan(urls)}"

          _ ->
            "INSERT INTO #{table} SELECT #{projection} FROM #{scan(urls)}"
        end

      with {:ok, _result} <- query(runtime, sql, urls, @stage_chunk_timeout_ms), do: :ok
    end
  end

  defp copy_staged(runtime, schema, table, staged) do
    sql = """
    COPY (SELECT * FROM #{table}#{order_by(schema)})
    TO $1 (FORMAT PARQUET, COMPRESSION #{codec(runtime.compression)}#{row_group_option(schema, runtime)})
    """

    with {:ok, _result} <- query(runtime, sql, [staged], @staged_copy_timeout_ms), do: :ok
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
    |> Enum.reduce_while({:ok, %{urls: [], row_count: 0}}, fn entry, {:ok, acc} ->
      case input(entry) do
        {:ok, url, row_count} ->
          {:cont, {:ok, %{acc | urls: [url | acc.urls], row_count: acc.row_count + row_count}}}

        {:error, reason} ->
          {:halt, {:error, reason}}
      end
    end)
    |> case do
      {:ok, %{urls: []}} -> {:error, :no_inputs}
      {:ok, inputs} -> {:ok, %{inputs | urls: Enum.reverse(inputs.urls)}}
      {:error, reason} -> {:error, reason}
    end
  end

  defp input(%{"url" => url, "row_count" => row_count})
       when is_binary(url) and is_integer(row_count) and row_count >= 0,
       do: {:ok, url, row_count}

  defp input(entry), do: {:error, {:invalid_manifest_entry, entry}}

  defp projection(runtime, schema, urls) do
    with {:ok, columns} <- input_columns(runtime, urls) do
      case columns -- Schema.names(schema) do
        [] -> Schema.projection(schema, columns)
        undeclared -> {:error, {:undeclared_columns, undeclared}}
      end
    end
  end

  defp input_columns(runtime, urls) do
    sql = "DESCRIBE SELECT * FROM #{scan(urls)}"

    with {:ok, result} <- query(runtime, sql, urls) do
      {:ok, Enum.map(result.rows, &hd/1)}
    end
  end

  defp copy(runtime, schema, projection, urls, staged) do
    sql = """
    COPY (SELECT #{projection} FROM #{scan(urls)}#{order_by(schema)})
    TO $#{length(urls) + 1} (FORMAT PARQUET, COMPRESSION #{codec(runtime.compression)}#{row_group_option(schema, runtime)})
    """

    with {:ok, _result} <- query(runtime, sql, urls ++ [staged], @staged_copy_timeout_ms),
         do: :ok
  end

  defp row_group_option(%Schema{} = schema, %Runtime{} = runtime) do
    case Schema.clustering_columns(schema) do
      [] -> ""
      _ -> ", ROW_GROUP_SIZE #{runtime.seal_row_group_size}"
    end
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

  defp scan(urls), do: "read_parquet([#{placeholders(urls)}], union_by_name := true)"

  defp placeholders(urls), do: Enum.map_join(1..length(urls), ", ", &"$#{&1}")

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
