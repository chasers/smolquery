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

  alias Smolquery.BufferService.SealConsumer
  alias Smolquery.Catalog
  alias Smolquery.Engine
  alias Smolquery.Schema
  alias Smolquery.Segments.Segment
  alias Smolquery.Segments.Store
  alias Smolquery.StorageService.HotTier
  alias Smolquery.StorageService.Runtime

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
         {:ok, entries} <- HotTier.manifest(runtime, table_ref),
         {:ok, inputs} <- inputs(entries, claim),
         {:ok, schema} <- Catalog.table_schema(runtime.catalog, table_ref),
         {:ok, projection} <- projection(runtime, schema, inputs.urls),
         {:ok, put} <-
           Store.put(runtime.store, key, &copy(runtime, projection, inputs.urls, &1)) do
      {:ok, segment(key, put, inputs.row_count)}
    end
  end

  defp output_key(%{keys: [key]}) do
    case Store.id(key) do
      {:ok, _id} -> {:ok, key}
      :error -> {:error, {:invalid_claim_key, key}}
    end
  end

  defp output_key(%{keys: keys}), do: {:error, {:unsupported_claim_keys, keys}}
  defp output_key(claim), do: {:error, {:invalid_claim, claim}}

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

  defp copy(runtime, projection, urls, staged) do
    sql = """
    COPY (SELECT #{projection} FROM #{scan(urls)})
    TO $#{length(urls) + 1} (FORMAT PARQUET, COMPRESSION #{codec(runtime.compression)})
    """

    with {:ok, _result} <- query(runtime, sql, urls ++ [staged]), do: :ok
  end

  defp scan(urls) do
    placeholders = Enum.map_join(1..length(urls), ", ", &"$#{&1}")

    "read_parquet([#{placeholders}], union_by_name := true)"
  end

  defp query(runtime, sql, params) do
    case Engine.query(Runtime.engine(runtime.name), sql, params) do
      {:ok, result} -> {:ok, result}
      {:error, error} -> {:error, {:merge_failed, Exception.message(error)}}
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
