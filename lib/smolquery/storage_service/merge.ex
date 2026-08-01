defmodule Smolquery.StorageService.Merge do
  @moduledoc """
  Turns a claim's micro-segments into one sealed segment.

  The merge runs inside DuckDB and writes straight to the store's staging path:

      COPY (SELECT * FROM read_parquet([urls], union_by_name := true)) TO staged

  So no segment's bytes ever become an Elixir term. That matters more here than
  anywhere else in the system — a sealed segment is the largest object smolquery
  writes — and it is the same reason `Smolquery.Segments.Writer` hands Polars a
  path rather than building a binary.

  `union_by_name` is what makes additive schema evolution work at the file level:
  micro-segments written before and after a column was added merge into one segment
  carrying the union, which is what the buffer's flush-on-schema-change already set
  up.

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

  ## Row counts come from the manifest, not a read-back

  The merged segment's `row_count` is the sum of its inputs', which the buffer
  already vouched for when it acked them. Counting the output instead would mean
  another round trip to say the same thing, and a disagreement between the two
  would mean the merge silently dropped rows — which `COPY` cannot do.
  """

  alias Smolquery.BufferService.SealConsumer
  alias Smolquery.Engine
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
  """
  @spec run(Runtime.t(), Store.table_ref(), SealConsumer.claim()) ::
          {:ok, Segment.t()} | {:error, term()}
  def run(%Runtime{} = runtime, table_ref, claim) do
    with {:ok, entries} <- HotTier.manifest(runtime, table_ref),
         {:ok, key} <- output_key(claim),
         {:ok, inputs} <- inputs(entries, claim),
         {:ok, put} <- Store.put(runtime.store, key, &copy(runtime, inputs.urls, &1)) do
      {:ok, segment(key, put, inputs.row_count)}
    end
  end

  defp output_key(%{keys: [key]}), do: {:ok, key}
  defp output_key(%{keys: keys}), do: {:error, {:unsupported_claim_keys, keys}}
  defp output_key(claim), do: {:error, {:invalid_claim, claim}}

  defp inputs(entries, %{ids: ids}) do
    claimed = MapSet.new(ids)

    entries
    |> Enum.filter(&MapSet.member?(claimed, &1["id"]))
    |> Enum.reduce(%{urls: [], row_count: 0}, fn entry, acc ->
      %{
        acc
        | urls: [entry["url"] | acc.urls],
          row_count: acc.row_count + (entry["row_count"] || 0)
      }
    end)
    |> case do
      %{urls: []} -> {:error, :no_inputs}
      inputs -> {:ok, %{inputs | urls: Enum.reverse(inputs.urls)}}
    end
  end

  defp copy(runtime, urls, staged) do
    count = length(urls)
    placeholders = Enum.map_join(1..count, ", ", &"$#{&1}")

    sql = """
    COPY (SELECT * FROM read_parquet([#{placeholders}], union_by_name := true))
    TO $#{count + 1} (FORMAT PARQUET)
    """

    case Engine.query(Runtime.engine(runtime.name), sql, urls ++ [staged]) do
      {:ok, _result} -> :ok
      {:error, error} -> {:error, {:merge_failed, Exception.message(error)}}
    end
  end

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
