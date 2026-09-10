defmodule Smolquery.BufferService.Backlog do
  @moduledoc """
  The valve on ingest that closes when a node's unsealed backlog is too deep
  to survive (T-457).

  A buffer node accepted commits at whatever rate a client sent them, no
  matter how far behind its own sealing was. The in-flight byte budget
  (`SmolqueryApi.Admission`) and the accumulator bounds (`max_buffered_rows`,
  `max_buffered_bytes`) bound what is *arriving*; nothing bounded what had
  arrived and not yet sealed. One stalled seal path (T-450) grew a node's
  hot manifest to 331,000 unsealed entries, and every downstream failure
  followed from that one unbounded number: the buffer pods outgrew a 4 Gi
  limit, then 6 Gi; a query pod died on any preview or `count(*)` of the
  tables (T-449, T-456); and the boot peak, which scales with the backlog at
  roughly `Runtime.boot_bytes_per_entry/0` per entry, meant a pod that
  restarted could no longer adopt its own backlog, so it crash-looped
  instead of running the heal that would have drained it.

  `admit/2` runs on the ingest side of a write, before the batch reaches
  the table's buffer process (`Smolquery.BufferService.Endpoint`), and
  reads two O(1) counters `Smolquery.BufferService.HotManifest.depth/2`
  keeps: the node's unsealed entries and their bytes. Either at or past its
  ceiling — `backlog_max_entries`, derived from the container's memory
  limit so a node can always adopt what it holds, and `backlog_max_bytes`,
  off unless configured — refuses the commit with `{:error, {:backlog_full,
  refusal}}`, which the ingest edge answers as the same retryable 429 every
  other shed-load refusal is, naming the table and the depth. Nothing about
  the seal path reads this valve: sealing keeps draining a node that
  refuses, which is the point, and a replicated shipment from a table's
  owner is never refused, so replicas cannot diverge.
  """

  alias Smolquery.BufferService.HotManifest
  alias Smolquery.BufferService.Runtime

  @typedoc """
  What a refusal names: the node's unsealed depth and the table's share of
  it, and which ceiling was reached.
  """
  @type refusal :: %{
          table_ref: Smolquery.Segments.Store.table_ref(),
          entries: non_neg_integer(),
          bytes: non_neg_integer(),
          table_entries: non_neg_integer(),
          table_bytes: non_neg_integer(),
          max_entries: pos_integer() | nil,
          max_bytes: pos_integer() | nil,
          limit: :entries | :bytes
        }

  @doc """
  `:ok` when the node can take one more commit for `table_ref`, else the
  refusal.

  A runtime with neither ceiling (a `Runtime.new/1` nobody resolved through
  `Runtime.with_backlog_ceiling/1`) admits everything.
  """
  @spec admit(Runtime.t(), Smolquery.Segments.Store.table_ref()) ::
          :ok | {:error, {:backlog_full, refusal()}}
  def admit(%Runtime{backlog_max_entries: nil, backlog_max_bytes: nil}, _table_ref), do: :ok

  def admit(%Runtime{} = runtime, table_ref) do
    %{entries: entries, bytes: bytes} = HotManifest.depth(runtime.manifest, :node)

    case reached(entries, runtime.backlog_max_entries, bytes, runtime.backlog_max_bytes) do
      nil ->
        :ok

      limit ->
        table = HotManifest.depth(runtime.manifest, table_ref)

        {:error,
         {:backlog_full,
          %{
            table_ref: table_ref,
            entries: entries,
            bytes: bytes,
            table_entries: table.entries,
            table_bytes: table.bytes,
            max_entries: runtime.backlog_max_entries,
            max_bytes: runtime.backlog_max_bytes,
            limit: limit
          }}}
    end
  end

  defp reached(entries, max_entries, bytes, max_bytes) do
    cond do
      is_integer(max_entries) and entries >= max_entries -> :entries
      is_integer(max_bytes) and bytes >= max_bytes -> :bytes
      true -> nil
    end
  end

  @doc """
  The refusal as the sentence a client reads in the 429: the node's depth,
  the table's share of it, and the ceiling reached.
  """
  @spec message(refusal()) :: String.t()
  def message(%{table_ref: {dataset, table}, limit: :entries} = refusal) do
    "hot tier backlog too deep on the buffer node: #{refusal.entries} unsealed " <>
      "micro-segments (#{refusal.table_entries} of them #{dataset}.#{table}'s) against a " <>
      "ceiling of #{refusal.max_entries}; retry later"
  end

  def message(%{table_ref: {dataset, table}, limit: :bytes} = refusal) do
    "hot tier backlog too deep on the buffer node: #{refusal.bytes} unsealed bytes " <>
      "(#{refusal.table_bytes} of them #{dataset}.#{table}'s) against a ceiling of " <>
      "#{refusal.max_bytes}; retry later"
  end
end
