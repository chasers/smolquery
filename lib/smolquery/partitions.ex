defmodule Smolquery.Partitions do
  @moduledoc """
  One table's writes as P sibling buffer identities — smolquery's answer to
  "insert on any replica" (PL-5 Stage 2, T-170).

  A table's hot tier is one `TableBuffer` on one ring-assigned node, so a
  single table's ingest is bounded by that node no matter how many nodes the
  cluster has. Partitioning multiplies the identity instead of rebuilding it:
  partition `i` of `{"logs", "events"}` is the ordinary buffer table ref
  `{"logs", "events__p2"}` — hashed onto the ring like any table, buffered,
  group-committed, replicated, sealed, and drained by machinery that never
  learns partitions exist. Partition 0 is the bare ref, so a table's existing
  hot tier *is* partition 0 and turning partitioning on needs no migration.

  Only three seams know the difference:

    * the ingest edge picks a partition per batch — sticky by `insertId` so
      retries dedup against the copy that holds their record, round-robin
      otherwise
    * the query planner expands a table into its partition refs and unions
      their hot manifests under the parent
    * the seal handoff maps a partition ref back to the parent for every
      catalog operation, since the sealed tier and the DuckLake catalog know
      only real tables

  `__p<N>` is reserved at table creation so a user table can never collide
  with another table's partition.

  ## The count is deployment-wide, and changing it has a window

  The ingest and query runtimes must agree on the count: a reader expanding
  fewer partitions than a writer filled would answer short. Change
  `write_partitions` only with the fleet at rest (a rollout applies it
  everywhere at once), and expect `insertId` retries that straddle a count
  change to be at-least-once, exactly like a retry that straddles a ring
  join. A count in a table's own catalog metadata is the follow-up that
  removes both caveats.
  """

  alias Smolquery.Segments.Store

  @separator "__p"
  @reserved ~r/__p[1-9][0-9]*$/

  @doc """
  Every buffer ref carrying `table_ref`'s rows: the parent (partition 0)
  first, then `__p1`..`__p<count-1>`.
  """
  @spec refs(Store.table_ref(), pos_integer()) :: [Store.table_ref()]
  def refs(table_ref, 1), do: [table_ref]

  def refs({dataset, table} = table_ref, count) when count > 1 do
    [
      table_ref
      | Enum.map(1..(count - 1), &{dataset, table <> @separator <> Integer.to_string(&1)})
    ]
  end

  @doc """
  The buffer ref one batch writes to.

  A batch carrying its idempotency key hashes to a stable partition, so a
  retry lands where the original's dedup record lives. Everything else
  round-robins on a node-wide counter.
  """
  @spec write_ref(Store.table_ref(), pos_integer(), String.t() | nil) :: Store.table_ref()
  def write_ref(table_ref, 1, _batch_id), do: table_ref

  def write_ref(table_ref, count, nil),
    do: at(table_ref, rem(:atomics.add_get(counter(), 1, 1), count))

  def write_ref(table_ref, count, batch_id),
    do: at(table_ref, :erlang.phash2(batch_id, count))

  @doc """
  The table `ref` carries rows for: itself, unless it names a partition.
  """
  @spec parent(Store.table_ref()) :: Store.table_ref()
  def parent({dataset, table} = table_ref) do
    case Regex.split(@reserved, table) do
      [^table] -> table_ref
      [parent, ""] -> {dataset, parent}
    end
  end

  @doc """
  Whether `table_id` names a partition — reserved, so `create table` refuses
  it rather than colliding with another table's hot tier.
  """
  @spec reserved?(String.t()) :: boolean()
  def reserved?(table_id) when is_binary(table_id), do: Regex.match?(@reserved, table_id)

  defp at(table_ref, 0), do: table_ref

  defp at({dataset, table}, index),
    do: {dataset, table <> @separator <> Integer.to_string(index)}

  defp counter do
    case :persistent_term.get({__MODULE__, :counter}, nil) do
      nil ->
        counter = :atomics.new(1, [])
        :persistent_term.put({__MODULE__, :counter}, counter)
        counter

      counter ->
        counter
    end
  end
end
