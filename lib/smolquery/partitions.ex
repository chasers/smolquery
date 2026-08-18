defmodule Smolquery.Partitions do
  @moduledoc """
  One table's writes as P sibling buffer identities — smolquery's answer to
  "insert on any replica" (PL-5 Stage 2, T-170).

  A table's hot tier is one `TableBuffer` on one ring-assigned node, so a
  single table's ingest is bounded by that node no matter how many nodes the
  cluster has. Partitioning multiplies the identity instead of rebuilding it:
  partition `i` of `{"logs", "events"}` is the ordinary buffer table ref
  `{"logs", "events__p2"}` — buffered, group-committed, replicated, sealed, and
  drained by machinery that never learns partitions exist. Partition 0 is the
  bare ref, so a table's existing hot tier *is* partition 0 and turning
  partitioning on needs no migration.

  Placement is the one thing the ring does learn. Hashing each partition
  independently splits a table's writes P ways but scatters them unevenly over
  the nodes, which the rig measured as a 27% throughput difference between two
  draws of the same six partitions. `Smolquery.BufferService.Ring` reads the
  index back out with `split/1` and walks partition `i` to the `i mod N`-th
  node clockwise from the parent, so P partitions cover
  `min(P, N)` nodes exactly rather than on average.

  Four seams know the difference:

    * the ingest edge picks a partition per batch — sticky by `insertId` so
      retries dedup against the copy that holds their record, round-robin
      otherwise
    * the query planner expands a table into its partition refs and unions
      their hot manifests under the parent
    * the seal handoff maps a partition ref back to the parent for every
      catalog operation, since the sealed tier and the DuckLake catalog know
      only real tables

    * the ring rotates a partition off its parent's position rather than
      hashing it, so a table's partitions land on distinct nodes

  `__p<N>` is reserved at table creation so a user table can never collide
  with another table's partition. The reservation guards new creates only: a
  table that already existed with a matching name is read as a partition from
  this release on — its owner rotates off its old node and the seal handoff
  maps it to a parent it never had. Audit the catalog for `__p<N>` names and
  rename them before deploying partitioned writes.

  ## Where the count comes from

  `write_partitions` is the deployment-wide default. A table can carry its
  own count in catalog metadata (T-304, `Smolquery.Catalog.partitions/2`),
  which rides `Smolquery.Schema` to every reader; `count/2` resolves the two
  by taking the maximum, so an effective count only ever rises.

  Raising a count is safe online because the lag runs in one direction: the
  query planner reads the catalog on every plan, while a writer's cached
  schema may trail by one `schema_cache_ttl_ms` — a reader expanding more
  partitions than a writer fills answers long, never short, and an unfilled
  partition's manifest is empty. Lowering is not supported: partitions past
  the count still hold hot rows a reader would no longer expand. The catalog
  write itself is monotonic (a lower count is a no-op), so concurrent raises
  converge on the maximum and no caller can lower a stored count.

  Two caveats survive from the deployment-wide days. Lowering the
  *default* still needs the fleet at rest for every table without its own
  catalog count — those tables have no floor but the default. And during the
  raise's cache window, two ingest nodes can hash one `insertId` with
  different counts, so a retry that straddles a count change is
  at-least-once, exactly like a retry that straddles a ring join.
  """

  alias Smolquery.Segments.Store

  @separator "__p"
  @reserved ~r/__p[1-9][0-9]*$/
  @indexed ~r/^(.*)__p([1-9][0-9]*)$/

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
  The partition count in effect for a table: its own catalog count when one
  is set, never below the deployment's `write_partitions`.

  The maximum keeps a catalog count raise-only against the default. A table
  with no catalog count follows the deployment default alone — lowering that
  default is still a fleet-at-rest operation for such tables, exactly as the
  moduledoc describes.
  """
  @spec count(pos_integer() | nil, pos_integer()) :: pos_integer()
  def count(nil, configured), do: configured
  def count(table_count, configured), do: max(table_count, configured)

  @doc """
  The most partitions any table may declare.

  Each partition costs a `TableBuffer`, a manifest, a claim, and a
  hot-manifest fetch per query plan per manifest URL, so the count is capped
  well past any useful `min(P, N)` and well inside the catalog column's
  INTEGER range. Both the API and the catalog refuse a larger value.
  """
  @spec max_count() :: pos_integer()
  def max_count, do: 64

  @doc """
  The table `ref` carries rows for: itself, unless it names a partition.
  """
  @spec parent(Store.table_ref()) :: Store.table_ref()
  def parent(table_ref), do: table_ref |> split() |> elem(0)

  @doc """
  `ref` as the table it belongs to and its index within that table.

  A ref naming no partition is partition 0 of itself, which is the identity
  `refs/2` already gives the parent. The index is what
  `Smolquery.BufferService.Ring` rotates placement by, so this is the one
  reading of the `__p<N>` convention: a second reading elsewhere could place a
  partition on one node and then look for it on another.

  Total, because the ring routes on an opaque term and hands whatever it was
  given straight here. Anything that is not a table ref is partition 0 of
  itself and routes exactly as it did before partitions existed.
  """
  @spec split(Store.table_ref() | term()) :: {Store.table_ref() | term(), non_neg_integer()}
  def split({dataset, table} = table_ref) when is_binary(table) do
    case Regex.run(@indexed, table) do
      [_, parent, index] -> {{dataset, parent}, String.to_integer(index)}
      nil -> {table_ref, 0}
    end
  end

  def split(key), do: {key, 0}

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
