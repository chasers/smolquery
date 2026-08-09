defmodule Smolquery.BufferService.Ring do
  @moduledoc """
  Which buffer node owns a routing key.

  The buffer service is the write path's serialization point: exactly one node
  accumulates a given table's rows, which is what makes group commit possible at
  all. This module is the mapping that decides which node that is, and the reason
  it is consistent hashing rather than modulo arithmetic is the fleet change —
  adding or losing a node should move a `1/n` share of keys, not reshuffle
  everything and hand every table's unsealed tail to a different node at once.

  ## The key is opaque, except for partitions

  `owner/2` hashes whatever term it is given, so anything that is not a table
  ref routes exactly as it always did.

  A table ref naming a partition is the one exception, and it is here rather
  than at the call site for a measured reason. Hashing each of a table's P
  partitions independently splits its writes P ways but leaves their *placement*
  to chance: at P partitions over N nodes only `N × (1 − ((N−1)/N)^P)` nodes own
  anything on average, so three partitions over three nodes covered 2.11 of them
  and the rig measured a 4-1-1 draw against a 3-2-1 draw as a 27% throughput
  difference. Partition `i` therefore takes the `i mod N`-th distinct node
  clockwise from its *parent* ref, which spreads a table exactly rather than
  probably, and reduces to the old behaviour at partition 0.

  Dealing trades the `1/n` invariant for that exactness: the rotation is
  positional over the live node count, so a membership change can move a
  partition between two surviving nodes — more churn than an independent hash
  would cost. Each move is fenced and drained like any ring change
  (`Smolquery.BufferService.RingEpoch`, the adopter, the member-wide manifest
  fan-out), and an `insertId` retry straddling one is at-least-once, exactly
  as `Smolquery.Partitions` documents for a count change.

  The rotation lives in this module, not in `BufferService.Routing`, because
  `BufferService.RingEpoch` asks `owner/2` and `own?/2` directly whether this
  node owns a ref. Two placement rules would let one node accept a commit that
  another believes it owns, which loses writes rather than slowing them.
  `successors/3` rotates by the same offset, so a partition's replica set still
  begins at that partition's own owner.

  ## Virtual nodes

  Each node is placed at 128 points around the hash space rather than one, so
  a small fleet still divides the space evenly; with one point per node, three
  nodes would split it into three arbitrary arcs. Lookup binary-searches the
  sorted points, so it stays cheap on the write path as the fleet grows.

  Milestone 3 runs a static single-node ring. Ring changes, rebalancing, and the
  handoff of an unsealed tail are Milestone 8.

  ## Usage

      {:ok, ring} = Smolquery.BufferService.Ring.new([:"buffer1@host", :"buffer2@host"])

      Smolquery.BufferService.Ring.owner(ring, {"analytics", "events"})
      #=> :"buffer2@host"

  """

  alias Smolquery.Partitions

  @enforce_keys [:nodes, :points]
  defstruct [:nodes, :points]

  @type t :: %__MODULE__{nodes: [node()], points: tuple()}

  @typedoc """
  Anything that identifies a unit of ownership — a table ref today.
  """
  @type routing_key :: term()

  @points_per_node 128
  @space 4_294_967_296

  @doc """
  A ring over `nodes`.

  Duplicates collapse and order does not matter: the same node set always
  produces the same ring, on every node in the cluster.
  """
  @spec new([node()]) :: {:ok, t()} | {:error, :empty_ring}
  def new(nodes) when is_list(nodes) do
    case nodes |> Enum.uniq() |> Enum.sort() do
      [] -> {:error, :empty_ring}
      nodes -> {:ok, %__MODULE__{nodes: nodes, points: points(nodes)}}
    end
  end

  @doc """
  Same as `new/1` but raises on an empty node list.
  """
  @spec new!([node()]) :: t()
  def new!(nodes) do
    case new(nodes) do
      {:ok, ring} -> ring
      {:error, reason} -> raise ArgumentError, "invalid ring: #{inspect(reason)}"
    end
  end

  @doc """
  The ring's nodes, sorted.
  """
  @spec nodes(t()) :: [node()]
  def nodes(%__MODULE__{nodes: nodes}), do: nodes

  @doc """
  The node owning `key`.
  """
  @spec owner(t(), routing_key()) :: node()
  def owner(%__MODULE__{} = ring, key), do: ring |> successors(key, 1) |> hd()

  @doc """
  Whether this node owns `key`.
  """
  @spec own?(t(), routing_key()) :: boolean()
  def own?(%__MODULE__{} = ring, key), do: owner(ring, key) == node()

  @doc """
  The first `count` distinct nodes clockwise from `key`'s position — the owner
  first, then its successors.

  This is the replica set (PL-5): followers are the next
  `replication_factor - 1` distinct nodes after the owner, so failover
  ownership and replica location are one computation — the node the ring
  promotes is a node that already holds the data. Fewer than `count` nodes in
  the ring returns them all.
  """
  @spec successors(t(), routing_key(), pos_integer()) :: [node()]
  def successors(%__MODULE__{nodes: [node]}, _key, _count), do: [node]

  def successors(%__MODULE__{nodes: nodes} = ring, key, count) do
    total = length(nodes)
    {anchor, index} = Partitions.split(key)
    wanted = min(count, total)

    case rem(index, total) do
      0 ->
        clockwise(ring, anchor, wanted)

      rotation ->
        ring |> clockwise(anchor, total) |> rotate(rotation) |> Enum.take(wanted)
    end
  end

  defp clockwise(%__MODULE__{points: points}, key, count) do
    hash = :erlang.phash2(key, @space)
    size = tuple_size(points)

    collect(points, size, search(points, hash, 0, size), count, [])
  end

  defp rotate(nodes, rotation), do: Enum.drop(nodes, rotation) ++ Enum.take(nodes, rotation)

  defp collect(_points, _size, _index, 0, acc), do: Enum.reverse(acc)

  defp collect(points, size, index, remaining, acc) do
    {_hash, node} = elem(points, rem(index, size))

    if node in acc do
      collect(points, size, index + 1, remaining, acc)
    else
      collect(points, size, index + 1, remaining - 1, [node | acc])
    end
  end

  defp points(nodes) do
    for node <- nodes, index <- 0..(@points_per_node - 1) do
      {:erlang.phash2({node, index}, @space), node}
    end
    |> Enum.sort()
    |> List.to_tuple()
  end

  defp search(points, hash, low, high) when low < high do
    middle = div(low + high, 2)
    {candidate, _node} = elem(points, middle)

    if candidate < hash do
      search(points, hash, middle + 1, high)
    else
      search(points, hash, low, middle)
    end
  end

  defp search(_points, _hash, low, _high), do: low
end
