defmodule Smolquery.BufferService.Ring do
  @moduledoc """
  Which buffer node owns a routing key.

  The buffer service is the write path's serialization point: exactly one node
  accumulates a given table's rows, which is what makes group commit possible at
  all. This module is the mapping that decides which node that is, and the reason
  it is consistent hashing rather than modulo arithmetic is the fleet change —
  adding or losing a node should move a `1/n` share of keys, not reshuffle
  everything and hand every table's unsealed tail to a different node at once.

  ## The key is opaque

  `owner/2` hashes whatever term it is given. Today callers pass a table ref,
  `{dataset, table}`, so one table lives on one node. That caps a single table's
  ingest at one node's throughput, and the fix when a real workload hits the cap
  is to route on `{dataset, table, partition}` instead — a change at the call
  site, with nothing here to rewrite. Keeping the key opaque is what buys that.

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
  def new([]), do: {:error, :empty_ring}

  def new(nodes) when is_list(nodes) do
    nodes = nodes |> Enum.uniq() |> Enum.sort()

    points =
      for node <- nodes, index <- 0..(@points_per_node - 1) do
        {:erlang.phash2({node, index}, @space), node}
      end

    {:ok, %__MODULE__{nodes: nodes, points: points |> Enum.sort() |> List.to_tuple()}}
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
  def owner(%__MODULE__{nodes: [node]}, _key), do: node

  def owner(%__MODULE__{points: points}, key) do
    hash = :erlang.phash2(key, @space)
    size = tuple_size(points)
    index = search(points, hash, 0, size)

    {_hash, node} = elem(points, rem(index, size))

    node
  end

  @doc """
  Whether this node owns `key`.
  """
  @spec own?(t(), routing_key()) :: boolean()
  def own?(%__MODULE__{} = ring, key), do: owner(ring, key) == node()

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
