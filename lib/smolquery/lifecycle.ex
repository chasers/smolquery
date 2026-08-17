defmodule Smolquery.Lifecycle do
  @moduledoc """
  Turns lifecycle `:telemetry` events into cluster-wide PubSub broadcasts.

  The services already announce the hot tier's life at their seams — a
  group commit, a seal attempt, a compaction swap — as `:telemetry` events
  (`Smolquery.Telemetry` holds the catalog). Those events are node-local by
  design. This module is the second consumer the telemetry seam promised: it
  attaches to the per-table events and rebroadcasts each on
  `Smolquery.PubSub`, whose pg adapter carries it to every connected node.
  A LiveView on a web node subscribes to one table's topic and hears a
  storage node's seal the moment it lands (T-295).

  Events roll up to the *parent* table: a partition ref like
  `{"bench", "otel_logs_v5__p1"}` broadcasts on its parent's topic, because
  a page shows a table, and its partitions are the mechanism, not the
  subject. The delivered message is `{:lifecycle, event}` with the event's
  `kind` (`:commit` | `:seal` | `:compaction`), the concrete `table_ref`
  (partition ref included), the emitting `node`, a `result`, the raw
  telemetry `measurements`, and an `at` timestamp in unix milliseconds.

  Only events whose metadata carries a `table_ref` broadcast — the metrics
  side keeps labels to closed sets, so `table_ref` rides in metadata purely
  for this module. The two consumers stay strictly parallel: `/metrics`
  counts only what this node itself emitted, and nothing this bridge
  carries over PubSub is ever written into another node's counters — a
  scrape is per-node state, and cross-node aggregation is the scraper's
  job, not the emitter's. What a node's scrape *does* count is this bridge's
  own output: each delivered broadcast emits
  `[:smolquery, :lifecycle, :broadcast]`, which `Smolquery.Telemetry` renders
  as `smolquery_lifecycle_broadcasts_total` by kind — how much lifecycle
  traffic this node pushes onto the bus, on the node that pushed it. The handler runs in the emitting process and must never
  raise: `:telemetry` silently detaches a raising handler, which would stop
  every broadcast. A broadcast that cannot be delivered (the pubsub not
  running, as in a bare unit test) is dropped rather than raised. The same
  contract shapes two details: the handler head accepts only a 2-tuple
  `table_ref` — an emitter passing anything else is skipped, not raised
  into a `topic/1` `MatchError` that would detach the handler — and `init`
  detaches before it attaches, because the handler outlives the process
  when a brutal kill skips `terminate/2`, and `{:error, :already_exists}`
  crashing every restart would turn one kill into a supervisor loop.
  """

  use GenServer

  alias Smolquery.Partitions
  alias Smolquery.Segments.Store

  @handler_id "smolquery-lifecycle"

  @events [
    [:smolquery, :buffer, :commit],
    [:smolquery, :seal, :attempt],
    [:smolquery, :compact, :swap]
  ]

  @type event :: %{
          kind: :commit | :seal | :compaction,
          table_ref: Store.table_ref(),
          node: node(),
          result: atom(),
          measurements: map(),
          at: integer()
        }

  @doc """
  Starts the bridge and attaches its telemetry handler.
  """
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc """
  Subscribes the caller to a table's lifecycle events.

  `table_ref` may be a partition ref; the subscription lands on the parent
  table's topic either way, matching how events are published.
  """
  @spec subscribe(Store.table_ref()) :: :ok | {:error, term()}
  def subscribe(table_ref) do
    Phoenix.PubSub.subscribe(Smolquery.PubSub, topic(table_ref))
  end

  @doc """
  The PubSub topic a table's lifecycle events broadcast on.
  """
  @spec topic(Store.table_ref()) :: String.t()
  def topic(table_ref) do
    {dataset, table} = Partitions.parent(table_ref)

    "lifecycle:#{dataset}.#{table}"
  end

  @impl GenServer
  def init(_opts) do
    _stale = :telemetry.detach(@handler_id)
    :ok = :telemetry.attach_many(@handler_id, @events, &__MODULE__.handle_event/4, nil)

    {:ok, nil}
  end

  @impl GenServer
  def terminate(_reason, _state), do: :telemetry.detach(@handler_id)

  @doc false
  def handle_event(
        [:smolquery, group, _name],
        measurements,
        %{table_ref: {_dataset, _table} = table_ref} = meta,
        nil
      ) do
    event = %{
      kind: kind(group),
      table_ref: table_ref,
      node: node(),
      result: Map.get(meta, :result, :ok),
      measurements: measurements,
      at: System.system_time(:millisecond)
    }

    delivered =
      try do
        Phoenix.PubSub.broadcast(Smolquery.PubSub, topic(table_ref), {:lifecycle, event})
      rescue
        ArgumentError -> {:error, :pubsub_unavailable}
      catch
        :exit, reason -> {:error, {:exit, reason}}
      end

    with :ok <- delivered do
      :telemetry.execute([:smolquery, :lifecycle, :broadcast], %{count: 1}, %{kind: event.kind})
    end

    :ok
  end

  def handle_event(_event, _measurements, _meta, nil), do: :ok

  defp kind(:buffer), do: :commit
  defp kind(:seal), do: :seal
  defp kind(:compact), do: :compaction
end
