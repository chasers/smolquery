defmodule Smolquery.BufferService.Client do
  @moduledoc """
  The only way in or out of the buffer service.

  Every other service reaches the hot tier through these functions and nothing
  else — no `GenServer.call` into a `TableBuffer`, no `Registry` lookup, no ETS
  read. That rule is what makes splitting the buffer service into its own
  deployment a config change rather than a rewrite.

  This module does one thing: resolve which node owns a table and hand the call to
  a transport. It holds no hot-tier logic — that is
  `Smolquery.BufferService.Endpoint`, which runs on the owning node. A caller
  cannot tell from the return value whether the work happened in this BEAM or
  across the cluster, which is the point.

  ## Ownership routes, it does not refuse

  A table belongs to exactly one buffer node, and a call for a table this node
  does not own is *forwarded* rather than rejected. Callers should not have to
  know the ring exists; `Smolquery.BufferService.Routing` answers ownership even
  on a node that runs no buffer at all, which is what lets a query node ask a
  buffer node for a hot manifest.

  ## Bulk and control travel separately

  Forward-batches go on a `{:bulk, table_ref}` channel and metadata on
  `:control`, so a manifest read a planner is waiting on does not queue behind
  a stream of writes, and one table's writes do not queue behind another's.
  See `Smolquery.BufferService.Transport` for why that split has to be explicit.

  ## The batch carries its schema

  The buffer service never reads the catalog, so it cannot look a table's schema
  up; the ingest edge validated against the catalog before forwarding and passes
  what it validated.

  ## Usage

      batch = %{schema: schema, rows: [%{"id" => 1}]}

      {:ok, ack} = Smolquery.BufferService.Client.write_batch(Smolquery.BufferService, table, batch)
      #=> {:ok, %{segment_id: "01K...", row_count: 1}}

  """

  alias Smolquery.BufferService.Endpoint
  alias Smolquery.BufferService.HotManifest.Entry
  alias Smolquery.BufferService.Routing
  alias Smolquery.BufferService.TableBuffer
  alias Smolquery.BufferService.Transport
  alias Smolquery.Segments.Store

  @type batch :: Endpoint.batch()

  @doc """
  Writes a forward-batch, returning once its rows are durable and queryable.

  The instance name is always explicit — a defaulted leading argument here would
  make `write_batch(table, batch)` and `write_batch(name, table)` impossible to
  tell apart, which has already cost this codebase a bug once in
  `Smolquery.Engine`.

  Errors a caller must expect:

    * `{:error, :buffer_full}` — shed load; the ingest edge turns this into a 429
    * `{:error, :buffer_service_unavailable}` — the owner does not run the
      `:buffer` role
    * `{:error, :draining}` — the owner is mid-drain (Milestone 8 L4); the
      ring has not moved yet, but this node has already stopped taking new
      writes for tables it owns, so a retry should expect the owner to
      change
    * `{:error, :not_owner | :ownership_settling | :ring_config_stale}` —
      the epoch fence refused the write (T-92,
      `Smolquery.BufferService.RingEpoch`): the node this call routed to is
      not the owner at the current ring epoch, just became the owner and is
      waiting out the previous owner's lease, or cannot verify its own lease.
      All three are retryable the way `:draining` is; routing catches up
      within an `epoch_refresh_ms`
    * `{:error, {:badrpc | :badtcp, reason}}` — the owner could not be reached

  `{:badrpc, :timeout}` is ambiguous: the call may have reached the owner and
  committed after the deadline. A batch carrying a `:batch_id` idempotency
  key makes the retry safe — the owner answers a committed id with the
  original ack instead of writing again (T-41), and the dedup record is
  fsynced in the manifest entry, so it survives a restart. What it survives
  *across an owner move* depends on the replicator (T-96) and on the kind of
  move: under `Smolquery.BufferService.Replicator.SegmentShipping` the entry
  — batch ids included — is on every follower before the ack, and an owner
  *loss* promotes a follower, so a retry against the promoted owner is
  answered with the original ack; this also covers a flush the caller saw
  *fail* after a follower had already applied it (the compensating drop can
  be lost with the owner), where the retry is answered from the surviving
  copy and the rows exist once. A node *joining* the ring is the move dedup
  does not survive: the joiner takes over its key range holding neither
  entries nor batch ids (nothing backfills), so a retry that routes to it
  commits again. Single-copy (`Replicator.None`), the record lives only on
  the owner, and across any owner move a `:batch_id` batch is at-least-once
  like any other.
  """
  @spec write_batch(atom(), Store.table_ref(), batch()) ::
          {:ok, TableBuffer.ack()}
          | {:ok, TableBuffer.ack(), [TableBuffer.row_errors()]}
          | {:invalid, [TableBuffer.row_errors()]}
          | {:error, term()}
  def write_batch(name, table_ref, batch) do
    routing = Routing.resolve(name)
    owner = Routing.owner(routing, table_ref)
    transport = Routing.transport(routing, owner)

    # Timed apart from the call because on a partitioned table most batches are
    # remote, and serializing a multi-megabyte frame to Arrow IPC before every
    # one of them is a per-request cost nothing had ever priced (T-182).
    started = System.monotonic_time(:microsecond)
    batch = wire_batch(transport, batch)

    :telemetry.execute(
      [:smolquery, :buffer, :wire],
      %{duration_us: System.monotonic_time(:microsecond) - started},
      %{transport: if(transport == Transport.Local, do: :local, else: :remote)}
    )

    Transport.invoke(
      transport,
      owner,
      {:bulk, table_ref},
      :write_batch,
      [name, table_ref, batch],
      timeout(routing, :write)
    )
  end

  # A DataFrame is a NIF resource; its reference means nothing after another
  # node's term decode, so a columnar batch crosses the wire as Arrow IPC
  # bytes and stays a live frame only for the in-BEAM transport.
  defp wire_batch(Transport.Local, batch), do: batch

  defp wire_batch(_remote, %{frame: frame} = batch) do
    batch
    |> Map.delete(:frame)
    |> Map.put(:frame_ipc, Explorer.DataFrame.dump_ipc!(frame))
  end

  # NDJSON is already bytes, so a remote batch needs no conversion — which is the
  # point of the passthrough path: no frame exists on this node to serialize.
  defp wire_batch(_remote, batch), do: batch

  @doc """
  The table's hot manifest — every micro-segment its owner holds for it.

  Entries carry the flush-time min-max stats a planner prunes on, and `sealed_at`,
  which is what makes the seal handoff exactly-once. Applying that rule is the
  planner's job: at catalog snapshot `S`, include an entry only if it is unsealed
  or `sealed_at > S`.
  """
  @spec hot_manifest(atom(), Store.table_ref()) :: {:ok, [Entry.t()]} | {:error, term()}
  def hot_manifest(name, table_ref) do
    route(name, :control, :hot_manifest, [name, table_ref], table_ref, :control)
  end

  @doc """
  Stamps `ids` as sealed at the catalog snapshot a sealer committed them in.

  Idempotent in every direction a crashed sealer can retry from: ids already
  sealed, and ids the grace-period sweep has since deleted, are both `:ok`. The
  segments stay readable until the grace period expires, because a query planned
  at an older snapshot is still entitled to them.
  """
  @spec retire(atom(), Store.table_ref(), [String.t()], non_neg_integer(), [String.t()] | nil) ::
          :ok | {:error, term()}
  def retire(name, table_ref, ids, snapshot, keys \\ nil) do
    route(name, :control, :retire, [name, table_ref, ids, snapshot, keys], table_ref, :control)
  end

  @doc """
  `retire/4`, pinned to the node holding the entries rather than routed to the
  ring's current owner.

  The sealer's path (Milestone 8 L6): a claim's `:origin` names where its
  micro-segments physically live, and a ring change between signal and seal
  means the ring owner and the holder are different nodes. `nil` falls back to
  ring routing, for claims that carry no origin.
  """
  @spec retire_at(
          node() | nil,
          atom(),
          Store.table_ref(),
          [String.t()],
          non_neg_integer(),
          [String.t()] | nil
        ) :: :ok | {:error, term()}
  def retire_at(node, name, table_ref, ids, snapshot, keys \\ nil)

  def retire_at(nil, name, table_ref, ids, snapshot, keys),
    do: retire(name, table_ref, ids, snapshot, keys)

  def retire_at(node, name, table_ref, ids, snapshot, keys) do
    name
    |> Routing.resolve()
    |> invoke(node, :control, :retire, [name, table_ref, ids, snapshot, keys], :control)
  end

  @doc """
  Flushes a table's accumulator now.

  For a caller that needs the tail durable without waiting out the interval — a
  test, or a drain before shutdown. Ordinary writes should let group commit do its
  job. A table with no buffer running has nothing accumulated, so flushing it is
  `:ok` rather than an error.
  """
  @spec flush(atom(), Store.table_ref()) :: :ok | {:error, term()}
  def flush(name, table_ref) do
    route(name, :control, :flush, [name, table_ref], table_ref, :control)
  end

  @doc """
  The node owning `table_ref`.
  """
  @spec owner(atom(), Store.table_ref()) :: node()
  def owner(name, table_ref), do: name |> Routing.resolve() |> Routing.owner(table_ref)

  @doc """
  Every node the ring currently names.

  What the query planner fans hot-manifest fetches out over (Milestone 8 L5):
  asking every member rather than only a table's current owner is what keeps
  acked rows visible while a ring change moves ownership away from the node
  still holding them.
  """
  @spec nodes(atom()) :: [node()]
  def nodes(name), do: name |> Routing.resolve() |> Routing.nodes()

  @doc """
  Every node a hot-manifest fetch must reach — `nodes/1` plus the nodes the
  deployment expects in the ring.

  What the planner fans out over, and deliberately not what writes route on. See
  `Smolquery.BufferService.Routing.manifest_nodes/1`: a crashed node leaves `:pg`
  exactly the way a drained one does, so a reader that asks only the live ring
  counts a crashed owner's tables short instead of failing (T-94).
  """
  @spec manifest_nodes(atom()) :: [node()]
  def manifest_nodes(name), do: Routing.manifest_nodes(name)

  @doc """
  How many of `manifest_nodes/1` a reader may fail to reach and still answer
  completely — `replication_factor - 1` under segment shipping, zero
  single-copy. See `Smolquery.BufferService.Routing.absence_tolerance/1`.
  """
  @spec absence_tolerance(atom()) :: non_neg_integer()
  def absence_tolerance(name), do: Routing.absence_tolerance(name)

  defp route(name, channel, function, args, table_ref, timeout_kind) do
    routing = Routing.resolve(name)

    invoke(routing, Routing.owner(routing, table_ref), channel, function, args, timeout_kind)
  end

  defp invoke(routing, node, channel, function, args, timeout_kind) do
    transport = Routing.transport(routing, node)

    Transport.invoke(transport, node, channel, function, args, timeout(routing, timeout_kind))
  end

  defp timeout(routing, :write), do: routing.write_timeout_ms
  defp timeout(routing, :control), do: routing.control_timeout_ms
end
