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

  Forward-batches go on the `:bulk` channel and metadata on `:control`, so a
  manifest read a planner is waiting on does not queue behind a stream of writes.
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
    * `{:error, {:badrpc | :badtcp, reason}}` — the owner could not be reached

  """
  @spec write_batch(atom(), Store.table_ref(), batch()) ::
          {:ok, TableBuffer.ack()} | {:error, term()}
  def write_batch(name, table_ref, batch) do
    route(name, :bulk, :write_batch, [name, table_ref, batch], table_ref, :write)
  end

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
  @spec retire(atom(), Store.table_ref(), [String.t()], non_neg_integer()) ::
          :ok | {:error, term()}
  def retire(name, table_ref, ids, snapshot) do
    route(name, :control, :retire, [name, table_ref, ids, snapshot], table_ref, :control)
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

  defp route(name, channel, function, args, table_ref, timeout_kind) do
    routing = Routing.resolve(name)
    owner = Routing.owner(routing, table_ref)
    transport = Routing.transport(routing, owner)

    Transport.invoke(transport, owner, channel, function, args, timeout(routing, timeout_kind))
  end

  defp timeout(routing, :write), do: routing.write_timeout_ms
  defp timeout(routing, :control), do: routing.control_timeout_ms
end
