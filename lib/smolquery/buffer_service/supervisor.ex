defmodule Smolquery.BufferService.Supervisor do
  @moduledoc """
  Top-level subtree for the `:buffer` role.

  Started only on nodes whose roles include `:buffer` (see `Smolquery.Roles`).
  Holds the hot tier: the manifest index, the table-to-buffer registry, and the
  partitioned supervisor that `TableBuffer` processes start under on demand.

  The strategy is `rest_for_one`, in that order, because the dependency runs one
  way. The manifest owns the ETS index every buffer writes into, so if it dies the
  registry and the buffers that hold stale handles must go with it — and each
  buffer rebuilds its table's entries from the log when it restarts. A single
  buffer crashing, by contrast, disturbs nothing else.

  `Adopter` comes next, once the pieces it needs are up: it starts a buffer for
  every owned table that already has a manifest log, so an unsealed tail is not
  stranded waiting for a write that may never come.

  `HotServer` is last — it only reads the manifest and the store, so nothing
  beneath it depends on it being up.

  Ring membership is the first child, a
  `Smolquery.Cluster.PgGroup.Member` (Milestone 8 L4) — a no-op when
  clustering is off, and otherwise what puts this node in the ring at all.
  Its pid dies with this subtree, so an ungraceful crash removes the node
  from the ring the same way `Smolquery.BufferService.Drain.drain/2`'s
  explicit `leave` does — and unlike a one-shot join from `init/1`, it
  re-asserts membership after a `:pg` scope restart. It is omitted entirely
  when the drain flag is already up: a subtree restarted after a completed
  drain must not silently rejoin a ring whose writes it refuses.

  `RingEpoch` follows it when epoch fencing is configured (T-92) —
  `:epoch_fencing` is set by `config/runtime.exs` wherever
  `CATALOG_DATABASE_URL` enables clustering, and `:epoch_store` opts a test
  in explicitly; a test that merely flips `Smolquery.Cluster` on gets no
  keeper and no gate, because the keeper without a reachable store fails
  every write closed, which is right in production and wrong in a test that
  never promised a Postgres. Membership asserts
  what `:pg` sees, the epoch keeper fences what this node may act on. It
  sits above the manifest and the buffers because they consume its gate; if
  it cannot keep its lease verified the writes it would have permitted must
  fail closed, which its published-but-stale state already guarantees even
  across its own restarts.

  The `flush_writer: :duckdb` write pool starts *below* the buffers, in its own
  `one_for_one` supervisor, and only for that writer. It is the one child this
  subtree runs that a client can make crash on demand — a fatal DuckDB error
  stops the connection (`Smolquery.Engine.Connection`) — so it must sit where
  its restarts cannot reach a row. The comment on the pool's own child spec,
  below, carries the whole argument.

  `ExpectedNodes` starts under the same condition (T-109) but *last*: the
  durable expected-fleet row lives in the same config store the epoch keeper
  CASes against, but unlike the epoch keeper nothing in this subtree
  consumes it — readers go through its ETS table — so under
  `rest_for_one` it must sit where its crash restarts nothing else. Query
  nodes start their own from `Smolquery.QueryService.Supervisor`; on a node
  running both roles the first one wins and the other start is `:ignore`d.
  """

  use Supervisor

  alias Smolquery.BufferService.Adopter
  alias Smolquery.BufferService.Drain
  alias Smolquery.BufferService.ExpectedNodes
  alias Smolquery.BufferService.HotManifest
  alias Smolquery.BufferService.HotServer
  alias Smolquery.BufferService.Ring
  alias Smolquery.BufferService.RingEpoch
  alias Smolquery.BufferService.Runtime
  alias Smolquery.Cluster.PgGroup

  # The write pool's own intensity, and it is deliberately far above a
  # supervisor's usual three-in-five.
  #
  # A member terminates only after `Smolquery.Engine`'s own supervisor gives up,
  # which takes at least four fatal DuckDB faults, and a fatal fault takes a
  # query that reached DuckDB — so 100 member restarts inside 10 seconds means
  # roughly 400 faulting `COPY` statements in ten seconds, a rate the flush path
  # this pool serves cannot produce. What the ceiling still catches is the other
  # shape: a member that cannot *start* (a `memory_limit` DuckDB rejects, an
  # extension that will not load) fails in milliseconds, and this stops it
  # spinning rather than letting it burn a scheduler until the node is restarted.
  @write_pool_max_restarts 100
  @write_pool_max_seconds 10

  @doc """
  Starts the buffer service.

  Takes any `Smolquery.BufferService.Runtime` option; application config supplies
  whatever is not passed.
  """
  @spec start_link(keyword()) :: Supervisor.on_start()
  def start_link(opts \\ []) do
    runtime = Runtime.new(opts)
    Smolquery.DeployedShape.announce(runtime)

    Supervisor.start_link(__MODULE__, runtime, name: Runtime.supervisor(runtime.name))
  end

  @impl Supervisor
  def init(%Runtime{} = runtime) do
    Runtime.put(runtime)

    children =
      [
        membership(runtime),
        ring_epoch(runtime),
        {HotManifest, name: Runtime.manifest(runtime.name)}
      ] ++
        [
          {Registry, keys: :unique, name: Runtime.registry(runtime.name)},
          Supervisor.child_spec(
            {Registry, keys: :unique, name: Runtime.committer_registry(runtime.name)},
            id: Runtime.committer_registry(runtime.name)
          ),
          {PartitionSupervisor,
           child_spec: DynamicSupervisor, name: Runtime.buffers(runtime.name)},
          {Adopter, runtime},
          Supervisor.child_spec(
            {Bandit,
             plug: {HotServer, runtime.name},
             ip: runtime.hot_server_ip,
             port: runtime.hot_server_port,
             startup_log: false},
            id: Runtime.hot_server(runtime.name)
          ),
          write_pool(runtime),
          expected_nodes(runtime)
        ]

    Supervisor.init(Enum.reject(children, &is_nil/1), strategy: :rest_for_one)
  end

  # Started only for `flush_writer: :duckdb`, so the default path pays nothing —
  # not a database, not a connection, not the extension load its bootstrap runs,
  # and not this child either: `write_pool/1` is `nil` for every other writer and
  # `init/1` rejects it, so the default child list is the one it always was.
  #
  # ## Why the pool is its own supervisor, and why it is down here
  #
  # The pool used to be the *first* children of this list. Under `rest_for_one`
  # that put it above every `TableBuffer` on the node, and a `Smolquery.Engine`
  # subtree can terminate for a reason a client controls:
  # `Smolquery.Engine.Connection` stops on a fatal DuckDB error (the whole
  # database is invalidated, not the one query — `connection.ex:206`), and
  # `Engine`'s own supervisor takes the emulator default of three restarts in
  # five seconds (`engine.ex:108`). A retry loop that reproduces such a fault
  # exhausts that quickly, and everything *after* the pool then restarted with
  # it: the `PartitionSupervisor` holding every table's buffer, and with it every
  # unacked caller in every `pending` list, including JSON-route callers on
  # tables that never saw an NDJSON body.
  #
  # So the pool is two things now. It is **its own `one_for_one` supervisor**, so
  # one member's restarts are one member's; and it is **below the buffers**, so
  # its own termination restarts nothing that holds a row. Nothing between here
  # and the top of the list depends on it: a flush resolves a pool member by name
  # (`Runtime.engine_for/2`), and a missing or unanswering engine exits the
  # encode task, which the committer turns into `{:error, {:encode_crashed, _}}`
  # and reports to its waiters like any other failed encode
  # (`table_buffer/committer.ex`, the `:DOWN` clause of `handle_info/2`). The
  # buffers need the engines to answer or to fail, not to exist.
  #
  # It sits before `expected_nodes/1` rather than last so that child keeps the
  # property its own comment claims — nothing restarts behind it. `ExpectedNodes`
  # is the one thing this subtree can restart on the pool's account, and it
  # consumes nothing from anybody.
  #
  # `restart: :transient` closes the last path back to the buffers. A supervisor
  # that exceeds its own intensity exits `:shutdown`, and a `:transient` child
  # that exits `:shutdown` is not restarted and is not counted against *this*
  # supervisor's intensity — so a pool that keeps failing can never walk the top
  # supervisor to its own ceiling and take the whole buffer service with it. A
  # pool that gives up leaves the NDJSON route answering errors on a node whose
  # JSON route, buffers and hot tier are untouched, which is the trade this
  # module wants.
  defp write_pool(%Runtime{flush_writer: :duckdb} = runtime) do
    %{
      id: Runtime.write_pool(runtime.name),
      type: :supervisor,
      restart: :transient,
      shutdown: :infinity,
      start:
        {Supervisor, :start_link,
         [
           write_engines(runtime),
           [
             strategy: :one_for_one,
             max_restarts: @write_pool_max_restarts,
             max_seconds: @write_pool_max_seconds,
             name: Runtime.write_pool(runtime.name)
           ]
         ]}
    }
  end

  defp write_pool(%Runtime{}), do: nil

  # The `:duckdb` flush writer needs its own DuckDB instances. Only reached
  # through `write_pool/1`, which is `nil` for every other writer, so `:polars`
  # deployments carry no extra process.
  #
  # Each member is sized for the pool by `Runtime.write_engine_budget/1`, whose
  # doc carries the whole argument — what multiplies when a member inherits
  # `Smolquery.Engine`'s config, why the division reads the configured thread
  # count rather than the schedulers, and where the division stops describing a
  # budget.
  #
  # The division is worth more than tidiness. Measured on this laptop with the
  # `bench/k6` harness, a pool of sixteen inheriting ten threads each held
  # 468,717 rows/s over four runs with a 20.4% spread; the same pool divided to
  # one thread each held 534,104 over five runs with a 9.9% spread. Declaring
  # 160 threads on ten cores did not buy capacity, it bought context switching.
  defp write_engines(%Runtime{} = runtime) do
    budget = Runtime.write_engine_budget(runtime)

    Enum.map(Runtime.engines(runtime), fn name ->
      Supervisor.child_spec(
        {Smolquery.Engine, [name: name, extensions: []] ++ budget},
        id: name
      )
    end)
  end

  defp membership(runtime) do
    unless Drain.draining?(runtime.name) do
      {PgGroup.Member, {Smolquery.BufferService, runtime.name}}
    end
  end

  defp ring_epoch(runtime) do
    if ExpectedNodes.configured?() do
      {RingEpoch, name: runtime.name, static: Ring.nodes(runtime.ring)}
    end
  end

  defp expected_nodes(runtime) do
    if ExpectedNodes.configured?() do
      {ExpectedNodes, name: runtime.name}
    end
  end
end
