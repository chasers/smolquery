defmodule Smolquery.Burst do
  @moduledoc """
  Burst: the pool of ephemeral runners a query can fan its scan out to
  (PL-77).

  A runner is a copy of this release booted on demand by a `FLAME.Backend`
  and leased through one `FLAME.Pool`, `#{inspect(__MODULE__.Pool)}`. The
  pool lives at the top of `Smolquery.Application`'s children so that the
  node that leases runners and the runner itself start the same tree.

  ## Runner mode

  The release boots as a runner when FLAME has set `FLAME_PARENT` in its
  environment (`FLAME.Parent.get/0`). A runner starts nothing a node does:
  no cluster membership, no role subtree, no catalog connection, no
  listener, and no internal secret. It holds the cluster cookie and whatever
  the backend passed it, and it reads sealed files with its own credential.
  `runner?/0` is the one question `Smolquery.Application` asks.

  ## Configuration

      config :smolquery, Smolquery.Burst,
        enabled: true,
        backend: :local,
        max: 64,
        boot_timeout_ms: 120_000,
        idle_shutdown_after_ms: 15_000

  `backend` is `:local` (`FLAME.LocalBackend`, for tests and development)
  or any module that implements `FLAME.Backend`. `max` bounds the runners
  this node may hold at once; it is per node, not per deployment. Each
  runner takes one shard at a time (`max_concurrency: 1`): a shard is one
  DuckDB engine that uses the whole sandbox.

  `idle_shutdown_after_ms` is the cost lever. The spike (T-673) measured a
  runner's work at 2 to 5 s per lease and the idle window as the rest of the
  bill; 15 s covers a dashboard's burst of queries without paying for a
  minute of silence.
  """

  @pool __MODULE__.Pool
  @call_timeout_ms 900_000
  @default_max 64
  @default_boot_timeout_ms 120_000
  @default_idle_shutdown_after_ms 15_000

  @doc "The pool runners are leased from."
  @spec pool() :: atom()
  def pool, do: @pool

  @doc "Whether this VM is a runner booted by a FLAME backend."
  @spec runner?() :: boolean()
  def runner?, do: FLAME.Parent.get() != nil

  @doc "Whether this node leases runners."
  @spec enabled?() :: boolean()
  def enabled?, do: Keyword.get(config(), :enabled, false)

  @doc """
  The pool's child spec, when this node leases runners or is itself a runner.

  FLAME forces `min` to 0 on a runner, so the same spec serves both sides.
  """
  @spec children() :: [Supervisor.child_spec() | {module(), term()}]
  def children do
    if runner?() or enabled?(), do: [pool_spec(config())], else: []
  end

  defp pool_spec(config) do
    {FLAME.Pool,
     name: @pool,
     backend: backend(Keyword.get(config, :backend, :local)),
     min: 0,
     max: Keyword.get(config, :max, @default_max),
     max_concurrency: 1,
     boot_timeout: Keyword.get(config, :boot_timeout_ms, @default_boot_timeout_ms),
     idle_shutdown_after:
       Keyword.get(config, :idle_shutdown_after_ms, @default_idle_shutdown_after_ms),
     timeout: @call_timeout_ms,
     log: :info}
  end

  defp backend(:local), do: FLAME.LocalBackend
  defp backend(module) when is_atom(module), do: module

  defp config, do: Application.get_env(:smolquery, __MODULE__, [])
end
