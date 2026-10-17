defmodule Smolquery.Burst do
  @moduledoc """
  Burst: the pool of ephemeral runners a query can fan its scan out to
  (PL-77).

  A runner is a copy of this release booted on demand by a `FLAME.Backend`
  and leased through one `FLAME.Pool`, `#{inspect(__MODULE__.Pool)}`. The
  pool is a child of `Smolquery.Application` on the node that leases runners.

  ## Runner mode

  The release boots as a runner when FLAME has set `FLAME_PARENT` in its
  environment (`FLAME.Parent.get/0`). A runner starts nothing a node does:
  no cluster membership, no role subtree, no catalog connection, no
  listener, no internal secret, and no pool of its own. `FLAME.Application`
  starts the Terminator that takes the parent's work; the pool belongs to
  the parent. `runner?/0` is the one question `Smolquery.Application` and
  `Smolquery.Roles` ask.

  ## Configuration

      config :smolquery, Smolquery.Burst,
        enabled: true,
        backend: :local,
        max: 64,
        boot_timeout_ms: 120_000,
        idle_shutdown_after_ms: 15_000,
        call_timeout_ms: 600_000

  `backend` is `:local` (`FLAME.LocalBackend`, for tests and development),
  a module that implements `FLAME.Backend`, or `{module, opts}` as
  `FLAME.Pool` takes it. `max` bounds the runners this node may hold at
  once; it is per node, not per deployment. Each runner takes one shard at
  a time (`max_concurrency: 1`): a shard is one DuckDB engine that uses the
  whole sandbox. The defaults live here; `config/runtime.exs` overrides a
  key only when its `SMOLQUERY_BURST_*` variable is set.

  `idle_shutdown_after_ms` is the cost lever. The spike (T-673) measured a
  runner's work at 2 to 5 s per lease and the idle window as the rest of the
  bill; 15 s covers a dashboard's burst of queries without paying for a
  minute of silence.

  `call_timeout_ms` is how long a `FLAME.call/3` without its own timeout
  waits. It sits below the 15-minute lifetime a Lambda runner has, so a
  stuck shard fails with a timeout exit the caller can catch, not with the
  `:kill` the pool deals out when the runner's host ends it (T-673).
  """

  @pool __MODULE__.Pool
  @defaults [
    max: 64,
    boot_timeout_ms: 120_000,
    idle_shutdown_after_ms: 15_000,
    call_timeout_ms: 600_000
  ]

  @doc "The pool runners are leased from."
  @spec pool() :: atom()
  def pool, do: @pool

  @doc "Whether this VM is a runner booted by a FLAME backend."
  @spec runner?() :: boolean()
  def runner?, do: FLAME.Parent.get() != nil

  @doc "Whether this node leases runners."
  @spec enabled?() :: boolean()
  def enabled?, do: Keyword.get(config(), :enabled, false)

  @doc "The most runners this node holds at once: the pool's `max`, configured or default."
  @spec max() :: pos_integer()
  def max, do: @defaults |> Keyword.merge(config()) |> Keyword.fetch!(:max)

  @doc """
  The pool's child spec when this node leases runners, else nothing.
  """
  @spec children() :: [{module(), keyword()}]
  def children do
    if enabled?(), do: [pool_spec(Keyword.merge(@defaults, config()))], else: []
  end

  defp pool_spec(config) do
    {FLAME.Pool,
     name: @pool,
     backend: backend(Keyword.get(config, :backend, :local)),
     min: 0,
     max: Keyword.fetch!(config, :max),
     max_concurrency: 1,
     boot_timeout: Keyword.fetch!(config, :boot_timeout_ms),
     idle_shutdown_after: Keyword.fetch!(config, :idle_shutdown_after_ms),
     timeout: Keyword.fetch!(config, :call_timeout_ms),
     log: :info}
  end

  defp backend(:local), do: FLAME.LocalBackend
  defp backend(module) when is_atom(module), do: module
  defp backend({module, opts} = backend) when is_atom(module) and is_list(opts), do: backend

  defp backend(other) do
    raise ArgumentError,
          "config :smolquery, Smolquery.Burst, backend: must be :local, a FLAME.Backend " <>
            "module, or {module, opts}, got: #{inspect(other)}"
  end

  defp config, do: Application.get_env(:smolquery, __MODULE__, [])
end
