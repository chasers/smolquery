defmodule Smolquery.BufferService.RingEpoch do
  @moduledoc """
  The fence closing the buffer ring's dual-owner window (T-92, PL-13).

  `Smolquery.BufferService.Routing` derives ownership from live `:pg`
  membership, which is eventually consistent: during a partition or a slow
  gossip window two nodes can each hold a member list naming themselves owner
  of the same table, and both accept and commit its writes. Reads survive that
  (the planner fans out over every member), but a batch retried into the
  divergence can commit on both nodes, and two nodes can independently seal
  the same table.

  This process makes ownership *decidable* by anchoring it to an epoch in a
  `Smolquery.Cluster.ConfigStore` — the strongly consistent Postgres the
  deployment already requires. Every `epoch_refresh_ms` it reads the store,
  proposes a compare-and-swap advance when live `:pg` membership differs from
  the stored configuration, and publishes the result for `check_write/2`. Of
  two divergent views, exactly one matches the configuration at the current
  epoch; the other node's writes are refused.

  ## The lease, and what fails when

  `check_write/2` refuses a write unless all three hold, in this order:

    * the published configuration was verified against the store within
      `epoch_lease_ms` — otherwise `{:error, :ring_config_stale}`. This is
      the fail-closed arm: a node cut off from the store cannot know it
      still owns anything, so after a grace of one lease it stops accepting.
      The store being down for longer than a lease therefore stops buffer
      writes — deliberately; sealing already depends on the same Postgres.
    * the ring built from the current configuration names this node the
      owner — otherwise `{:error, :not_owner}`.
    * every configuration replaced within the last lease also named this
      node the owner — otherwise `{:error, :ownership_settling}`. A node
      that just acquired a table waits out the previous owner's lease before
      accepting, because until that lease expires the previous owner may
      still be accepting on a stale-but-leased view. Settle obligations are
      carried across epoch changes rather than replaced by them: a second
      advance inside the window (a replacement joining moments after a node
      died) must not release the wait the first advance started. This arm is
      what actually closes the window; the wait is bounded, and a caller
      retries into it the same way it retries `{:error, :draining}`.

  The guarantee rests on bounded clock *rate* drift only — `age_ms` is
  computed by the store's clock, deadlines by each node's monotonic clock;
  no two nodes' clocks are ever compared.

  ## Publication

  The configuration lands in `:persistent_term` (reads are free on the write
  path) and the verification timestamp in an `:atomics` cell (bumped every
  refresh — a `:persistent_term` put per second would trigger a global GC
  per second). A node where this process has never run — clustering off,
  every test that predates it — has no published configuration, and
  `check_write/2` answers `:ok`: single-copy, single-node behavior is
  unchanged. If this process dies, the atomic stops advancing and every gate
  fails closed one lease later; deliberately, there is no cleanup that could
  turn a crash into fail-open.

  Started by `Smolquery.BufferService.Supervisor` only where epoch fencing
  is configured: `config/runtime.exs` sets `:epoch_fencing` wherever
  `CATALOG_DATABASE_URL` enables clustering (the store is that same
  Postgres), and an explicit `:epoch_store` opts in without it. Tests start
  one directly against a `Smolquery.Cluster.ConfigStore.Memory` with an
  injected `:members` reader.

  ## Configuration

      config :smolquery, Smolquery.BufferService,
        epoch_fencing: true,
        epoch_lease_ms: 10_000,
        epoch_refresh_ms: 1_000

  """

  use GenServer

  require Logger

  alias Smolquery.BufferService.Ring
  alias Smolquery.BufferService.Routing
  alias Smolquery.Cluster.PgGroup

  @default_lease_ms 10_000
  @default_refresh_ms 1_000

  @type check_error :: :ring_config_stale | :not_owner | :ownership_settling

  @doc """
  Starts the epoch keeper for buffer instance `:name`.

  ## Options

    * `:name` — the buffer instance (required)
    * `:static` — fallback member list when `:pg` has no members (required)
    * `:store` — `{module, start_opts}`; defaults to
      `Smolquery.Cluster.ConfigStore.Postgres` on `Smolquery.Cluster`'s
      `:postgres` config
    * `:members` — zero-arity reader of live membership, a test seam;
      defaults to `Smolquery.Cluster.PgGroup.nodes/3` over `:static`
    * `:lease_ms` / `:refresh_ms` — override the instance configuration

  """
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) do
    name = Keyword.fetch!(opts, :name)

    GenServer.start_link(__MODULE__, opts, name: process_name(name))
  end

  @doc """
  The registered name of instance `name`'s epoch keeper.
  """
  @spec process_name(atom()) :: atom()
  def process_name(name), do: Module.concat(name, "RingEpoch")

  @doc """
  Whether this node may accept a write for `table_ref` right now.

  Free when no epoch keeper has ever published here — one `:persistent_term`
  read — so unclustered deployments pay nothing.
  """
  @spec check_write(atom(), term()) :: :ok | {:error, check_error()}
  def check_write(name, table_ref) do
    case :persistent_term.get(key(name), nil) do
      nil -> :ok
      config -> check(config, table_ref, System.monotonic_time(:millisecond))
    end
  end

  @doc """
  Whether this node is `table_ref`'s owner at the current epoch — the seal
  gate (T-96). A node with no published configuration owns whatever the
  static ring says (single-node behavior, unchanged); a node whose lease
  lapsed owns nothing, the same fail-closed arm as `check_write/2`. Unlike
  `check_write/2` this ignores settling: an owner-elect freezing a claim is
  safe (claims are deterministic and idempotent), and waiting a lease to
  seal would only stretch the unsealed window.
  """
  @spec owner?(atom(), term()) :: boolean()
  def owner?(name, table_ref) do
    case :persistent_term.get(key(name), nil) do
      nil ->
        Routing.owner(Routing.resolve(name), table_ref) == node()

      config ->
        now = System.monotonic_time(:millisecond)

        now - :atomics.get(config.verified, 1) <= config.lease_ms and
          Ring.owner(config.ring, table_ref) == node()
    end
  end

  @doc """
  The epoch this node currently holds, `nil` before the first verified
  refresh or where no keeper runs. Stamped onto replica traffic so a
  follower can refuse a shipment from a configuration older than its own.
  """
  @spec current_epoch(atom()) :: non_neg_integer() | nil
  def current_epoch(name) do
    case :persistent_term.get(key(name), nil) do
      nil -> nil
      config -> config.epoch
    end
  end

  @doc """
  Forces a refresh now, returning once it completed. For tests, which
  otherwise race the refresh interval.
  """
  @spec refresh(atom()) :: :ok
  def refresh(name), do: GenServer.call(process_name(name), :refresh)

  @impl GenServer
  def init(opts) do
    name = Keyword.fetch!(opts, :name)
    static = Keyword.fetch!(opts, :static)
    {store_impl, store_opts} = store_spec(opts)

    case store_impl.start_link(store_opts) do
      {:ok, store} ->
        config = Application.get_env(:smolquery, Smolquery.BufferService, [])

        state = %{
          name: name,
          scope: "buffer:#{name}",
          store_impl: store_impl,
          store: store,
          setup_done: false,
          members: Keyword.get(opts, :members, fn -> members(name, static) end),
          lease_ms: option(opts, config, :lease_ms, :epoch_lease_ms, @default_lease_ms),
          refresh_ms: option(opts, config, :refresh_ms, :epoch_refresh_ms, @default_refresh_ms),
          epoch: nil,
          settles: [],
          verified: :atomics.new(1, signed: true)
        }

        publish_prestale(state)

        {:ok, state, {:continue, :refresh}}

      {:error, reason} ->
        {:stop, reason}
    end
  end

  @impl GenServer
  def handle_continue(:refresh, state) do
    state = state |> attempt_refresh() |> schedule()

    {:noreply, state}
  end

  @impl GenServer
  def handle_info(:refresh, state) do
    {:noreply, state |> attempt_refresh() |> schedule()}
  end

  @impl GenServer
  def handle_call(:refresh, _from, state) do
    {:reply, :ok, attempt_refresh(state)}
  end

  defp check(config, table_ref, now) do
    cond do
      now - :atomics.get(config.verified, 1) > config.lease_ms ->
        {:error, :ring_config_stale}

      Ring.owner(config.ring, table_ref) != node() ->
        {:error, :not_owner}

      settling?(config, table_ref, now) ->
        {:error, :ownership_settling}

      true ->
        :ok
    end
  end

  defp settling?(%{settles: []}, _table_ref, _now), do: false

  defp settling?(config, table_ref, now) do
    Enum.any?(config.settles, fn {ring, settle_until} ->
      now < settle_until and Ring.owner(ring, table_ref) != node()
    end)
  end

  defp attempt_refresh(state) do
    verified_at = System.monotonic_time(:millisecond)

    with {:ok, state} <- setup(state),
         members = Enum.sort(state.members.()),
         {:ok, config} <- current(state, members),
         {:ok, config} <- reconcile(state, config, members) do
      publish(state, config, verified_at)
    else
      {:error, reason} ->
        Logger.warning(fn ->
          "ring epoch refresh failed for #{inspect(state.name)}: #{inspect(reason)}"
        end)

        state
    end
  end

  defp setup(%{setup_done: true} = state), do: {:ok, state}

  defp setup(state) do
    case state.store_impl.setup(state.store) do
      :ok -> {:ok, %{state | setup_done: true}}
      {:error, reason} -> {:error, reason}
    end
  end

  defp current(state, members) do
    case state.store_impl.fetch(state.store, state.scope) do
      {:ok, config} -> {:ok, config}
      :not_found -> state.store_impl.ensure(state.store, state.scope, members)
      {:error, reason} -> {:error, reason}
    end
  end

  defp reconcile(_state, %{members: members} = config, members), do: {:ok, config}

  defp reconcile(state, config, members) do
    case state.store_impl.advance(state.store, state.scope, config.epoch, members) do
      {:ok, advanced} -> {:ok, advanced}
      {:error, :conflict} -> refetch(state)
      {:error, reason} -> {:error, reason}
    end
  end

  defp refetch(state) do
    case state.store_impl.fetch(state.store, state.scope) do
      {:ok, config} -> {:ok, config}
      :not_found -> {:error, :not_found}
      {:error, reason} -> {:error, reason}
    end
  end

  defp publish(state, config, verified_at) do
    :atomics.put(state.verified, 1, verified_at)

    if config.epoch == state.epoch do
      state
    else
      settles = carry_settles(state, config, System.monotonic_time(:millisecond))

      :persistent_term.put(key(state.name), %{
        epoch: config.epoch,
        ring: Ring.new!(config.members),
        settles: settles,
        lease_ms: state.lease_ms,
        verified: state.verified
      })

      %{state | epoch: config.epoch, settles: settles}
    end
  end

  defp carry_settles(state, config, now) do
    live = Enum.reject(state.settles, fn {_ring, settle_until} -> settle_until <= now end)

    case prev_ring(config.prev_members) do
      nil -> live
      ring -> [{ring, now + max(state.lease_ms - config.age_ms, 0)} | live]
    end
  end

  defp publish_prestale(state) do
    :atomics.put(state.verified, 1, System.monotonic_time(:millisecond) - state.lease_ms - 1)

    :persistent_term.put(key(state.name), %{
      epoch: nil,
      ring: Ring.new!(state.members.()),
      settles: [],
      lease_ms: state.lease_ms,
      verified: state.verified
    })
  end

  defp prev_ring(nil), do: nil
  defp prev_ring(members), do: Ring.new!(members)

  defp members(name, static), do: PgGroup.nodes(Smolquery.BufferService, name, static)

  defp store_spec(opts) do
    configured =
      Application.get_env(:smolquery, Smolquery.BufferService, [])
      |> Keyword.get(:epoch_store)

    Keyword.get(opts, :store) || configured || postgres_store()
  end

  defp postgres_store do
    postgres =
      Application.get_env(:smolquery, Smolquery.Cluster, [])
      |> Keyword.get(:postgres) ||
        raise ArgumentError,
              "RingEpoch needs an :epoch_store or Smolquery.Cluster :postgres config"

    {Smolquery.Cluster.ConfigStore.Postgres, postgres}
  end

  defp option(opts, config, opt_key, config_key, default) do
    Keyword.get(opts, opt_key) || Keyword.get(config, config_key, default)
  end

  defp schedule(state) do
    Process.send_after(self(), :refresh, state.refresh_ms)

    state
  end

  defp key(name), do: {__MODULE__, name}
end
