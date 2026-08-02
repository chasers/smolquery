defmodule Smolquery.BufferService.ExpectedNodes do
  @moduledoc """
  The buffer fleet the deployment expects, as live CAS-backed state instead
  of static per-node configuration (T-109, PL-14).

  `Smolquery.BufferService.Routing.manifest_nodes/1` unions live ring
  membership with the expected set so a crashed owner's absence fails reads
  loudly instead of answering short (T-94). The *rule* was right; the
  substrate was not: `:expected_nodes` app config can only change by
  redeploying every node, no query node hears about a change it was not
  restarted for, and there is no atomic "the fleet is now this set"
  operation for a resize orchestrator (T-110) to CAS against.

  This keeper moves the set onto the pattern `Smolquery.BufferService.RingEpoch`
  already proved for ring membership (T-92): one row in the same
  `Smolquery.Cluster.ConfigStore` Postgres, scope `"expected:<name>"` beside
  the epoch's `"buffer:<name>"`, advanced by compare-and-swap and polled
  every `refresh_ms` into a keeper-owned named ETS table so the read path
  costs one lookup. ETS rather than `:persistent_term`, deliberately: a
  term is written at boot and never again — re-putting one triggers a
  global heap scan, and this state changes at runtime by design. Not a new
  failure domain: sealing and epoch fencing already require this Postgres.

  ## Propagation is eventual, and both stale directions fail safe

  A change reaches every node within one refresh interval, not
  synchronously. A node added to the row before it is live makes reads fail
  loudly (expected-but-absent) for up to a refresh; a node removed after
  draining keeps failing reads on stale readers for up to a refresh. Both
  degrade toward "reads briefly unavailable", never "reads silently short" —
  the same conservative direction `manifest_nodes/1` already chose. The
  normal path avoids even that: T-110's scale-up leg waits for a node to
  join `:pg` before adding it, and its scale-down leg drains before
  removing.

  ## Bootstrap

  The first refresh seeds the row from the node's static `:expected_nodes`
  config (`Smolquery.Cluster.ConfigStore.ensure/3` — the existing row wins a
  race), so upgrading a deployment from the static-config world is a no-op:
  day one reads answer exactly what the config said, and only `resize/3`
  changes the row after that. A node whose static config is *empty* seeds
  nothing and keeps polling: the expected set is per fleet, not per role,
  and only some roles are told the fleet (the kind deployment sets
  `SMOLQUERY_BUFFER_NODES` on api pods, not buffer pods) — an empty-config
  keeper racing the seed must not make its own ignorance authoritative for
  every reader. Where no keeper runs (single-node, dev, tests that predate
  it), `list/1` answers the static config directly and nothing changes.
  """

  use GenServer

  require Logger

  alias Smolquery.BufferService.ConfigStoreKeeper

  @default_refresh_ms 1_000

  @doc """
  Starts the expected-nodes keeper for buffer instance `:name`.

  ## Options

    * `:name` — the buffer instance (required)
    * `:store` — `{module, start_opts}`; defaults to
      `Smolquery.Cluster.ConfigStore.Postgres` on `Smolquery.Cluster`'s
      `:postgres` config
    * `:static` — the seed for a row that does not exist yet; defaults to
      the instance's `:expected_nodes` configuration
    * `:refresh_ms` — the poll interval (default #{@default_refresh_ms})

  A node may run several subtrees that each want the keeper (a buffer and a
  query role on one node); the first start wins and later ones are
  `:ignore`d, since the published state is per instance, not per subtree.
  """
  @spec start_link(keyword()) :: GenServer.on_start() | :ignore
  def start_link(opts) do
    name = Keyword.fetch!(opts, :name)

    case GenServer.start_link(__MODULE__, opts, name: process_name(name)) do
      {:error, {:already_started, _pid}} -> :ignore
      other -> other
    end
  end

  @doc """
  The registered name of instance `name`'s expected-nodes keeper.
  """
  @spec process_name(atom()) :: atom()
  def process_name(name), do: Module.concat(name, "ExpectedNodes")

  @doc """
  The nodes the deployment currently expects for `name`.

  One ETS lookup where a keeper has published; the static `:expected_nodes`
  configuration where none ever ran (or a keeper is mid-restart), which is
  exactly the pre-T-109 answer.
  """
  @spec list(atom()) :: [node()]
  def list(name) do
    case current(name) do
      {:ok, config} -> config.members
      :error -> static()
    end
  end

  @doc """
  The published configuration — the epoch a `resize/3` must name to win its
  compare-and-swap, and the members that epoch expects. `:error` before the
  first verified refresh or where no keeper runs.
  """
  @spec current(atom()) :: {:ok, %{epoch: non_neg_integer(), members: [node()]}} | :error
  def current(name) do
    with table when table != :undefined <- :ets.whereis(process_name(name)),
         [{:config, config}] <- :ets.lookup(table, :config) do
      {:ok, config}
    else
      _absent -> :error
    end
  rescue
    ArgumentError -> :error
  end

  @doc """
  Advances the expected set to `members`, iff the row is still at
  `expected_epoch` — the atomic "the fleet is now this set" operation
  (T-110's CAS target). `{:error, :conflict}` means another resize won;
  refetch via `current/1` and reconsider.

  The new set publishes here immediately and reaches every other node
  within one refresh interval.
  """
  @spec resize(atom(), non_neg_integer(), [node()]) ::
          {:ok, %{epoch: non_neg_integer(), members: [node()]}} | {:error, term()}
  def resize(name, expected_epoch, members) do
    GenServer.call(process_name(name), {:resize, expected_epoch, members})
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
    {store_impl, store_opts} = ConfigStoreKeeper.store_spec(opts)

    case store_impl.start_link(store_opts) do
      {:ok, store} ->
        state = %{
          name: name,
          scope: "expected:#{name}",
          table: new_table(name),
          store_impl: store_impl,
          store: store,
          setup_done: false,
          static: Keyword.get(opts, :static) || static(),
          refresh_ms: Keyword.get(opts, :refresh_ms, @default_refresh_ms),
          epoch: nil
        }

        {:ok, state, {:continue, :refresh}}

      {:error, reason} ->
        {:stop, reason}
    end
  end

  @impl GenServer
  def handle_continue(:refresh, state) do
    {:noreply, state |> attempt_refresh() |> schedule()}
  end

  @impl GenServer
  def handle_info(:refresh, state) do
    {:noreply, state |> attempt_refresh() |> schedule()}
  end

  @impl GenServer
  def handle_call(:refresh, _from, state) do
    {:reply, :ok, attempt_refresh(state)}
  end

  def handle_call({:resize, expected_epoch, members}, _from, state) do
    with {:ok, state} <- ConfigStoreKeeper.setup(state),
         {:ok, config} <-
           state.store_impl.advance(state.store, state.scope, expected_epoch, members) do
      {:reply, {:ok, published(config)}, publish(state, config)}
    else
      {:error, reason} -> {:reply, {:error, reason}, state}
    end
  end

  defp attempt_refresh(state) do
    with {:ok, state} <- ConfigStoreKeeper.setup(state),
         {:ok, config} <- current_row(state) do
      publish(state, config)
    else
      :unseeded ->
        state

      {:error, reason} ->
        Logger.warning(fn ->
          "expected-nodes refresh failed for #{inspect(state.name)}: #{inspect(reason)}"
        end)

        state
    end
  end

  defp current_row(state) do
    case state.store_impl.fetch(state.store, state.scope) do
      {:ok, config} -> {:ok, config}
      :not_found -> seed(state)
      {:error, reason} -> {:error, reason}
    end
  end

  defp seed(%{static: []}), do: :unseeded
  defp seed(state), do: state.store_impl.ensure(state.store, state.scope, state.static)

  defp publish(state, config) do
    if config.epoch == state.epoch do
      state
    else
      :ets.insert(state.table, {:config, published(config)})

      %{state | epoch: config.epoch}
    end
  end

  defp new_table(name) do
    :ets.new(process_name(name), [:named_table, :protected, read_concurrency: true])
  end

  defp published(config), do: %{epoch: config.epoch, members: config.members}

  defp static do
    :smolquery
    |> Application.get_env(Smolquery.BufferService, [])
    |> Keyword.get(:expected_nodes, [])
  end

  defp schedule(state) do
    Process.send_after(self(), :refresh, state.refresh_ms)

    state
  end

  @doc """
  Whether the deployment configures a config store to keep this state in —
  the same condition that starts `Smolquery.BufferService.RingEpoch`, shared
  by every subtree that wants a keeper.
  """
  @spec configured?() :: boolean()
  def configured? do
    config = Application.get_env(:smolquery, Smolquery.BufferService, [])

    Keyword.has_key?(config, :epoch_store) or
      (Smolquery.Cluster.enabled?() and Keyword.get(config, :epoch_fencing, false))
  end
end
