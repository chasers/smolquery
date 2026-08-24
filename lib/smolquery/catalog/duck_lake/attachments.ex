defmodule Smolquery.Catalog.DuckLake.Attachments do
  @moduledoc """
  Which dataset lakes a catalog engine has attached, and the bound on how
  many (PL-51 D6).

  A dataset with its own catalog is its own DuckLake, attached to the same
  engine under `ds_<name>` on first touch; one with its own storage needs a
  DuckDB secret for its bucket. Neither happens at boot: the target is tens
  of thousands of datasets, and a cluster start cannot probe them all. So
  `ensure/2` is the one path onto an engine for a dataset, and this server is
  the record of what that engine holds.

  ## Why a server, and why one per engine

  An `ATTACH` belongs to the DuckDB instance, so every connection slot of an
  engine shares it. Two callers touching the same unattached dataset at once
  would each run the attach; `ATTACH IF NOT EXISTS` makes the second a no-op,
  but the secret and the side tables behind it are several statements, and
  the eviction below must not detach a lake another caller is mid-attach on.
  Serializing through one process per engine settles all of that without a
  lock DuckDB does not offer. The server sits after the engine in a
  `rest_for_one` tree, so an engine that restarts — and so loses every
  attachment — takes this record down with it and starts empty.

  ## The bound

  Every attached Postgres lake holds a connection to that Postgres. The
  server keeps at most `:limit` datasets attached; past it, the least
  recently used is detached and its secret dropped before the next attach.
  A detach is safe at any moment: nothing durable lives in the attachment,
  and the next touch re-attaches. There is no idle sweep yet — a quiet
  tenant keeps its connection until the limit evicts it — because there is
  no workload to size a window against.

  ## What is not here

  The dataset row is read from the default lake on every touch of an
  unattached dataset, not cached; a cache is a measured decision, not a
  default. A change to a dataset's credentials does not reach an engine that
  already holds it — that is PL-51 layer 5.
  """

  use GenServer

  require Logger

  alias Smolquery.Catalog.Dataset
  alias Smolquery.Catalog.DuckLake
  alias Smolquery.Engine

  @default_limit 64

  @type option ::
          {:engine, atom()}
          | {:data_path, String.t()}
          | {:limit, pos_integer()}

  @doc """
  The registered name of the server that serves `engine`.
  """
  @spec name(atom()) :: atom()
  def name(engine), do: Module.concat(engine, "Attachments")

  @doc """
  Starts the server for `:engine`.

  ## Options

    * `:engine` (required) — the engine whose attachments this server owns
    * `:data_path` (required) — the default lake's data path; a dataset
      with its own catalog but no storage of its own gets a directory under it
    * `:limit` — most datasets kept attached at once (`#{@default_limit}`)

  """
  @spec start_link([option()]) :: GenServer.on_start()
  def start_link(opts) do
    engine = Keyword.fetch!(opts, :engine)
    GenServer.start_link(__MODULE__, opts, name: name(engine))
  end

  @doc """
  Makes `dataset` reachable on `engine`: its secret created, its lake
  attached, its side tables and schema present. A dataset on the deployment
  defaults for both axes needs nothing and answers `:ok` at once.

  Idempotent, and serialized per engine. A failure — an unreachable Postgres,
  a refused credential, a lake at a format this extension cannot open — is
  the attach statement's own error, and leaves nothing attached.
  """
  @spec ensure(atom(), Dataset.t()) :: :ok | {:error, term()}
  def ensure(_engine, %Dataset{catalog: nil, storage: nil}), do: :ok

  def ensure(engine, %Dataset{} = dataset) do
    GenServer.call(name(engine), {:ensure, dataset}, :infinity)
  end

  @doc """
  The names of the datasets `engine` currently holds, most recently used
  first.
  """
  @spec attached(atom()) :: [String.t()]
  def attached(engine), do: GenServer.call(name(engine), :attached)

  @impl GenServer
  def init(opts) do
    {:ok,
     %{
       engine: Keyword.fetch!(opts, :engine),
       data_path: Keyword.fetch!(opts, :data_path),
       limit: Keyword.get(opts, :limit, @default_limit),
       attached: %{}
     }}
  end

  @impl GenServer
  def handle_call({:ensure, %Dataset{name: name} = dataset}, _from, state) do
    if Map.has_key?(state.attached, name) do
      {:reply, :ok, touch(state, name)}
    else
      attach(state, dataset)
    end
  end

  def handle_call(:attached, _from, state) do
    names =
      state.attached
      |> Enum.sort_by(fn {_name, used_at} -> used_at end, :desc)
      |> Enum.map(fn {name, _used_at} -> name end)

    {:reply, names, state}
  end

  defp attach(state, dataset) do
    with {:ok, statements} <- DuckLake.dataset_statements(dataset, state.data_path),
         state <- evict(state),
         :ok <- run(state.engine, DuckLake.dataset_extension_statements(dataset) ++ statements) do
      {:reply, :ok, touch(state, dataset.name)}
    else
      {:error, reason} -> {:reply, {:error, reason}, state}
    end
  end

  defp evict(%{attached: attached, limit: limit} = state) when map_size(attached) < limit,
    do: state

  defp evict(state) do
    {victim, _used_at} = Enum.min_by(state.attached, fn {_name, used_at} -> used_at end)

    case run(state.engine, DuckLake.dataset_detach_statements(victim)) do
      :ok ->
        :ok

      {:error, reason} ->
        Logger.warning("detach of dataset #{victim} failed: #{inspect(reason)}")
    end

    %{state | attached: Map.delete(state.attached, victim)}
  end

  defp run(engine, statements) do
    Enum.reduce_while(statements, :ok, fn sql, :ok ->
      case Engine.query(engine, sql, [], :infinity) do
        {:ok, _result} -> {:cont, :ok}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
  end

  defp touch(state, name) do
    %{state | attached: Map.put(state.attached, name, System.monotonic_time())}
  end
end
