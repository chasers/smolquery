defmodule Smolquery.Engine.ExtensionLoader do
  @moduledoc """
  Loads DuckDB extensions one connection at a time on this node.

  `LOAD` verifies an extension's signature, and DuckDB's
  `ExtensionHelper::CheckExtensionSignature` is not safe to run from two
  instances at once against the same extension directory: a burst of job
  engines starting together threw a C++ exception inside it, which ended in
  `std::terminate` and took the whole VM down, every job with it (T-415).

  Every `INSTALL` and `LOAD` a `Smolquery.Engine.Connection` runs, at its
  start and for a federated query's extension later, goes through this
  process, whose mailbox is the queue: loads run in the order asked, one at
  a time, and the next waiter runs the moment the one before it finishes.
  A polled lock would not do: `:global`'s sleeps up to eight seconds
  between attempts, so a burst of starts would wait on a free lock. A load
  is milliseconds once the extension is on disk. A cold `INSTALL` downloads
  under the queue, which every other engine was waiting on anyway.

  What the queue does not cover is an extension DuckDB autoloads at first
  use, such as `json` for a `read_json` on an engine that did not list it.
  An engine that lists everything it uses never autoloads, which is why a
  query job's engine lists its lake's metadata driver beside `ducklake`.

  A VM without the application started, such as a script, has no loader
  and loads inline.
  """

  use GenServer

  @typedoc """
  A DuckDB extension: a core one by name, or `{name, :community}` for one
  from the community repository.
  """
  @type extension :: atom() | String.t() | {atom() | String.t(), :community}

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, :ok, name: Keyword.get(opts, :name, __MODULE__))
  end

  @doc """
  Installs and loads `extensions` on the ADBC connection `adbc`, in order,
  after every load queued before it: `:ok`, or
  `{:error, {:extension_failed, name, reason}}` for the first that fails.
  Nothing to load returns at once without queueing.
  """
  @spec load(GenServer.server(), GenServer.server(), [extension()]) :: :ok | {:error, term()}
  def load(loader \\ __MODULE__, adbc, extensions)

  def load(_loader, _adbc, []), do: :ok

  def load(loader, adbc, extensions) do
    case GenServer.whereis(loader) do
      nil -> load_each(adbc, extensions)
      pid -> GenServer.call(pid, {:load, adbc, extensions}, :infinity)
    end
  end

  @impl true
  def init(:ok), do: {:ok, %{}}

  @impl true
  def handle_call({:load, adbc, extensions}, _from, state),
    do: {:reply, load_each(adbc, extensions), state}

  defp load_each(adbc, extensions) do
    Enum.reduce_while(extensions, :ok, fn extension, :ok ->
      {name, source} = source(extension)

      case install_and_load(adbc, name, source) do
        :ok -> {:cont, :ok}
        {:error, reason} -> {:halt, {:error, {:extension_failed, name, reason}}}
      end
    end)
  end

  defp source({extension, :community}), do: {to_string(extension), " FROM community"}
  defp source(extension), do: {to_string(extension), ""}

  defp install_and_load(adbc, name, source) do
    with {:ok, _installed} <- Adbc.Connection.query(adbc, "INSTALL #{name}#{source}"),
         {:ok, _loaded} <- Adbc.Connection.query(adbc, "LOAD #{name}") do
      :ok
    end
  catch
    :exit, reason -> {:error, {:connection_exited, reason}}
  end
end
