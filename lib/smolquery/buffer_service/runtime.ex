defmodule Smolquery.BufferService.Runtime do
  @moduledoc """
  A running buffer service's resolved configuration.

  Turning application config into handles — a store, a manifest, an ownership ring
  — costs a little work that the write path must not pay per batch. So it happens
  once at boot and lands in `:persistent_term`, where `Client` reads it for free.
  That is the whole reason this module exists: without it, `Client.write_batch/3`
  would rebuild a 128-point hash ring on every insert.

  Naming is derived from one instance name, the way `Smolquery.Engine` names its
  database and connection, so a test can run an isolated buffer service beside the
  application's own.

  ## Configuration

      config :smolquery, Smolquery.BufferService,
        dir: "priv/data/buffer",
        flush_interval_ms: 1_000,
        flush_max_rows: 100_000,
        flush_max_bytes: 8_000_000,
        max_buffered_rows: 500_000,
        max_buffered_bytes: 64_000_000,
        write_timeout_ms: 15_000,
        seal_max_bytes: 67_108_864,
        seal_max_files: 64,
        seal_max_age_ms: 60_000,
        seal_retry_ms: 30_000,
        retire_grace_ms: 600_000,
        maintenance_interval_ms: 5_000,
        seal_consumer: {Smolquery.BufferService.SealLog, []},
        ring: [:"buffer1@host"]

  `:dir` is the root: segments go to a `Store.Local` beneath `segments/`, manifest
  logs to `manifests/`. They are separate because they answer to different rules —
  segments may move to an object store, while the log stays on the node that gave
  the ack.

  Pass `:store` to override the segment store outright:

      store: {Smolquery.Segments.Store.Local, dir: "/mnt/fast/buffer"}

  `retire_grace_ms` must exceed the longest query a planner can hold open. It is
  how long a retired micro-segment stays readable after a sealer committed it, and
  deleting one out from under an in-flight scan is exactly what it prevents.

  `hot_server_ip` / `hot_server_port` are where `HotServer` binds to serve
  micro-segments to DuckDB over `httpfs`. They say nothing about how a segment's
  URL is built — `HotServer` composes that from each request's own host and port,
  so the same manifest response is correct whether this bound its configured port
  or an OS-assigned one (`0`, what tests use to run many instances side by side).
  """

  alias Smolquery.BufferService.HotManifest
  alias Smolquery.BufferService.Ring
  alias Smolquery.Segments.Store

  @enforce_keys [:name, :manifest, :store, :ring]
  defstruct [
    :name,
    :manifest,
    :store,
    :ring,
    flush_interval_ms: 1_000,
    flush_max_rows: 100_000,
    flush_max_bytes: 8_000_000,
    max_buffered_rows: 500_000,
    max_buffered_bytes: 64_000_000,
    write_timeout_ms: 15_000,
    seal_max_bytes: 67_108_864,
    seal_max_files: 64,
    seal_max_age_ms: 60_000,
    seal_retry_ms: 30_000,
    retire_grace_ms: 600_000,
    maintenance_interval_ms: 5_000,
    seal_consumer: {Smolquery.BufferService.SealLog, []},
    hot_server_ip: {127, 0, 0, 1},
    hot_server_port: 4001
  ]

  @type t :: %__MODULE__{
          name: atom(),
          manifest: HotManifest.t(),
          store: Store.t(),
          ring: Ring.t(),
          flush_interval_ms: pos_integer(),
          flush_max_rows: pos_integer(),
          flush_max_bytes: pos_integer(),
          max_buffered_rows: pos_integer(),
          max_buffered_bytes: pos_integer(),
          write_timeout_ms: timeout(),
          seal_max_bytes: pos_integer(),
          seal_max_files: pos_integer(),
          seal_max_age_ms: pos_integer(),
          seal_retry_ms: pos_integer(),
          retire_grace_ms: pos_integer(),
          maintenance_interval_ms: pos_integer(),
          seal_consumer: {module(), term()},
          hot_server_ip: :inet.ip_address(),
          hot_server_port: :inet.port_number()
        }

  @limits [
    :flush_interval_ms,
    :flush_max_rows,
    :flush_max_bytes,
    :max_buffered_rows,
    :max_buffered_bytes,
    :write_timeout_ms,
    :seal_max_bytes,
    :seal_max_files,
    :seal_max_age_ms,
    :seal_retry_ms,
    :retire_grace_ms,
    :maintenance_interval_ms,
    :seal_consumer,
    :hot_server_ip,
    :hot_server_port
  ]

  @default_dir "priv/data/buffer"

  @doc """
  Resolves configuration into a runtime.

  Application config for `Smolquery.BufferService` supplies the defaults; `opts`
  overrides them, so a test passes what it needs and inherits the rest.
  """
  @spec new(keyword()) :: t()
  def new(opts \\ []) do
    config = Keyword.merge(Application.get_env(:smolquery, Smolquery.BufferService, []), opts)
    name = Keyword.get(config, :name, Smolquery.BufferService)
    dir = Keyword.get(config, :dir, @default_dir)
    store = build_store(config, dir)

    struct!(
      %__MODULE__{
        name: name,
        store: store,
        manifest:
          HotManifest.new(
            name: manifest(name),
            log_dir: Keyword.get(config, :log_dir, Path.join(dir, "manifests")),
            store: store
          ),
        ring: Ring.new!(Keyword.get(config, :ring, [node()]))
      },
      Keyword.take(config, @limits)
    )
  end

  @doc """
  Publishes a runtime for `Client` to read.
  """
  @spec put(t()) :: :ok
  def put(%__MODULE__{} = runtime), do: :persistent_term.put(key(runtime.name), runtime)

  @doc """
  The published runtime for an instance.

  `:error` means the buffer service is not running on this node — a caller is
  asking a node that does not hold the `:buffer` role.
  """
  @spec fetch(atom()) :: {:ok, t()} | :error
  def fetch(name) do
    {:ok, :persistent_term.get(key(name))}
  rescue
    ArgumentError -> :error
  end

  @doc """
  Withdraws a published runtime.
  """
  @spec delete(atom()) :: boolean()
  def delete(name), do: :persistent_term.erase(key(name))

  @doc """
  The top-level supervisor for an instance, as `Supervisor.start_link/1` names it.
  """
  @spec supervisor(atom()) :: atom()
  def supervisor(name), do: Module.concat(name, "Supervisor")

  @doc """
  The registry mapping a table to its `TableBuffer`.
  """
  @spec registry(atom()) :: atom()
  def registry(name), do: Module.concat(name, "Registry")

  @doc """
  The child id `HotServer`'s listener is started under.

  Bandit's own id is an opaque ref, so this is what lets a test find the
  listener's pid — to read the OS-assigned port back when `hot_server_port` is
  `0` — without depending on that ref.
  """
  @spec hot_server(atom()) :: atom()
  def hot_server(name), do: Module.concat(name, "HotServer")

  @doc """
  The partition supervisor `TableBuffer` processes start under.
  """
  @spec buffers(atom()) :: atom()
  def buffers(name), do: Module.concat(name, "Buffers")

  @doc """
  The process and ETS table holding the hot manifest.
  """
  @spec manifest(atom()) :: atom()
  def manifest(name), do: Module.concat(name, "HotManifest")

  @doc """
  The name a table's buffer registers under.
  """
  @spec via(t(), Store.table_ref()) :: GenServer.name()
  def via(%__MODULE__{name: name}, table_ref),
    do: {:via, Registry, {registry(name), table_ref}}

  defp build_store(config, dir) do
    case Keyword.get(config, :store) do
      nil -> Store.Local.new(dir: Path.join(dir, "segments"))
      {impl, opts} -> impl.new(opts)
      %Store{} = store -> store
    end
  end

  defp key(name), do: {__MODULE__, name}
end
