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
        flush_max_bytes: 2_000_000,
        max_buffered_rows: 500_000,
        max_buffered_bytes: 64_000_000,
        ack_budget_ms: 5_000,
        write_timeout_ms: 15_000,
        control_timeout_ms: 15_000,
        seal_max_bytes: 67_108_864,
        seal_max_files: 64,
        seal_max_age_ms: 60_000,
        seal_retry_ms: 30_000,
        retire_grace_ms: 600_000,
        maintenance_interval_ms: 5_000,
        seal_consumer: {Smolquery.BufferService.SealLog, []},
        compression: :lz4raw,
        encode_concurrency: 2,
        ring: [:"buffer1@host"]

  `:dir` is the root: segments go to a `Store.Local` beneath `segments/`, manifest
  logs to `manifests/`. They are separate because they answer to different rules —
  segments may move to an object store, while the log stays on the node that gave
  the ack.

  Pass `:store` to override the segment store outright:

      store: {Smolquery.Segments.Store.Local, dir: "/mnt/fast/buffer"}

  `:replicator` is what a group commit requires beyond this node's disk
  before it acks (`Smolquery.BufferService.Replicator`), defaulting to the
  single-copy `Smolquery.BufferService.Replicator.None`:

      replicator: {Smolquery.BufferService.Replicator.None, []}

  `ack_budget_ms` is the write path's latency promise (PL-9): a batch whose
  Little's-law wait estimate exceeds it is refused at the `Endpoint` with
  `{:error, {:overloaded, predicted_ms}}` rather than queued in the buffer's
  mailbox toward a timeout. `:infinity` disables admission and restores
  silent queueing. It bounds *waiting*, where `max_buffered_rows`/`bytes`
  bound *memory* — the bench proved the queue that hurts is the mailbox,
  which the memory bounds never see.

  `flush_writer` defaults to `:duckdb`, the path the ingest edge's NDJSON
  passthrough is built around: the spooled bytes become Parquet in one `COPY`
  without ever being Elixir terms. It used to default to `:polars`, which meant
  a fresh deployment silently came up on the slower path *and* — because
  `Smolquery.IngestService`'s `ndjson_passthrough` is derived from this value —
  silently disabled the passthrough with it. Two defaults compounding into the
  slow path is not a default anyone chooses on purpose.

  `flush_max_bytes` defaults to 2 MB rather than 8 MB. Measured on the
  comparison rig at 2,346-byte rows: 8 MB gave 24,067 rows/s at a 926 ms ack
  and 2 MB gave 29,533 at 780 ms. There is a floor under it — 1 MB drops to
  20,467 and 500 KB to 13,733 — because the encode has a fixed per-commit cost
  that stops being worth paying once the accumulate wait is short enough. The
  optimum is a function of how fast a partition fills, so re-measure it for a
  very different row width.

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
  alias Smolquery.BufferService.Replicator
  alias Smolquery.BufferService.Ring
  alias Smolquery.Segments.Store

  @enforce_keys [:name, :manifest, :store, :ring, :replicator]
  defstruct [
    :name,
    :manifest,
    :store,
    :ring,
    :replicator,
    :spool_dir,
    flush_interval_ms: 1_000,
    flush_max_rows: 100_000,
    flush_max_bytes: 2_000_000,
    max_buffered_rows: 500_000,
    max_buffered_bytes: 64_000_000,
    ack_budget_ms: 5_000,
    write_timeout_ms: 15_000,
    control_timeout_ms: 15_000,
    seal_max_bytes: 67_108_864,
    seal_max_files: 64,
    seal_max_age_ms: 60_000,
    seal_retry_ms: 30_000,
    retire_grace_ms: 600_000,
    maintenance_interval_ms: 5_000,
    seal_consumer: {Smolquery.BufferService.SealLog, []},
    compression: :zstd,
    encode_concurrency: 2,
    flush_writer: :duckdb,
    write_pool_size: 1,
    row_validator: nil,
    hot_server_ip: {127, 0, 0, 1},
    hot_server_port: 4001
  ]

  @type t :: %__MODULE__{
          name: atom(),
          manifest: HotManifest.t(),
          store: Store.t(),
          ring: Ring.t(),
          replicator: Replicator.t(),
          spool_dir: Path.t(),
          flush_interval_ms: pos_integer(),
          flush_max_rows: pos_integer(),
          flush_max_bytes: pos_integer(),
          max_buffered_rows: pos_integer(),
          max_buffered_bytes: pos_integer(),
          ack_budget_ms: timeout(),
          write_timeout_ms: timeout(),
          control_timeout_ms: timeout(),
          seal_max_bytes: pos_integer(),
          seal_max_files: pos_integer(),
          seal_max_age_ms: pos_integer(),
          seal_retry_ms: pos_integer(),
          retire_grace_ms: pos_integer(),
          maintenance_interval_ms: pos_integer(),
          seal_consumer: {module(), term()},
          compression: atom(),
          encode_concurrency: pos_integer(),
          flush_writer: :polars | :duckdb,
          write_pool_size: pos_integer(),
          row_validator: {module(), atom()} | nil,
          hot_server_ip: :inet.ip_address(),
          hot_server_port: :inet.port_number()
        }

  @limits [
    :flush_interval_ms,
    :flush_max_rows,
    :flush_max_bytes,
    :max_buffered_rows,
    :max_buffered_bytes,
    :ack_budget_ms,
    :write_timeout_ms,
    :control_timeout_ms,
    :seal_max_bytes,
    :seal_max_files,
    :seal_max_age_ms,
    :seal_retry_ms,
    :retire_grace_ms,
    :maintenance_interval_ms,
    :seal_consumer,
    :compression,
    :encode_concurrency,
    :flush_writer,
    :write_pool_size,
    :row_validator,
    :hot_server_ip,
    :hot_server_port
  ]

  @codecs [:lz4raw, :zstd, :snappy, :gzip, :uncompressed]

  @default_dir "priv/data/buffer"

  @doc """
  Resolves configuration into a runtime.

  Application config for `Smolquery.BufferService` supplies the defaults; `opts`
  overrides them, so a test passes what it needs and inherits the rest.

  Raises on a `compression` outside #{inspect(@codecs)}, for the same reason
  `Smolquery.StorageService.Runtime` does: a codec discovered bad per group
  commit would fail every flush rather than once, here, at boot. The default
  stays `:zstd` because switching to `:lz4raw` measured *neutral* on the
  replication rig (71.6k vs 73.2k rows/s, run-to-run noise) — the group
  commit's serial cost lives in its other legs, not the codec — and cheaper
  encode was the only reason to prefer lz4 for segments the sealer re-encodes
  within seconds anyway.
  """
  @spec new(keyword()) :: t()
  def new(opts \\ []) do
    config = Keyword.merge(Application.get_env(:smolquery, Smolquery.BufferService, []), opts)
    validate_compression!(Keyword.get(config, :compression, :zstd))
    name = Keyword.get(config, :name, Smolquery.BufferService)
    dir = Keyword.get(config, :dir, @default_dir)
    store = build_store(config, dir)

    struct!(
      %__MODULE__{
        name: name,
        store: store,
        spool_dir: Keyword.get(config, :spool_dir, Path.join(dir, "spool")),
        manifest:
          HotManifest.new(
            name: manifest(name),
            log_dir: Keyword.get(config, :log_dir, Path.join(dir, "manifests")),
            store: store
          ),
        ring: Ring.new!(Keyword.get(config, :ring, [node()])),
        replicator:
          Replicator.new(
            Keyword.get(config, :replicator, {Smolquery.BufferService.Replicator.None, []})
          )
      },
      Keyword.take(config, @limits)
    )
  end

  @doc """
  The DuckDB instance a `:duckdb` flush writes its segment with.

  Its own instance, not the query path's, for two reasons stated elsewhere in
  this codebase: `Smolquery.Engine.Connection` wraps one ADBC connection and is
  therefore a per-query mutex, and a DuckDB internal fault invalidates the whole
  database it happens on. Sharing one with reads would make a flush queue behind
  a user's scan, and let a scan's rare fault take unacked rows with it.
  """
  @spec engine(atom(), non_neg_integer()) :: atom()
  def engine(name, index), do: Module.concat(name, "Engine#{index}")

  @doc """
  Which of the pool's DuckDB instances writes the segment named by `key`.

  Hashed on the segment id, not the table: a table has one buffer and one
  committer, so hashing on the table would send every one of its flushes to the
  same connection and the pool would do nothing for the case that needs it most.
  """
  @spec engine_for(t(), term()) :: atom()
  def engine_for(%__MODULE__{name: name, write_pool_size: size}, key),
    do: engine(name, :erlang.phash2(key, size))

  @doc """
  Every DuckDB instance the write pool runs, in index order.
  """
  @spec engines(t()) :: [atom()]
  def engines(%__MODULE__{name: name, write_pool_size: size}),
    do: Enum.map(0..(size - 1), &engine(name, &1))

  defp validate_compression!(compression) when compression in @codecs, do: :ok

  defp validate_compression!(compression) do
    raise ArgumentError,
          "unsupported hot-tier compression: #{inspect(compression)} " <>
            "(expected one of #{inspect(@codecs)})"
  end

  use Smolquery.Runtime

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
  The registry mapping a table to its `TableBuffer.Committer`.

  Separate from `registry/1` so everything that enumerates buffers — drain,
  load queries — keeps seeing exactly one process per table.
  """
  @spec committer_registry(atom()) :: atom()
  def committer_registry(name), do: Module.concat(name, "CommitterRegistry")

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

  @doc """
  The name a table's committer registers under.
  """
  @spec committer_via(t(), Store.table_ref()) :: GenServer.name()
  def committer_via(%__MODULE__{name: name}, table_ref),
    do: {:via, Registry, {committer_registry(name), table_ref}}

  defp build_store(config, dir) do
    case Keyword.get(config, :store) do
      nil -> Store.Local.new(dir: Path.join(dir, "segments"))
      {impl, opts} -> impl.new(opts)
      %Store{} = store -> store
    end
  end
end
