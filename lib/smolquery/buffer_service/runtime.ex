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
        flush_idle_interval_ms: 5,
        commit_siblings: 5,
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
        ring: [:"buffer1@host"]

  `:dir` is the root: segments go to a `Store.Local` beneath `segments/`, manifest
  logs to `manifests/`. They are separate because they answer to different rules —
  segments may move to an object store, while the log stays on the node that gave
  the ack.

  Pass `:store` to override the segment store outright:

      store: {Smolquery.Segments.Store.Local, dir: "/mnt/fast/buffer"}

  `:write_pool_size` is how many DuckDB instances the `:duckdb` flush spreads
  its encodes over — see `engine_for/2` for why it hashes on the segment id,
  and `engines/1` for the pool itself. It must be a positive integer no larger
  than `@max_write_pool_size`; `new/1` refuses anything else at boot rather
  than let a node come up looking configured. Each member is a whole
  `Smolquery.Engine` subtree carrying that module's own `:memory_limit`, so the
  declared DuckDB memory budget is `write_pool_size ×` it —
  `:write_engine_memory_limit` (default `nil`) narrows the pool's members
  without touching the query engine's.

  Unset, it is `default_write_pool_size/0` — the node's scheduler count, capped
  at `@max_write_pool_size`. It used to be `1`, a quarter of the load rig's
  reference tuning and an eighth of what that rig measured fastest; a stock
  install was the one shape nobody benched. `:encode_concurrency` had the same
  problem at `2` and is now `default_encode_concurrency/0`, one encode per
  scheduler — so a single-scheduler container encodes serially, where it used to
  overlap two. That is the arithmetic saying what the node was told about its own
  CPU, and `2` was never a measurement.

  Both resolve in `new/1` rather than in the struct, because a struct default
  is evaluated when this module compiles: a release built on an eight-scheduler
  builder would carry that eight to every host it ran on, which is the trap
  `Smolquery.Engine`'s `:threads` already fell into in `config/config.exs`. The
  `1` and `2` still sitting in the struct are the floor a hand-built `%Runtime{}`
  gets, not what a node boots on.

  Deriving the pool from the schedulers multiplies the declared DuckDB memory by
  the same factor, since nothing divides a size string — a sixteen-scheduler
  host inheriting a `2GB` engine limit declares 32 GB of write budget. Set
  `:write_engine_memory_limit` on a host where that matters;
  `Smolquery.DeployedShape` prints both factors on the `buffer shape:` line
  at boot.

  `:write_engine_threads` (default `nil`) does the same for threads. Left unset,
  the pool divides `Smolquery.Engine`'s thread count by its own size, in
  `Smolquery.BufferService.Supervisor`, where the size is known. That division
  describes a budget only while the pool is smaller than the thread count; above
  that it reaches its floor of one and stops dividing anything, which is where an
  operator states the number instead.

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

  `commit_siblings` and `flush_idle_interval_ms` make the group-commit wait
  adaptive, after Postgres's `commit_delay`/`commit_siblings` (T-202). An
  accumulation window that opens with fewer than `commit_siblings` inserts
  already in flight closes after `flush_idle_interval_ms` instead of
  `flush_interval_ms`. The wait only buys batching when other writers are
  active; at one writer it is pure ack latency. `commit_siblings: 0` turns
  the short window off. See `Smolquery.BufferService.TableBuffer` for what
  counts as in flight and when the choice is made.

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
    flush_idle_interval_ms: 5,
    commit_siblings: 5,
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
    write_engine_memory_limit: nil,
    write_engine_threads: nil,
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
          flush_idle_interval_ms: non_neg_integer(),
          commit_siblings: non_neg_integer(),
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
          write_engine_memory_limit: String.t() | nil,
          write_engine_threads: pos_integer() | nil,
          row_validator: {module(), atom()} | nil,
          hot_server_ip: :inet.ip_address(),
          hot_server_port: :inet.port_number()
        }

  @limits [
    :flush_interval_ms,
    :flush_idle_interval_ms,
    :commit_siblings,
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
    :write_engine_memory_limit,
    :write_engine_threads,
    :row_validator,
    :hot_server_ip,
    :hot_server_port
  ]

  @codecs [:lz4raw, :zstd, :snappy, :gzip, :uncompressed]

  # A sanity ceiling, not a tuned one. Each pool member is a whole
  # `Smolquery.Engine` subtree — an `Adbc.Database`, a connection, and that
  # module's own `:memory_limit` and thread count — so the pool's declared budget
  # scales linearly with this number. The ceiling is here so a fat-fingered extra
  # digit is refused at boot rather than declaring tens of gigabytes of DuckDB
  # memory limits and a thread per member per scheduler.
  @max_write_pool_size 32

  @default_dir "priv/data/buffer"

  @doc """
  Resolves configuration into a runtime.

  Application config for `Smolquery.BufferService` supplies the defaults; `opts`
  overrides them, so a test passes what it needs and inherits the rest.

  `:write_pool_size` and `:encode_concurrency` are the two whose default is a
  measurement of the host rather than a number in this file — see
  `default_write_pool_size/0` and `default_encode_concurrency/0`. They are
  resolved here, at boot, for the reason the moduledoc gives: a struct default
  would freeze the *builder's* scheduler count into the release.

  Raises on a `compression` outside #{inspect(@codecs)}, for the same reason
  `Smolquery.StorageService.Runtime` does: a codec discovered bad per group
  commit would fail every flush rather than once, here, at boot. The default
  stays `:zstd` because switching to `:lz4raw` measured *neutral* on the
  replication rig (71.6k vs 73.2k rows/s, run-to-run noise) — the group
  commit's serial cost lives in its other legs, not the codec — and cheaper
  encode was the only reason to prefer lz4 for segments the sealer re-encodes
  within seconds anyway.

  Raises for the same reason on a `write_pool_size` that is not a usable pool
  size. `struct!/2` raises only on an *unknown* key, so a known key holding
  nonsense would be written verbatim and detonate somewhere that cannot report
  it: `0` names an engine `Engine-1` that was never started and then raises
  inside `:erlang.phash2/2` on the first flush, taking the committer and every
  unacked batch with it.

  `:encode_concurrency` gets the same gate: a `0` reaching the committer
  starts no encodes, and the table silently fills to `buffer_full`.

  `:commit_siblings` and `:flush_idle_interval_ms` are gated too: a negative
  idle interval detonates in `Process.send_after/3` at a table's first
  accumulation, far from the config that set it.
  """
  @spec new(keyword()) :: t()
  def new(opts \\ []) do
    config =
      :smolquery
      |> Application.get_env(Smolquery.BufferService, [])
      |> Keyword.merge(opts)
      |> Keyword.put_new_lazy(:write_pool_size, &default_write_pool_size/0)
      |> Keyword.put_new_lazy(:encode_concurrency, &default_encode_concurrency/0)

    validate_compression!(Keyword.get(config, :compression, :zstd))
    validate_write_pool_size!(Keyword.fetch!(config, :write_pool_size))
    validate_encode_concurrency!(Keyword.fetch!(config, :encode_concurrency))
    validate_commit_siblings!(Keyword.get(config, :commit_siblings, 5))
    validate_flush_idle_interval!(Keyword.get(config, :flush_idle_interval_ms, 5))
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
  The `:write_pool_size` a node comes up on when nothing configures one.

  One DuckDB instance per scheduler, capped at #{@max_write_pool_size}. The
  pool exists to stop a table's flushes queueing behind one connection, so the
  count that bounds it is the node's own parallelism — the same number
  `write_engine_budget/1` then divides among the members.

  The cap is the reason this is `min/2` and not the raw scheduler count: a
  64-core host would otherwise declare 64 DuckDB instances, each with
  `Smolquery.Engine`'s whole memory limit.
  """
  @spec default_write_pool_size() :: pos_integer()
  def default_write_pool_size, do: min(System.schedulers_online(), @max_write_pool_size)

  @doc """
  The `:encode_concurrency` a node comes up on when nothing configures one.

  The scheduler count, with no floor and no ceiling. Up to
  `default_write_pool_size/0`'s cap that is one encode per pool member; past
  it the extra encodes queue on the members' connections.

  There is no special case for a single-scheduler host. Such a node is being
  told it has one core, and the honest reading of that is one encode — the
  standing `2` it replaces was never a measurement, only a number that had
  always been there.
  """
  @spec default_encode_concurrency() :: pos_integer()
  def default_encode_concurrency, do: System.schedulers_online()

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
  The supervisor the `:duckdb` write pool's instances run under.

  Its own supervisor, below the buffers — see
  `Smolquery.BufferService.Supervisor` for why a pool member's restarts must not
  reach the process that holds a table's unacked rows.
  """
  @spec write_pool(atom()) :: atom()
  def write_pool(name), do: Module.concat(name, "WritePool")

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

  The step is explicit: `0..-1` defaults to a *descending* range in Elixir, so a
  `write_pool_size` of `0` used to name an engine `Engine-1` rather than name
  none. `new/1` refuses that value now, and this is what keeps the two from
  disagreeing again.
  """
  @spec engines(t()) :: [atom()]
  def engines(%__MODULE__{name: name, write_pool_size: size}),
    do: Enum.map(0..(size - 1)//1, &engine(name, &1))

  @doc """
  The `Smolquery.Engine` options that size one member of the write pool.

  Each member is sized for the pool rather than left to inherit
  `Smolquery.Engine`'s application config whole, because that config describes
  *one* instance: a pool of eight inheriting `threads: System.schedulers_online()`
  declares eight times the node's schedulers, and `memory_limit: "2GB"` eight
  times over. Threads divide here. The memory limit cannot be divided without
  parsing DuckDB's size grammar, so it is a configured value instead —
  `:write_engine_memory_limit`, whose docstring above states the multiplication
  an operator is choosing when they leave it unset.

  The number divided is `Smolquery.Engine`'s *configured* thread count, not the
  scheduler count. Dividing the schedulers would ignore the setting this exists
  to respect: a node whose operator lowered `Smolquery.Engine`'s `:threads` to 4
  on a sixteen-scheduler host would hand a pool of one sixteen threads, and a
  change meant to narrow the declared budget would widen it. `config/test.exs`
  sets that value deliberately and is the case that shows it soonest.

  `:write_engine_threads` replaces the division outright, because the division
  only describes a budget while the pool is smaller than the thread count.
  Above that it reaches its floor of one and an operator who wants a different
  shape has to say the number rather than infer it.

  Two callers, deliberately: `Smolquery.BufferService.Supervisor` starts each
  pool member with exactly this, and `Smolquery.DeployedShape` announces it —
  the resolved budget is part of the shape a node came up on, or the division
  becomes one more derived value nothing states out loud.
  """
  @spec write_engine_budget(t()) :: keyword()
  def write_engine_budget(%__MODULE__{write_pool_size: size} = runtime) do
    threads = [threads: runtime.write_engine_threads || max(div(engine_threads(), size), 1)]

    case runtime.write_engine_memory_limit do
      nil -> threads
      limit -> [{:memory_limit, limit} | threads]
    end
  end

  defp engine_threads do
    :smolquery
    |> Application.get_env(Smolquery.Engine, [])
    |> Keyword.get(:threads, System.schedulers_online())
  end

  defp validate_compression!(compression) when compression in @codecs, do: :ok

  defp validate_compression!(compression) do
    raise ArgumentError,
          "unsupported hot-tier compression: #{inspect(compression)} " <>
            "(expected one of #{inspect(@codecs)})"
  end

  defp validate_write_pool_size!(size)
       when is_integer(size) and size > 0 and size <= @max_write_pool_size,
       do: :ok

  defp validate_write_pool_size!(size) do
    raise ArgumentError,
          "unusable write pool size: #{inspect(size)} " <>
            "(expected an integer in 1..#{@max_write_pool_size})"
  end

  defp validate_encode_concurrency!(concurrency)
       when is_integer(concurrency) and concurrency > 0,
       do: :ok

  defp validate_encode_concurrency!(concurrency) do
    raise ArgumentError,
          "unusable encode concurrency: #{inspect(concurrency)} " <>
            "(expected a positive integer)"
  end

  defp validate_commit_siblings!(count) when is_integer(count) and count >= 0, do: :ok

  defp validate_commit_siblings!(count) do
    raise ArgumentError,
          "unusable commit siblings: #{inspect(count)} " <>
            "(expected a non-negative integer)"
  end

  defp validate_flush_idle_interval!(interval) when is_integer(interval) and interval >= 0,
    do: :ok

  defp validate_flush_idle_interval!(interval) do
    raise ArgumentError,
          "unusable idle flush interval: #{inspect(interval)} " <>
            "(expected a non-negative integer of milliseconds)"
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
