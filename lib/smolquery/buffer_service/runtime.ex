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
        encode_concurrency: 1,
        flush_writer: :polars,
        write_pool_size: 1,
        write_engine_memory_limit: nil,
        buffer_fullsweep_after: 10,
        buffer_min_heap_size: nil,
        committer_fullsweep_after: 0,
        committer_min_heap_size: nil,
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
        ring: [:"buffer1@host"]

  `:dir` is the root: segments go to a `Store.Local` beneath `segments/`, manifest
  logs to `manifests/`. They are separate because they answer to different rules —
  segments may move to an object store, while the log stays on the node that gave
  the ack.

  Pass `:store` to override the segment store outright:

      store: {Smolquery.Segments.Store.Local, dir: "/mnt/fast/buffer"}

  `:fsync` (default `true`) is passed to both the default `Store.Local` and the
  `HotManifest` — the two fsyncs on the write path. An explicit `:store` keeps
  its own opts (pass `fsync: false` there to turn the segment sync off alone);
  the top-level key still reaches the manifest. Turning either off is a
  durability trade — see `HotManifest` and `Store.Local`.

  ## `flush_writer` — who encodes the Parquet

  `:flush_writer` (default `:polars`) chooses which encoder a flush runs, and it
  is opt-in for good reason.

    * `:polars` — the default and the only path with a per-row contract. Rows are
      validated per index by `Smolquery.IngestService.Validator`, arrive here as
      Elixir terms, and `Smolquery.Segments.Writer` encodes them through
      `Explorer.DataFrame`. A row this table cannot accept comes back to its
      caller in `insertErrors` and the rest of the batch commits.
    * `:duckdb` — the API spools an `application/x-ndjson` body to disk unparsed
      and DuckDB reads it, sorts it and writes the Parquet itself, so no row in
      the batch ever becomes an Elixir term. What it gives up is that contract:
      nothing validates the body, one line DuckDB cannot coerce fails the whole
      `COPY` — and a flush groups every request in the window, so it fails
      theirs too — and `insertErrors` is therefore always empty. It also needs a
      **local** segment store (see below) and starts one `Smolquery.Engine` per
      `:write_pool_size`, which the default path pays nothing for.

  Only `:polars` and `:duckdb` are accepted; `new/1` refuses anything else at
  boot rather than let a node come up looking configured and start no engines.
  The flag is on the struct so the API layer can read it and refuse an NDJSON
  body on a `:polars` node before a byte reaches disk — `docs/api.md` states the
  weaker contract the `:duckdb` route offers.

  Because the `:duckdb` flush writes its segment with `COPY … TO` and then reads
  the file back for its row count and stats, `new/1` also refuses `:duckdb`
  together with a store whose `Smolquery.Segments.Store.shared?/1` is `true`: a
  shared store would upload the segment and only then fail the read-back, and the
  object it left behind is named by no manifest, catalog, or GC pass.

  `:write_pool_size` (default `1`) is how many DuckDB instances the `:duckdb`
  flush spreads its encodes over — see `engine_for/2` for why it hashes on the
  segment id, and `engines/1` for the pool itself. It must be a positive integer:
  `0` used to start an engine literally named `Engine-1` and then raise inside
  `:erlang.phash2/2` on the first flush, taking the committer and every unacked
  batch with it. Each member is a whole `Smolquery.Engine` subtree carrying that
  module's own `:memory_limit`, so the declared DuckDB memory budget is
  `write_pool_size ×` it — `:write_engine_memory_limit` narrows the pool's
  members without touching the query engine's.

  `:encode_concurrency` (default `1`) is how many of a table's Parquet encodes
  may run at once. It bounds only the encode — the manifest append and the
  replication round stay serialized in the table's
  `Smolquery.BufferService.TableBuffer.Committer`, which is what keeps one
  writer per manifest log. At `1` a commit runs inline exactly as it always
  has; above `1`, up to that many encodes are in flight and the next commit
  waits, so a table's resident rows are bounded by the number of slots.

  ## Heap policy for the two processes that hold a batch

  A table's `TableBuffer` and its `Committer` are the processes decoded rows
  live in, and `Smolquery.Heap` explains why the emulator's generational
  default is the wrong shape for them. Four keys set the two flags on each,
  and every one of them is a starting point for measurement rather than a
  tuned number:

    * `:committer_fullsweep_after` (default `0`) — the committer's live set
      between commits is a queue, a struct and a log file descriptor; a
      commit's rows are garbage the moment it reports done. A full sweep costs
      the live set rather than the garbage, so at `0` — every collection a full
      sweep — the rows are reclaimed where they die instead of being promoted
      to an old heap that only a major collection empties. Above
      `encode_concurrency: 1` the encode runs in a short-lived task instead,
      which is deliberately left alone: its heap dies with it, and no
      collection has to find that out.
    * `:buffer_fullsweep_after` (default `10`) — the buffer is the opposite
      case in one respect and the same in another. Its accumulator is *live*
      until the handoff, up to `flush_max_bytes` of it, so full sweeping every
      collection would copy that tail over and over; after the handoff the same
      bytes are garbage on an old heap. Ten bounds how long a handed-off
      accumulator survives without charging every collection for the live one.
    * `:buffer_min_heap_size` / `:committer_min_heap_size` (default `nil`) —
      the heap floor in words, unset. It is per process and there is one pair
      per table, so a floor that helps one table's throughput multiplies by
      every table the node owns. Raise it deliberately against a measurement,
      not by default.

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
    flush_interval_ms: 1_000,
    flush_max_rows: 100_000,
    flush_max_bytes: 8_000_000,
    max_buffered_rows: 500_000,
    max_buffered_bytes: 64_000_000,
    encode_concurrency: 1,
    flush_writer: :polars,
    write_pool_size: 1,
    write_engine_memory_limit: nil,
    buffer_fullsweep_after: 10,
    buffer_min_heap_size: nil,
    committer_fullsweep_after: 0,
    committer_min_heap_size: nil,
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
    hot_server_ip: {127, 0, 0, 1},
    hot_server_port: 4001
  ]

  @type t :: %__MODULE__{
          name: atom(),
          manifest: HotManifest.t(),
          store: Store.t(),
          ring: Ring.t(),
          replicator: Replicator.t(),
          flush_interval_ms: pos_integer(),
          flush_max_rows: pos_integer(),
          flush_max_bytes: pos_integer(),
          max_buffered_rows: pos_integer(),
          max_buffered_bytes: pos_integer(),
          encode_concurrency: pos_integer(),
          flush_writer: :polars | :duckdb,
          write_pool_size: pos_integer(),
          write_engine_memory_limit: String.t() | nil,
          buffer_fullsweep_after: non_neg_integer() | nil,
          buffer_min_heap_size: non_neg_integer() | nil,
          committer_fullsweep_after: non_neg_integer() | nil,
          committer_min_heap_size: non_neg_integer() | nil,
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
          hot_server_ip: :inet.ip_address(),
          hot_server_port: :inet.port_number()
        }

  @limits [
    :flush_interval_ms,
    :flush_max_rows,
    :flush_max_bytes,
    :max_buffered_rows,
    :max_buffered_bytes,
    :encode_concurrency,
    :flush_writer,
    :write_pool_size,
    :write_engine_memory_limit,
    :buffer_fullsweep_after,
    :buffer_min_heap_size,
    :committer_fullsweep_after,
    :committer_min_heap_size,
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
    :hot_server_ip,
    :hot_server_port
  ]

  @default_dir "priv/data/buffer"

  # A sanity ceiling, not a tuned one. Each pool member is a whole
  # `Smolquery.Engine` subtree — an `Adbc.Database`, a connection, and this
  # module's own `:memory_limit` and thread count — so the pool's declared
  # budget scales linearly with this number, and `engine_for/2`'s own docstring
  # measures the returns as stopping at 4. The ceiling is here so a fat-fingered
  # extra digit is refused at boot rather than declaring tens of gigabytes of
  # DuckDB memory limits and a thread per member per scheduler.
  @max_write_pool_size 32

  # How much of `write_timeout_ms` is left to everything after the encode — the
  # manifest append and its fsync, the replication round, the reply. The flush's
  # DuckDB calls get the rest, so the *inner* call fails first with a reportable
  # error instead of the caller giving up and the late exit taking the committer
  # with it. See `Smolquery.Segments.Writer`'s `@default_timeout`.
  @engine_timeout_margin_ms 5_000

  @doc """
  Resolves configuration into a runtime.

  Application config for `Smolquery.BufferService` supplies the defaults; `opts`
  overrides them, so a test passes what it needs and inherits the rest.

  Raises `ArgumentError` on a configuration this service cannot run — a
  `flush_writer` it does not implement, a `write_pool_size` that is not a usable
  pool size, or `flush_writer: :duckdb` against a shared segment store. Every one
  of those otherwise boots a node that looks healthy and destroys unacked rows on
  its first flush, so a buffer that cannot start says so at boot, the way
  `Smolquery.BufferService.TableBuffer.Committer` already does for
  `encode_concurrency`.
  """
  @spec new(keyword()) :: t()
  def new(opts \\ []) do
    config = Keyword.merge(Application.get_env(:smolquery, Smolquery.BufferService, []), opts)
    name = Keyword.get(config, :name, Smolquery.BufferService)
    dir = Keyword.get(config, :dir, @default_dir)
    fsync = Keyword.get(config, :fsync, true)
    store = build_store(config, dir, fsync)
    limits = validate!(Keyword.take(config, @limits), store)

    struct!(
      %__MODULE__{
        name: name,
        store: store,
        manifest:
          HotManifest.new(
            name: manifest(name),
            log_dir: Keyword.get(config, :log_dir, Path.join(dir, "manifests")),
            store: store,
            fsync: fsync
          ),
        ring: Ring.new!(Keyword.get(config, :ring, [node()])),
        replicator:
          Replicator.new(
            Keyword.get(config, :replicator, {Smolquery.BufferService.Replicator.None, []})
          )
      },
      limits
    )
  end

  # `struct!/2` is the whole of the pipeline below, and it raises only on an
  # *unknown* key: a known key holding nonsense is written verbatim and detonates
  # somewhere that cannot report it. These are the keys whose values reach code
  # with no defence of its own — `Runtime.engines/1`'s range, `engine_for/2`'s
  # `:erlang.phash2/2` divisor, `Supervisor.write_engines/1`'s clause head — so
  # they are checked here, on the same keyword list `struct!/2` is about to take.
  defp validate!(limits, store) do
    Enum.each(limits, &validate_limit!/1)
    validate_store!(Keyword.get(limits, :flush_writer, :polars), store)

    limits
  end

  defp validate_limit!({:flush_writer, writer}) when writer in [:polars, :duckdb], do: :ok

  defp validate_limit!({:flush_writer, other}) do
    raise ArgumentError,
          "flush_writer must be :polars or :duckdb, got: #{inspect(other)}"
  end

  defp validate_limit!({:write_pool_size, size})
       when is_integer(size) and size > 0 and size <= @max_write_pool_size,
       do: :ok

  defp validate_limit!({:write_pool_size, other}) do
    raise ArgumentError,
          "write_pool_size must be an integer in 1..#{@max_write_pool_size}, " <>
            "got: #{inspect(other)}"
  end

  defp validate_limit!({:write_engine_memory_limit, limit})
       when is_nil(limit) or is_binary(limit),
       do: :ok

  defp validate_limit!({:write_engine_memory_limit, other}) do
    raise ArgumentError,
          "write_engine_memory_limit must be a DuckDB memory string like \"512MB\", " <>
            "got: #{inspect(other)}"
  end

  defp validate_limit!({_key, _value}), do: :ok

  # A `:duckdb` flush writes its segment with `COPY … TO <path>` and then reads
  # the file back for its row count and stats, through pool engines started
  # without an object store's credential statements. Against a shared store the
  # `COPY` succeeds, the object is uploaded, and the read-back fails — leaving an
  # orphan no manifest, catalog or GC pass names, once per flush, forever. Refuse
  # the node rather than the first write.
  defp validate_store!(:duckdb, store) do
    if Store.shared?(store) do
      raise ArgumentError,
            "flush_writer: :duckdb requires a local segment store; " <>
              "#{inspect(store.impl)} returns locations the flush's COPY cannot write " <>
              "and the write pool has no credentials to read back"
    end

    :ok
  end

  defp validate_store!(_writer, _store), do: :ok

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
  The DuckDB instance a `:duckdb` flush writes its segment with.

  Its own instance, not the query path's, for two reasons the code states
  elsewhere: `Smolquery.Engine.Connection` wraps one ADBC connection in a
  GenServer and so is a per-query mutex, and a DuckDB internal fault invalidates
  the whole database it happens on. Sharing one with reads would make a flush
  queue behind a user's scan and let a scan's rare internal fault take unacked
  rows with it. `Smolquery.StorageService.Runtime` already splits merge from
  catalog commits for the first of those reasons.
  """
  @spec engine(atom(), non_neg_integer()) :: atom()
  def engine(name, index), do: Module.concat(name, "Engine#{index}")

  @doc """
  Which of the pool's DuckDB instances writes the segment named by `key`.

  Hashed on the **segment id**, not the table: a table has one buffer and one
  committer, so hashing on the table would send every one of its flushes to the
  same connection and the pool would do nothing for the case that needs it most.
  Per-segment spreads a table's concurrent encodes across instances.

  Measured: with a single instance, `encode_concurrency` stops paying at 4 —
  slots run in parallel and then queue on the one `Smolquery.Engine.Connection`,
  which wraps one ADBC connection.
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
  How long one of a `:duckdb` flush's DuckDB calls may take.

  Shorter than `write_timeout_ms` by the margin the manifest append and the
  replication round need after the encode, so a slow `COPY` fails *inside* the
  flush — as an error the commit reports to its waiters — rather than the caller
  timing out first and the eventual exit taking the committer, and every unacked
  batch it holds, with it. Pass it to `Smolquery.Segments.Writer` as `:timeout`.
  """
  @spec engine_timeout(t()) :: timeout()
  def engine_timeout(%__MODULE__{write_timeout_ms: :infinity}), do: :infinity

  def engine_timeout(%__MODULE__{write_timeout_ms: timeout}),
    do: max(timeout - @engine_timeout_margin_ms, 1_000)

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

  defp build_store(config, dir, fsync) do
    case Keyword.get(config, :store) do
      nil -> Store.Local.new(dir: Path.join(dir, "segments"), fsync: fsync)
      {impl, opts} -> impl.new(opts)
      %Store{} = store -> store
    end
  end
end
