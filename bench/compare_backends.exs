Code.require_file("support.exs", __DIR__)
Code.require_file("otel_support.exs", __DIR__)
Code.require_file("compare_support.exs", __DIR__)

defmodule Bench.CompareSupport.Backend.Smolquery do
  @moduledoc """
  The smolquery arm of the comparison bench, driven over HTTP from outside.

  ## Why both arms hide behind one behaviour

  The comparison's honesty rests on the driver being unable to tell the arms
  apart. Every difference between them — how a table is created, what a write
  mode means, when "settled" is true, which caches exist to drop — is absorbed
  here and in `Bench.CompareSupport.Backend.ClickHouse`, so the code the driver
  wraps its timer around is the same expression on both sides. Anything that
  leaks arm-specific work into the driver is a bias, and a bias in a benchmark
  that is trying to prove something about its own author's system is worth
  nothing.

  ## Why the node runs in its own OS process

  `bench/otel_logs.exs` boots the node inside the driver's BEAM, which is the
  right call when the driver is the only client there is: the reported `cores`
  figure is then honestly labelled as "node plus co-resident client". It is
  useless here. The ClickHouse arm's server is a separate OS process by
  construction, so any CPU or RSS number that includes the driver on our side
  and excludes it on theirs is comparing two different things. This backend
  therefore starts a peer BEAM with `:peer.start/1` — the pattern
  `bench/cluster_ingest.exs` already uses for a real fleet — and hands the
  driver that peer's OS pid. `Bench.CompareSupport.sample_start/2` then samples
  exactly one process per arm, by pid, from outside.

  ## Why everything after boot goes over HTTP

  `:erpc` is used for lifecycle only: configuring the peer, booting it, reading
  its pid and its bound port, forcing a seal, measuring its disk, tearing it
  down. Not one measured operation travels that way. The ClickHouse arm has no
  choice but to speak HTTP, so a smolquery arm driven over Erlang distribution
  would be comparing two client stacks — Finch against `:erpc` — while claiming
  to compare two servers.

  ## Ports

  `mix run` has already booted a smolquery node in the driver's own BEAM holding
  the configured API, web, hot-server, and gen_rpc ports, so the peer cannot
  reuse them. The API endpoint binds port `0` and the bound port is read back
  through `SmolqueryApi.Endpoint.base_url/0`; the web endpoint does not bind at
  all. The hot server cannot bind `0`, because its readers reach it at a
  configured address and would have no way to learn an OS-assigned one, so it
  takes a fixed alternate port (`COMPARE_HOT_PORT`).

  Moving the port means moving `buffer_base_url`, not only `buffer_hot_port`.
  Single-node — which this is — `buffer_hot_port` is never consulted at all:
  `Smolquery.QueryService.Planner`'s `manifest_urls/1` and
  `Smolquery.StorageService.HotTier`'s `base_url/2` both derive a per-node URL
  from it *only* when `Smolquery.Cluster.enabled?/0`, and otherwise use
  `buffer_base_url` verbatim; `Smolquery.EngineSecrets.hot_tier/2` scopes the
  engine's `httpfs` secret to the same value. Left at its configured default the
  peer's planner and sealer would both read the hot tier of the node `mix run`
  booted in the *driver's* BEAM, which answers `200` with an empty list — an
  empty hot tier is a legitimate answer, so nothing raises. The sealer then finds
  none of its claim's inputs and fails `:no_inputs` forever, and every query sees
  neither tier.

  """

  @behaviour Bench.CompareSupport.Backend

  import Bench.Support, only: [env: 2, ms: 1]

  require Logger

  alias Smolquery.BufferService.HotManifest
  alias Smolquery.BufferService.HotManifest.Entry
  alias Smolquery.BufferService.Runtime, as: BufferRuntime
  alias Smolquery.BufferService.TableBuffer
  alias Smolquery.Segments.Store
  alias Smolquery.StorageService.Compactor
  alias Smolquery.StorageService.GC
  alias Smolquery.StorageService.Runtime, as: StorageRuntime
  alias Smolquery.StorageService.Sealer

  @bench_dir __DIR__
  @peer_files ~w(support.exs otel_support.exs compare_support.exs compare_backends.exs)

  @cookie :smolquery_compare_bench
  @driver_node :"smolquery_compare@127.0.0.1"
  @roles [:api, :ingest, :buffer, :storage, :query]
  @copied_apps [:smolquery, :adbc, :explorer, :phoenix, :gen_rpc]

  @boot_timeout_ms 300_000
  @control_timeout_ms 120_000
  @poll_ms 250
  @max_results 10_000

  @impl true
  def name, do: "smolquery"

  @doc """
  Boots a smolquery node in a peer BEAM and returns a handle to it over HTTP.

  The peer inherits the driver's application environment for the handful of
  apps whose configuration is not baked into `smolquery.app` — `mix run`
  applies `config/runtime.exs` in this BEAM and the peer would otherwise start
  from the compile-time configuration alone — and then overrides the ports it
  cannot share (see the module doc), the role list, and `retire_grace_ms`.

  `retire_grace_ms` is cut to `RETIRE_GRACE_MS` (default 1_000) from its
  production default of ten minutes. The grace period exists so an in-flight
  scan holding an older snapshot cannot lose rows underneath it; `settle/1`
  runs when nothing is querying, and at the stock value the hot tier's files
  would still be on disk when `disk_bytes/1` measures, reporting a table that
  is on disk twice.

  A failure anywhere in here stops the peer before returning. A leaked BEAM
  holding the scratch directory and the alternate ports would poison every
  later run in the sweep, and it would do so silently.
  """
  @impl true
  def setup(opts) do
    with :ok <- ensure_distributed() do
      index = System.unique_integer([:positive])
      dir = Keyword.get_lazy(opts, :dir, fn -> scratch_dir(index) end)

      case start_peer(index) do
        {:ok, peer, node} -> boot(peer, node, dir, opts)
        {:error, reason} -> {:error, {:peer_start_failed, reason}}
      end
    end
  end

  defp boot(peer, node, dir, opts) do
    configure_peer(node)

    {:ok, _started} =
      :erpc.call(node, Application, :ensure_all_started, [:smolquery], @boot_timeout_ms)

    _req = :erpc.call(node, Bench.Otel, :boot!, [dir], @boot_timeout_ms)

    {:ok,
     %{
       peer: peer,
       node: node,
       dir: dir,
       os_pid: peer_os_pid(node),
       req: request(node),
       dataset: Keyword.get(opts, :dataset, Bench.Otel.dataset()),
       table: Keyword.get(opts, :table, Bench.Otel.table()),
       settle_timeout_ms: settle_timeout_ms()
     }}
  catch
    kind, reason ->
      stop_peer(peer)
      {:error, {:peer_boot_failed, Exception.format(kind, reason, __STACKTRACE__)}}
  end

  @doc """
  Creates the dataset, the table, and — as a separate `PATCH` — its sort key.

  Every status is asserted rather than assumed. A `PATCH` that quietly failed
  would leave a table whose segments are written in arrival order while the
  report claims a clustering key, which is the one failure mode that makes the
  whole comparison meaningless and looks like nothing at all from outside.
  """
  @impl true
  def create_table(state, columns, clustering) do
    schema = for {name, type} <- columns, do: %{"name" => name, "type" => type}

    with :ok <-
           expect(post(state, "/v1/datasets", %{"id" => state.dataset}), 200, :create_dataset),
         :ok <-
           expect(
             post(state, "/v1/datasets/#{state.dataset}/tables", %{
               "id" => state.table,
               "schema" => schema
             }),
             200,
             :create_table
           ) do
      apply_clustering(state, clustering)
    end
  end

  defp apply_clustering(state, clustering) do
    case patch(state, table_url(state), %{"clustering" => clustering}) do
      {:ok, %{status: 200} = response} ->
        body = decode_json_body(response)

        case body do
          %{"clustering" => ^clustering} ->
            :ok

          _mismatched ->
            {:error, {:clustering_not_applied, clustering, body["clustering"]}}
        end

      other ->
        {:error, {:clustering_failed, describe(other)}}
    end
  end

  @doc """
  Posts one already-encoded insert body and reports how many rows it took.

  The `:mode` option is accepted and ignored, because smolquery has exactly one
  durability mode: a `200` here means the Parquet is in the store and the
  manifest entry is fsynced. That single number is compared against ClickHouse's
  `:durable_async` mode — async insert with wait plus table-level fsync — which
  amortizes the same way our `TableBuffer` group commit does; `:default`,
  `:durable`, and `:async` are reported beside it as context.

  A `429` becomes `{:error, :buffer_full}` and is surfaced rather than retried.
  A driver that quietly retried through backpressure would report a throughput
  the system does not actually sustain — the shed request is the measurement.
  """
  @impl true
  def insert(state, rows, _opts) do
    case post_body(state, insert_url(state), rows) do
      {:ok, %{status: 200} = response} ->
        body = decode_json_body(response)
        {:ok, %{rows: body["insertedRows"]}}

      {:ok, %{status: 429}} ->
        {:error, :buffer_full}

      other ->
        {:error, {:insert_failed, describe(other)}}
    end
  end

  @doc """
  The identity half of the driver's encode seam; see the ClickHouse arm's.

  The driver encodes rows once, outside its timer, by calling this on whichever
  backend it is driving. Here there is nothing to do — the fixture's body is
  already the wire format this API takes — so the call costs nothing and the
  driver keeps one code path for both arms.
  """
  @spec encode_rows(iodata()) :: iodata()
  def encode_rows(rows), do: rows

  @doc """
  Runs one query and reports its wall time and result size.

  `rows_read` and `bytes_read` are `nil`. The query API answers with the result
  and the job, not with a scan profile, and the only way to get one is a second
  `EXPLAIN ANALYZE` execution — which would double the query, warm the cache
  between repetitions, and still report DuckDB's operator cardinalities rather
  than the bytes the plan touched. The contract says a fabricated scan figure is
  worse than none, so this reports none.

  `rows` is the job's `totalRows`, not the length of the returned page: the API
  caps a page at 10,000 rows while ClickHouse streams the whole result set, so
  the row *count* is comparable even though the bytes serialized are not.

  JSON decoding is deliberately outside the timer. The ClickHouse arm requests
  `FORMAT TabSeparated` with `decode_body: false` and counts lines only after its
  own timer closes, so it parses nothing inside the measured window. Decoding up
  to `#{@max_results}` rows of a 62-column result here would charge this arm for
  client-side work the other arm never pays — and would do so hardest on Q3, Q4,
  Q8, and Q10, exactly where a fair read comparison matters most.
  """
  @impl true
  def query(state, sql) do
    {us, result} =
      :timer.tc(fn ->
        post(state, "/v1/queries", %{"query" => sql, "maxResults" => @max_results})
      end)

    case result do
      {:ok, %{status: 200} = response} ->
        body = decode_json_body(response)

        {:ok,
         %{
           rows: body["totalRows"] || length(body["rows"] || []),
           ms: ms(us),
           rows_read: nil,
           bytes_read: nil
         }}

      other ->
        {:error, {:query_failed, describe(other)}}
    end
  end

  @doc """
  Blocks until every row is sealed, the hot tier is empty, and compaction is quiet.

  Three things have to be true before `disk_bytes/1` means anything, and none of
  them happen on their own within a bench's patience: the buffer seals on
  `seal_max_bytes` / `seal_max_files` / `seal_max_age_ms`, the sealed
  micro-segments are only deleted once `retire_grace_ms` has passed, and the
  compactor merges on its own interval. So this drives all three from outside
  over `:erpc` — `TableBuffer.force_seal/2` and `TableBuffer.maintain/2` per
  round, then `Compactor.sweep/2` until a sweep finds nothing left to merge,
  then a `GC.sweep/2` so superseded segments are not counted as live data.

  On timeout this returns `{:error, {:settle_timeout, outstanding}}` carrying
  what was still in flight. It never returns `:ok` on a timeout: disk measured
  mid-seal is a fabricated number, and a fabricated number that looks plausible
  is the worst thing this bench could produce.
  """
  @impl true
  def settle(state) do
    :erpc.call(
      state.node,
      __MODULE__,
      :settle_on_node,
      [{state.dataset, state.table}, state.settle_timeout_ms],
      state.settle_timeout_ms + @control_timeout_ms
    )
  end

  @doc """
  `settle/1`'s body, running on the node under test rather than on the driver.

  Public because `:erpc` needs a named function to call, and this module is
  required on the peer precisely so that this one can live next to the callback
  it implements.

  Anything raised or exited here is turned into an `{:error, _}` rather than
  allowed to cross back as an exception: `:erpc` re-raises on the driver, and a
  settle that blew up would abort the sweep instead of failing one measurement.
  """
  @spec settle_on_node({String.t(), String.t()}, timeout()) :: :ok | {:error, term()}
  def settle_on_node(table_ref, timeout_ms) do
    deadline = System.monotonic_time(:millisecond) + timeout_ms

    with {:ok, buffer} <- runtime(BufferRuntime, Smolquery.BufferService),
         {:ok, storage} <- runtime(StorageRuntime, Smolquery.StorageService),
         :ok <- drain_hot_tier(buffer, storage, table_ref, deadline) do
      quiesce_compaction(storage, deadline)
    end
  catch
    kind, reason ->
      {:error, {:settle_failed, Exception.format(kind, reason, __STACKTRACE__)}}
  end

  defp drain_hot_tier(buffer, storage, table_ref, deadline) do
    force_seal(buffer, table_ref)
    outstanding = outstanding(buffer, storage, table_ref)

    cond do
      outstanding.entries == 0 and outstanding.sealing == 0 ->
        :ok

      System.monotonic_time(:millisecond) >= deadline ->
        {:error, {:settle_timeout, outstanding}}

      true ->
        Process.sleep(@poll_ms)
        drain_hot_tier(buffer, storage, table_ref, deadline)
    end
  end

  defp force_seal(buffer, table_ref) do
    case Registry.lookup(BufferRuntime.registry(buffer.name), table_ref) do
      [{pid, _load}] ->
        TableBuffer.flush(pid, @control_timeout_ms)
        TableBuffer.force_seal(pid, @control_timeout_ms)
        TableBuffer.maintain(pid, @control_timeout_ms)

      [] ->
        :ok
    end
  catch
    :exit, reason ->
      Logger.warning(
        "settle: forcing a seal for #{inspect(table_ref)} failed: #{inspect(reason)}"
      )

      :ok
  end

  defp outstanding(buffer, storage, table_ref) do
    entries = HotManifest.entries(buffer.manifest, table_ref)

    %{
      entries: length(entries),
      unsealed: Enum.count(entries, &(not Entry.sealed?(&1))),
      sealing: Enum.count(Sealer.sealing(storage.name), &(&1 == table_ref)),
      seal_failures: Map.get(Sealer.failures(storage.name), table_ref, 0)
    }
  end

  defp quiesce_compaction(storage, deadline) do
    case Compactor.sweep(storage.name, @control_timeout_ms) do
      {:ok, %{compacted: [], failed: []}} ->
        collect_garbage(storage)

      {:ok, %{failed: [_first | _rest] = failed}} ->
        {:error, {:compaction_failed, failed}}

      {:ok, %{compacted: [_first | _rest] = compacted}} ->
        if System.monotonic_time(:millisecond) >= deadline do
          {:error, {:settle_timeout, %{compacting: length(compacted)}}}
        else
          quiesce_compaction(storage, deadline)
        end

      {:error, reason} ->
        {:error, {:compaction_failed, reason}}
    end
  end

  defp collect_garbage(storage) do
    case GC.sweep(storage.name, @control_timeout_ms) do
      {:ok, _swept} -> :ok
      {:error, reason} -> {:error, {:gc_failed, reason}}
    end
  end

  @doc """
  Every byte the table currently occupies, across both tiers.

  The store handles come from the published runtimes rather than from
  configuration, so this measures where the node actually wrote — a store
  pointed elsewhere by `config/runtime.exs`, or a sealed tier on a different
  filesystem, is followed rather than guessed at. The hot tier is included and
  should be empty after `settle/1`; if it is not, this reports the extra rather
  than hiding it.

  The DuckLake catalog's own metadata is excluded, which matches ClickHouse's
  `system.parts` accounting excluding its system tables.
  """
  @impl true
  def disk_bytes(state) do
    :erpc.call(
      state.node,
      __MODULE__,
      :table_bytes,
      [{state.dataset, state.table}],
      @control_timeout_ms
    )
  end

  @doc """
  `disk_bytes/1`'s body, running on the node under test.
  """
  @spec table_bytes({String.t(), String.t()}) :: {:ok, non_neg_integer()} | {:error, term()}
  def table_bytes(table_ref) do
    with {:ok, prefix} <- Store.prefix(table_ref),
         {:ok, buffer} <- runtime(BufferRuntime, Smolquery.BufferService),
         {:ok, storage} <- runtime(StorageRuntime, Smolquery.StorageService) do
      sum_bytes([storage.store, buffer.store], prefix)
    end
  end

  defp sum_bytes(stores, prefix) do
    Enum.reduce_while(stores, {:ok, 0}, fn store, {:ok, total} ->
      case Store.list(store, prefix) do
        {:ok, keys} -> {:cont, {:ok, total + Enum.sum(Enum.map(keys, &key_bytes(store, &1)))}}
        {:error, reason} -> {:halt, {:error, {:store_unreadable, reason}}}
      end
    end)
  end

  defp key_bytes(store, key) do
    case store |> Store.location(key) |> File.stat() do
      {:ok, %File.Stat{size: size}} -> size
      {:error, _absent} -> 0
    end
  end

  @impl true
  def os_pid(state), do: state.os_pid

  @doc """
  Drops the OS page cache. There is no engine-level cache here to drop.

  This is not a stub. smolquery has nothing analogous to ClickHouse's mark or
  uncompressed cache: `Smolquery.QueryService.Runner` starts a private DuckDB
  engine per job and takes it down with the job, so no cross-query state
  survives to be dropped. What is left is the page cache the segments were read
  through, which is `purge` on darwin and `vm.drop_caches` on linux.

  The linux path needs root. When it is unavailable this logs plainly that the
  page cache was *not* dropped and still returns `:ok`, so the driver records a
  warm "cold" number as warm rather than dying halfway through a sweep — and so
  the same thing happens on the ClickHouse arm, whose script makes the same
  choice.
  """
  @impl true
  def drop_caches(_state), do: drop_page_cache()

  @doc """
  Stops the node and removes its scratch directory.

  Safe to call twice and safe to call after a failed `setup/1`: a peer that is
  already gone answers with an exit, which is what "already stopped" looks like.
  """
  @impl true
  def teardown(state) do
    stop_node(state)
    stop_peer(state[:peer])
    if dir = state[:dir], do: File.rm_rf(dir)

    :ok
  end

  defp stop_node(%{node: node, dir: dir}) do
    :erpc.call(node, Bench.Otel, :teardown!, [dir], @control_timeout_ms)
    :ok
  catch
    kind, reason ->
      Logger.debug("teardown: peer already down (#{inspect(kind)} #{inspect(reason)})")
      :ok
  end

  defp stop_node(_state), do: :ok

  defp stop_peer(nil), do: :ok

  defp stop_peer(peer) do
    :peer.stop(peer)
    :ok
  catch
    :exit, _reason -> :ok
  end

  defp ensure_distributed do
    case Node.start(@driver_node, :longnames) do
      {:ok, _pid} -> set_cookie()
      {:error, {:already_started, _pid}} -> set_cookie()
      {:error, reason} -> {:error, {:distribution_unavailable, reason}}
    end
  end

  defp set_cookie do
    Node.set_cookie(@cookie)

    :ok
  end

  defp start_peer(index) do
    :peer.start(%{
      name: :"smolquery_compare_peer_#{index}",
      host: ~c"127.0.0.1",
      longnames: true,
      args: [
        ~c"-setcookie",
        Atom.to_charlist(@cookie),
        ~c"-kernel",
        ~c"prevent_overlapping_partitions",
        ~c"false"
      ]
    })
  end

  defp configure_peer(node) do
    :erpc.call(node, :code, :add_paths, [:code.get_path()])
    {:ok, _elixir} = :erpc.call(node, :application, :ensure_all_started, [:elixir])
    :ok = :erpc.call(node, Logger, :configure, [[level: :warning]])
    start_mix(node)

    for app <- @copied_apps, {key, value} <- Application.get_all_env(app) do
      :erpc.call(node, Application, :put_env, [app, key, value])
    end

    for {key, value} <- peer_env() do
      :erpc.call(node, Application, :put_env, [:smolquery, key, value])
    end

    for {key, value} <- [tcp_server_port: gen_rpc_port(), tcp_client_port: gen_rpc_port()] do
      :erpc.call(node, :application, :set_env, [:gen_rpc, key, value, [persistent: true]])
    end

    for file <- @peer_files do
      :erpc.call(node, Code, :require_file, [Path.join(@bench_dir, file)], @boot_timeout_ms)
    end

    :ok
  end

  defp peer_env do
    [
      {:roles, @roles},
      {SmolqueryApi.Endpoint, merged(SmolqueryApi.Endpoint, http: [ip: {127, 0, 0, 1}, port: 0])},
      {SmolqueryWeb.Endpoint,
       merged(SmolqueryWeb.Endpoint, server: false, code_reloader: false, watchers: [])},
      {Smolquery.BufferService,
       merged(Smolquery.BufferService,
         hot_server_port: hot_port(),
         retire_grace_ms: retire_grace_ms()
       )},
      {Smolquery.QueryService,
       merged(Smolquery.QueryService,
         buffer_base_url: hot_base_url(),
         buffer_hot_port: hot_port()
       )},
      {Smolquery.StorageService,
       merged(Smolquery.StorageService,
         buffer_base_url: hot_base_url(),
         buffer_hot_port: hot_port()
       )}
    ]
  end

  # `mix.exs` declares esbuild and tailwind `runtime: Mix.env() == :dev`, so under
  # `mix run` they are in `smolquery.app`'s applications list and
  # `ensure_all_started(:smolquery)` starts them on the peer too. Their `start/2`
  # reaches for `Mix.ProjectStack`, which a bare `:peer` node does not run, and the
  # boot dies with a `:noproc` that names esbuild rather than the missing Mix.
  #
  # Starting Mix is the smaller fix than pruning the application list: the peer must
  # run the real `:smolquery` application, because `Bench.Otel.boot!/1` restarts role
  # subtrees through `Smolquery.Supervisor` and there is no supervisor to restart if
  # the app was assembled by hand. `Mix.ProjectStack` with no project pushed answers
  # `peek/0` harmlessly, which is all esbuild needs; nothing here builds an asset.
  defp start_mix(node) do
    {:ok, _mix} = :erpc.call(node, :application, :ensure_all_started, [:mix])
    :ok = :erpc.call(node, Mix, :start, [])
    :ok = :erpc.call(node, Mix, :env, [Mix.env()])
  end

  defp merged(key, overrides),
    do: Keyword.merge(Application.get_env(:smolquery, key, []), overrides)

  defp peer_os_pid(node),
    do: node |> :erpc.call(:os, :getpid, [], @control_timeout_ms) |> List.to_integer()

  defp request(node) do
    base_url = :erpc.call(node, Bench.Otel, :base_url, [], @control_timeout_ms)

    api_key =
      node
      |> :erpc.call(Application, :get_env, [:smolquery, SmolqueryApi, []], @control_timeout_ms)
      |> Keyword.fetch!(:api_key)

    Req.new(
      base_url: base_url,
      auth: {:bearer, api_key},
      retry: false,
      decode_body: false,
      receive_timeout: settle_timeout_ms()
    )
  end

  defp runtime(module, name) do
    case module.fetch(name) do
      {:ok, runtime} -> {:ok, runtime}
      :error -> {:error, {:runtime_unavailable, module, name}}
    end
  end

  defp post(state, url, json), do: Req.post(state.req, url: url, json: json)

  defp post_body(state, url, body) do
    Req.post(state.req,
      url: url,
      headers: [{"content-type", "application/json"}],
      body: body
    )
  end

  defp patch(state, url, json), do: Req.patch(state.req, url: url, json: json)

  defp decode_json_body(%{body: body}),
    do: body |> IO.iodata_to_binary() |> JSON.decode!()

  defp expect({:ok, %{status: status}}, status, _step), do: :ok
  defp expect(other, _status, step), do: {:error, {step, describe(other)}}

  defp describe({:ok, response}), do: {:http, response.status, response.body}
  defp describe({:error, exception}), do: {:transport, Exception.message(exception)}

  defp table_url(state), do: "/v1/datasets/#{state.dataset}/tables/#{state.table}"
  defp insert_url(state), do: table_url(state) <> "/insert"

  defp drop_page_cache do
    case :os.type() do
      {:unix, :darwin} ->
        run_page_cache_drop("purge", [])

      {:unix, :linux} ->
        run_page_cache_drop("sh", ["-c", "sync; echo 3 > /proc/sys/vm/drop_caches"])

      other ->
        warn_warm("no page-cache drop is implemented for #{inspect(other)}")
    end
  end

  defp run_page_cache_drop(command, args) do
    case System.find_executable(command) do
      nil ->
        warn_warm("#{command} is not on PATH")

      path ->
        case System.cmd(path, args, stderr_to_stdout: true) do
          {_output, 0} -> :ok
          {output, status} -> warn_warm("#{command} exited #{status}: #{String.trim(output)}")
        end
    end
  end

  defp warn_warm(why) do
    Logger.warning("smolquery: the OS page cache was NOT dropped (#{why}); cold numbers are warm")

    :ok
  end

  defp scratch_dir(index),
    do: Path.join(System.tmp_dir!(), "smolquery-bench-compare-#{index}")

  defp settle_timeout_ms, do: env("SETTLE_TIMEOUT_MS", 600_000)
  defp retire_grace_ms, do: env("RETIRE_GRACE_MS", 1_000)
  defp hot_port, do: env("COMPARE_HOT_PORT", 4_101)
  defp hot_base_url, do: "http://127.0.0.1:#{hot_port()}"
  defp gen_rpc_port, do: env("COMPARE_GEN_RPC_PORT", 5_390)
end

defmodule Bench.CompareSupport.Backend.ClickHouse do
  @moduledoc """
  The ClickHouse arm, against a server somebody else started.

  This backend never installs, starts, or stops a ClickHouse server. The
  operator brings one up with `make ch_up` (`scripts/clickhouse/up.sh`) and
  takes it down with `make ch_down`. A benchmark that silently boots its own
  subject is one whose starting state nobody knows — which version, which
  settings, how warm, how full — and every number it prints inherits that
  ignorance. `setup/1` therefore checks that a server answers and refuses to
  run if one does not.

  Everything else this module does is HTTP `POST` to that endpoint with the SQL
  as the request body and per-query *session* settings as query-string
  parameters — the one ClickHouse interface where a session setting can be
  attached to a single statement without changing the server's state between
  measurements. MergeTree table settings such as `fsync_after_insert` cannot
  travel that way; `insert/3` applies those with a tracked
  `ALTER TABLE … MODIFY SETTING` instead.

  Results come back as `FORMAT TabSeparated` throughout, so a returned row count
  is a newline count and needs no parser in the measured path.

  See `Bench.CompareSupport.Backend.Smolquery` for why both arms hide behind one
  behaviour at all.
  """

  @behaviour Bench.CompareSupport.Backend

  import Bench.Support, only: [env: 2, ms: 1]

  require Logger

  alias Bench.CompareSupport

  @default_url "http://127.0.0.1:8123"
  @default_data_root ".cache/clickhouse-data"
  @drop_caches_script "scripts/clickhouse/drop-caches.sh"
  @page_cache_marker "PAGE_CACHE_DROPPED="

  @driver_body_prefix "{\"rows\":"
  @insert_settings [date_time_input_format: "best_effort"]

  @impl true
  def name, do: "clickhouse"

  @doc """
  Points the backend at a running server and reads its pid.

  `CLICKHOUSE_URL` (default `#{@default_url}`) and `CLICKHOUSE_DATA_ROOT`
  (default `#{@default_data_root}` under the repo) locate it. The pid file at
  `<data root>/clickhouse.pid` is a contract with `scripts/clickhouse/up.sh`,
  which writes exactly one decimal pid there.

  An unparseable or missing pid file is not fatal. It costs the run its CPU and
  RSS series for this arm, which is worth less than the run itself; `os_pid/1`
  answers `nil` and the driver records that it has no resource figures rather
  than refusing to produce latency ones. An unreachable *server*, by contrast,
  is fatal, because there is nothing to measure.
  """
  @impl true
  def setup(opts) do
    url = System.get_env("CLICKHOUSE_URL", @default_url)
    data_root = System.get_env("CLICKHOUSE_DATA_ROOT", Path.join(File.cwd!(), @default_data_root))

    state = %{
      url: url,
      req:
        Req.new(base_url: url, retry: false, decode_body: false, receive_timeout: timeout_ms()),
      data_root: data_root,
      pid_file: Path.join(data_root, "clickhouse.pid"),
      os_pid: read_pid(Path.join(data_root, "clickhouse.pid")),
      script: Path.join(File.cwd!(), @drop_caches_script),
      database: Keyword.get(opts, :database, "smolbench"),
      table: Keyword.get(opts, :table, "otel_logs"),
      ddl_opts: Keyword.take(opts, [:table, :database, :low_cardinality, :codec]),
      settle_timeout_ms: env("SETTLE_TIMEOUT_MS", 600_000)
    }

    case run_sql(state, "SELECT 1") do
      {:ok, _body, _headers} ->
        {:ok, state}

      {:error, reason} ->
        Logger.error(
          "clickhouse is not answering at #{url} (#{inspect(reason)}) — start it with `make ch_up`"
        )

        {:error, {:clickhouse_unreachable, url}}
    end
  end

  @doc """
  Creates the database and the table from `Bench.CompareSupport`'s DDL.

  No DDL is built here. The schema is generated once, in one place, from
  `Bench.Otel.columns/0`, so the two arms cannot drift apart column by column
  without somebody noticing. `columns` therefore goes unused; `clustering` is
  used only to assert that the driver and the DDL generator still agree on the
  sort key, which is the one thing a caller could get wrong that would look
  entirely fine in the output.
  """
  @impl true
  def create_table(state, _columns, clustering) do
    if clustering == CompareSupport.clustering_key() do
      with {:ok, _created, _h1} <-
             run_sql(state, CompareSupport.clickhouse_create_database(state.ddl_opts)),
           {:ok, _dropped, _h2} <-
             run_sql(state, CompareSupport.clickhouse_drop_table(state.ddl_opts)),
           {:ok, _table, _h3} <- run_sql(state, CompareSupport.clickhouse_ddl(state.ddl_opts)) do
        clear_table_fsync(state)
        :ok
      end
    else
      {:error, {:clustering_mismatch, clustering, CompareSupport.clustering_key()}}
    end
  end

  @doc """
  Inserts one batch as `JSONEachRow`, under the settings its write mode names.

  ## The four modes

  smolquery's `200` means the segment is in the store and its manifest entry is
  fsynced, and that ack amortizes: one `TableBuffer` group commit covers many
  client batches and pays one segment fsync plus one manifest-log fsync for the
  whole commit. ClickHouse's stock insert does not fsync at all, so comparing
  our number against it compares two different promises. The four modes report
  the spectrum rather than picking one:

    * `:default` — ClickHouse as people actually run it (`fsync_after_insert=0`).
    * `:durable` — per-INSERT part fsync (`fsync_after_insert=1`,
      `fsync_part_directory=1`). Same durability promise, but unamortized: one
      fsync per client batch. Useful context; not the headline.
    * `:async` — `async_insert=1, wait_for_async_insert=1`. Server-side group
      commit, no fsync. Flatters ClickHouse relative to our ack.
    * `:durable_async` — async insert with wait *plus* the table fsync settings.
      ClickHouse forms its own group commit and fsyncs before acking. That is
      the true analog of our `TableBuffer`, and the like-for-like number the
      write comparison is judged on.

  `fsync_after_insert` and `fsync_part_directory` are MergeTree *table*
  settings, not session settings — ClickHouse rejects them as query params
  (`UNKNOWN_SETTING`). Before the first insert of a mode this function issues
  `ALTER TABLE … MODIFY SETTING` so the table matches that mode (`1` for the
  durable modes, explicitly `0` for the others — otherwise a table left durable
  by a previous cell silently poisons the next). The last applied value is
  tracked per `{database, table}` in `:persistent_term`, so cells that stay in
  one mode pay one `ALTER` at the mode transition, not one per batch. Scratch
  and corpus are different tables and each carries its own tracked state.

  That transition `ALTER` lands inside the timed window of the first batch of a
  cell. It inflates that single batch's latency — a known, bounded distortion
  that moves `max` but not `p50`/`p95` at realistic batch counts. It is not
  hidden; it is priced into the first sample on purpose rather than paid outside
  the timer where a reader would never see it.

  `date_time_input_format=best_effort` is on for every mode. Both arms are
  handed the same bytes — the fixture's ISO 8601 timestamps with `T` and `Z` —
  and ClickHouse's stock date parser rejects that shape. Relaxing the parser is
  the honest fix; rewriting timestamps client-side for one arm only would move
  the cost into the driver and out of the comparison.

  ## The encode seam

  The driver's body is smolquery's insert shape, `%{"rows" => [...]}`, and
  `JSONEachRow` wants newline-delimited objects with no wrapper. That decode and
  re-encode is real work that only this arm pays, it is entirely client-side,
  and it must not land inside the measured window. `encode_rows/1` is the seam:
  the driver calls it before starting its timer and passes the result here. This
  function calls it again — it is idempotent, and on an already-encoded payload
  the second call is a prefix comparison — so a driver that forgets still
  produces correct results, just a biased throughput number.

  ## Written-row accounting

  ClickHouse's `X-ClickHouse-Summary` can answer `written_rows: "0"` on a 200
  while rows were accepted — especially under `:async`, where attribution can
  lag the ack. A silent zero here would be indistinguishable from a real empty
  insert in the driver's throughput table, which is why this code refuses to
  treat a parsed zero as authoritative: only a strictly positive
  `written_rows` is trusted, and anything else falls back to counting lines in
  the payload that was sent. A genuine empty insert still reports zero, because
  the payload then has zero lines too; what it will not do is invent a
  catastrophic `0 rows/s` result from an unattributed async ack.
  """
  @impl true
  def insert(state, rows, opts) do
    mode = Keyword.get(opts, :mode, :default)
    encoded = encode_rows(rows)
    settings = Keyword.merge(@insert_settings, mode_settings(mode))

    with :ok <- ensure_table_fsync(state, mode),
         {:ok, _body, headers} <-
           run_sql(state, [insert_statement(state), encoded], settings) do
      {:ok, %{rows: inserted_rows(headers, encoded)}}
    end
  end

  @doc """
  The driver's insert body as `JSONEachRow`, computed outside the measured window.

  Idempotent, so it is safe to call on its own output. The driver's body is a
  single-key object and therefore always begins with `#{@driver_body_prefix}`;
  a `JSONEachRow` line is one fixture row, and the fixture has no `rows` column,
  so the prefix distinguishes the two without inspecting the payload further.
  """
  @spec encode_rows(iodata()) :: binary()
  def encode_rows(rows) do
    binary = IO.iodata_to_binary(rows)

    if String.starts_with?(binary, @driver_body_prefix) do
      binary
      |> JSON.decode!()
      |> Map.fetch!("rows")
      |> Enum.map(&[JSON.encode_to_iodata!(&1), ?\n])
      |> IO.iodata_to_binary()
    else
      binary
    end
  end

  defp mode_settings(:default), do: []
  defp mode_settings(:durable), do: []
  defp mode_settings(:async), do: [async_insert: 1, wait_for_async_insert: 1]
  defp mode_settings(:durable_async), do: [async_insert: 1, wait_for_async_insert: 1]

  defp mode_fsync(:default), do: 0
  defp mode_fsync(:async), do: 0
  defp mode_fsync(:durable), do: 1
  defp mode_fsync(:durable_async), do: 1

  defp fsync_key(state), do: {__MODULE__, :fsync, state.database, state.table}

  defp clear_table_fsync(state), do: :persistent_term.erase(fsync_key(state))

  defp ensure_table_fsync(state, mode) do
    desired = mode_fsync(mode)

    case :persistent_term.get(fsync_key(state), :unset) do
      ^desired ->
        :ok

      _previous ->
        case apply_table_fsync(state, desired) do
          :ok ->
            :persistent_term.put(fsync_key(state), desired)
            :ok

          {:error, _reason} = error ->
            error
        end
    end
  end

  defp apply_table_fsync(state, value) do
    sql = """
    ALTER TABLE `#{state.database}`.`#{state.table}`
    MODIFY SETTING fsync_after_insert = #{value}, fsync_part_directory = #{value}
    """

    case run_sql(state, sql) do
      {:ok, _body, _headers} -> :ok
      {:error, _reason} = error -> error
    end
  end

  defp insert_statement(state),
    do: "INSERT INTO `#{state.database}`.`#{state.table}` FORMAT JSONEachRow\n"

  defp inserted_rows(headers, encoded) do
    with [summary] <- Map.get(headers, "x-clickhouse-summary", []),
         {:ok, %{"written_rows" => written}} <- JSON.decode(summary),
         {rows, _rest} <- Integer.parse(written),
         true <- rows > 0 do
      rows
    else
      _unreported -> count_lines(encoded)
    end
  end

  @doc """
  Merges every part into one and waits for background merges to stop.

  `OPTIMIZE TABLE … FINAL` is the ClickHouse equivalent of forcing a seal and
  a compaction: without it `system.parts` still holds whatever the inserts left
  behind, and both the disk figure and the query numbers would describe a table
  mid-merge. It can run for a long time on a large table, so it is issued with
  the whole settle budget as its receive timeout, and the poll that follows
  watches `system.merges` and `system.mutations` rather than trusting the
  statement's return.

  A timeout is an error, never `:ok` — the same rule as the smolquery arm, for
  the same reason.
  """
  @impl true
  def settle(state) do
    deadline = System.monotonic_time(:millisecond) + state.settle_timeout_ms

    with {:ok, _body, _headers} <-
           run_sql(
             state,
             "OPTIMIZE TABLE `#{state.database}`.`#{state.table}` FINAL",
             [],
             receive_timeout: state.settle_timeout_ms
           ) do
      await_quiet(state, deadline)
    end
  end

  defp await_quiet(state, deadline) do
    case run_sql(state, quiet_sql(state)) do
      {:ok, body, _headers} -> quiet_or_wait(state, deadline, parse_ints(body))
      {:error, _reason} = error -> error
    end
  end

  defp quiet_or_wait(_state, _deadline, [0, 0]), do: :ok

  defp quiet_or_wait(state, deadline, outstanding) do
    if System.monotonic_time(:millisecond) >= deadline do
      {:error,
       {:settle_timeout, %{merges: Enum.at(outstanding, 0), mutations: Enum.at(outstanding, 1)}}}
    else
      Process.sleep(500)
      await_quiet(state, deadline)
    end
  end

  defp quiet_sql(state) do
    """
    SELECT
      (SELECT count() FROM system.merges
        WHERE database = '#{state.database}' AND table = '#{state.table}'),
      (SELECT count() FROM system.mutations
        WHERE database = '#{state.database}' AND table = '#{state.table}' AND is_done = 0)
    FORMAT TabSeparated
    """
  end

  @doc """
  Runs one query and reports its wall time, result size, and what it scanned.

  The scan figures come from the response's `X-ClickHouse-Summary` header —
  the same `read_rows` / `read_bytes` ClickHouse would write to
  `system.query_log`, delivered synchronously with the result. Parsing them is
  client-side and happens after the measured window; nothing is re-queried.

  The earlier recipe (generate a `query_id`, `SYSTEM FLUSH LOGS`, then read
  `system.query_log`) cannot work against this bench's server: `bench.xml` is
  the full `--config-file` and has no `<query_log>` section, so the table does
  not exist. Session settings cannot create it. The summary header is the
  accounting that survives that config.

  When the header is missing or unparseable both figures come back `nil`. A
  latency without a scanned-bytes figure explains nothing, and a guessed one
  explains something false.
  """
  @impl true
  def query(state, sql) do
    {us, result} = :timer.tc(fn -> run_sql(state, tsv(sql)) end)

    case result do
      {:ok, body, headers} ->
        {rows_read, bytes_read} = scan_stats(headers)

        {:ok,
         %{rows: count_lines(body), ms: ms(us), rows_read: rows_read, bytes_read: bytes_read}}

      {:error, _reason} = error ->
        error
    end
  end

  defp tsv(sql),
    do: [sql |> String.trim() |> String.trim_trailing(";"), "\nFORMAT TabSeparated\n"]

  defp scan_stats(headers) do
    with [summary] <- Map.get(headers, "x-clickhouse-summary", []),
         {:ok, %{"read_rows" => rows, "read_bytes" => bytes}} <- JSON.decode(summary),
         {rows_read, _rest} <- Integer.parse(rows),
         {bytes_read, _rest} <- Integer.parse(bytes) do
      {rows_read, bytes_read}
    else
      _unavailable -> {nil, nil}
    end
  end

  @doc """
  The table's live bytes on disk, per ClickHouse's own `system.parts`.

  Only meaningful after `settle/1`: `active` excludes parts a merge has
  superseded but not yet deleted, and before the merge finishes there are more
  of those than there is data.
  """
  @impl true
  def disk_bytes(state) do
    sql = """
    SELECT sum(bytes_on_disk) FROM system.parts
    WHERE database = '#{state.database}' AND table = '#{state.table}' AND active
    FORMAT TabSeparated
    """

    case run_sql(state, sql) do
      {:ok, body, _headers} -> {:ok, body |> parse_ints() |> List.first(0)}
      {:error, _reason} = error -> error
    end
  end

  @impl true
  def os_pid(state), do: state.os_pid

  @doc """
  Runs `#{@drop_caches_script}`, which drops ClickHouse's caches and the page cache.

  The logic lives in the script rather than here because the ClickHouse-side
  drops are version-dependent statements that have to tolerate not existing, and
  because the operator has to be able to run the same thing by hand when a
  number looks suspiciously warm. The script prints
  `#{@page_cache_marker}yes|no`; that marker is parsed and reported, and a `no`
  is a warning rather than a failure, so the driver can record that the cold
  numbers are warm instead of losing the run.
  """
  @impl true
  def drop_caches(state) do
    if runnable?(state.script) do
      {output, _status} =
        System.cmd(state.script, [],
          env: [{"CLICKHOUSE_HTTP", state.url}],
          stderr_to_stdout: true
        )

      report_page_cache(output)
    else
      Logger.warning(
        "clickhouse: #{state.script} is missing or not executable — no cache was dropped"
      )

      :ok
    end
  end

  defp runnable?(path) do
    case File.stat(path) do
      {:ok, %File.Stat{type: :regular, mode: mode}} -> Bitwise.band(mode, 0o111) != 0
      _absent -> false
    end
  end

  defp report_page_cache(output) do
    if not String.contains?(output, @page_cache_marker <> "yes") do
      Logger.warning("clickhouse: the OS page cache was NOT dropped; cold numbers are warm")
    end

    :ok
  end

  @doc """
  Releases nothing, and deliberately leaves the server running.

  The operator started it and the operator stops it, with `make ch_down`. A
  backend that shut down a server it did not start would make a failed run
  indistinguishable from a clean one, and would throw away the server logs that
  are the only explanation for whatever went wrong.
  """
  @impl true
  def teardown(_state), do: :ok

  defp run_sql(state, sql, settings \\ [], req_opts \\ []) do
    options = Keyword.merge([url: "/", params: settings, body: sql], req_opts)

    case Req.post(state.req, options) do
      {:ok, %Req.Response{status: 200} = response} ->
        {:ok, IO.iodata_to_binary(response.body), response.headers}

      {:ok, %Req.Response{} = response} ->
        {:error, {:clickhouse_error, response.status, IO.iodata_to_binary(response.body)}}

      {:error, exception} ->
        {:error, {:clickhouse_transport, Exception.message(exception)}}
    end
  end

  defp read_pid(path) do
    with {:ok, contents} <- File.read(path),
         {pid, _rest} <- contents |> String.trim() |> Integer.parse() do
      pid
    else
      _unreadable -> nil
    end
  end

  defp parse_ints(body) do
    body
    |> String.trim()
    |> String.split(["\t", "\n"], trim: true)
    |> Enum.flat_map(fn field ->
      case Integer.parse(field) do
        {value, _rest} -> [value]
        :error -> []
      end
    end)
  end

  defp count_lines(data) do
    data |> IO.iodata_to_binary() |> :binary.matches("\n") |> length()
  end

  defp timeout_ms, do: env("CLICKHOUSE_TIMEOUT_MS", 600_000)
end
