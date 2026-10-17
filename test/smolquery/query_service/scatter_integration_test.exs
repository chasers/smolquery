defmodule Smolquery.QueryService.ScatterIntegrationTest do
  @moduledoc """
  The distributed path through the public surface (PL-49): two query
  services over the same catalog and buffer, one with `distributed`
  enabled, and every answer compared between them. The scatter telemetry
  event is the proof the distributed instance actually scattered rather
  than silently falling back — a fallback would pass the equality
  assertions too, and prove nothing.

  Workers here are `local_workers` instances dispatched through
  `Smolquery.QueryService.WorkerTransport` to this node, which is a direct
  call; the gen_rpc path to a peer is `worker_transport_peer_test`.
  Genuine cross-host scatter is the kind cluster's to prove, like hot-tier
  fan-out before it (T-77).

  The table is seeded before either query service starts: each one warms an
  engine pool that attaches the same sqlite lake, and under CI load those
  attaches held the file lock long enough to exhaust the catalog's commit
  retries (`{:error, :commit_conflict}` out of `seed/3`).

  A test that changes the table's columns does it there too, as `@tag alter:`
  (T-517). Run from the test body, the `ALTER` raced the same warm-up and lost
  about one run in ten: sqlite answered `database is locked` to every one of
  the catalog's five attempts. What such a test proves is how files sealed
  before the change read after it, on every shard, and that does not depend on
  which side of the services' start the change was made.

  A test that gives the distributed instance a file cache does it as
  `@tag file_cache:` too (T-630): the janitor indexes the cache directory
  when it starts, so `warm: true` writes a block for every sealed file
  before then.
  """

  use ExUnit.Case, async: false

  alias Explorer.DataFrame
  alias Smolquery.BufferService
  alias Smolquery.BufferService.HotServer
  alias Smolquery.Catalog
  alias Smolquery.Catalog.DuckLake
  alias Smolquery.QueryService
  alias Smolquery.QueryService.Client
  alias Smolquery.QueryService.PartialWorker
  alias Smolquery.Schema
  alias Smolquery.Schema.Field
  alias Smolquery.Segments.Store
  alias Smolquery.Segments.Store.Local
  alias Smolquery.Test.FileCacheFixture
  alias Smolquery.Test.SegmentFixture

  @moduletag :integration
  @moduletag :tmp_dir

  @lake __MODULE__.Lake
  @table {"analytics", "events"}

  setup context do
    metadata = "sqlite:#{Path.join(context.tmp_dir, "catalog.sqlite")}"
    data_path = Path.join(context.tmp_dir, "data")

    start_supervised!({DuckLake, name: @lake, metadata: metadata, data_path: data_path})

    catalog = DuckLake.new(engine: @lake)
    :ok = Catalog.create_dataset(catalog, "analytics")
    :ok = Catalog.create_table(catalog, @table, schema())

    buffer = :"scatter_buffer_#{:erlang.unique_integer([:positive])}"

    start_supervised!(
      {BufferService.Supervisor,
       name: buffer, dir: Path.join(context.tmp_dir, "buffer"), flush_interval_ms: 25},
      id: buffer
    )

    on_exit(fn -> BufferService.Runtime.delete(buffer) end)

    seed(catalog, buffer, context.tmp_dir)

    for change <- Map.get(context, :alter, []),
        do: :ok = Catalog.alter_table(catalog, @table, change)

    shared = [
      catalog: catalog,
      buffer_base_url: HotServer.base_url(buffer),
      engine_extensions: [:httpfs],
      allowed_directories: [context.tmp_dir],
      job_bootstrap: [
        DuckLake.attach_statement(DuckLake.default_catalog(), metadata, data_path)
      ]
    ]

    control = :"scatter_control_#{:erlang.unique_integer([:positive])}"

    start_supervised!(
      {QueryService.Supervisor,
       [name: control, distributed: [enabled: false, min_files: 4, local_workers: 3]] ++ shared},
      id: control
    )

    distributed = :"scatter_distributed_#{:erlang.unique_integer([:positive])}"

    start_supervised!(
      {QueryService.Supervisor,
       [
         name: distributed,
         distributed:
           Keyword.merge(
             [enabled: true, min_files: 4, local_workers: 3],
             Map.get(context, :distributed, [])
           )
       ] ++ burst(context) ++ file_cache(context, catalog) ++ shared},
      id: distributed
    )

    on_exit(fn ->
      QueryService.Runtime.delete(control)
      QueryService.Runtime.delete(distributed)
    end)

    attach_telemetry()

    %{control: control, distributed: distributed, catalog: catalog}
  end

  defp file_cache(%{file_cache: opts, tmp_dir: tmp_dir}, catalog) do
    directory = Path.join(tmp_dir, "cache")
    File.mkdir_p!(directory)

    if Keyword.get(opts, :warm, false) do
      {:ok, files} = Catalog.segment_files(catalog, @table, :current)

      for file <- files,
          do:
            File.write!(
              Path.join(directory, FileCacheFixture.block_name(Path.basename(file.path))),
              "x"
            )
    end

    [file_cache: [directory: directory, bypass_bytes: Keyword.fetch!(opts, :bypass_bytes)]]
  end

  defp file_cache(_context, _catalog), do: []

  defp burst(%{burst: max} = context) when is_integer(max),
    do: burst(%{context | burst: [max: max]})

  defp burst(%{burst: opts, tmp_dir: tmp_dir}) do
    pool = :"scatter_burst_pool_#{:erlang.unique_integer([:positive])}"
    {dead, opts} = Keyword.pop(opts, :dead, false)
    {spill, opts} = Keyword.pop(opts, :spill, false)
    max = Keyword.get(opts, :max, 2)

    unless dead do
      start_supervised!(
        {FLAME.Pool,
         name: pool, backend: FLAME.LocalBackend, min: 0, max: max, max_concurrency: 1}
      )
    end

    burst = [burst: Keyword.merge([enabled: true, pool: pool, max: max], opts)]

    if spill,
      do: [{:store, seed_store(tmp_dir)} | burst],
      else: burst
  end

  defp seed_store(tmp_dir), do: Local.new(dir: Path.join(tmp_dir, "seg"))

  defp burst(_context), do: []

  defp attach_shard_decisions do
    parent = self()
    handler = "scatter-shard-cache-#{System.unique_integer([:positive])}"

    :telemetry.attach(
      handler,
      [:smolquery, :query, :file_cache, :shard],
      fn _event, _measurements, meta, _config -> send(parent, {:shard_cache, meta.decision}) end,
      nil
    )

    on_exit(fn -> :telemetry.detach(handler) end)
  end

  defp shard_decisions(count),
    do: for(_shard <- 1..count, do: assert_receive({:shard_cache, decision}) && decision)

  defp schema do
    Schema.new!([{"id", :int64, nullable: false}, {"name", :string}])
  end

  defp seed(catalog, buffer, tmp_dir) do
    store = seed_store(tmp_dir)

    segments =
      for batch <- 0..5 do
        rows = for i <- 1..2, do: %{"id" => batch * 2 + i, "name" => "g-#{rem(batch, 3)}"}
        {:ok, segment} = SegmentFixture.write(rows, schema(), store: store)

        segment
      end

    {:ok, _snapshot} = Catalog.register_segments(catalog, @table, segments)

    hot = for i <- 13..15, do: %{"id" => i, "name" => "g-#{rem(i, 3)}"}
    {:ok, identified} = Catalog.table_schema(catalog, @table)

    {:ok, _ack} =
      BufferService.Client.write_batch(buffer, @table, %{schema: identified, rows: hot})
  end

  defp attach_telemetry do
    parent = self()
    handler = "scatter-test-#{System.unique_integer([:positive])}"

    :telemetry.attach(
      handler,
      [:smolquery, :query, :scatter],
      fn _event, measurements, meta, _config -> send(parent, {:scatter, measurements, meta}) end,
      nil
    )

    on_exit(fn -> :telemetry.detach(handler) end)
  end

  defp both(control, distributed, sql, opts \\ []) do
    assert {:ok, %{state: :done}, control_frame} = Client.query(control, sql, opts)
    assert {:ok, %{state: :done}, distributed_frame} = Client.query(distributed, sql, opts)

    assert DataFrame.names(control_frame) == DataFrame.names(distributed_frame)

    assert_columns(
      DataFrame.to_columns(control_frame),
      DataFrame.to_columns(distributed_frame)
    )
  end

  defp assert_columns(control, distributed) do
    assert Map.keys(control) == Map.keys(distributed)

    Enum.each(control, fn {name, values} ->
      Enum.zip(values, distributed[name])
      |> Enum.each(fn
        {left, right} when is_float(left) and is_float(right) ->
          assert abs(left - right) <= 1.0e-9 * max(1.0, max(abs(left), abs(right)))

        {left, right} ->
          assert left == right
      end)
    end)
  end

  test "global aggregates scatter and answer exactly", %{
    control: control,
    distributed: distributed
  } do
    both(
      control,
      distributed,
      "SELECT count(*) AS n, sum(id) AS s, avg(id) AS a, min(id) AS lo, max(id) AS hi " <>
        "FROM analytics.events"
    )

    assert_received {:scatter, %{shards: shards, partial_bytes: bytes}, %{workers: _workers}}
    assert shards == 3
    assert bytes > 0
  end

  test "a distributed answer lands on the job as scatter", %{distributed: distributed} do
    assert {:ok, %{state: :done} = job, _frame} =
             Client.query(distributed, "SELECT count(*) AS n, sum(id) AS s FROM analytics.events")

    assert %{shards: 3, partial_bytes: bytes} = job.scatter
    assert bytes > 0
  end

  test "the per-job option turns scatter on against the deployment default", %{
    control: control
  } do
    assert {:ok, %{state: :done} = job, _frame} =
             Client.query(control, "SELECT count(*) AS n, sum(id) AS s FROM analytics.events",
               distributed: true
             )

    assert %{shards: 3} = job.scatter
    assert_received {:scatter, _measurements, _meta}
  end

  test "the per-job option turns scatter off against an enabled deployment", %{
    distributed: distributed
  } do
    assert {:ok, %{state: :done} = job, _frame} =
             Client.query(distributed, "SELECT count(*) AS n, sum(id) AS s FROM analytics.events",
               distributed: false
             )

    assert job.scatter == nil
    refute_received {:scatter, _measurements, _meta}
  end

  test "a bare count(*) never scatters: the footers answer it (T-448)", %{
    control: control,
    distributed: distributed
  } do
    both(control, distributed, "SELECT count(*) AS n FROM analytics.events")

    assert {:ok, %{state: :done} = job, frame} =
             Client.query(distributed, "SELECT count(*) AS n FROM analytics.events")

    assert DataFrame.to_columns(frame) == %{"n" => [15]}
    assert job.scatter == nil
    refute_received {:scatter, _measurements, _meta}
  end

  test "a grouped top-k scatters and answers exactly", %{
    control: control,
    distributed: distributed
  } do
    both(
      control,
      distributed,
      "SELECT name, count(*) AS n, sum(id) AS s FROM analytics.events " <>
        "GROUP BY name ORDER BY s DESC, name LIMIT 2"
    )

    assert_received {:scatter, _measurements, _meta}
  end

  test "aggregates that merge through a value scatter and answer exactly (T-539)", %{
    control: control,
    distributed: distributed
  } do
    both(
      control,
      distributed,
      "SELECT name, count(DISTINCT id % 3) AS kinds, arg_max(id, id) AS newest, " <>
        "arg_min(id, id) AS oldest FROM analytics.events GROUP BY name ORDER BY name"
    )

    assert_received {:scatter, _measurements, _meta}
  end

  @tag distributed: [value_list_max_rows: 1]
  test "an aggregate that ships values runs on one engine over a plan with more rows than value_list_max_rows",
       %{control: control, distributed: distributed} do
    both(control, distributed, "SELECT count(DISTINCT id % 3) AS kinds FROM analytics.events")

    refute_received {:scatter, _measurements, _meta}

    both(control, distributed, "SELECT count(id) AS n FROM analytics.events")

    assert_received {:scatter, _measurements, _meta}
  end

  test "a sample under a LIMIT scatters as rows, and the outer statement answers exactly (T-540)",
       %{control: control, distributed: distributed} do
    both(
      control,
      distributed,
      "WITH sampled AS (SELECT name AS param0 FROM analytics.events WHERE id >= 0 LIMIT 100000) " <>
        "SELECT count(*) AS n, count(DISTINCT param0) AS names, min(param0) AS first FROM sampled"
    )

    assert_received {:scatter, _measurements, _meta}

    both(
      control,
      distributed,
      "SELECT name, count(*) AS n FROM (SELECT name FROM analytics.events LIMIT 100000) e " <>
        "GROUP BY name ORDER BY name"
    )

    assert_received {:scatter, _measurements, _meta}
  end

  @tag distributed: [row_partial_max_rows: 10]
  test "a sample whose LIMIT is over row_partial_max_rows runs on one engine", %{
    control: control,
    distributed: distributed
  } do
    both(
      control,
      distributed,
      "SELECT count(*) AS n FROM (SELECT name FROM analytics.events LIMIT 100000)"
    )

    refute_received {:scatter, _measurements, _meta}
  end

  test "a filtered aggregate scatters and answers exactly", %{
    control: control,
    distributed: distributed
  } do
    both(
      control,
      distributed,
      "SELECT count(*) AS n FROM analytics.events WHERE id > 6"
    )

    assert_received {:scatter, _measurements, _meta}
  end

  test "a filtered aggregate binds its parameter on every partial (T-410)", %{
    control: control,
    distributed: distributed
  } do
    both(
      control,
      distributed,
      "SELECT count(*) AS n FROM analytics.events WHERE id > $1",
      params: [6]
    )

    assert_received {:scatter, _measurements, _meta}
  end

  test "the control service never scatters", %{control: control} do
    assert {:ok, %{state: :done}, _frame} =
             Client.query(control, "SELECT count(*) AS n FROM analytics.events")

    refute_received {:scatter, _measurements, _meta}
  end

  @tag alter: [{:drop_column, "name"}, {:add_column, Field.new!("name", :int64)}]
  test "a dropped name re-added as a new column reads NULL from sealed files on every shard (PL-62)",
       %{control: control, distributed: distributed} do
    both(control, distributed, "SELECT count(name) AS named, count(*) AS n FROM analytics.events")

    assert {:ok, %{state: :done} = job, frame} =
             Client.query(
               distributed,
               "SELECT count(name) AS named, sum(name) AS s FROM analytics.events"
             )

    assert %{shards: 3} = job.scatter
    assert DataFrame.to_columns(frame) == %{"named" => [0], "s" => [nil]}
  end

  @tag alter: [{:add_column, Field.new!("late", :string)}]
  test "a column added after the files sealed still scatters, and reads NULL from them (PL-61)",
       %{distributed: distributed} do
    assert {:ok, %{state: :done} = job, frame} =
             Client.query(
               distributed,
               "SELECT count(late) AS late, count(*) AS n FROM analytics.events"
             )

    assert %{shards: 3} = job.scatter
    assert DataFrame.to_columns(frame) == %{"late" => [0], "n" => [15]}
  end

  @tag file_cache: [bypass_bytes: 0]
  test "a cold cache is bypassed by every shard, and the job says so (T-630)", %{
    distributed: distributed
  } do
    attach_shard_decisions()

    assert {:ok, %{state: :done} = job, _frame} =
             Client.query(distributed, "SELECT sum(id) AS s FROM analytics.events")

    assert %{shards: 3} = job.scatter
    assert job.file_cache == :bypassed
    assert shard_decisions(3) == [:bypassed, :bypassed, :bypassed]
  end

  @tag file_cache: [bypass_bytes: 0, warm: true]
  test "each shard reads through a cache that holds its files (T-630)", %{
    distributed: distributed
  } do
    attach_shard_decisions()

    assert {:ok, %{state: :done} = job, _frame} =
             Client.query(distributed, "SELECT sum(id) AS s FROM analytics.events")

    assert %{shards: 3} = job.scatter
    assert job.file_cache == :used
    assert shard_decisions(3) == [:used, :used, :used]

    assert {:ok, %{state: :done} = off, _frame} =
             Client.query(distributed, "SELECT sum(id) AS s FROM analytics.events",
               file_cache: false
             )

    assert off.file_cache == :off
    assert shard_decisions(3) == [:off, :off, :off]
  end

  @tag file_cache: [bypass_bytes: 1_000_000_000]
  test "a cold cache under the threshold for the node's files is used by every shard (T-630)",
       %{distributed: distributed} do
    attach_shard_decisions()

    assert {:ok, %{state: :done} = job, _frame} =
             Client.query(distributed, "SELECT sum(id) AS s FROM analytics.events")

    assert %{shards: 3} = job.scatter
    assert job.file_cache == :used
    assert shard_decisions(3) == [:used, :used, :used]
  end

  @tag file_cache: [bypass_bytes: 0, warm: true]
  test "a shard decides against its own node's cache, not the coordinator's verdict (T-630)",
       %{distributed: distributed, catalog: catalog} do
    {:ok, files} = Catalog.segment_files(catalog, @table, :current)
    {:ok, schema} = Catalog.table_schema(catalog, @table)

    request = %{
      table_ref: @table,
      schema: schema,
      files:
        Enum.map(files, &%{"url" => &1.path, "snapshot" => &1.snapshot, "bytes" => &1.bytes}),
      node_files: Enum.map(files, &%{"url" => &1.path, "bytes" => &1.bytes}),
      partial_sql: "SELECT sum(id) AS s FROM analytics.events",
      allowed_paths: [],
      file_cache: :bypassed
    }

    assert {:ok, %{rows: 1, file_cache: :used}} =
             PartialWorker.run(distributed, Map.put(request, :file_cache_mode, :auto))

    assert {:ok, %{rows: 1, file_cache: :bypassed}} = PartialWorker.run(distributed, request)
  end

  test "a scattered query traces gather, merge and one shard per worker (T-691)",
       %{distributed: distributed} do
    assert {:ok, %{state: :done} = job, _frame} =
             Client.query(distributed, "SELECT sum(id) AS s FROM analytics.events", trace: true)

    assert %{shards: shards} = job.scatter
    names = Enum.map(job.trace, & &1.name)

    assert :gather in names
    assert :merge in names
    assert Enum.count(names, &(&1 == :shard)) == shards

    gather = Enum.find(job.trace, &(&1.name == :gather))
    merge = Enum.find(job.trace, &(&1.name == :merge))
    assert gather.meta == %{shards: shards, outcome: :ok}
    assert merge.meta == %{outcome: :ok}
    assert merge.start_us >= gather.start_us + gather.duration_us

    for shard <- Enum.filter(job.trace, &(&1.name == :shard)) do
      assert shard.start_us >= gather.start_us
      assert shard.start_us + shard.duration_us <= gather.start_us + gather.duration_us

      assert %{kind: :peer, target: target, node: node, files: files, sealed_bytes: bytes} =
               shard.meta

      assert target == node()
      assert node == node()
      assert is_integer(files) and files > 0
      assert is_integer(bytes) and bytes > 0

      assert %{engine: source, engine_us: engine, describe_us: describe, scan_us: scan} =
               shard.meta

      assert %{rows: 1, worker_us: worker} = shard.meta
      assert source in [:warm, :cold]
      assert engine + describe + scan <= worker
      assert worker <= shard.duration_us
    end

    assert job.trace
           |> Enum.filter(&(&1.name == :shard))
           |> Enum.map(& &1.meta.shard)
           |> Enum.sort() ==
             Enum.to_list(0..(shards - 1))
  end

  @tag burst: 2
  test "a burst query answers exactly, hot rows included, from peers and runners (T-676)",
       %{control: control, distributed: distributed} do
    sql = "SELECT count(*) AS n, sum(id) AS s, max(id) AS m FROM analytics.events"
    both(control, distributed, sql, burst: 2)

    assert {:ok, %{state: :done} = job, frame} =
             Client.query(distributed, sql, burst: 2, trace: true)

    assert %{"n" => [15], "m" => [15]} = DataFrame.to_columns(frame)

    {:ok, runtime} = QueryService.Runtime.fetch(distributed)
    shards = Enum.filter(job.trace, &(&1.name == :shard))
    runners = Enum.filter(shards, &(&1.meta.kind == :runner))
    peers = Enum.filter(shards, &(&1.meta.kind == :peer))

    assert [_, _] = runners
    assert [_, _, _] = peers
    assert job.scatter.shards == 5

    for runner <- runners do
      assert runner.meta.hot_files == 0
      assert runner.meta.target == inspect(runtime.burst.pool)
      assert runner.meta.node == node()
      assert %{engine: :standalone, rows: 1, wait_us: wait} = runner.meta
      assert is_integer(wait) and wait >= 0
    end

    assert Enum.sum_by(peers, & &1.meta.hot_files) >= 1
    assert Enum.all?(peers, &(&1.meta.target == node()))

    assert job.scatter.backend == "local"

    assert_receive {:scatter, %{shards: 5, peers: 3, burst: 2},
                    %{workers: [worker], backend: "local", burst_refused: nil}}

    assert worker == node()
  end

  test "a count asked of a node that leases no runners scatters over its peers and says no_pool",
       %{distributed: distributed} do
    assert {:ok, %{state: :done} = job, _frame} =
             Client.query(distributed, "SELECT sum(id) AS s FROM analytics.events",
               burst: 2,
               trace: true
             )

    assert [_, _, _] = shards = Enum.filter(job.trace, &(&1.name == :shard))
    assert Enum.all?(shards, &(&1.meta.kind == :peer))

    assert %{shards: 3, peers: 3, burst: 0, burst_refused: :no_pool, burst_wait_ms: 0} =
             job.scatter

    assert job.scatter.backend == nil
    assert_receive {:scatter, _measurements, %{burst_refused: :no_pool, backend: nil}}
  end

  test "auto on a node that leases no runners is quietly peers only", %{distributed: distributed} do
    assert {:ok, %{state: :done} = job, _frame} =
             Client.query(distributed, "SELECT sum(id) AS s FROM analytics.events", burst: :auto)

    assert %{shards: 3, peers: 3, burst: 0, burst_refused: nil} = job.scatter
  end

  @tag burst: [max: 2, min_scan_bytes: 0, target_bytes: 1]
  test "auto bursts over the threshold, one runner per target_bytes up to the pool's max (T-677)",
       %{control: control, distributed: distributed} do
    sql = "SELECT count(*) AS n, max(id) AS m FROM analytics.events"
    both(control, distributed, sql, burst: :auto)

    assert {:ok, %{state: :done} = job, frame} = Client.query(distributed, sql, burst: :auto)
    assert %{"n" => [15], "m" => [15]} = DataFrame.to_columns(frame)
    assert %{shards: 5, peers: 3, burst: 2, burst_refused: nil, burst_wait_ms: wait} = job.scatter
    assert is_integer(wait) and wait >= 0
  end

  @tag burst: 2
  test "auto under the threshold stays on peers and says so", %{distributed: distributed} do
    assert {:ok, %{state: :done} = job, _frame} =
             Client.query(distributed, "SELECT sum(id) AS s FROM analytics.events", burst: :auto)

    assert %{shards: 3, burst: 0, burst_refused: {:under_threshold, bytes, 8_589_934_592}} =
             job.scatter

    assert is_integer(bytes) and bytes > 0
  end

  @tag burst: [dead: true]
  test "a runner shard that cannot be leased is retried on a peer, and the job says runners_unreachable (T-677)",
       %{control: control, distributed: distributed} do
    sql = "SELECT count(*) AS n, max(id) AS m FROM analytics.events"
    both(control, distributed, sql, burst: 2)

    assert {:ok, %{state: :done} = job, _frame} =
             Client.query(distributed, sql, burst: 2, trace: true)

    assert %{shards: 5, peers: 5, burst: 0, burst_refused: :runners_unreachable} = job.scatter

    shards = Enum.filter(job.trace, &(&1.name == :shard))
    failed = Enum.filter(shards, &(&1.meta.kind == :runner))
    retried = Enum.filter(shards, &Map.get(&1.meta, :retry, false))

    assert [_, _] = failed
    assert Enum.all?(failed, &(&1.meta.error =~ "worker_unreachable"))
    assert [_, _] = retried
    assert Enum.all?(retried, &(&1.meta.kind == :peer and &1.meta.rows == 1))

    assert Enum.sort(Enum.map(retried, & &1.meta.shard)) ==
             Enum.sort(Enum.map(failed, & &1.meta.shard))
  end

  @tag burst: [max: 2, spill: true, spill_inline_max_bytes: 0]
  test "a runner partial over the inline max spills to the store, merges from there, and is deleted after (T-678)",
       %{control: control, distributed: distributed, tmp_dir: tmp_dir} do
    sql = "SELECT count(*) AS n, max(id) AS m FROM analytics.events"
    both(control, distributed, sql, burst: 2)

    assert {:ok, %{state: :done} = job, frame} =
             Client.query(distributed, sql, burst: 2, trace: true)

    assert %{"n" => [15], "m" => [15]} = DataFrame.to_columns(frame)
    assert %{shards: 5, peers: 3, burst: 2, partial_bytes: bytes} = job.scatter
    assert bytes > 0

    shards = Enum.filter(job.trace, &(&1.name == :shard))
    runners = Enum.filter(shards, &(&1.meta.kind == :runner))
    peers = Enum.filter(shards, &(&1.meta.kind == :peer))

    assert [_, _] = runners
    assert Enum.all?(runners, &(&1.meta.spilled and &1.meta.partial_bytes > 0))
    assert Enum.all?(peers, &(not &1.meta.spilled and &1.meta.partial_bytes > 0))

    assert {:ok, []} = Store.list(seed_store(tmp_dir), "burst-partials")
  end

  @tag burst: [dead: true, spill: true, spill_inline_max_bytes: 0]
  test "runner shards retried on peers leave nothing under the spill prefix (T-678)",
       %{distributed: distributed, tmp_dir: tmp_dir} do
    assert {:ok, %{state: :done} = job, _frame} =
             Client.query(distributed, "SELECT count(*) AS n, max(id) AS m FROM analytics.events",
               burst: 2
             )

    assert %{burst: 0, peers: 5, burst_refused: :runners_unreachable} = job.scatter
    assert {:ok, []} = Store.list(seed_store(tmp_dir), "burst-partials")
  end

  @tag burst: [dead: true, spill: true, spill_inline_max_bytes: 0, retry_floor_ms: 600_000]
  test "a gather that fails sweeps the spill prefix before the fallback (T-678)",
       %{distributed: distributed, tmp_dir: tmp_dir} do
    assert {:ok, %{state: :done} = job, frame} =
             Client.query(distributed, "SELECT count(*) AS n, max(id) AS m FROM analytics.events",
               burst: 2,
               trace: true
             )

    assert %{"n" => [15], "m" => [15]} = DataFrame.to_columns(frame)
    assert Enum.find(job.trace, &(&1.name == :gather)).meta.outcome == :error
    assert job.scatter == nil
    assert {:ok, []} = Store.list(seed_store(tmp_dir), "burst-partials")
  end

  @tag burst: [dead: true, retry_floor_ms: 600_000]
  test "under the retry floor a failed runner shard sends the job to the single-engine fallback",
       %{distributed: distributed} do
    assert {:ok, %{state: :done} = job, frame} =
             Client.query(distributed, "SELECT count(*) AS n, max(id) AS m FROM analytics.events",
               burst: 2,
               trace: true
             )

    assert %{"n" => [15], "m" => [15]} = DataFrame.to_columns(frame)
    assert Enum.find(job.trace, &(&1.name == :gather)).meta.outcome == :error
    assert job.scatter == nil
  end

  test "a query that does not decompose still answers", %{distributed: distributed} do
    assert {:ok, %{state: :done} = job, frame} =
             Client.query(distributed, "SELECT id, name FROM analytics.events ORDER BY id")

    assert DataFrame.to_columns(frame)["id"] == Enum.to_list(1..15)
    assert job.scatter == nil
    refute_received {:scatter, _measurements, _meta}
  end
end
