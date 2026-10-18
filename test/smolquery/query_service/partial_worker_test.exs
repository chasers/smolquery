defmodule Smolquery.QueryService.PartialWorkerTest do
  use ExUnit.Case, async: false

  alias Explorer.DataFrame
  alias Smolquery.QueryService.PartialWorker
  alias Smolquery.QueryService.Runtime
  alias Smolquery.Schema
  alias Smolquery.Segments.Store
  alias Smolquery.Test.FlameParent
  alias Smolquery.Test.MapCatalog
  alias Smolquery.Test.SegmentFixture

  @moduletag :tmp_dir
  @table {"analytics", "events"}

  setup %{tmp_dir: tmp_dir} do
    store = Store.Local.new(dir: tmp_dir)
    schema = Schema.new!([{"id", :int64}])

    {:ok, segment} =
      SegmentFixture.write([%{"id" => 1}, %{"id" => 2}, %{"id" => 3}], schema,
        store: store,
        prefix: "analytics/events"
      )

    spec = %{
      extensions: [:httpfs],
      memory_limit: "256MB",
      threads: 2,
      store: store,
      lockdown: true,
      allowed_directories: [tmp_dir],
      clickhouse_functions: false
    }

    request = %{
      engine: spec,
      table_ref: @table,
      schema: schema,
      files: [%{"url" => segment.path, "snapshot" => 1, "bytes" => segment.byte_size}],
      partial_sql: "SELECT sum(id) AS s FROM analytics.events",
      allowed_paths: []
    }

    %{store: store, spec: spec, request: request, tmp_dir: tmp_dir}
  end

  describe "run_standalone/1" do
    test "answers a shard with nothing but the request", %{request: request} do
      assert {:ok, %{rows: 1, file_cache: nil, parquet: parquet} = reply} =
               PartialWorker.run_standalone(request)

      assert DataFrame.to_columns(DataFrame.load_parquet!(parquet))["s"] == [6]
      assert reply.node == node()

      assert %{engine: :standalone, engine_us: engine, describe_us: describe, scan_us: scan} =
               reply.timings

      assert engine > 0 and describe > 0 and scan > 0
      assert reply.timings.worker_us >= describe + scan
    end

    test "runs without lockdown when the spec says so", %{request: request} do
      request = %{request | engine: %{request.engine | lockdown: false}}
      assert {:ok, %{rows: 1}} = PartialWorker.run_standalone(request)
    end

    test "counts its cold engine as a standalone source", %{request: request} do
      parent = self()
      handler = "partial-worker-engine-#{System.unique_integer([:positive])}"

      :telemetry.attach(
        handler,
        [:smolquery, :query, :engine],
        fn _event, _measurements, meta, _config -> send(parent, {:engine, meta}) end,
        nil
      )

      on_exit(fn -> :telemetry.detach(handler) end)

      assert {:ok, _reply} = PartialWorker.run_standalone(request)
      assert_receive {:engine, %{source: :standalone}}
    end

    test "a partial over the inline max spills to the store and answers its location (T-678)",
         %{request: request, store: store} do
      key = "burst-partials/job/0.parquet"
      request = Map.put(request, :spill, %{key: key, inline_max_bytes: 0})

      assert {:ok,
              %{parquet: nil, rows: 1, spilled: %{location: location, key: ^key, bytes: bytes}}} =
               PartialWorker.run_standalone(request)

      assert bytes > 0
      assert location == Store.location(store, key)
      assert DataFrame.to_columns(DataFrame.from_parquet!(location))["s"] == [6]
    end

    test "a spill the store refuses is the shard's failure, not a crash", %{
      request: request,
      tmp_dir: tmp_dir
    } do
      not_a_dir = Path.join(tmp_dir, "a-file")
      File.write!(not_a_dir, "x")
      broken = Store.Local.new(dir: not_a_dir)

      request =
        request
        |> Map.put(:engine, %{request.engine | store: broken})
        |> Map.put(:spill, %{key: "burst-partials/job/0.parquet", inline_max_bytes: 0})

      assert {:error, {:spill_failed, {:put_failed, "burst-partials/job/0.parquet", _reason}}} =
               PartialWorker.run_standalone(request)
    end

    test "a partial under the inline max stays inline", %{request: request} do
      request =
        Map.put(request, :spill, %{
          key: "burst-partials/job/0.parquet",
          inline_max_bytes: 10_000_000
        })

      assert {:ok, %{parquet: parquet, spilled: nil}} = PartialWorker.run_standalone(request)
      assert is_binary(parquet)
    end
  end

  describe "standalone_options/1" do
    test "loads the store's extensions, sets the limits, renders only the sealed secret",
         %{spec: spec} do
      %Store{} = s3 = Store.S3.new(bucket: "sealed", staging_dir: "/tmp/staging")
      options = PartialWorker.standalone_options(%{spec | store: s3})

      assert options[:extensions] == [:httpfs, :aws]
      assert options[:settings] == [memory_limit: "256MB", threads: 2]
      assert options[:max_rows] == :infinity
      refute Keyword.has_key?(options, :temp_directory)

      assert [secret] = options[:statements]
      assert secret =~ "CREATE SECRET IF NOT EXISTS smolquery_sealed_tier"
      assert secret =~ "credential_chain"
      refute Enum.any?(options[:statements], &(&1 =~ "ATTACH"))
      refute Enum.any?(options[:statements], &(&1 =~ "smolquery_hot_tier"))
      refute Enum.any?(options[:extensions], &(&1 in [:ducklake, {:cache_httpfs, :community}]))
    end

    test "a local store needs no secret and no extra extension", %{spec: spec} do
      options = PartialWorker.standalone_options(spec)

      assert options[:extensions] == [:httpfs]
      assert options[:statements] == []
    end

    test "drops an unset thread count", %{spec: spec} do
      options = PartialWorker.standalone_options(%{spec | threads: nil})
      assert options[:settings] == [memory_limit: "256MB"]
    end

    test "on a runner the host's limits replace the coordinator's", %{spec: spec} do
      previous = Application.get_env(:smolquery, Smolquery.Burst)

      on_exit(fn ->
        if previous,
          do: Application.put_env(:smolquery, Smolquery.Burst, previous),
          else: Application.delete_env(:smolquery, Smolquery.Burst)
      end)

      FlameParent.put_env()

      Application.put_env(:smolquery, Smolquery.Burst,
        runner_memory_limit: "7GB",
        runner_threads: 6
      )

      options = PartialWorker.standalone_options(spec)
      assert options[:settings] == [memory_limit: "7GB", threads: 6]
    end
  end

  describe "engine_spec/1" do
    test "takes the worker's limits and the store without its keys, and no local directories",
         %{tmp_dir: tmp_dir} do
      runtime =
        Runtime.new(
          name: :partial_worker_spec_test,
          catalog: MapCatalog.new(),
          store:
            {Store.S3,
             [bucket: "sealed", staging_dir: tmp_dir, access_key_id: "k", secret_access_key: "s"]},
          allowed_directories: [tmp_dir],
          engine_extensions: [:httpfs],
          job_memory_limit: "2GB",
          read_engine_threads: 4,
          distributed: [worker_memory_limit: "1GB", worker_threads: 3],
          lockdown: true,
          clickhouse_functions: false
        )

      spec = PartialWorker.engine_spec(runtime)

      assert spec.memory_limit == "1GB"
      assert spec.threads == 3
      assert spec.extensions == [:httpfs]
      assert spec.lockdown
      refute spec.clickhouse_functions
      assert spec.allowed_directories == []
      refute Map.has_key?(spec, :temp_directory)

      assert %Store{impl: Store.S3, config: config} = spec.store
      assert config.bucket == "sealed"
      assert config.access_key_id == nil
      assert config.secret_access_key == nil
    end

    test "falls back to the job's limits and keeps the directories of a local store",
         %{tmp_dir: tmp_dir} do
      runtime =
        Runtime.new(
          name: :partial_worker_spec_fallback_test,
          catalog: MapCatalog.new(),
          store: {Store.Local, [dir: tmp_dir]},
          allowed_directories: [tmp_dir],
          job_memory_limit: "2GB",
          read_engine_threads: 4
        )

      spec = PartialWorker.engine_spec(runtime)

      assert spec.memory_limit == "2GB"
      assert spec.threads == 4
      assert tmp_dir in spec.allowed_directories
      assert %Store{impl: Store.Local} = spec.store
    end
  end

  describe "the runner's own credentials" do
    setup do
      previous = Application.get_env(:smolquery, Smolquery.QueryService)

      on_exit(fn ->
        if previous,
          do: Application.put_env(:smolquery, Smolquery.QueryService, previous),
          else: Application.delete_env(:smolquery, Smolquery.QueryService)
      end)

      %{keyless: Store.S3.new(bucket: "sealed", staging_dir: "/tmp/staging")}
    end

    test "a static key configured here for the same bucket fills a keyless request store",
         %{spec: spec, keyless: keyless} do
      put_local_store(bucket: "sealed", access_key_id: "k", secret_access_key: "s")

      options = PartialWorker.standalone_options(%{spec | store: keyless})

      assert [secret] = options[:statements]
      assert secret =~ "KEY_ID 'k'"
      assert secret =~ "SECRET 's'"
      refute secret =~ "credential_chain"
      assert options[:extensions] == [:httpfs]
    end

    test "a key for another bucket or endpoint is not used", %{spec: spec, keyless: keyless} do
      put_local_store(bucket: "other", access_key_id: "k", secret_access_key: "s")
      assert [secret] = PartialWorker.standalone_options(%{spec | store: keyless})[:statements]
      assert secret =~ "credential_chain"

      put_local_store(
        bucket: "sealed",
        endpoint: "http://minio:9000",
        access_key_id: "k",
        secret_access_key: "s"
      )

      assert [secret] = PartialWorker.standalone_options(%{spec | store: keyless})[:statements]
      assert secret =~ "credential_chain"
    end

    test "a request store that still carries keys keeps them", %{spec: spec} do
      put_local_store(bucket: "sealed", access_key_id: "local", secret_access_key: "local")

      %Store{} =
        keyed =
        Store.S3.new(
          bucket: "sealed",
          staging_dir: "/tmp/staging",
          access_key_id: "sent",
          secret_access_key: "sent"
        )

      assert [secret] = PartialWorker.standalone_options(%{spec | store: keyed})[:statements]
      assert secret =~ "KEY_ID 'sent'"
    end

    test "spill_store/1 signs the spill with this node's keys for the bucket and stages under tmp",
         %{keyless: keyless} do
      put_local_store(bucket: "sealed", access_key_id: "k", secret_access_key: "s")

      %Store{impl: Store.S3, config: config} = PartialWorker.spill_store(keyless)

      assert config.access_key_id == "k"
      assert config.secret_access_key == "s"
      assert String.starts_with?(config.staging_dir, System.tmp_dir!())

      Application.delete_env(:smolquery, Smolquery.QueryService)
      %Store{config: chain} = PartialWorker.spill_store(keyless)
      assert chain.access_key_id == nil
    end

    test "without a key of its own, the node reads through the credential chain",
         %{spec: spec, keyless: keyless} do
      Application.delete_env(:smolquery, Smolquery.QueryService)

      options = PartialWorker.standalone_options(%{spec | store: keyless})

      assert [secret] = options[:statements]
      assert secret =~ "credential_chain"
      assert :aws in options[:extensions]
    end

    defp put_local_store(options) do
      Application.put_env(:smolquery, Smolquery.QueryService,
        store: {Store.S3, Keyword.put(options, :staging_dir, "/tmp/s")}
      )
    end
  end
end
