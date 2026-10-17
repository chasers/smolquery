defmodule Smolquery.QueryService.PartialWorkerTest do
  use ExUnit.Case, async: false

  alias Explorer.DataFrame
  alias Smolquery.QueryService.PartialWorker
  alias Smolquery.QueryService.Runtime
  alias Smolquery.Schema
  alias Smolquery.Segments.Store
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
      clickhouse_functions: false,
      temp_directory: nil
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
      assert {:ok, %{rows: 1, file_cache: nil, parquet: parquet}} =
               PartialWorker.run_standalone(request)

      assert DataFrame.to_columns(DataFrame.load_parquet!(parquet))["s"] == [6]
    end

    test "runs without lockdown when the spec says so", %{request: request} do
      request = %{request | engine: %{request.engine | lockdown: false}}
      assert {:ok, %{rows: 1}} = PartialWorker.run_standalone(request)
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

    test "drops unset settings", %{spec: spec} do
      options = PartialWorker.standalone_options(%{spec | threads: nil, temp_directory: "/tmp/d"})
      assert options[:settings] == [memory_limit: "256MB", temp_directory: "/tmp/d"]
    end
  end

  describe "engine_spec/1" do
    test "takes the worker's limits and the store without its keys", %{tmp_dir: tmp_dir} do
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
      assert tmp_dir in spec.allowed_directories
      assert spec.temp_directory == nil

      assert %Store{impl: Store.S3, config: config} = spec.store
      assert config.bucket == "sealed"
      assert config.access_key_id == nil
      assert config.secret_access_key == nil
    end

    test "falls back to the job's limits when the worker has none" do
      runtime =
        Runtime.new(
          name: :partial_worker_spec_fallback_test,
          catalog: MapCatalog.new(),
          job_memory_limit: "2GB",
          read_engine_threads: 4
        )

      spec = PartialWorker.engine_spec(runtime)

      assert spec.memory_limit == "2GB"
      assert spec.threads == 4
      assert spec.store == nil
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

      :ok
    end

    test "a static key configured on this node fills a keyless request store", %{spec: spec} do
      Application.put_env(:smolquery, Smolquery.QueryService,
        store:
          {Store.S3,
           [bucket: "sealed", staging_dir: "/tmp/s", access_key_id: "k", secret_access_key: "s"]}
      )

      %Store{} = keyless = Store.S3.new(bucket: "sealed", staging_dir: "/tmp/staging")
      options = PartialWorker.standalone_options(%{spec | store: keyless})

      assert [secret] = options[:statements]
      assert secret =~ "KEY_ID 'k'"
      assert secret =~ "SECRET 's'"
      refute secret =~ "credential_chain"
      assert options[:extensions] == [:httpfs]
    end

    test "without a key of its own, the node reads through the credential chain", %{spec: spec} do
      Application.delete_env(:smolquery, Smolquery.QueryService)

      %Store{} = keyless = Store.S3.new(bucket: "sealed", staging_dir: "/tmp/staging")
      options = PartialWorker.standalone_options(%{spec | store: keyless})

      assert [secret] = options[:statements]
      assert secret =~ "credential_chain"
      assert :aws in options[:extensions]
    end
  end
end
