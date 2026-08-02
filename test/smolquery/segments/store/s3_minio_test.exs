defmodule Smolquery.Segments.Store.S3MinioTest do
  @moduledoc """
  Milestone 8 L3: `Store.S3` against a real S3-compatible service (MinIO in
  CI/kind, per PL-11 D7) — the behavior contract `Store.Local` already has
  tests for, plus the one thing only an object store needs proven: a
  segment `Store.S3.put/3` writes is readable back by DuckDB's `httpfs`
  once `create_secret_statement/1` has run, the same as
  `StorageService.Supervisor`/`QueryService.Runner` bootstrap it.

  The last two tests pin seams the L7 kind cluster found broken. The storage
  subtree's *catalog* engine also reads segments back (DuckLake's
  add-data-files collects footer stats), so a registration commits only if
  that engine was bootstrapped with the sealed tier's secret too — not just
  the merge engine. And a locked-down job engine (PL-8 D7) must keep the
  sealed tier's `s3://` prefix readable after external access is disabled,
  or every query touching sealed data fails with a permission error.
  """

  use ExUnit.Case, async: false

  alias Smolquery.BufferService
  alias Smolquery.BufferService.HotServer
  alias Smolquery.Catalog
  alias Smolquery.Catalog.DuckLake
  alias Smolquery.Engine
  alias Smolquery.QueryService
  alias Smolquery.Schema
  alias Smolquery.Segments.Store
  alias Smolquery.Segments.Store.S3
  alias Smolquery.Segments.Writer
  alias Smolquery.StorageService
  alias Smolquery.StorageService.Runtime

  @moduletag :integration
  @moduletag :tmp_dir

  @bucket "smolquery-store-s3-test"

  setup context do
    config = %{
      bucket: @bucket,
      access_key_id: System.get_env("TEST_S3_ACCESS_KEY_ID", "smolquery"),
      secret_access_key: System.get_env("TEST_S3_SECRET_ACCESS_KEY", "smolquery-secret"),
      endpoint: System.get_env("TEST_S3_ENDPOINT", "http://localhost:9000")
    }

    ensure_bucket!(config)

    store =
      S3.new(
        bucket: config.bucket,
        access_key_id: config.access_key_id,
        secret_access_key: config.secret_access_key,
        endpoint: config.endpoint,
        staging_dir: Path.join(context.tmp_dir, "staging")
      )

    %{store: store}
  end

  test "put/3 uploads and returns the s3:// location and byte size", %{store: store} do
    key = unique_key()

    assert {:ok, %{location: location, byte_size: size}} =
             Store.put(store, key, &File.write!(&1, "hello"))

    assert location == "s3://#{@bucket}/#{key}"
    assert size == byte_size("hello")
  end

  test "location/2 round-trips through a real read", %{store: store} do
    key = unique_key()
    {:ok, _put} = Store.put(store, key, &File.write!(&1, "abc123"))

    assert {:ok, %{status: 200, body: "abc123"}} = get_object(store, key)
  end

  test "list/2 returns every key under a prefix, sorted", %{store: store} do
    prefix = "listing-#{:erlang.unique_integer([:positive])}"
    keys = for i <- 1..3, do: "#{prefix}/#{i}.parquet"

    for key <- keys, do: {:ok, _} = Store.put(store, key, &File.write!(&1, "x"))

    assert {:ok, ^keys} = Store.list(store, prefix)
  end

  test "list/2 follows continuation tokens past the page size", %{store: store} do
    prefix = "paging-#{:erlang.unique_integer([:positive])}"
    keys = for i <- 1..3, do: "#{prefix}/#{i}.parquet"

    for key <- keys, do: {:ok, _} = Store.put(store, key, &File.write!(&1, "x"))

    %Store{config: config} = store
    paged = %Store{store | config: %S3{config | list_max_keys: 1}}

    assert {:ok, ^keys} = Store.list(paged, prefix)
  end

  test "list/2 scopes a prefix as a directory, not a string prefix", %{store: store} do
    base = "scoping-#{:erlang.unique_integer([:positive])}"
    {:ok, _} = Store.put(store, "#{base}/events/1.parquet", &File.write!(&1, "x"))
    {:ok, _} = Store.put(store, "#{base}/events_v2/1.parquet", &File.write!(&1, "x"))

    assert Store.list(store, "#{base}/events") == {:ok, ["#{base}/events/1.parquet"]}
  end

  test "list/2 returns only parquet keys — foreign objects are not segments", %{store: store} do
    prefix = "foreign-#{:erlang.unique_integer([:positive])}"
    {:ok, _} = Store.put(store, "#{prefix}/1.parquet", &File.write!(&1, "x"))

    %Store{config: config} = store

    {:ok, %{status: status}} =
      Req.put(request(config), url: "s3://#{@bucket}/#{prefix}/export.csv", body: "a,b")

    assert status in 200..299
    assert Store.list(store, prefix) == {:ok, ["#{prefix}/1.parquet"]}
  end

  test "delete/2 removes the object, and is :ok on a key that never existed", %{store: store} do
    key = unique_key()
    {:ok, _put} = Store.put(store, key, &File.write!(&1, "x"))

    assert Store.delete(store, key) == :ok
    assert {:ok, %{status: 404}} = get_object(store, key)
    assert Store.delete(store, key) == :ok
  end

  test "sweep_staging/2 removes stale staged files but keeps fresh ones", %{store: store} do
    %Store{config: %S3{staging_dir: dir}} = store
    stale = Path.join([dir, ".tmp", "stale.parquet.1"])
    fresh = Path.join([dir, ".tmp", "fresh.parquet.1"])
    File.mkdir_p!(Path.dirname(stale))
    File.write!(stale, "x")
    File.write!(fresh, "x")
    File.touch!(stale, System.os_time(:second) - 3600)

    assert {:ok, swept} = Store.sweep_staging(store, 60_000)

    assert Path.basename(stale) in swept
    refute File.exists?(stale)
    assert File.exists?(fresh)
  end

  test "a segment written through the store reads back through DuckDB httpfs", %{
    store: store,
    tmp_dir: tmp_dir
  } do
    %Store{config: config} = store
    schema = Schema.new!([{"id", :int64, nullable: false}])
    rows = for i <- 1..5, do: %{"id" => i}

    {:ok, segment} = Writer.write(rows, schema, store: store, prefix: "engine-read")

    {:ok, _pid} =
      Engine.start_link(
        name: __MODULE__.Lake,
        extensions: [:httpfs],
        statements: [S3.create_secret_statement(config)]
      )

    result =
      Engine.query!(__MODULE__.Lake, "SELECT count(*) FROM read_parquet(?)", [segment.path])

    assert result.rows |> List.flatten() |> List.first() == 5
  after
    File.rm_rf!(tmp_dir)
  end

  test "the storage subtree's catalog engine registers a segment living on S3", %{
    store: store,
    tmp_dir: tmp_dir
  } do
    %Store{config: %S3{} = config} = store
    name = :"storage_s3_catalog_#{:erlang.unique_integer([:positive])}"

    start_supervised!(
      {StorageService.Supervisor,
       name: name,
       dir: Path.join(tmp_dir, "sealed"),
       store:
         {S3,
          bucket: config.bucket,
          access_key_id: config.access_key_id,
          secret_access_key: config.secret_access_key,
          endpoint: config.endpoint,
          staging_dir: config.staging_dir},
       catalog: [
         metadata: "sqlite:#{Path.join(tmp_dir, "catalog.sqlite")}",
         data_path: Path.join(tmp_dir, "lake")
       ]}
    )

    on_exit(fn -> Runtime.delete(name) end)

    {:ok, runtime} = Runtime.fetch(name)

    schema = Schema.new!([{"id", :int64, nullable: false}])
    rows = for i <- 1..5, do: %{"id" => i}
    {:ok, segment} = Writer.write(rows, schema, store: store, prefix: "catalog-register")

    :ok = Catalog.create_dataset(runtime.catalog, "smoke")
    :ok = Catalog.create_table(runtime.catalog, {"smoke", "a"}, schema)

    assert {:ok, _snapshot} =
             Catalog.register_segments(runtime.catalog, {"smoke", "a"}, [segment])
  end

  test "a locked-down query reads sealed segments back from S3", %{
    store: store,
    tmp_dir: tmp_dir
  } do
    %Store{config: %S3{} = config} = store
    metadata = "sqlite:#{Path.join(tmp_dir, "catalog.sqlite")}"
    data_path = Path.join(tmp_dir, "lake")
    lake = :"s3_query_lake_#{:erlang.unique_integer([:positive])}"

    start_supervised!(
      {DuckLake,
       name: lake,
       metadata: metadata,
       data_path: data_path,
       extensions: [:httpfs],
       statements: [S3.create_secret_statement(config)]}
    )

    catalog = DuckLake.new(engine: lake)

    schema = Schema.new!([{"id", :int64, nullable: false}])
    :ok = Catalog.create_dataset(catalog, "smoke")
    :ok = Catalog.create_table(catalog, {"smoke", "q"}, schema)

    rows = for i <- 1..5, do: %{"id" => i}
    {:ok, segment} = Writer.write(rows, schema, store: store, prefix: "query-lockdown")
    {:ok, _snapshot} = Catalog.register_segments(catalog, {"smoke", "q"}, [segment])

    buffer = :"s3_query_buffer_#{:erlang.unique_integer([:positive])}"

    start_supervised!(
      {BufferService.Supervisor, name: buffer, dir: Path.join(tmp_dir, "buffer")},
      id: buffer
    )

    on_exit(fn -> BufferService.Runtime.delete(buffer) end)

    query = :"s3_query_#{:erlang.unique_integer([:positive])}"

    start_supervised!(
      {QueryService.Supervisor,
       name: query,
       catalog: catalog,
       buffer_base_url: HotServer.base_url(buffer),
       engine_extensions: [:httpfs],
       allowed_directories: [tmp_dir],
       store:
         {S3,
          bucket: config.bucket,
          access_key_id: config.access_key_id,
          secret_access_key: config.secret_access_key,
          endpoint: config.endpoint,
          staging_dir: config.staging_dir},
       job_bootstrap: [
         DuckLake.attach_statement(DuckLake.default_catalog(), metadata, data_path)
       ]},
      id: query
    )

    on_exit(fn -> QueryService.Runtime.delete(query) end)

    assert {:ok, job, frame} =
             QueryService.Client.query(query, "SELECT sum(id)::BIGINT AS s FROM smoke.q")

    assert job.state == :done
    assert Explorer.DataFrame.to_columns(frame)["s"] == [15]
  end

  defp unique_key, do: "segments/#{:erlang.unique_integer([:positive])}.parquet"

  defp get_object(%Store{config: %S3{} = config}, key) do
    Req.get(request(config), url: "s3://#{config.bucket}/#{key}", raw: true)
  end

  defp request(%S3{} = config) do
    Req.new()
    |> ReqS3.attach(
      aws_sigv4: [
        access_key_id: config.access_key_id,
        secret_access_key: config.secret_access_key
      ],
      aws_endpoint_url_s3: config.endpoint
    )
  end

  defp ensure_bucket!(config) do
    request =
      Req.new()
      |> ReqS3.attach(
        aws_sigv4: [
          access_key_id: config.access_key_id,
          secret_access_key: config.secret_access_key
        ],
        aws_endpoint_url_s3: config.endpoint
      )

    case Req.put(request, url: "s3://#{config.bucket}") do
      {:ok, %{status: status}} when status in [200, 409] -> :ok
      other -> raise "failed to ensure bucket #{config.bucket}: #{inspect(other)}"
    end
  end
end
