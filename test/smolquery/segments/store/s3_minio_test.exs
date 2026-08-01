defmodule Smolquery.Segments.Store.S3MinioTest do
  @moduledoc """
  Milestone 8 L3: `Store.S3` against a real S3-compatible service (MinIO in
  CI/kind, per PL-11 D7) — the behavior contract `Store.Local` already has
  tests for, plus the one thing only an object store needs proven: a
  segment `Store.S3.put/3` writes is readable back by DuckDB's `httpfs`
  once `create_secret_statement/1` has run, the same as
  `StorageService.Supervisor`/`QueryService.Runner` bootstrap it.
  """

  use ExUnit.Case, async: false

  alias Smolquery.Engine
  alias Smolquery.Schema
  alias Smolquery.Segments.Store
  alias Smolquery.Segments.Store.S3
  alias Smolquery.Segments.Writer

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
