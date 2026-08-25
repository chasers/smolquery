defmodule Smolquery.Segments.Store.S3TelemetryTest do
  @moduledoc """
  T-379: every HTTP request `Store.S3` makes emits one
  `[:smolquery, :s3, :request]` event, with the op, the status, and the
  bytes a caller would want to price the object store with. A stub bucket
  answers the four request kinds so each path is driven for real through
  `Req` and `ReqS3`.
  """

  use ExUnit.Case, async: false

  alias Smolquery.Segments.Store
  alias Smolquery.Segments.Store.S3
  alias Smolquery.Test.FakeParquet

  defmodule BucketStub do
    @moduledoc false

    @behaviour Plug

    import Plug.Conn

    @impl Plug
    def init(opts), do: opts

    @impl Plug
    def call(%{method: "PUT"} = conn, table) do
      {:ok, body, conn} = read_body(conn)

      if :ets.insert_new(table, {conn.request_path, body}),
        do: send_resp(conn, 200, ""),
        else: send_resp(conn, 412, "")
    end

    def call(%{method: "HEAD"} = conn, table) do
      case :ets.lookup(table, conn.request_path) do
        [{_key, body}] -> send_resp(conn, 200, body)
        [] -> send_resp(conn, 404, "")
      end
    end

    def call(%{method: "GET"} = conn, table) do
      contents =
        Enum.map_join(:ets.tab2list(table), fn {path, _body} ->
          "<Contents><Key>#{String.trim_leading(path, "/sealed/")}</Key></Contents>"
        end)

      conn
      |> put_resp_content_type("application/xml")
      |> send_resp(
        200,
        "<ListBucketResult><IsTruncated>false</IsTruncated>#{contents}</ListBucketResult>"
      )
    end

    def call(%{method: "DELETE"} = conn, table) do
      :ets.delete(table, conn.request_path)
      send_resp(conn, 204, "")
    end
  end

  @moduletag :tmp_dir

  setup context do
    table = :ets.new(:s3_telemetry_stub, [:public, :set])

    server =
      start_supervised!({Bandit, plug: {BucketStub, table}, port: 0, startup_log: false})

    {:ok, {_address, port}} = ThousandIsland.listener_info(server)

    store =
      S3.new(
        bucket: "sealed",
        access_key_id: "test",
        secret_access_key: "test-secret",
        staging_dir: Path.join(context.tmp_dir, "staging"),
        endpoint: "http://127.0.0.1:#{port}"
      )

    handler = "s3-telemetry-#{:erlang.unique_integer([:positive])}"
    test = self()

    :telemetry.attach(
      handler,
      [:smolquery, :s3, :request],
      fn _event, measurements, meta, _config -> send(test, {:s3, measurements, meta}) end,
      nil
    )

    on_exit(fn -> :telemetry.detach(handler) end)

    %{store: store}
  end

  test "a put emits one event carrying the object size", %{store: store} do
    bytes = FakeParquet.bytes("hello")

    assert {:ok, _put} = Store.put(store, "table/one.parquet", &File.write!(&1, bytes))

    assert_receive {:s3, %{duration_us: duration, bytes: size},
                    %{op: :put, status: 200, result: :ok}}

    assert size == byte_size(bytes)
    assert is_integer(duration) and duration >= 0
    refute_receive {:s3, _measurements, _meta}
  end

  test "a retried put emits the 412 and then the head", %{store: store} do
    bytes = FakeParquet.bytes("hello")

    assert {:ok, _first} = Store.put(store, "table/one.parquet", &File.write!(&1, bytes))
    assert_receive {:s3, _measurements, %{op: :put, status: 200}}

    assert {:ok, _retry} = Store.put(store, "table/one.parquet", &File.write!(&1, bytes))
    assert_receive {:s3, %{bytes: size}, %{op: :put, status: 412, result: :ok}}
    assert size == byte_size(bytes)
    assert_receive {:s3, %{bytes: 0}, %{op: :head, status: 200, result: :ok}}
  end

  test "a list emits one event per page and a delete one event", %{store: store} do
    assert {:ok, _put} =
             Store.put(store, "table/one.parquet", &File.write!(&1, FakeParquet.bytes("a")))

    assert_receive {:s3, _measurements, %{op: :put}}

    assert {:ok, ["table/one.parquet"]} = Store.list(store, "table")
    assert_receive {:s3, %{bytes: 0}, %{op: :list, status: 200, result: :ok}}

    assert :ok = Store.delete(store, "table/one.parquet")
    assert_receive {:s3, %{bytes: 0}, %{op: :delete, status: 204, result: :ok}}
  end

  test "a request that gets no response is an error with no status", context do
    store =
      S3.new(
        bucket: "sealed",
        access_key_id: "test",
        secret_access_key: "test-secret",
        staging_dir: Path.join(context.tmp_dir, "staging"),
        endpoint: "http://127.0.0.1:1"
      )

    assert {:error, _reason} = Store.delete(store, "table/one.parquet")
    assert_receive {:s3, %{bytes: 0}, %{op: :delete, status: nil, result: :error}}, 5_000
  end
end
