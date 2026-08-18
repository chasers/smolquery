defmodule Smolquery.Segments.Store.S3ConditionalPutTest do
  @moduledoc """
  T-308: `put/3` sends `If-None-Match: *` on every sealed-segment PUT, so a
  crash-recovery retry to a key S3 already holds gets a `412 Precondition
  Failed` instead of silently overwriting it. A stub server plays "first PUT
  to a key wins, every later PUT to that key gets 412" the way a bucket with
  conditional writes does.
  """

  use ExUnit.Case, async: false

  alias Smolquery.Segments.Store
  alias Smolquery.Segments.Store.S3
  alias Smolquery.Test.FakeParquet

  defmodule FirstWriteWinsStub do
    @moduledoc false

    @behaviour Plug

    import Plug.Conn

    @impl Plug
    def init(opts), do: opts

    @impl Plug
    def call(%{method: "PUT"} = conn, table) do
      {:ok, body, conn} = read_body(conn)
      key = conn.request_path

      cond do
        get_req_header(conn, "if-none-match") != ["*"] ->
          send_resp(conn, 400, "missing If-None-Match")

        :ets.insert_new(table, {key, body}) ->
          send_resp(conn, 200, "")

        true ->
          send_resp(conn, 412, "")
      end
    end

    def call(%{method: "HEAD"} = conn, table) do
      case :ets.lookup(table, conn.request_path) do
        [{_key, body}] -> send_resp(conn, 200, body)
        [] -> send_resp(conn, 404, "")
      end
    end
  end

  defmodule LostRaceStub do
    @moduledoc false

    @behaviour Plug

    import Plug.Conn

    @impl Plug
    def init(opts), do: opts

    @impl Plug
    def call(%{method: "PUT"} = conn, table) do
      {:ok, _body, conn} = read_body(conn)

      if :ets.insert_new(table, {conn.request_path, "the-winner's-committed-bytes"}) do
        send_resp(conn, 409, "ConditionalRequestConflict")
      else
        send_resp(conn, 412, "")
      end
    end

    def call(%{method: "HEAD"} = conn, table) do
      [{_key, body}] = :ets.lookup(table, conn.request_path)
      send_resp(conn, 200, body)
    end
  end

  @moduletag :tmp_dir

  setup context do
    {store, table} = stub_store(context, FirstWriteWinsStub)
    %{store: store, table: table}
  end

  defp stub_store(context, plug_module) do
    table = :ets.new(:s3_conditional_put_stub, [:public, :set])

    server =
      start_supervised!(
        {Bandit, plug: {plug_module, table}, port: 0, startup_log: false},
        id: plug_module
      )

    {:ok, {_address, port}} = ThousandIsland.listener_info(server)

    store =
      S3.new(
        bucket: "sealed",
        access_key_id: "test",
        secret_access_key: "test-secret",
        staging_dir: Path.join(context.tmp_dir, "staging"),
        endpoint: "http://127.0.0.1:#{port}"
      )

    {store, table}
  end

  test "the first put/3 for a key succeeds", %{store: store} do
    assert {:ok, %{location: "s3://sealed/table/one.parquet"}} =
             Store.put(store, "table/one.parquet", &File.write!(&1, FakeParquet.bytes("hello")))
  end

  test "a retry to an already-committed key gets 412 and is treated as success", %{
    store: store
  } do
    assert {:ok, first} =
             Store.put(store, "table/one.parquet", &File.write!(&1, FakeParquet.bytes("hello")))

    assert {:ok, %{location: "s3://sealed/table/one.parquet"} = retry} =
             Store.put(
               store,
               "table/one.parquet",
               &File.write!(&1, FakeParquet.bytes("truncated-retry"))
             )

    assert retry.byte_size == first.byte_size
  end

  test "a truncated encode never sends a PUT to the bucket", %{store: store, table: table} do
    assert Store.put(store, "table/one.parquet", &File.write(&1, "truncated bytes")) ==
             {:error, {:put_failed, "table/one.parquet", :truncated_parquet}}

    assert :ets.tab2list(table) == []
  end

  test "a put that loses a conditional-write race retries into the 412 no-op", context do
    {store, _table} = stub_store(context, LostRaceStub)

    assert {:ok, put} =
             Store.put(store, "table/one.parquet", &File.write!(&1, FakeParquet.bytes("loser")))

    assert put.location == "s3://sealed/table/one.parquet"
    assert put.byte_size == byte_size("the-winner's-committed-bytes")
  end
end
