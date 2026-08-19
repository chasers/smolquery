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

  defmodule FirstWriteWinsStub do
    @moduledoc false

    @behaviour Plug

    import Plug.Conn

    @impl Plug
    def init(opts), do: opts

    @impl Plug
    def call(%{method: "PUT"} = conn, table) do
      {:ok, _body, conn} = read_body(conn)
      key = conn.request_path

      cond do
        get_req_header(conn, "if-none-match") != ["*"] ->
          send_resp(conn, 400, "missing If-None-Match")

        :ets.insert_new(table, {key}) ->
          send_resp(conn, 200, "")

        true ->
          send_resp(conn, 412, "")
      end
    end
  end

  @moduletag :tmp_dir

  setup context do
    table = :ets.new(:s3_conditional_put_stub, [:public, :set])

    server =
      start_supervised!({Bandit, plug: {FirstWriteWinsStub, table}, port: 0, startup_log: false})

    {:ok, {_address, port}} = ThousandIsland.listener_info(server)

    store =
      S3.new(
        bucket: "sealed",
        access_key_id: "test",
        secret_access_key: "test-secret",
        staging_dir: Path.join(context.tmp_dir, "staging"),
        endpoint: "http://127.0.0.1:#{port}"
      )

    %{store: store}
  end

  test "the first put/3 for a key succeeds", %{store: store} do
    assert {:ok, %{location: "s3://sealed/table/one.parquet"}} =
             Store.put(store, "table/one.parquet", &File.write!(&1, "hello"))
  end

  test "a retry to an already-committed key gets 412 and is treated as success", %{
    store: store
  } do
    assert {:ok, _put} = Store.put(store, "table/one.parquet", &File.write!(&1, "hello"))

    assert {:ok, %{location: "s3://sealed/table/one.parquet"}} =
             Store.put(store, "table/one.parquet", &File.write!(&1, "truncated-retry"))
  end
end
