defmodule SmolqueryApi.ClickHouseControllerTest do
  use ExUnit.Case, async: true

  import Bitwise
  import Plug.Conn, only: [put_req_header: 3, get_resp_header: 2]
  import Plug.Test

  alias Smolquery.BufferService
  alias Smolquery.Catalog
  alias Smolquery.IngestService
  alias Smolquery.Schema
  alias Smolquery.Test.ApiEndpoint
  alias Smolquery.Test.MapCatalog
  alias SmolqueryApi.Runtime

  @moduletag :tmp_dir

  @key "clickhouse-test-key"
  @table {"logs", "events"}

  setup context do
    buffer = :"ch_buffer_#{:erlang.unique_integer([:positive])}"

    start_supervised!(
      {BufferService.Supervisor,
       name: buffer, dir: Path.join(context.tmp_dir, "buffer"), flush_max_rows: 1},
      id: buffer
    )

    on_exit(fn -> BufferService.Runtime.delete(buffer) end)

    catalog = MapCatalog.new()
    :ok = Catalog.create_dataset(catalog, "logs")

    :ok =
      Catalog.create_table(
        catalog,
        @table,
        Schema.new!([{"id", :int64, nullable: false}, {"msg", :string}, {"ts", :timestamp_ns}])
      )

    ingest = :"ch_ingest_#{:erlang.unique_integer([:positive])}"

    start_supervised!(
      {IngestService.Supervisor, name: ingest, catalog: catalog, buffer_name: buffer},
      id: ingest
    )

    on_exit(fn -> IngestService.Runtime.delete(ingest) end)

    name = :"ch_api_#{:erlang.unique_integer([:positive])}"
    Runtime.put(Runtime.new(name: name, api_key: @key, catalog: catalog, ingest_name: ingest))
    on_exit(fn -> Runtime.delete(name) end)

    %{name: name, buffer: buffer}
  end

  defp leb(n) when n < 128, do: <<n>>
  defp leb(n), do: <<1::1, n &&& 127::7, leb(n >>> 7)::binary>>
  defp str(bytes), do: [leb(byte_size(bytes)), bytes]

  defp typed(rows) do
    IO.iodata_to_binary([
      leb(3),
      Enum.map(["id", "msg", "ts"], &str/1),
      Enum.map(["Nullable(Int64)", "String", "DateTime64(9, 'UTC')"], &str/1),
      Enum.map(rows, fn {id, msg, ns} ->
        [
          if(id, do: [0, <<id::little-signed-64>>], else: <<1>>),
          str(msg),
          <<ns::little-signed-64>>
        ]
      end)
    ])
  end

  defp post(name, params, body, headers \\ []) do
    conn(:post, "/?" <> URI.encode_query(params), body)
    |> put_req_header("authorization", "Bearer #{@key}")
    |> put_req_header("content-type", "application/octet-stream")
    |> then(fn conn ->
      Enum.reduce(headers, conn, fn {k, v}, c -> put_req_header(c, k, v) end)
    end)
    |> then(&ApiEndpoint.request(name, &1))
  end

  defp landed(buffer) do
    {:ok, entries} = BufferService.Client.hot_manifest(buffer, @table)
    Enum.sum(Enum.map(entries, & &1.row_count))
  end

  test "a typed insert lands every row and answers with a summary", %{name: name, buffer: buffer} do
    body = typed([{1, "a", 1_789_380_000_123_456_789}, {2, "b", 0}])

    response =
      post(name, %{"query" => "INSERT INTO logs.events FORMAT RowBinaryWithNamesAndTypes"}, body)

    assert response.status == 200
    assert response.resp_body == ""
    assert [summary] = get_resp_header(response, "x-clickhouse-summary")
    assert %{"written_rows" => "2"} = JSON.decode!(summary)
    assert landed(buffer) == 2
  end

  test "a plain body binds the statement's column list, and the database comes from the parameter",
       %{name: name, buffer: buffer} do
    body = IO.iodata_to_binary([0, str("hi"), <<7::little-signed-64>>])

    response =
      post(
        name,
        %{"query" => "INSERT INTO events (msg, id) FORMAT RowBinary", "database" => "logs"},
        body
      )

    assert response.status == 200
    assert landed(buffer) == 1
  end

  test "an unqualified table takes X-ClickHouse-Database", %{name: name, buffer: buffer} do
    body = typed([{1, "a", 0}])

    response =
      post(name, %{"query" => "INSERT INTO events FORMAT RowBinaryWithNamesAndTypes"}, body, [
        {"x-clickhouse-database", "logs"}
      ])

    assert response.status == 200
    assert landed(buffer) == 1
  end

  test "a refused row writes nothing, the way ClickHouse refuses an insert", %{
    name: name,
    buffer: buffer
  } do
    body = typed([{1, "a", 0}, {nil, "b", 0}])

    response =
      post(name, %{"query" => "INSERT INTO logs.events FORMAT RowBinaryWithNamesAndTypes"}, body)

    assert response.status == 400
    assert get_resp_header(response, "x-clickhouse-exception-code") == ["117"]
    assert response.resp_body =~ "Code: 117. DB::Exception: 1 row(s) refused, nothing was written"
    assert response.resp_body =~ "row 1: column id must not be null"
    assert landed(buffer) == 0
  end

  test "input_format_allow_errors_num writes the rows that are not refused", %{
    name: name,
    buffer: buffer
  } do
    body = typed([{1, "a", 0}, {nil, "b", 0}])

    response =
      post(
        name,
        %{
          "query" => "INSERT INTO logs.events FORMAT RowBinaryWithNamesAndTypes",
          "input_format_allow_errors_num" => "10"
        },
        body
      )

    assert response.status == 200

    assert %{"written_rows" => "1"} =
             JSON.decode!(hd(get_resp_header(response, "x-clickhouse-summary")))

    assert landed(buffer) == 1
  end

  test "failures answer ClickHouse's text form and exception code", %{name: name} do
    cases = [
      {%{"query" => "INSERT INTO logs.nope FORMAT RowBinary"}, 404, "60"},
      {%{"query" => "INSERT INTO logs.events FORMAT"}, 400, "62"},
      {%{"query" => "INSERT INTO logs.events FORMAT Native"}, 404, "73"},
      {%{"query" => "SELECT 1"}, 501, "48"},
      {%{}, 400, "62"}
    ]

    for {params, status, code} <- cases do
      response = post(name, params, typed([{1, "a", 0}]))

      assert response.status == status, inspect(params)
      assert get_resp_header(response, "x-clickhouse-exception-code") == [code]
      assert response.resp_body =~ "Code: #{code}. DB::Exception: "
    end
  end

  test "a request without the bearer key is refused before the body is read", %{name: name} do
    response =
      conn(:post, "/?query=INSERT%20INTO%20logs.events%20FORMAT%20RowBinary", "")
      |> then(&ApiEndpoint.request(name, &1))

    assert response.status == 401
  end

  test "a body that ends mid-row is CANNOT_READ_ALL_DATA and writes nothing", %{
    name: name,
    buffer: buffer
  } do
    body = typed([{1, "a", 0}])
    short = binary_part(body, 0, byte_size(body) - 1)

    response =
      post(name, %{"query" => "INSERT INTO logs.events FORMAT RowBinaryWithNamesAndTypes"}, short)

    assert response.status == 400
    assert get_resp_header(response, "x-clickhouse-exception-code") == ["33"]
    assert landed(buffer) == 0
  end
end
