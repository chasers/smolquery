defmodule SmolqueryApi.AdmissionTest do
  use ExUnit.Case, async: true

  import Plug.Conn, only: [put_req_header: 3]
  import Plug.Test

  alias Smolquery.BufferService
  alias Smolquery.Catalog
  alias Smolquery.IngestService
  alias Smolquery.Schema
  alias Smolquery.Test.ApiEndpoint
  alias Smolquery.Test.MapCatalog
  alias SmolqueryApi.Admission
  alias SmolqueryApi.Runtime

  @moduletag :tmp_dir

  @key "admission-test-key"
  @path "/v1/datasets/analytics/tables/events/insert"

  setup context do
    buffer = :"admission_buffer_#{:erlang.unique_integer([:positive])}"

    start_supervised!(
      {BufferService.Supervisor,
       name: buffer, dir: Path.join(context.tmp_dir, "buffer"), flush_max_rows: 1},
      id: buffer
    )

    on_exit(fn -> BufferService.Runtime.delete(buffer) end)

    catalog = MapCatalog.new()
    :ok = Catalog.create_dataset(catalog, "analytics")

    :ok =
      Catalog.create_table(
        catalog,
        {"analytics", "events"},
        Schema.new!([{"id", :int64, nullable: false}])
      )

    ingest = :"admission_ingest_#{:erlang.unique_integer([:positive])}"

    start_supervised!(
      {IngestService.Supervisor, name: ingest, catalog: catalog, buffer_name: buffer},
      id: ingest
    )

    on_exit(fn -> IngestService.Runtime.delete(ingest) end)

    name = :"api_admission_#{:erlang.unique_integer([:positive])}"

    runtime =
      Runtime.new(
        name: name,
        api_key: @key,
        catalog: catalog,
        ingest_name: ingest,
        insert_max_in_flight_bytes: 100
      )

    Runtime.put(runtime)
    on_exit(fn -> Runtime.delete(name) end)

    start_supervised!({Admission, runtime}, id: {:admission, name})

    %{name: name}
  end

  defp post_ndjson(name, body) do
    conn(:post, @path, body)
    |> put_req_header("content-type", "application/x-ndjson")
    |> put_req_header("content-length", Integer.to_string(byte_size(body)))
    |> put_req_header("authorization", "Bearer #{@key}")
    |> then(&ApiEndpoint.request(name, &1))
  end

  defp hold(name, bytes) do
    server = Admission.server(name)
    {:ok, reservation} = GenServer.call(server, {:admit, bytes, self()})
    {server, reservation}
  end

  test "an admitted insert lands and releases its reservation", %{name: name} do
    response = post_ndjson(name, ~s({"id": 1}\n))

    assert response.status == 200
    assert Admission.in_flight(name) == 0
  end

  test "a full counter refuses before the body, with the 429 contract", %{name: name} do
    hold(name, 95)

    response = post_ndjson(name, ~s({"id": 1}\n))

    assert response.status == 429
    assert Plug.Conn.get_resp_header(response, "retry-after") == ["1"]
    assert %{"error" => %{"status" => "RESOURCE_EXHAUSTED"}} = JSON.decode!(response.resp_body)
  end

  test "a released reservation readmits the next insert", %{name: name} do
    {server, reservation} = hold(name, 95)
    assert post_ndjson(name, ~s({"id": 1}\n)).status == 429

    Admission.release(server, reservation)

    assert post_ndjson(name, ~s({"id": 1}\n)).status == 200
  end

  test "an idle counter admits a body over the limit", %{name: name} do
    body = ~s({"id": 1, "pad": "#{String.duplicate("x", 200)}"}\n)
    assert byte_size(body) > 100

    assert post_ndjson(name, body).status == 200
    assert Admission.in_flight(name) == 0
  end

  test "a crashed holder frees its bytes", %{name: name} do
    server = Admission.server(name)

    holder =
      spawn(fn ->
        GenServer.call(server, {:admit, 95, self()})

        receive do
          :never -> :ok
        end
      end)

    await(fn -> Admission.in_flight(name) == 95 end)
    Process.exit(holder, :kill)
    await(fn -> Admission.in_flight(name) == 0 end)
  end

  test "routes without ingest bodies pass a full counter", %{name: name} do
    hold(name, 100)

    response =
      conn(:get, "/v1/datasets/analytics/tables")
      |> put_req_header("authorization", "Bearer #{@key}")
      |> then(&ApiEndpoint.request(name, &1))

    assert response.status == 200
  end

  test "an instance without an admission server passes uncounted", %{name: name} do
    stop_supervised!({:admission, name})

    assert post_ndjson(name, ~s({"id": 1}\n)).status == 200
  end

  test "a server that dies between the lookup and the call passes uncounted", %{name: name} do
    stop_supervised!({:admission, name})

    stub = spawn(fn -> receive do: (_call -> :ok) end)
    Process.register(stub, Admission.server(name))

    assert post_ndjson(name, ~s({"id": 1}\n)).status == 200
  end

  test "a stray message does not crash the server", %{name: name} do
    server = Process.whereis(Admission.server(name))
    send(server, :stray)

    assert Admission.in_flight(name) == 0
    assert Process.alive?(server)
  end

  defp await(check, attempts \\ 50) do
    cond do
      check.() -> :ok
      attempts == 0 -> flunk("condition never held")
      true -> Process.sleep(10) && await(check, attempts - 1)
    end
  end
end
