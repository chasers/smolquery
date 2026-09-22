defmodule SmolqueryVictoriaMetrics.RouterTest do
  use ExUnit.Case, async: true

  import Plug.Conn, only: [put_req_header: 3, get_resp_header: 2]
  import Plug.Test

  alias SmolqueryApi.Admission
  alias SmolqueryVictoriaMetrics.Router
  alias SmolqueryVictoriaMetrics.Runtime

  @password "router-test-password"

  setup do
    name = :"vm_router_#{:erlang.unique_integer([:positive])}"
    Runtime.put(Runtime.new(name: name, password: @password, max_ndjson_bytes: 1_000))
    on_exit(fn -> Runtime.delete(name) end)

    handler = "vm-router-test-#{name}"
    test = self()

    :telemetry.attach(
      handler,
      [:smolquery, :victoriametrics, :stop],
      fn _event, _measurements, %{conn: conn}, _config ->
        send(test, {:stopped, conn.request_path, conn.private[:smolquery_victoriametrics_kind]})
      end,
      nil
    )

    on_exit(fn -> :telemetry.detach(handler) end)

    %{name: name}
  end

  defp request(conn, name), do: Router.call(conn, Router.init(name))

  defp authed(conn), do: put_req_header(conn, "authorization", "Bearer " <> @password)

  defp write(path), do: conn(:post, path, "") |> put_req_header("content-type", "text/plain")

  test "the health checks answer OK without a password", %{name: name} do
    for path <- ["/health", "/-/healthy", "/-/ready"], method <- [:get, :head] do
      response = request(conn(method, path), name)

      assert response.status == 200, "#{method} #{path}"
      assert_receive {:stopped, ^path, :health}
    end
  end

  test "a missing or wrong password is 401 on every other path", %{name: name} do
    conns = [
      write("/api/v1/write"),
      conn(:get, "/api/v1/query"),
      conn(:get, "/nowhere"),
      write("/api/v1/write") |> put_req_header("authorization", "Bearer wrong")
    ]

    for conn <- conns do
      response = request(conn, name)

      assert response.status == 401

      assert %{"status" => "error", "errorType" => "unauthorized"} =
               JSON.decode!(response.resp_body)
    end
  end

  test "an instance with no published runtime refuses as a wrong password does" do
    response = write("/api/v1/write") |> authed() |> request(:vm_router_never_started)

    assert response.status == 401
  end

  test "every remote-write path reaches the write, kind write", %{name: name} do
    for path <- [
          "/api/v1/write",
          "/prometheus/api/v1/write",
          "/insert/0/prometheus/api/v1/write",
          "/insert/42:7/prometheus/api/v1/write"
        ] do
      response = write(path) |> authed() |> request(name)

      assert response.status == 415, path
      assert_receive {:stopped, ^path, :write}
    end
  end

  test "an unknown path or method with the password is 404, kind other", %{name: name} do
    for conn <- [
          conn(:get, "/api/v1/write"),
          conn(:post, "/api/v2/write", ""),
          conn(:post, "/insert/0/api/v1/write", ""),
          conn(:get, "/api/v1/query")
        ] do
      response = conn |> authed() |> request(name)
      path = conn.request_path

      assert response.status == 404, "#{conn.method} #{path}"
      assert JSON.decode!(response.resp_body)["error"] =~ path
      assert_receive {:stopped, ^path, nil}
    end
  end

  test "a write past the edge's in-flight bytes is 429 before its body is read", %{name: name} do
    start_supervised!({Admission, name: name, limit: 10})

    held =
      write("/api/v1/write")
      |> put_req_header("content-length", "10")
      |> authed()

    {:ok, _admitted} = Admission.admit_body(held, name, 1_000)

    response =
      write("/api/v1/write")
      |> put_req_header("content-length", "10")
      |> authed()
      |> request(name)

    assert response.status == 429
    assert get_resp_header(response, "retry-after") == ["1"]
  end
end
