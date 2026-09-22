defmodule SmolqueryVictoriaMetrics.StatusTest do
  use ExUnit.Case, async: true

  import Plug.Test

  alias SmolqueryVictoriaMetrics.Status

  doctest Status

  @bodies %{
    buildinfo: ~s({"status":"success","data":{"version":"2.24.0"}}),
    metadata: ~s({"status":"success","data":{}}),
    rules: ~s({"status":"success","data":{"groups":[]}}),
    alerts: ~s({"status":"success","data":{"alerts":[]}}),
    notifiers: ~s({"status":"success","data":{"notifiers":[]}}),
    query_exemplars: ~s({"status":"success","data":[]})
  }

  test "route/1 names each path under /api/v1, and nothing else" do
    assert Status.route(["status", "buildinfo"]) == :buildinfo
    assert Status.route(["metadata"]) == :metadata
    assert Status.route(["rules"]) == :rules
    assert Status.route(["alerts"]) == :alerts
    assert Status.route(["notifiers"]) == :notifiers
    assert Status.route(["query_exemplars"]) == :query_exemplars
    assert Status.route(["status", "tsdb"]) == nil
    assert Status.route(["status"]) == nil
  end

  test "body/1 is VictoriaMetrics' answer, byte for byte" do
    for {route, body} <- @bodies do
      assert IO.iodata_to_binary(Status.body(route)) == body, inspect(route)
    end
  end

  test "call/2 answers 200 with JSON" do
    for {route, body} <- @bodies do
      response = Status.call(conn(:get, "/"), route)

      assert response.status == 200
      assert response.resp_body == body

      assert Plug.Conn.get_resp_header(response, "content-type") == [
               "application/json; charset=utf-8"
             ]
    end
  end
end
