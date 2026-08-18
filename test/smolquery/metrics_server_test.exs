defmodule Smolquery.MetricsServerTest do
  use ExUnit.Case, async: true

  import Plug.Conn, only: [put_req_header: 3]
  import Plug.Test

  alias Smolquery.InternalSecret
  alias Smolquery.MetricsServer

  defp scrape(conn), do: MetricsServer.call(conn, [])

  defp proven(conn), do: put_req_header(conn, InternalSecret.header(), InternalSecret.value())

  test "answers Prometheus text to the internal secret" do
    response = scrape(proven(conn(:get, "/metrics")))

    assert response.status == 200
    assert response.resp_body =~ "smolquery_"

    assert {"content-type", "text/plain" <> _charset} =
             List.keyfind(response.resp_headers, "content-type", 0)
  end

  test "refuses a scrape without the secret, and one holding a wrong one" do
    assert scrape(conn(:get, "/metrics")).status == 401

    wrong = put_req_header(conn(:get, "/metrics"), InternalSecret.header(), "wrong")
    assert scrape(wrong).status == 401
  end

  test "answers 404 on any other path, and on a non-GET method" do
    assert scrape(proven(conn(:get, "/healthz"))).status == 404
    assert scrape(proven(conn(:post, "/metrics"))).status == 404
  end

  test "listens on every node regardless of roles" do
    assert Smolquery.Roles.enabled() == []

    response =
      Req.get!(MetricsServer.base_url() <> "/metrics",
        headers: [{InternalSecret.header(), InternalSecret.value()}],
        retry: false
      )

    assert response.status == 200
    assert response.body =~ "smolquery_"
  end
end
