defmodule SmolqueryVictoriaMetrics.ErrorsTest do
  use ExUnit.Case, async: true

  import Plug.Conn, only: [get_resp_header: 2]
  import Plug.Test

  alias SmolqueryVictoriaMetrics.Errors

  test "sends the Prometheus API's error body" do
    conn = Errors.send_error(conn(:get, "/"), {400, "bad_data", "no good", nil})

    assert conn.status == 400
    assert [content_type] = get_resp_header(conn, "content-type")
    assert content_type =~ "application/json"
    assert get_resp_header(conn, "retry-after") == []

    assert JSON.decode!(conn.resp_body) == %{
             "status" => "error",
             "errorType" => "bad_data",
             "error" => "no good"
           }
  end

  test "carries retry-after when a retry can succeed" do
    conn = Errors.send_error(conn(:get, "/"), {503, "unavailable", "later", 5})

    assert conn.status == 503
    assert get_resp_header(conn, "retry-after") == ["5"]
  end

  test "a message that quotes bytes that are not UTF-8 is still sent" do
    conn = Errors.send_error(conn(:get, "/"), {400, "bad_data", "bad " <> <<0xFF>>, nil})

    assert %{"error" => "bad �"} = JSON.decode!(conn.resp_body)
  end
end
