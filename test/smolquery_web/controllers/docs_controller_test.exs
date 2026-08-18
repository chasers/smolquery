defmodule SmolqueryWeb.DocsControllerTest do
  use SmolqueryWeb.ConnCase, async: false

  setup do
    {:ok, runtime: start_web!()}
  end

  test "requires the UI credential" do
    assert get(Phoenix.ConnTest.build_conn(), ~p"/v1/docs.json").status == 401
  end

  test "serves the API spec as JSON", %{conn: conn} do
    conn = get(conn, ~p"/v1/docs.json")

    assert conn.status == 200

    body = JSON.decode!(conn.resp_body)
    assert body["name"] == "smolquery HTTP API"
    assert is_list(body["routes"])
  end
end
