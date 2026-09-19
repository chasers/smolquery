defmodule SmolqueryClickHouse.ErrorsTest do
  use ExUnit.Case, async: true

  import Plug.Conn, only: [get_resp_header: 2]
  import Plug.Test

  alias SmolqueryClickHouse.Errors

  test "answers ClickHouse's text form with the exception code" do
    response =
      Errors.send_exception(
        conn(:post, "/"),
        {400, 62, "SYNTAX_ERROR", "expected INSERT", nil}
      )

    assert response.status == 400
    assert response.resp_body == "Code: 62. DB::Exception: expected INSERT. (SYNTAX_ERROR)\n"
    assert get_resp_header(response, "x-clickhouse-exception-code") == ["62"]
    assert get_resp_header(response, "retry-after") == []
  end

  test "a retryable failure carries retry-after" do
    response =
      Errors.send_exception(
        conn(:post, "/"),
        {429, 202, "TOO_MANY_SIMULTANEOUS_QUERIES", "retry later", 5}
      )

    assert get_resp_header(response, "retry-after") == ["5"]
  end
end
