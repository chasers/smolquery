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

  describe "engine_failure/1" do
    alias SmolqueryClickHouse.Errors

    test "reads the engine's message into ClickHouse's code" do
      assert {400, 62, "SYNTAX_ERROR", _, nil} =
               Errors.engine_failure("Parser Error: syntax error at or near \"x\"")

      assert {404, 60, "UNKNOWN_TABLE", _, nil} =
               Errors.engine_failure("Catalog Error: Table with name t does not exist!")

      assert {404, 46, "UNKNOWN_FUNCTION", _, nil} =
               Errors.engine_failure("Catalog Error: Scalar Function with name f does not exist!")

      assert {400, 47, "UNKNOWN_IDENTIFIER", _, nil} =
               Errors.engine_failure("Binder Error: Referenced column \"c\" not found")

      assert {400, 1002, "UNKNOWN_EXCEPTION", _, nil} =
               Errors.engine_failure("Conversion Error: Could not convert")
    end

    test "a failure that is the server's is a 500 a client may retry" do
      for message <- [
            "IO Error: disk",
            "HTTP Error: 503",
            "Connection Error: refused",
            "Out of Memory Error: x"
          ] do
        assert {500, 1002, "UNKNOWN_EXCEPTION", ^message, nil} = Errors.engine_failure(message)
      end
    end
  end
end
