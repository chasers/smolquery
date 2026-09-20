defmodule SmolqueryClickHouse.UnansweredTest do
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog
  import Plug.Conn, only: [put_req_header: 3]
  import Plug.Test

  alias SmolqueryClickHouse.Runtime
  alias SmolqueryClickHouse.Unanswered

  @syntax_error {400, 62, "SYNTAX_ERROR", "syntax error at or near \"ARRAY\"\nLINE 1", nil}

  defp runtime(mode),
    do: Runtime.new(name: :"unanswered_#{mode}", password: "p", unanswered_log: mode)

  defp hyperdx(conn), do: put_req_header(conn, "user-agent", "hyperdx 2.1.0")

  defp counted(code, fun) do
    handler = "unanswered-#{:erlang.unique_integer([:positive])}"
    parent = self()

    :telemetry.attach(
      handler,
      [:smolquery, :clickhouse, :unanswered],
      fn _event, _measurements, meta, _config -> send(parent, {:unanswered, meta.code}) end,
      nil
    )

    try do
      fun.()
    after
      :telemetry.detach(handler)
    end

    assert_received {:unanswered, ^code}
  end

  describe "record/4" do
    test "logs the statement redacted by default, on one line, with the client and the error" do
      sql =
        "SELECT Body FROM t\nARRAY JOIN tags AS tag WHERE tag = 'secret term' AND b = 'it\\'s'"

      log =
        capture_log(fn ->
          counted(62, fn ->
            Unanswered.record(hyperdx(conn(:post, "/")), runtime(:redacted), sql, @syntax_error)
          end)
        end)

      assert log =~ "clickhouse edge could not answer: code=62 name=SYNTAX_ERROR"
      assert log =~ ~s|user_agent="hyperdx 2.1.0"|
      assert log =~ "SELECT Body FROM t ARRAY JOIN tags AS tag WHERE tag = '?' AND b = '?'"
      assert log =~ "syntax error at or near"
      refute log =~ "LINE 1"
      refute log =~ "secret term"
    end

    test "verbatim keeps the literals" do
      log =
        capture_log(fn ->
          Unanswered.record(
            conn(:post, "/"),
            runtime(:verbatim),
            "SELECT JSONExtract(x, 'Tuple(String)')",
            {404, 46, "UNKNOWN_FUNCTION", "no such function", nil}
          )
        end)

      assert log =~ "JSONExtract(x, 'Tuple(String)')"
      assert log =~ ~s|user_agent=""|
    end

    test "off logs nothing and still counts" do
      log =
        capture_log(fn ->
          counted(62, fn ->
            Unanswered.record(conn(:post, "/"), runtime(:off), "SELECT", @syntax_error)
          end)
        end)

      refute log =~ "could not answer"
    end

    test "a refusal that says nothing about the dialect is neither logged nor counted" do
      for exception <- [
            {500, 159, "TIMEOUT_EXCEEDED", "too slow", nil},
            {429, 202, "TOO_MANY_SIMULTANEOUS_QUERIES", "busy", 1},
            {400, 396, "TOO_MANY_ROWS_OR_BYTES", "too many", nil}
          ] do
        log =
          capture_log(fn ->
            Unanswered.record(conn(:post, "/"), runtime(:redacted), "SELECT 1", exception)
          end)

        refute log =~ "could not answer"
        refute_received {:unanswered, _code}
      end
    end
  end

  describe "line/2" do
    test "cuts a long statement" do
      line = Unanswered.line("SELECT " <> String.duplicate("x, ", 5_000) <> "1", :verbatim)

      assert byte_size(line) < 8_300
      assert String.ends_with?(line, " …")
    end

    test "leaves quoted names and comments, which are not a user's values" do
      assert Unanswered.line(~s|SELECT "a b", `c` -- note\nFROM t WHERE x = 'v'|, :redacted) ==
               ~s|SELECT "a b", `c` -- note FROM t WHERE x = '?'|
    end
  end
end
