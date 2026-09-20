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

  describe "redaction holds for the error too (review of T-480)" do
    defp redacted_log(sql, exception) do
      capture_log(fn ->
        Unanswered.record(conn(:post, "/"), runtime(:redacted), sql, exception)
      end)
    end

    test "a parameter's value in a BAD_ARGUMENTS message is not logged" do
      log =
        redacted_log(
          "SELECT {id:UInt64}",
          {400, 36, "BAD_ARGUMENTS",
           ~s|Value "alice@example.com" cannot be parsed as UInt64 for query parameter id|, nil}
        )

      assert log =~ "code=36"
      assert log =~ "cannot be parsed as UInt64 for query parameter id"
      refute log =~ "alice"
    end

    test "a literal the parser quotes back is not logged, and a keyword still is" do
      literal =
        redacted_log(
          "SELECT 'secret2' 'x'",
          {400, 62, "SYNTAX_ERROR", ~s|syntax error at or near "'secret2'"|, nil}
        )

      refute literal =~ "secret2"

      keyword =
        redacted_log(
          "SELECT 1 ARRAY JOIN x",
          {400, 62, "SYNTAX_ERROR", ~s|syntax error at or near "ARRAY"|, nil}
        )

      assert keyword =~ "ARRAY"
      assert keyword =~ "syntax error at or near"
    end

    test "a dollar-quoted string and a comment are redacted with the literals" do
      assert Unanswered.line(
               "SELECT $$top secret$$, $t$more$t$ FROM t -- for bob\nWHERE a = 'v' /* and carol */",
               :redacted
             ) ==
               "SELECT $$?$$, $$?$$ FROM t /* ? */ WHERE a = '?' /* ? */"
    end
  end

  describe "line/2" do
    test "cuts a long statement" do
      line = Unanswered.line("SELECT " <> String.duplicate("x, ", 5_000) <> "1", :verbatim)

      assert byte_size(line) < 8_300
      assert String.ends_with?(line, " …")
    end

    test "leaves quoted names, which are not a user's values" do
      assert Unanswered.line(~s|SELECT "a b", `c`\nFROM t WHERE x = 'v'|, :redacted) ==
               ~s|SELECT "a b", `c` FROM t WHERE x = '?'|
    end
  end
end
