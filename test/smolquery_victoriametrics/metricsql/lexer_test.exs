defmodule SmolqueryVictoriaMetrics.MetricsQL.LexerTest do
  use ExUnit.Case, async: true

  alias SmolqueryVictoriaMetrics.MetricsQL.Lexer

  defp kinds(query) do
    {:ok, tokens} = Lexer.tokenize(query)
    Enum.map(tokens, fn {kind, text, _position} -> {kind, text} end)
  end

  describe "tokenize/1" do
    test "splits a selector with a subquery and an offset" do
      assert kinds(~S|m{a="b"}[5m:1m] offset -1h|) == [
               ident: "m",
               punct: "{",
               ident: "a",
               filter_op: "=",
               string: ~S|"b"|,
               punct: "}",
               punct: "[",
               duration: "5m",
               ident: ":1m",
               punct: "]",
               ident: "offset",
               op: "-",
               duration: "1h",
               eof: ""
             ]
    end

    test "tells durations from numbers by VictoriaMetrics' rules" do
      assert kinds("5m 5M 5ms 1Ms 5mb 45mi 1h-5m 3.5d 2i 12Ki 1.5e3 .5 0x1F 017 0b101 1_000") == [
               duration: "5m",
               number: "5M",
               duration: "5ms",
               duration: "1Ms",
               number: "5mb",
               number: "45mi",
               duration: "1h-5m",
               duration: "3.5d",
               duration: "2i",
               number: "12Ki",
               number: "1.5e3",
               number: ".5",
               number: "0x1F",
               number: "017",
               number: "0b101",
               number: "1_000",
               eof: ""
             ]
    end

    test "reads operators, longest first, and label filter operators" do
      assert kinds("a>=b!=c=~d!~e=f<=g==h") == [
               ident: "a",
               op: ">=",
               ident: "b",
               op: "!=",
               ident: "c",
               filter_op: "=~",
               ident: "d",
               filter_op: "!~",
               ident: "e",
               filter_op: "=",
               ident: "f",
               op: "<=",
               ident: "g",
               op: "==",
               ident: "h",
               eof: ""
             ]
    end

    test "keeps identifiers with escapes, unicode, dots and colons whole" do
      assert kinds(~S|foo\ bar 温度 a.b:c \x2Ef|) == [
               ident: ~S|foo\ bar|,
               ident: "温度",
               ident: "a.b:c",
               ident: ~S|\x2Ef|,
               eof: ""
             ]
    end

    test "reads the three quotes, with escaped quotes inside" do
      assert kinds(~S|"a\"b" 'c\'d' `e"f`|) == [
               string: ~S|"a\"b"|,
               string: ~S|'c\'d'|,
               string: "`e\"f`",
               eof: ""
             ]
    end

    test "maps Grafana's interval variables to $__interval" do
      assert kinds("$__interval $__rate_interval") == [
               duration: "$__interval",
               duration: "$__interval",
               eof: ""
             ]
    end

    test "skips comments and counts lines and columns" do
      assert {:ok, tokens} = Lexer.tokenize("# head\n  sum # tail\n(x) # end")

      assert tokens == [
               {:ident, "sum", {2, 3}},
               {:punct, "(", {3, 1}},
               {:ident, "x", {3, 2}},
               {:punct, ")", {3, 3}},
               {:eof, "", {3, 10}}
             ]
    end

    test "names what it cannot read and where" do
      assert Lexer.tokenize("m + $x") == {:error, {:syntax, ~s|cannot recognize "$x" at 1:5|}}

      assert Lexer.tokenize(~S|"abc|) ==
               {:error, {:syntax, ~s|cannot find the closing quote " of the string at 1:1|}}

      assert Lexer.tokenize("1.2e") ==
               {:error, {:syntax, ~s|missing exponent part in "1.2e" at 1:1|}}

      assert Lexer.tokenize(<<"m", 0xFF>>) == {:error, {:syntax, "the query is not valid UTF-8"}}
    end
  end

  test "ident_prefix?/1 accepts letters, _ and : and valid escapes" do
    assert Lexer.ident_prefix?("abc")
    assert Lexer.ident_prefix?(":x")
    assert Lexer.ident_prefix?(~S|\3foo|)
    refute Lexer.ident_prefix?("3foo")
    refute Lexer.ident_prefix?(~S|\xZZ|)
  end

  test "duration?/1 is true only for a whole duration" do
    assert Lexer.duration?("1h30m")
    assert Lexer.duration?("$__interval")
    refute Lexer.duration?("5M")
    refute Lexer.duration?("5m3")
  end

  test "number_prefix?/1 is true for a digit or a dot and a digit" do
    assert Lexer.number_prefix?("1")
    assert Lexer.number_prefix?(".5")
    refute Lexer.number_prefix?(".")
    refute Lexer.number_prefix?("x")
  end

  test "unexpected/2 and error_at/2 place a message at a token" do
    assert Lexer.unexpected({:ident, "x", {1, 4}}, "a number") ==
             {:error, {:syntax, ~s|unexpected token "x" at 1:4; want a number|}}

    assert Lexer.unexpected({:eof, "", {2, 1}}, "a number") ==
             {:error, {:syntax, "unexpected end of query at 2:1; want a number"}}

    assert Lexer.error_at({:punct, "@", {1, 9}}, "duplicate @ modifier") ==
             {:error, {:syntax, "duplicate @ modifier at 1:9"}}
  end
end
