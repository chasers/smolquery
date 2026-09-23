defmodule SmolqueryVictoriaMetrics.MetricsQL.PrinterTest do
  use ExUnit.Case, async: true

  alias SmolqueryVictoriaMetrics.MetricsQL.Ast.LabelFilter
  alias SmolqueryVictoriaMetrics.MetricsQL.Ast.MetricExpr
  alias SmolqueryVictoriaMetrics.MetricsQL.Ast.StringLiteral
  alias SmolqueryVictoriaMetrics.MetricsQL.Parser
  alias SmolqueryVictoriaMetrics.MetricsQL.Printer

  defp reprint(query) do
    {:ok, expr} = Parser.parse(query)
    Printer.to_string(expr)
  end

  test "prints canonical MetricsQL" do
    assert reprint(~S|SUM BY (a) (RATE(m{x="y"}[5m:1m] @ 12 offset 1h)) LIMIT 3|) ==
             ~S|sum(RATE(m{x="y"}[5m:1m] offset 1h @ 12)) by(a) limit 3|

    assert reprint("a+on(x)group_left b") == "a + on(x) group_left() b"
    assert reprint("a > BOOL 1") == "a >bool 1"
    assert reprint("m * fill_left(0) fill_right(0) n") == "m * fill(0) n"
    assert reprint("x / a keep_metric_names") == "(x / a) keep_metric_names"
    assert reprint("(a, b)") == "(a, b)"
  end

  test "parenthesizes a subquery's expression only where it must" do
    assert reprint("(sum(m))[5m]") == "sum(m)[5m]"
    assert reprint("(sum(m) by (a))[5m]") == "(sum(m) by(a))[5m]"
    assert reprint("(a + b)[5m:]") == "(a + b)[5m:]"
    assert reprint("m @ (end() - 1h)") == "m @ (end() - 1h)"
  end

  test "parenthesizes a right operand that would read as a modifier" do
    assert reprint("a + (on)") == "a + (on)"
    assert reprint("a + (GROUP_LEFT)") == "a + (GROUP_LEFT)"
    assert reprint("a + b offset 5m") == "a + (b offset 5m)"
  end

  test "keeps a string on the left of + apart from what follows" do
    assert reprint(~S|("a") + "b"|) == ~S|("a") + "b"|
    assert reprint(~S|("a") + b|) == ~S|("a") + b|
    assert reprint(~S|"a" + b{x="y"}|) == ~S|"a" + b{x="y"}|
  end

  test "prints every name of an or when one branch is only the name" do
    assert reprint(~S|{__name__="a",b="1" or __name__="a"}|) ==
             ~S|{__name__="a",b="1" or __name__="a"}|

    assert reprint(~S|a{b="1" or c="2"}|) == ~S|a{b="1" or c="2"}|
  end

  test "prints a metric named like a number, or with no name, in braces" do
    sets = [[%LabelFilter{name: "__name__", op: :eq, value: "NaN"}]]
    assert Printer.to_string(%MetricExpr{filter_sets: sets}) == ~S|{__name__="NaN"}|

    assert Printer.to_string(%MetricExpr{
             filter_sets: [[%LabelFilter{name: "", op: :eq, value: "v"}]]
           }) == ~S|{""="v"}|
  end

  test "escapes identifiers and quotes strings as Go does" do
    assert reprint(~S|{"metric name","a-b"="x\ty"}|) == ~S|metric\ name{a\-b="x\ty"}|
    assert Printer.to_string(%StringLiteral{value: <<"é", 0xFF>>}) == ~S|"é\xff"|
  end

  test "what used to print into a different tree prints back to itself" do
    for {query, text} <- [
          {~s|{__name__="sum"} or b|, "(sum) or b"},
          {~s|{__name__="count"} unless b|, "(count) unless b"},
          {~s|a > {__name__="bool"}[5m]|, "a > (bool[5m])"},
          {~s|a + {__name__="on"} offset 5m|, "a + (on offset 5m)"},
          {~S|{__name__="a\U000F0000b"}|, "a\\udb80\\udc00b"}
        ] do
      {:ok, expr} = Parser.parse(query)

      assert Printer.to_string(expr) == text
      assert Parser.parse(text) == {:ok, expr}, query
    end
  end
end
