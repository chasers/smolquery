defmodule SmolqueryVictoriaMetrics.MetricsQL.SelectorTest do
  use ExUnit.Case, async: true

  alias SmolqueryVictoriaMetrics.MetricsQL.Ast.LabelFilter
  alias SmolqueryVictoriaMetrics.MetricsQL.Ast.MetricExpr
  alias SmolqueryVictoriaMetrics.MetricsQL.Lexer
  alias SmolqueryVictoriaMetrics.MetricsQL.Selector

  defp metric(query) do
    {:ok, tokens} = Lexer.tokenize(query)

    case Selector.metric(tokens) do
      {:ok, expr, [{:eof, "", _position}]} -> {:ok, expr}
      other -> other
    end
  end

  defp filter(name, op, value), do: %LabelFilter{name: name, op: op, value: value}
  defp named(name), do: filter("__name__", :eq, name)

  describe "metric/1" do
    test "puts the metric name first, however it was written" do
      expected = {:ok, %MetricExpr{filter_sets: [[named("m"), filter("a", :eq, "1")]]}}

      assert metric(~S|m{a="1"}|) == expected
      assert metric(~S|{"m", a="1"}|) == expected
      assert metric(~S|{a="1", __name__="m"}|) == expected
      assert metric(~S|m{"a"="1",}|) == expected
    end

    test "reads every matcher, quoted and escaped label names" do
      assert metric(~S|{a!="1", b=~"x.*", c!~'y', "d e"="z", f\-g="w"}|) ==
               {:ok,
                %MetricExpr{
                  filter_sets: [
                    [
                      filter("a", :neq, "1"),
                      filter("b", :re, "x.*"),
                      filter("c", :nre, "y"),
                      filter("d e", :eq, "z"),
                      filter("f-g", :eq, "w")
                    ]
                  ]
                }}
    end

    test "reads or as alternative filter sets and shares a common name" do
      assert metric(~S|m{a="1" or b="2"}|) ==
               {:ok,
                %MetricExpr{
                  filter_sets: [
                    [named("m"), filter("a", :eq, "1")],
                    [named("m"), filter("b", :eq, "2")]
                  ]
                }}

      assert metric(~S|{__name__="m",a="1" OR b="2"}|) == metric(~S|m{a="1" or b="2"}|)

      assert metric(~S|{__name__=~"m.*",a="1" or b="2"}|) ==
               {:ok,
                %MetricExpr{
                  filter_sets: [
                    [filter("__name__", :re, "m.*"), filter("a", :eq, "1")],
                    [filter("b", :eq, "2")]
                  ]
                }}
    end

    test "drops repeated filters and repeated sets" do
      assert metric(~S|m{a="1",a="1",a="2" or a="1",a="2"}|) ==
               {:ok,
                %MetricExpr{
                  filter_sets: [[named("m"), filter("a", :eq, "1"), filter("a", :eq, "2")]]
                }}
    end

    test "reads {} as no filter sets" do
      assert metric("{}") == {:ok, %MetricExpr{filter_sets: []}}
      assert metric("m{}") == {:ok, %MetricExpr{filter_sets: [[named("m")]]}}
    end

    test "joins string literals in a value" do
      assert metric(~S|m{a="x" + 'y'}|) ==
               {:ok, %MetricExpr{filter_sets: [[named("m"), filter("a", :eq, "xy")]]}}
    end

    test "refuses two metric names, a bad regexp, a filter without a value" do
      assert {:error,
              {:syntax,
               "metric name must not be set twice: \"a\" or \"b\" in the selector at 1:1"}} =
               metric(~S|{"a", __name__="b"}|)

      assert {:error,
              {:syntax, "invalid regexp \"x(\" for a: missing closing parenthesis at 1:3"}} =
               metric(~S|{a=~"x("}|)

      assert {:error, {:syntax, ~s|unexpected token "}" at 1:3; want "=", "!=", "=~" or "!~"|}} =
               metric("{a}")

      assert {:error, {:syntax, ~s|unexpected token "}" at 1:10; want a label name|}} =
               metric(~S|{a="b" or}|)
    end
  end

  describe "string_value/1" do
    test "joins literals and stops before an operator that is not a concatenation" do
      {:ok, tokens} = Lexer.tokenize(~S|"a" + "b" + f(x)|)
      assert {:ok, "ab", [{:op, "+", _position} | _rest]} = Selector.string_value(tokens)
    end

    test "refuses an identifier after +, which would be a WITH reference" do
      {:ok, tokens} = Lexer.tokenize(~S|"a" + b|)

      assert Selector.string_value(tokens) ==
               {:error, {:syntax, ~s|unexpected token "b" at 1:7; want a string after "+"|}}
    end
  end
end
