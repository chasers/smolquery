defmodule SmolqueryVictoriaMetrics.MetricsQL.ParserTest do
  use ExUnit.Case, async: true

  alias SmolqueryVictoriaMetrics.MetricsQL.Ast.AggrFuncExpr
  alias SmolqueryVictoriaMetrics.MetricsQL.Ast.BinaryOpExpr
  alias SmolqueryVictoriaMetrics.MetricsQL.Ast.Duration
  alias SmolqueryVictoriaMetrics.MetricsQL.Ast.FuncExpr
  alias SmolqueryVictoriaMetrics.MetricsQL.Ast.LabelFilter
  alias SmolqueryVictoriaMetrics.MetricsQL.Ast.MetricExpr
  alias SmolqueryVictoriaMetrics.MetricsQL.Ast.Modifier
  alias SmolqueryVictoriaMetrics.MetricsQL.Ast.Number
  alias SmolqueryVictoriaMetrics.MetricsQL.Ast.ParensExpr
  alias SmolqueryVictoriaMetrics.MetricsQL.Ast.RollupExpr
  alias SmolqueryVictoriaMetrics.MetricsQL.Ast.StringLiteral
  alias SmolqueryVictoriaMetrics.MetricsQL.Parser

  defp parse!(query) do
    {:ok, expr} = Parser.parse(query)
    expr
  end

  defp metric(name), do: %MetricExpr{filter_sets: [[name_filter(name)]]}
  defp name_filter(name), do: %LabelFilter{name: "__name__", op: :eq, value: name}

  test "reads a rollup with window, step, offset and @" do
    assert parse!("m[5m:1m] offset -1h @ end()") == %RollupExpr{
             expr: metric("m"),
             window: %Duration{text: "5m", ms: 300_000.0, steps: 0},
             step: %Duration{text: "1m", ms: 60_000.0, steps: 0},
             offset: %Duration{text: "-1h", ms: -3_600_000.0, steps: 0},
             at: %FuncExpr{name: "end", args: []}
           }
  end

  test "reads offset and @ in either order" do
    assert parse!("m @ 100 offset 5m") == parse!("m offset 5m @ 100")
  end

  test "reads an inherited step, an omitted window and a window on any expression" do
    assert %RollupExpr{window: %Duration{text: "1h"}, step: nil, inherit_step: true} =
             parse!("rate(m)[1h:]")

    assert %RollupExpr{window: nil, step: %Duration{text: "1m"}} = parse!("m[:1m]")

    assert %RollupExpr{expr: %FuncExpr{name: "rate"}, window: %Duration{text: "5m"}} =
             parse!("rate(m)[5m]")

    assert %RollupExpr{window: %Duration{text: "90", ms: 90_000.0}} = parse!("m[90]")
  end

  test "reads $__interval as no window, and as one step elsewhere" do
    assert parse!("rate(m[$__interval])") == %FuncExpr{name: "rate", args: [metric("m")]}

    assert %RollupExpr{offset: %Duration{text: "1i", ms: 0, steps: 1.0}} =
             parse!("m offset $__rate_interval")
  end

  test "reads unary minus as 0 - x and unary plus as nothing" do
    assert parse!("-m") == %BinaryOpExpr{
             op: :-,
             left: %Number{value: 0.0, text: "0"},
             right: metric("m")
           }

    assert parse!("+m") == metric("m")
  end

  test "reads numbers, strings and durations as expressions" do
    assert parse!("12Ki") == %Number{value: 12_288.0, text: "12Ki"}
    assert parse!("NaN") == %Number{value: :nan, text: "NaN"}
    assert parse!(~S|"a" + 'b' + `c`|) == %StringLiteral{value: "abc"}
    assert parse!("1h") == %Duration{text: "1h", ms: 3_600_000.0, steps: 0}
  end

  test "reads a call with keep_metric_names and a case kept as typed" do
    assert parse!(~S|RATE(m[5m]) keep_metric_names|) == %FuncExpr{
             name: "RATE",
             args: [
               %RollupExpr{
                 expr: metric("m"),
                 window: %Duration{text: "5m", ms: 300_000.0, steps: 0}
               }
             ],
             keep_metric_names: true
           }
  end

  test "reads an aggregation with its modifier before or after, and a limit" do
    expected = %AggrFuncExpr{
      name: "topk",
      args: [%Number{value: 3.0, text: "3"}, metric("m")],
      modifier: %Modifier{op: :by, labels: ["a", "b c"]},
      limit: 10
    }

    assert parse!(~S|TOPK BY (a, "b c") (3, m) limit 10|) == expected
    assert parse!(~S|topk(3, m) by (a, "b c",) limit 10|) == expected
    assert %AggrFuncExpr{limit: nil} = parse!("sum(m) limit 0")
  end

  test "reads every binary operator modifier" do
    assert parse!(
             ~S|a * on(x) group_left(y) prefix "p_" fill_left(-1) fill_right(inf) b keep_metric_names|
           ) ==
             %BinaryOpExpr{
               op: :*,
               left: metric("a"),
               right: metric("b"),
               group_modifier: %Modifier{op: :on, labels: ["x"]},
               join_modifier: %Modifier{op: :group_left, labels: ["y"]},
               join_prefix: "p_",
               fill_left: %Number{value: -1.0, text: "-1"},
               fill_right: %Number{value: :inf, text: "inf"},
               keep_metric_names: true
             }

    assert %BinaryOpExpr{op: :>, bool: true} = parse!("a > bool 1")

    assert %BinaryOpExpr{join_modifier: %Modifier{labels: :all}} =
             parse!("a + ignoring(x) group_right(*) b")

    assert %BinaryOpExpr{fill_left: fill, fill_right: fill} = parse!("a - fill(-inf) b")
    assert fill == %Number{value: :neg_inf, text: "-inf"}
  end

  test "reads MetricsQL's operators" do
    for {word, op} <- [{"default", :default}, {"if", :if}, {"IFNOT", :ifnot}, {"atan2", :atan2}] do
      assert %BinaryOpExpr{op: ^op} = parse!("a #{word} b")
    end
  end

  test "drops parentheses around one expression and keeps a union" do
    assert parse!("((m))") == metric("m")

    assert parse!("(a, (b), ())") == %ParensExpr{
             exprs: [metric("a"), metric("b"), %ParensExpr{exprs: []}]
           }

    assert %BinaryOpExpr{keep_metric_names: true} = parse!("(a + b) keep_metric_names")
  end

  test "reads keywords as metric names where they cannot be keywords" do
    assert parse!("offset") == metric("offset")
    assert parse!("sum + by") == %BinaryOpExpr{op: :+, left: metric("sum"), right: metric("by")}
    assert %BinaryOpExpr{right: %MetricExpr{}} = parse!("a + (on)")
  end

  test "refuses a bool that is not on a comparison, and joins on set operators" do
    assert {:error, {:syntax, "bool modifier cannot be applied to + at 1:5"}} =
             Parser.parse("a + bool b")

    assert {:error, {:syntax, "group_left cannot be applied to and at 1:13"}} =
             Parser.parse("a and on(x) group_left b")

    assert {:error, {:syntax, "fill cannot be applied to default at 1:11"}} =
             Parser.parse("a default fill(0) b")
  end

  test "refuses a limit that is not an integer, and a duplicate @" do
    assert {:error, {:syntax, ~s|unexpected token "1.5" at 1:14; want an integer limit|}} =
             Parser.parse("sum(m) limit 1.5")

    assert {:error, {:syntax, "duplicate @ modifier at 1:17"}} =
             Parser.parse("m @ 1 offset 1m @ 2")
  end

  test "refuses a star in on(), ignoring(), by() and without()" do
    for query <- ["a + on(*) b", "a + ignoring(*) b", "sum(m) by (*)", "sum(m) without(*)"] do
      assert {:error, {:syntax, message}} = Parser.parse(query)
      assert message =~ ~s|unexpected token "*"|
    end
  end
end
