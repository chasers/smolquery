defmodule SmolqueryVictoriaMetrics.MetricsQLTest do
  @moduledoc """
  The corpus of `parser_test.go` from VictoriaMetrics' `metricsql` v0.87.4,
  every case of it, read from `test/support/fixtures/victoriametrics/metricsql_corpus.jsonl`
  (the fixture's README says what each field means), plus the round trip
  `parse |> to_string |> parse` over the corpus and over generated trees.
  """

  use ExUnit.Case, async: true

  alias SmolqueryVictoriaMetrics.MetricsQL

  doctest SmolqueryVictoriaMetrics.MetricsQL

  @corpus "../support/fixtures/victoriametrics/metricsql_corpus.jsonl"
          |> Path.expand(__DIR__)
          |> File.read!()
          |> String.split("\n", trim: true)
          |> Enum.map(&JSON.decode!/1)

  @parsed Enum.filter(@corpus, &Map.has_key?(&1, "output"))
  @refused Enum.filter(@corpus, &Map.has_key?(&1, "error"))

  describe "the metricsql v0.87.4 corpus" do
    test "carries every case of parser_test.go" do
      assert Enum.count(@corpus) == 838
      assert Enum.count(@parsed) + length(@refused) == 838
    end

    test "prints each accepted query as VictoriaMetrics does, or as the note says" do
      for %{"input" => input, "output" => output} = entry <- @parsed do
        assert {:ok, expr} = MetricsQL.parse(input), "#{inspect(input)} did not parse"
        assert MetricsQL.to_string(expr) == output, "#{inspect(input)} (#{entry["note"]})"
      end
    end

    test "differs from VictoriaMetrics' printing only where a note says why" do
      for %{"output" => output} = entry <- @parsed, Map.has_key?(entry, "vm") do
        assert entry["note"] in ["folding", "or_branch"]
        refute output == entry["vm"]
      end
    end

    test "refuses each rejected query with the kind of error recorded" do
      for %{"input" => input, "error" => kind} <- @refused do
        assert {:error, {reason, _detail}} = MetricsQL.parse(input), "#{inspect(input)} parsed"
        assert Atom.to_string(reason) == kind, "#{inspect(input)}: #{reason}"
      end
    end

    test "refuses what VictoriaMetrics accepts only for WITH templates and argument counts" do
      for %{"error" => kind, "note" => note} <- @refused do
        assert {kind, note} in [{"unsupported", "with"}, {"arity", "arity"}]
      end
    end

    test "round-trips every accepted query" do
      for %{"input" => input} <- @parsed do
        {:ok, expr} = MetricsQL.parse(input)
        assert MetricsQL.parse(MetricsQL.to_string(expr)) == {:ok, expr}, inspect(input)
      end
    end
  end

  describe "parse/1" do
    test "answers WITH templates as unsupported, wherever they appear" do
      assert MetricsQL.parse("with (x = 1) x") == {:error, {:unsupported, "WITH templates"}}

      assert MetricsQL.parse("rate(m) + WITH (f(a) = a) f(1)") ==
               {:error, {:unsupported, "WITH templates"}}
    end

    test "reads with as a metric name when no parenthesis follows" do
      assert {:ok, %{op: :/}} = MetricsQL.parse("with / by")
    end

    test "names the position of a syntax error" do
      assert MetricsQL.parse("sum(rate(m[5m])") ==
               {:error, {:syntax, ~s|unexpected end of query at 1:16; want "," or ")"|}}

      assert MetricsQL.parse("m{a=\"b\"}\n  + $") ==
               {:error, {:syntax, ~s|cannot recognize "$" at 2:5|}}
    end

    test "refuses an empty query" do
      assert {:error, {:syntax, "unexpected end of query at 1:1; want an expression"}} =
               MetricsQL.parse("")

      assert {:error, {:syntax, _message}} = MetricsQL.parse("  # only a comment")
    end

    test "refuses an unknown function by name" do
      assert MetricsQL.parse("rate(m) + frobnicate(m)") ==
               {:error, {:unknown_function, "frobnicate"}}
    end

    test "refuses a wrong argument count for a function whose count is fixed" do
      assert MetricsQL.parse("rate(m[5m], 1)") == {:error, {:arity, "rate() takes 1 arg; got 2"}}

      assert MetricsQL.parse("label_replace(m, \"a\")") ==
               {:error, {:arity, "label_replace() takes 5 args; got 2"}}

      assert MetricsQL.parse("topk(m)") == {:error, {:arity, "topk() takes 2 args; got 1"}}

      assert MetricsQL.parse("round(m, 1, 2)") ==
               {:error, {:arity, "round() takes 1 to 2 args; got 3"}}

      assert MetricsQL.parse("sum()") == {:error, {:arity, "sum() takes at least 1 arg; got 0"}}
    end

    test "accepts any count for a variadic function" do
      assert {:ok, _expr} = MetricsQL.parse("union(a, b, c, d)")
      assert {:ok, _expr} = MetricsQL.parse("sum(a, b, c)")
      assert {:ok, _expr} = MetricsQL.parse(~S|label_del(m, "a", "b", "c")|)
    end

    test "refuses keep_metric_names on an aggregate" do
      assert {:error, {:syntax, message}} = MetricsQL.parse("sum(rate(m[5m])) keep_metric_names")
      assert message =~ "cannot be applied to the aggregate function sum()"
    end

    test "refuses a window or a subquery on a range vector" do
      for query <- ["(m[5m])[10m:1m]", "(m[5m])[10m]", "((rate(m)[5m]))[1h:]"] do
        assert {:error, {:syntax, message}} = MetricsQL.parse(query)
        assert message =~ "cannot be applied to a range vector"
      end
    end

    test "accepts a subquery on a subquery and on any instant expression" do
      assert {:ok, _expr} = MetricsQL.parse("(m[:1m])[5m:1m]")
      assert {:ok, _expr} = MetricsQL.parse("(sum(rate(m[1m])))[5m:1m]")
      assert {:ok, _expr} = MetricsQL.parse("(m[5m]) offset 1h")
    end
  end

  describe "operator precedence and associativity" do
    @cases [
      {"a + b * c", "a + (b * c)"},
      {"a * b + c", "(a * b) + c"},
      {"a - b - c", "(a - b) - c"},
      {"a / b / c", "(a / b) / c"},
      {"a ^ b ^ c", "a ^ (b ^ c)"},
      {"a * b ^ c", "a * (b ^ c)"},
      {"a atan2 b + c", "(a atan2 b) + c"},
      {"a % b * c", "(a % b) * c"},
      {"a + b > c", "(a + b) > c"},
      {"a > b and c < d", "(a > b) and (c < d)"},
      {"a and b or c and d", "(a and b) or (c and d)"},
      {"a unless b and c", "(a unless b) and c"},
      {"a or b if c", "(a or b) if c"},
      {"a if b ifnot c", "(a if b) ifnot c"},
      {"a default b if c", "a default (b if c)"},
      {"a if b default c", "(a if b) default c"},
      {"-a ^ 2", "0 - (a ^ 2)"},
      {"-a * b", "0 - (a * b)"},
      {"-a + b", "(0 - a) + b"},
      {"2 ^ -a", "2 ^ (0 - a)"},
      {"(a + b) * c", "(a + b) * c"},
      {"a == bool b == bool c", "(a ==bool b) ==bool c"}
    ]

    test "groups as VictoriaMetrics does" do
      for {query, grouped} <- @cases do
        assert {:ok, expr} = MetricsQL.parse(query)
        assert MetricsQL.to_string(expr) == grouped, query
      end
    end
  end

  describe "function_kind/1" do
    test "names the kind of each function, case-insensitively" do
      assert MetricsQL.function_kind("rate") == :rollup
      assert MetricsQL.function_kind("Label_Replace") == :transform
      assert MetricsQL.function_kind("topk") == :aggregate
      assert MetricsQL.function_kind("nope") == :unknown
    end
  end

  describe "duration_to_ms/2" do
    test "resolves units and steps" do
      assert MetricsQL.duration_to_ms("5m", 1000) == {:ok, 300_000}
      assert MetricsQL.duration_to_ms("1.5h", 1000) == {:ok, 5_400_000}
      assert MetricsQL.duration_to_ms("2i", 15_000) == {:ok, 30_000}
      assert MetricsQL.duration_to_ms("$__interval", 15_000) == {:ok, 15_000}
      assert MetricsQL.duration_to_ms("-5m", 1000) == {:ok, -300_000}
      assert {:error, _message} = MetricsQL.duration_to_ms("5x", 1000)
    end
  end

  describe "generated trees" do
    test "print and parse back to themselves" do
      :rand.seed(:exsss, {563, 70, 152})

      for _round <- 1..400 do
        query = random_expr(4)
        assert {:ok, expr} = MetricsQL.parse(query), query
        printed = MetricsQL.to_string(expr)
        assert MetricsQL.parse(printed) == {:ok, expr}, "#{query} printed as #{printed}"
      end
    end
  end

  @leaves [
    "m",
    ~S|m{a="b",c!~"d.*"}|,
    ~S|{__name__=~"x.*" or job="j"}|,
    "m[5m]",
    "m[5m:1m] offset -1h",
    "m @ end()",
    "1.5",
    "0x1f",
    "12Ki",
    "Inf",
    "5m",
    ~S|("s")|,
    "time()",
    "(on)",
    "group_left"
  ]
  @ops ~w(+ - * / % ^ atan2 == != > < >= <= and or unless if ifnot default)
  @wrappers [
    "rate($)",
    "abs($) keep_metric_names",
    "sum($) by (a)",
    "topk(3, $) limit 2",
    "quantile(0.9, $)",
    "-$",
    "($)",
    "rate($)[5m:]",
    "($) offset 5m",
    "($, m)",
    "($ + x) keep_metric_names"
  ]

  defp random_expr(0), do: Enum.random(@leaves)

  defp random_expr(depth) do
    case :rand.uniform(3) do
      1 -> random_expr(0)
      2 -> String.replace(Enum.random(@wrappers), "$", random_expr(depth - 1))
      3 -> "#{random_expr(depth - 1)} #{operator()} #{random_expr(depth - 1)}"
    end
  end

  defp operator do
    op = Enum.random(@ops)

    cond do
      op in ~w(== != > < >= <=) and :rand.uniform(2) == 1 -> op <> " bool"
      op in ~w(+ - * /) and :rand.uniform(3) == 1 -> op <> " on(a) group_left(b) prefix \"p_\""
      op in ~w(+ *) and :rand.uniform(3) == 1 -> op <> " ignoring(c) fill(0)"
      true -> op
    end
  end
end
