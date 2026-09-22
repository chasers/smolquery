defmodule SmolqueryVictoriaMetrics.EvalExecCorpusTest do
  @moduledoc """
  `TestExecSuccess` and `TestExecError` of VictoriaMetrics v1.152.0's
  `app/vmselect/promql/exec_test.go`, every case of them, read from
  `test/support/fixtures/victoriametrics/exec_corpus.jsonl` and
  `exec_errors.jsonl` (the fixtures' README says how they were made).

  Each success case runs as VictoriaMetrics runs it: start 1000s, end 2000s,
  step 200s, against storage that holds no series, so every case is built
  from `time()`, numbers, `label_set` and the like. A case carries its
  expected series, in order, with labels and values (`null` for NaN,
  `"+Inf"`/`"-Inf"` for the infinities), compared at VictoriaMetrics'
  relative precision of `1e-13`. A case marked `skip` needs something out
  of this layer's scope, and must still be refused with exactly that.
  """

  use ExUnit.Case, async: true

  alias SmolqueryVictoriaMetrics.Eval
  alias SmolqueryVictoriaMetrics.Eval.Value
  alias SmolqueryVictoriaMetrics.MetricsQL

  @fixtures Path.expand("../support/fixtures/victoriametrics", __DIR__)
  @external_resource Path.join(@fixtures, "exec_corpus.jsonl")
  @external_resource Path.join(@fixtures, "exec_errors.jsonl")

  @corpus @fixtures
          |> Path.join("exec_corpus.jsonl")
          |> File.read!()
          |> String.split("\n", trim: true)
          |> Enum.map(&JSON.decode!/1)

  @errors @fixtures
          |> Path.join("exec_errors.jsonl")
          |> File.read!()
          |> String.split("\n", trim: true)
          |> Enum.map(&JSON.decode!/1)

  @carried Enum.reject(@corpus, &Map.has_key?(&1, "skip"))
  @skipped Enum.filter(@corpus, &Map.has_key?(&1, "skip"))

  defp context(start, finish, step) do
    %{
      start_ms: start,
      end_ms: finish,
      step_ms: step,
      lookback_ms: 300_000,
      max_points: 10_000,
      fetch: fn _selector, _range -> {:ok, []} end
    }
  end

  defp run(query, context) do
    with {:ok, expr} <- MetricsQL.parse(query), do: Eval.run(expr, context)
  end

  defp close?(nil, nil), do: true
  defp close?("+Inf", v), do: is_float(v) and v >= Value.inf()
  defp close?("-Inf", v), do: is_float(v) and v <= Value.neg_inf()

  defp close?(expected, v) when is_number(expected) and is_float(v),
    do: expected == v or abs(v - expected) / abs(expected) <= 1.0e-13

  defp close?(_expected, _v), do: false

  defp matches?(expected, series) do
    length(expected) == length(series) and
      Enum.zip(expected, series)
      |> Enum.all?(fn {%{"labels" => labels, "values" => values}, one} ->
        labels == one.labels and length(values) == length(one.values) and
          values |> Enum.zip(one.values) |> Enum.all?(fn {e, {_t, v}} -> close?(e, v) end)
      end)
  end

  describe "TestExecSuccess" do
    test "carries every case, 535 of 610 evaluated" do
      assert Enum.count(@corpus) == 610
      assert Enum.count(@carried) == 535
    end

    test "evaluates each carried case to VictoriaMetrics' series, in its order" do
      context = context(1_000_000, 2_000_000, 200_000)

      failures =
        for %{"name" => name, "query" => query, "result" => expected} <- @carried,
            answer = run(query, context),
            not match?({:ok, series, _stats} when is_list(series), answer) or
              not matches?(expected, elem(answer, 1)),
            do: {name, query, answer}

      assert failures == []
    end

    test "refuses each skipped case with the reason recorded" do
      context = context(1_000_000, 2_000_000, 200_000)

      for %{"query" => query, "skip" => what} <- @skipped do
        assert {:error, {:unsupported, ^what}} = run(query, context), query
      end
    end

    test "skips only for rand, timezone_offset, WITH, histogram() and unported rollups" do
      for %{"skip" => what} <- @skipped do
        assert what in [
                 "WITH templates",
                 "transform function rand()",
                 "transform function rand_normal()",
                 "transform function rand_exponential()",
                 "transform function timezone_offset()",
                 "aggregate function histogram()"
               ] or String.starts_with?(what, "rollup function "),
               what
      end
    end
  end

  describe "TestExecError" do
    test "carries every case" do
      assert Enum.count(@errors) == 224
    end

    test "refuses each query at the stage and with the kind recorded" do
      context = context(1_000, 2_000, 100)

      for %{"query" => query, "stage" => stage, "error" => kind} = entry <- @errors do
        case stage do
          "parse" ->
            assert {:error, {reason, _message}} = MetricsQL.parse(query), query
            assert Atom.to_string(reason) == kind, query

          "eval" ->
            assert {:ok, expr} = MetricsQL.parse(query), query
            assert {:error, {reason, detail}} = Eval.run(expr, context), query
            assert Atom.to_string(reason) == kind, query
            if entry["skip"], do: assert(detail == entry["skip"], query)
        end
      end
    end
  end
end
