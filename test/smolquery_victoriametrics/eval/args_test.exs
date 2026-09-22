defmodule SmolqueryVictoriaMetrics.Eval.ArgsTest do
  use ExUnit.Case, async: true

  alias SmolqueryVictoriaMetrics.Eval.Args
  alias SmolqueryVictoriaMetrics.Eval.Series

  @grid [0, 1000]

  defp number(v), do: [Series.constant(@grid, v)]
  defp text(s), do: [Series.string(@grid, s)]

  test "count/2 and at_least/2" do
    assert Args.count([number(1.0)], 1) == :ok

    assert Args.count([], 1) ==
             {:error, {:invalid_argument, "unexpected number of args; got 0; want 1"}}

    assert Args.at_least([number(1.0)], 1) == :ok

    assert {:error, {:invalid_argument, "not enough args; got 0; want at least 1"}} =
             Args.at_least([], 1)
  end

  test "scalar/2 and integer/2 take one series" do
    assert Args.scalar(number(2.5), 0) == {:ok, [2.5, 2.5]}

    assert Args.scalar(number(1.0) ++ number(2.0), 1) ==
             {:error, {:invalid_argument, "arg #2 must be a scalar"}}

    assert Args.integer(number(2.9), 0) == {:ok, 2}
    assert Args.integer(number(nil), 0) == {:ok, 0}
  end

  test "to_integer/1 truncates and bounds" do
    assert Args.to_integer(-2.9) == -2
    assert Args.to_integer(1.0e30) == 9_223_372_036_854_775_807
    assert Args.to_integer(nil) == 0
  end

  test "string/2, strings/2 and pairs/1 read string literals" do
    assert Args.string(text("foo"), 0) == {:ok, "foo"}
    assert Args.string(text(""), 0) == {:ok, ""}
    assert Args.string(number(1.0), 2) == {:error, {:invalid_argument, "arg #3 must be a string"}}
    assert Args.strings([text("a"), text("b")], 1) == {:ok, ["a", "b"]}

    assert Args.strings([text("a"), number(1.0)], 1) ==
             {:error, {:invalid_argument, "arg #3 must be a string"}}

    assert Args.pairs([text("a"), text("b")]) == {:ok, [{"a", "b"}]}

    assert {:error, {:invalid_argument, "the number of string args must be even; got 1"}} =
             Args.pairs([text("a")])
  end

  test "invalid/1" do
    assert Args.invalid("nope") == {:error, {:invalid_argument, "nope"}}
  end
end
