defmodule SmolqueryVictoriaMetrics.MetricsQL.DurationsTest do
  use ExUnit.Case, async: true

  alias SmolqueryVictoriaMetrics.MetricsQL.Durations

  describe "parse/1" do
    test "splits a duration into milliseconds and steps" do
      assert Durations.parse("1h2i") == {:ok, {3_600_000.0, 2.0}}
      assert Durations.parse("90") == {:ok, {90_000.0, 0}}
      assert Durations.parse("-1.5") == {:ok, {-1500.0, 0}}
    end

    test "refuses what is not a duration" do
      assert Durations.parse("") == {:error, "duration cannot be empty"}
      assert Durations.parse("5M") == {:error, ~s|cannot parse duration "5M"|}
      assert Durations.parse("5mi") == {:error, ~s|cannot parse duration "5mi"|}
      assert Durations.parse("5k") == {:error, ~s|cannot parse duration "5k"|}
    end
  end

  describe "to_ms/2" do
    test "adds the parts of a combined duration" do
      assert Durations.to_ms("1h30m", 0) == {:ok, 5_400_000}
      assert Durations.to_ms("0.34H4m5S", 0) == {:ok, 1_469_000}
      assert Durations.to_ms("1w1d1y", 0) == {:ok, 604_800_000 + 86_400_000 + 31_536_000_000}
      assert Durations.to_ms("13.4ms", 0) == {:ok, 13}
    end

    test "makes every part after a negative one negative" do
      assert Durations.to_ms("1h-5m", 0) == {:ok, 3_300_000}
      assert Durations.to_ms("2h-5m10s", 0) == {:ok, 7_200_000 - 300_000 - 10_000}
      assert Durations.to_ms("-5m", 0) == {:ok, -300_000}
      assert Durations.to_ms("5w4h-3.4m13.4ms", 0) == {:ok, 3_038_195_986}
    end

    test "resolves steps against the step given" do
      assert Durations.to_ms("3i", 15_000) == {:ok, 45_000}
      assert Durations.to_ms("1m1i", 30_000) == {:ok, 90_000}
    end
  end

  test "resolve/3 truncates toward zero and clamps to int64" do
    assert Durations.resolve(1.9, 0, 0) == 1
    assert Durations.resolve(-1.9, 0, 0) == -1
    assert Durations.resolve(1.0e30, 0, 0) == 9_223_372_036_854_775_807
    assert Durations.resolve(-1.0e30, 0, 0) == -9_223_372_036_854_775_808
  end

  test "a zero part after a minus does not make later parts negative" do
    assert Durations.to_ms("1h-0m5m", 1_000) == {:ok, 3_900_000}
    assert Durations.to_ms("1h-5m5m", 1_000) == {:ok, 3_000_000}
  end

  test "a part past the largest double is refused, not raised" do
    assert {:error, message} = Durations.parse(String.duplicate("9", 400) <> "ms")
    assert message =~ "too big duration"
  end
end
