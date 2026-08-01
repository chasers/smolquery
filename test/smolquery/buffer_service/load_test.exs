defmodule Smolquery.BufferService.LoadTest do
  use ExUnit.Case, async: true

  alias Smolquery.BufferService.Load

  describe "admit/3" do
    test "no counters, no budget, or no rate estimate all admit" do
      assert Load.admit(nil, 1_000_000, 5) == :ok
      assert Load.admit(Load.new(), 1_000_000, :infinity) == :ok
      assert Load.admit(Load.new(), 1_000_000, 5) == :ok
    end

    test "refuses once the predicted wait exceeds the budget, and says by how much" do
      load = Load.new()
      Load.sample_rate(load, 1_000, 1_000_000)

      assert Load.admit(load, 1_000, 5_000) == :ok
      assert Load.admit(load, 10_000, 5_000) == {:error, {:overloaded, 10_000}}

      Load.enter(load, 6_000)
      assert Load.admit(load, 1, 5_000) == {:error, {:overloaded, 6_001}}
    end
  end

  describe "the meter" do
    test "enter, leave, and drained move outstanding; it never reads negative" do
      load = Load.new()

      Load.enter(load, 100)
      assert Load.outstanding(load) == 100

      Load.leave(load, 40)
      Load.drained(load, 60)
      assert Load.outstanding(load) == 0

      Load.drained(load, 10)
      assert Load.outstanding(load) == 0
    end

    test "small flushes do not train the rate" do
      load = Load.new()

      Load.sample_rate(load, 999, 1_000)
      assert Load.predicted_wait_ms(load, 1_000) == :unknown
    end

    test "the rate is an EMA, not the last sample" do
      load = Load.new()

      Load.sample_rate(load, 1_000, 1_000_000)
      Load.sample_rate(load, 4_000, 1_000_000)

      assert Load.predicted_wait_ms(load, 1_750) == 1_000
    end
  end
end
