defmodule Smolquery.Burst.PolicyTest do
  use ExUnit.Case, async: true

  alias Smolquery.Burst.Policy

  @gib 1_073_741_824
  @config %{
    enabled: true,
    max: 32,
    max_per_job: 16,
    min_scan_bytes: 8 * @gib,
    target_bytes: @gib
  }

  describe "runners/3" do
    test "false and 0 are peers only, and not a refusal worth reporting" do
      assert Policy.runners(false, 100 * @gib, @config) == {:none, :off}
      assert Policy.runners(0, 100 * @gib, @config) == {:none, :off}
    end

    test "auto bursts from the threshold up, one runner per target_bytes" do
      assert Policy.runners(:auto, 8 * @gib - 1, @config) ==
               {:none, {:under_threshold, 8 * @gib - 1, 8 * @gib}}

      assert Policy.runners(:auto, 8 * @gib, @config) == {:ok, 8}
      assert Policy.runners(:auto, 8 * @gib + 1, @config) == {:ok, 9}
    end

    test "an integer asks for exactly that many" do
      assert Policy.runners(3, 0, @config) == {:ok, 3}
      assert Policy.runners(3, 100 * @gib, @config) == {:ok, 3}
    end

    test "every answer is capped at max_per_job and at the pool's max" do
      assert Policy.runners(:auto, 100 * @gib, @config) == {:ok, 16}
      assert Policy.runners(100, 0, @config) == {:ok, 16}
      assert Policy.runners(100, 0, %{@config | max: 4}) == {:ok, 4}
      assert Policy.runners(:auto, 100 * @gib, %{@config | max_per_job: 64}) == {:ok, 32}
    end

    test "a scan of zero bytes over a zero threshold still takes one runner" do
      assert Policy.runners(:auto, 0, %{@config | min_scan_bytes: 0}) == {:ok, 1}
    end

    test "a node without a pool refuses an integer, and quietly ignores auto" do
      off = %{@config | enabled: false}

      assert Policy.runners(4, 100 * @gib, off) == {:none, :no_pool}
      assert Policy.runners(:auto, 100 * @gib, off) == {:none, :off}
      assert Policy.runners(false, 100 * @gib, off) == {:none, :off}
    end
  end

  describe "request?/1" do
    test "accepts false, auto and a count" do
      assert Policy.request?(false)
      assert Policy.request?(:auto)
      assert Policy.request?(0)
      assert Policy.request?(128)
    end

    test "refuses anything else" do
      refute Policy.request?(true)
      refute Policy.request?("auto")
      refute Policy.request?(-1)
      refute Policy.request?(nil)
    end
  end
end
