defmodule SmolqueryVictoriaMetrics.PrefetchTest do
  use ExUnit.Case, async: true

  alias SmolqueryVictoriaMetrics.Prefetch

  defp stages(test, start) do
    %{
      fetch:
        {fn selector, range ->
           send(test, {:started, selector, self()})
           start.(selector, range)
         end,
         fn answer ->
           send(test, {:taken, answer, self()})
           {:ok, [answer]}
         end},
      aggregate:
        {fn plan ->
           send(test, {:started, plan, self()})
           {:ok, {:frame, plan}}
         end, fn answer -> {:ok, [answer], %{series: 1, samples: 0}} end}
    }
  end

  defp gated(selector, range) do
    receive do
      :go -> {:ok, {:frame, selector, range}}
    end
  end

  defp answered(selector, range), do: {:ok, {:frame, selector, range}}

  defp both(context) do
    with {:ok, a} <- context.fetch.(:a, {0, 1}),
         {:ok, b} <- context.fetch.(:b, {0, 1}),
         do: {:ok, a ++ b}
  end

  describe "context/4" do
    test "starts the recorded reads at once, and takes them in the evaluating process" do
      test = self()

      task =
        Task.async(fn ->
          Prefetch.context(%{}, stages(test, &gated/2), [max_concurrency: 4], &both/1)
        end)

      assert_receive {:started, :a, a}
      assert_receive {:started, :b, b}
      send(a, :go)
      send(b, :go)
      context = Task.await(task)
      refute_received {:taken, _answer, _pid}

      assert both(context) == {:ok, [{:frame, :a, {0, 1}}, {:frame, :b, {0, 1}}]}
      assert_received {:taken, {:frame, :a, {0, 1}}, ^test}
      assert_received {:taken, {:frame, :b, {0, 1}}, ^test}
      refute_received {:started, _selector, _pid}
    end

    test "starts no more than max_concurrency at a time" do
      test = self()

      task =
        Task.async(fn ->
          Prefetch.context(%{}, stages(test, &gated/2), [max_concurrency: 1], &both/1)
        end)

      assert_receive {:started, first, pid}
      refute_receive {:started, _selector, _pid}, 50
      send(pid, :go)

      assert_receive {:started, second, other}
      assert Enum.sort([first, second]) == [:a, :b]
      send(other, :go)
      Task.await(task)
    end

    test "holds a read's error, starts one retry? accepts again, and starts an unrecorded read when reached" do
      test = self()

      start = fn
        :a, _range -> {:error, :too_many_jobs}
        :b, _range -> {:error, {:too_many_series, 1}}
        selector, range -> answered(selector, range)
      end

      context =
        Prefetch.context(
          %{},
          stages(test, start),
          [max_concurrency: 4, retry?: &(&1 == :too_many_jobs)],
          &both/1
        )

      assert_received {:started, :a, _pid}
      assert_received {:started, :b, _pid}

      assert context.fetch.(:a, {0, 1}) == {:error, :too_many_jobs}
      assert_received {:started, :a, ^test}
      assert context.fetch.(:b, {0, 1}) == {:error, {:too_many_series, 1}}
      refute_received {:started, :b, _pid}
      assert context.fetch.(:c, {2, 3}) == {:ok, [{:frame, :c, {2, 3}}]}
      assert_received {:started, :c, ^test}
    end

    test "starts pushed aggregates beside selectors, and a read asked for twice once" do
      test = self()

      evaluate = fn context ->
        context.fetch.(:a, {0, 1})
        context.aggregate.(:plan)
        context.fetch.(:a, {0, 1})
      end

      context = Prefetch.context(%{}, stages(test, &answered/2), [max_concurrency: 4], evaluate)
      assert_received {:started, :a, _pid}
      refute_received {:started, :a, _pid}
      assert_received {:started, :plan, _pid}

      assert context.aggregate.(:plan) == {:ok, [{:frame, :plan}], %{series: 1, samples: 0}}
      assert context.fetch.(:a, {0, 1}) == {:ok, [{:frame, :a, {0, 1}}]}
      assert context.fetch.(:a, {0, 1}) == {:ok, [{:frame, :a, {0, 1}}]}
      refute_received {:started, _read, _pid}
    end

    test "a query with one read or none starts it when the evaluation reaches it" do
      test = self()
      one = fn context -> context.fetch.(:a, {0, 1}) end

      context =
        Prefetch.context(%{step_ms: 15}, stages(test, &answered/2), [max_concurrency: 4], one)

      refute_received {:started, _read, _pid}
      assert context.step_ms == 15
      assert one.(context) == {:ok, [{:frame, :a, {0, 1}}]}
      assert_received {:started, :a, ^test}
    end
  end
end
