defmodule Smolquery.HeapTest do
  use ExUnit.Case, async: true

  alias Smolquery.Heap

  describe "tune/1" do
    test "sets both flags on the calling process" do
      %{fullsweep_after: fullsweep_after, min_heap_size: min_heap_size} =
        tuned(fullsweep_after: 3, min_heap_size: 4_096)

      assert fullsweep_after == 3
      assert min_heap_size >= 4_096
    end

    test "a nil leaves the flag at whatever it already was" do
      before = tuned([])

      assert tuned(fullsweep_after: nil, min_heap_size: nil) == before
    end

    test "one flag set does not disturb the other" do
      %{min_heap_size: before} = tuned([])

      assert %{fullsweep_after: 0, min_heap_size: ^before} = tuned(fullsweep_after: 0)
    end

    test "ignores keys that are not heap flags" do
      assert tuned(priority: :high) == tuned([])
      assert Process.info(self(), :priority) == {:priority, :normal}
    end

    test "returns :ok" do
      assert Heap.tune(fullsweep_after: 1) == :ok
    end
  end

  defp tuned(opts) do
    task =
      Task.async(fn ->
        :ok = Heap.tune(opts)

        Map.new([:fullsweep_after, :min_heap_size], fn flag ->
          {flag, self() |> Process.info(:garbage_collection) |> elem(1) |> Keyword.fetch!(flag)}
        end)
      end)

    Task.await(task)
  end
end
