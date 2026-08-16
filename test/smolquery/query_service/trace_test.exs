defmodule Smolquery.QueryService.TraceTest do
  use ExUnit.Case, async: true

  alias Smolquery.QueryService.Trace

  describe "span/3" do
    test "returns the function's result, error tuples included" do
      assert Trace.span(:serialize, fn -> {:ok, :parsed} end) == {:ok, :parsed}
      assert Trace.span(:serialize, fn -> {:error, :nope} end) == {:error, :nope}
    end
  end

  describe "attach/2 and stop/1" do
    test "collects the owner's spans and its tasks', rebased to the earliest" do
      collector = Trace.attach("job-1", self())

      Trace.span(:serialize, fn -> Process.sleep(1) end)

      Task.async(fn ->
        Trace.span(:manifest_fetch, %{url: "http://n1:4321"}, fn -> :ok end)
      end)
      |> Task.await()

      spans = Trace.stop(collector)

      assert Enum.map(spans, & &1.name) == [:serialize, :manifest_fetch]
      assert [%{start_us: 0} = first, second] = spans
      assert first.duration_us > 0
      assert second.start_us > 0
      assert second.meta == %{url: "http://n1:4321"}
    end

    test "a stranger's spans are not collected" do
      collector = Trace.attach("job-2", self())

      {:ok, stranger} =
        Task.start(fn ->
          Process.delete(:"$callers")

          Trace.span(:build, fn -> :ok end)
        end)

      ref = Process.monitor(stranger)
      assert_receive {:DOWN, ^ref, :process, ^stranger, :normal}

      assert Trace.stop(collector) == []
    end

    test "stopping detaches: later spans go nowhere and the table is gone" do
      collector = Trace.attach("job-3", self())

      assert Trace.stop(collector) == []

      Trace.span(:serialize, fn -> :ok end)

      assert :ets.info(collector.table) == :undefined
    end
  end
end
