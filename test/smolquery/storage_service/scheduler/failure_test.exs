defmodule Smolquery.StorageService.Scheduler.FailureTest do
  @moduledoc "Which kind of failure a compaction failure is."

  use ExUnit.Case, async: true

  alias Smolquery.Engine.CallExited
  alias Smolquery.StorageService.Scheduler.Failure

  describe "engine_call_exited?/1" do
    test "matches bare sizing and merge exits" do
      exit = %CallExited{reason: :timeout}

      assert Failure.engine_call_exited?({:sizing_failed, exit})
      assert Failure.engine_call_exited?({:merge_failed, exit})
    end

    test "matches a final COPY's exit, which the store wraps" do
      wrapped =
        {:put_failed, "analytics/events/x.parquet",
         {:merge_failed, %CallExited{reason: :timeout}}}

      assert Failure.engine_call_exited?(wrapped)
    end

    test "does not match plain errors" do
      refute Failure.engine_call_exited?({:merge_failed, %Adbc.Error{message: "x"}})

      refute Failure.engine_call_exited?(
               {:put_failed, "x.parquet", {:merge_failed, %Adbc.Error{message: "x"}}}
             )
    end
  end

  describe "temp_cap_bytes/1 (T-603)" do
    @offload "Out of Memory Error: failed to offload data block of size 256.0 KiB " <>
               "(22.3 GiB/22.3 GiB used)"

    test "reads the temp limit a merge ran into, bare or wrapped by the store" do
      bytes = trunc(22.3 * 1_073_741_824)

      assert Failure.temp_cap_bytes({:merge_failed, %Adbc.Error{message: @offload}}) ==
               {:ok, bytes}

      assert Failure.temp_cap_bytes(
               {:put_failed, "k", {:merge_failed, %Adbc.Error{message: @offload}}}
             ) == {:ok, bytes}
    end

    test "answers :error for a memory OOM and anything else" do
      memory =
        "Out of Memory Error: could not allocate block of size 256.0 KiB (30.4 MiB/30.5 MiB used)"

      assert Failure.temp_cap_bytes({:merge_failed, %Adbc.Error{message: memory}}) == :error
      assert Failure.temp_cap_bytes(:boom) == :error
    end
  end
end
