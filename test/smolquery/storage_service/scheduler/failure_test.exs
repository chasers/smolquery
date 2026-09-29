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
end
