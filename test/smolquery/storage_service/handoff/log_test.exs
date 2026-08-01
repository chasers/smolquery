defmodule Smolquery.StorageService.Handoff.LogTest do
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog

  alias Smolquery.StorageService.Handoff
  alias Smolquery.StorageService.Runtime

  setup do
    previous = Logger.level()
    Logger.configure(level: :info)
    on_exit(fn -> Logger.configure(level: previous) end)

    %{runtime: Runtime.new(name: __MODULE__.Instance, dir: "/tmp/handoff-log-test")}
  end

  describe "seal/4" do
    test "reports the claim and refuses to claim success", %{runtime: runtime} do
      log =
        capture_log(fn ->
          assert Handoff.Log.seal([], runtime, {"analytics", "events"}, ["a", "b", "c"]) ==
                   {:error, :not_implemented}
        end)

      assert log =~ "seal analytics.events: 3 micro-segments claimed"
      assert log =~ "merge not implemented"
    end

    test "dispatches through the behaviour", %{runtime: runtime} do
      capture_log(fn ->
        assert Handoff.seal({Handoff.Log, []}, runtime, {"analytics", "events"}, ["a"]) ==
                 {:error, :not_implemented}
      end)
    end
  end
end
