defmodule Smolquery.BufferService.SealLogTest do
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog

  alias Smolquery.BufferService.SealLog

  setup do
    previous = Logger.level()
    Logger.configure(level: :info)
    on_exit(fn -> Logger.configure(level: previous) end)
  end

  describe "seal_ready/3" do
    test "acknowledges the signal and says what nobody is sealing" do
      log =
        capture_log(fn ->
          assert SealLog.seal_ready([], {"analytics", "events"}, ["a", "b", "c"]) == :ok
        end)

      assert log =~ "seal_ready analytics.events: 3 unsealed micro-segments"
    end
  end
end
