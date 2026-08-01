defmodule Smolquery.StorageService.ClientTest do
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog

  alias Smolquery.StorageService.Client
  alias Smolquery.StorageService.Runtime
  alias Smolquery.StorageService.Sealer
  alias Smolquery.Test.HandoffProbe

  @events {"analytics", "events"}

  defp claim(ids), do: %{ids: ids, keys: ["analytics/events/sealed.parquet"]}

  describe "seal_ready/3 with a storage service running" do
    setup context do
      name = :"storage_client_#{:erlang.unique_integer([:positive])}"

      runtime =
        Runtime.new(
          name: name,
          dir: Path.join(context.tmp_dir, "sealed"),
          handoff: {HandoffProbe, {self(), :ok}}
        )

      start_supervised!({Task.Supervisor, name: Runtime.seals(name)})
      start_supervised!({Sealer, runtime})
      Runtime.put(runtime)
      on_exit(fn -> Runtime.delete(name) end)

      %{name: name}
    end

    @tag :tmp_dir
    test "reaches the sealer", %{name: name} do
      assert Client.seal_ready([name: name], @events, claim(["a"])) == :ok

      assert_receive {:sealing, @events, %{ids: ["a"]}, attempt}
      HandoffProbe.release(attempt)
    end
  end

  describe "seal_ready/3 without a storage service" do
    test "reports rather than raising, so the signalling buffer survives" do
      log =
        capture_log(fn ->
          assert Client.seal_ready([name: __MODULE__.Absent], @events, claim(["a", "b"])) == :ok
        end)

      assert log =~ "seal_ready analytics.events: 2 unsealed micro-segments"
      assert log =~ "is not running on this node"
    end

    test "defaults to the application's instance name" do
      log = capture_log(fn -> assert Client.seal_ready([], @events, claim([])) == :ok end)

      assert log =~ inspect(Smolquery.StorageService)
    end
  end
end
