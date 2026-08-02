defmodule Smolquery.StorageService.ClientTest do
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog

  alias Smolquery.Cluster
  alias Smolquery.StorageService.Client
  alias Smolquery.StorageService.Routing
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

  describe "seal_ready/3 routing to the storage ring's owner (Milestone 8 L6)" do
    setup context do
      name = :"storage_client_owned_#{:erlang.unique_integer([:positive])}"

      runtime =
        Runtime.new(
          name: name,
          dir: Path.join(context.tmp_dir, "sealed"),
          ring: [node(), :"storage1@elsewhere.invalid"],
          handoff: {HandoffProbe, {self(), :ok}}
        )

      start_supervised!({Task.Supervisor, name: Runtime.seals(name)}, id: {:seals, name})
      start_supervised!({Sealer, runtime}, id: {:sealer, name})
      Runtime.put(runtime)
      on_exit(fn -> Runtime.delete(name) end)

      %{name: name, foreign_table: find_table_owned_by(name, :"storage1@elsewhere.invalid")}
    end

    @tag :tmp_dir
    test "a table this node's ring hands to another node is never handed to the local sealer",
         %{name: name, foreign_table: foreign_table} do
      assert Client.seal_ready([name: name], foreign_table, claim(["a"])) == :ok

      refute_receive {:sealing, ^foreign_table, _claim, _attempt}, 50
    end
  end

  describe "seal_ready/3 with clustering on and no local storage service" do
    setup do
      previous = Application.fetch_env(:smolquery, Cluster)

      Application.put_env(:smolquery, Cluster,
        enabled: true,
        postgres: [hostname: "localhost", port: 5432, username: "postgres", database: "postgres"]
      )

      name = :"storage_client_clustered_#{:erlang.unique_integer([:positive])}"
      on_exit(fn -> Routing.forget(name) end)

      on_exit(fn ->
        case previous do
          {:ok, value} -> Application.put_env(:smolquery, Cluster, value)
          :error -> Application.delete_env(:smolquery, Cluster)
        end
      end)

      %{name: name}
    end

    test "trusts the ring instead of warning that nothing is running here", %{name: name} do
      log =
        capture_log(fn ->
          assert Client.seal_ready([name: name], @events, claim(["a"])) == :ok
        end)

      refute log =~ "is not running on this node"
    end
  end

  defp find_table_owned_by(name, target_node) do
    routing = Routing.resolve(name)

    Enum.find(1..1_000, fn i ->
      Routing.owner(routing, {"analytics", "t#{i}"}) == target_node
    end)
    |> then(&{"analytics", "t#{&1}"})
  end
end
