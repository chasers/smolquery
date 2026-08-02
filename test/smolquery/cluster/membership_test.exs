defmodule Smolquery.Cluster.MembershipTest do
  use ExUnit.Case, async: false

  alias Smolquery.Cluster.Membership

  @moduletag :integration

  setup do
    ensure_distributed()
    name = :"membership_#{:erlang.unique_integer([:positive])}"
    {:ok, _pid} = Membership.start_link(name: name, debounce_ms: 300)
    %{name: name}
  end

  test "delivers the current members immediately on subscribe", %{name: name} do
    assert :ok = Membership.subscribe(name)
    assert_receive {:cluster_membership, members}
    assert members == Membership.members(name)
    assert node() in members
  end

  test "broadcasts a debounced update when a node joins, then leaves", %{name: name} do
    :ok = Membership.subscribe(name)
    assert_receive {:cluster_membership, _initial}

    {:ok, peer, node} = start_peer()

    assert_receive {:cluster_membership, joined}, 2_000
    assert node in joined

    safely_stop(peer)

    assert_receive {:cluster_membership, left}, 2_000
    refute node in left
  end

  test "coalesces nodes joining inside the debounce window into one broadcast" do
    name = :"membership_coalesce_#{:erlang.unique_integer([:positive])}"
    {:ok, _pid} = Membership.start_link(name: name, debounce_ms: 1_000)

    :ok = Membership.subscribe(name)
    assert_receive {:cluster_membership, initial}

    [{:ok, peer_a, node_a}, {:ok, peer_b, node_b}] =
      [Task.async(&boot_peer/0), Task.async(&boot_peer/0)]
      |> Task.await_many(15_000)

    on_exit(fn ->
      safely_stop(peer_a)
      safely_stop(peer_b)
    end)

    assert_receive {:cluster_membership, members}, 3_000
    assert node_a in members
    assert node_b in members
    refute_receive {:cluster_membership, _more}, 250

    safely_stop(peer_a)
    safely_stop(peer_b)

    assert length(members) == length(initial) + 2
  end

  test "a subscriber that exits is dropped without needing to unsubscribe", %{name: name} do
    task = Task.async(fn -> Membership.subscribe(name) end)
    Task.await(task)

    assert :ok = Membership.subscribe(name)
    assert_receive {:cluster_membership, _members}
  end

  defp ensure_distributed do
    case Node.start(:"smolquery_membership_primary@127.0.0.1", :longnames) do
      {:ok, _pid} -> :ok
      {:error, {:already_started, _pid}} -> :ok
    end

    Node.set_cookie(:smolquery_test_cookie)
  end

  defp start_peer do
    {:ok, peer, node} =
      :peer.start_link(%{
        name: :"membership_peer_#{:erlang.unique_integer([:positive])}",
        host: ~c"127.0.0.1",
        longnames: true,
        args: [~c"-setcookie", ~c"smolquery_test_cookie"]
      })

    on_exit(fn -> safely_stop(peer) end)

    {:ok, peer, node}
  end

  defp boot_peer do
    :peer.start(%{
      name: :"membership_peer_#{:erlang.unique_integer([:positive])}",
      host: ~c"127.0.0.1",
      longnames: true,
      args: [~c"-setcookie", ~c"smolquery_test_cookie"]
    })
  end

  defp safely_stop(peer) do
    :peer.stop(peer)
  catch
    :exit, _reason -> :ok
  end
end
