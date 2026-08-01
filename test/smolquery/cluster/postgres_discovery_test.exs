defmodule Smolquery.Cluster.PostgresDiscoveryTest do
  use ExUnit.Case, async: false

  alias Smolquery.Cluster
  alias Smolquery.Cluster.Membership

  @moduletag :integration

  setup do
    ensure_distributed()

    previous = Application.fetch_env(:smolquery, Cluster)
    Application.put_env(:smolquery, Cluster, enabled: true, postgres: postgres_config())

    on_exit(fn -> restore(previous) end)

    {:ok, sup} = Supervisor.start_link(Cluster.children(), strategy: :one_for_one)
    on_exit(fn -> safely_stop_supervisor(sup) end)

    :ok
  end

  test "a peer node configured against the same Postgres is discovered" do
    :ok = Membership.subscribe()
    assert_receive {:cluster_membership, _initial}

    {:ok, peer, node} = start_peer()

    :erpc.call(node, :code, :add_paths, [:code.get_path()])
    :erpc.call(node, Application, :put_env, [:smolquery, :roles, []])

    :erpc.call(node, Application, :put_env, [
      :smolquery,
      Cluster,
      [enabled: true, postgres: postgres_config()]
    ])

    {:ok, _started} = :erpc.call(node, Application, :ensure_all_started, [:smolquery])

    assert_receive {:cluster_membership, members}, 15_000
    assert node in members

    safely_stop(peer)

    assert_receive {:cluster_membership, members}, 15_000
    refute node in members
  end

  defp postgres_config do
    [
      hostname: System.get_env("TEST_POSTGRES_HOST", "localhost"),
      port: System.get_env("TEST_POSTGRES_PORT", "5432") |> String.to_integer(),
      username: System.get_env("TEST_POSTGRES_USER", "postgres"),
      password: System.get_env("TEST_POSTGRES_PASSWORD", "postgres"),
      database: System.get_env("TEST_POSTGRES_DATABASE", "postgres")
    ]
  end

  defp ensure_distributed do
    case Node.start(:"smolquery_cluster_primary@127.0.0.1", :longnames) do
      {:ok, _pid} -> :ok
      {:error, {:already_started, _pid}} -> :ok
    end

    Node.set_cookie(:smolquery_test_cookie)
  end

  defp start_peer do
    {:ok, peer, node} =
      :peer.start_link(%{
        name: :"cluster_peer_#{:erlang.unique_integer([:positive])}",
        host: ~c"127.0.0.1",
        longnames: true,
        args: [~c"-setcookie", ~c"smolquery_test_cookie"]
      })

    on_exit(fn -> safely_stop(peer) end)

    {:ok, peer, node}
  end

  defp safely_stop(peer) do
    :peer.stop(peer)
  catch
    :exit, _reason -> :ok
  end

  defp safely_stop_supervisor(sup) do
    Supervisor.stop(sup)
  catch
    :exit, _reason -> :ok
  end

  defp restore({:ok, value}), do: Application.put_env(:smolquery, Cluster, value)
  defp restore(:error), do: Application.delete_env(:smolquery, Cluster)
end
