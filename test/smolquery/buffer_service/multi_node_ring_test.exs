defmodule Smolquery.BufferService.MultiNodeRingTest do
  @moduledoc """
  Milestone 8 L4: the ring adapts as a second buffer node joins and leaves,
  with no restart and no static `ring:` config change — replacing what
  `Smolquery.BufferService.Routing`'s pre-M8 moduledoc called out as future
  work ("ring changes at runtime are Milestone 8").
  """

  use ExUnit.Case, async: false

  alias Smolquery.BufferService.Client
  alias Smolquery.BufferService.Ring
  alias Smolquery.BufferService.Routing
  alias Smolquery.Cluster
  alias Smolquery.Schema

  @moduletag :integration
  @moduletag :tmp_dir

  # Matches config/test.exs's gen_rpc port — already listening for the whole
  # suite, the same fixed point `gen_rpc_test.exs` builds its own peer around.
  @primary_port 15_371
  @peer_port 15_473

  defp batch(range) do
    %{schema: Schema.new!([{"id", :int64}]), rows: for(i <- range, do: %{"id" => i})}
  end

  setup context do
    ensure_distributed()
    ensure_pg_scope!()

    previous_cluster = Application.fetch_env(:smolquery, Cluster)

    Application.put_env(:smolquery, Cluster,
      enabled: true,
      postgres: [
        hostname: "localhost",
        port: 5432,
        username: "postgres",
        password: "postgres",
        database: "postgres"
      ]
    )

    name = :"ring_#{:erlang.unique_integer([:positive])}"

    options = [
      name: name,
      dir: Path.join(context.tmp_dir, "primary"),
      flush_interval_ms: 25,
      hot_server_port: 0,
      ring: [node()]
    ]

    previous_gen_rpc = :application.get_env(:gen_rpc, :client_config_per_node)
    previous_buffer = Application.fetch_env(:smolquery, Smolquery.BufferService)

    Application.put_env(:smolquery, Smolquery.BufferService, options)
    {:ok, primary_sup} = Smolquery.BufferService.Supervisor.start_link(options)
    Routing.forget(name)

    on_exit(fn ->
      safely_stop_supervisor(primary_sup)
      Routing.forget(name)
      restore_buffer_env(previous_buffer)
      restore_gen_rpc(previous_gen_rpc)
      restore_cluster(previous_cluster)
    end)

    %{name: name}
  end

  test "a joining peer takes ownership of the tables the ring hands it, a leaving one gives it back",
       %{name: name} do
    {:ok, peer, peer_node} = start_peer()

    :erpc.call(peer_node, :code, :add_paths, [:code.get_path()])
    :erpc.call(peer_node, Application, :put_env, [:smolquery, :roles, [:buffer]])

    :erpc.call(peer_node, Application, :put_env, [
      :smolquery,
      Cluster,
      [
        enabled: true,
        postgres: [
          hostname: "localhost",
          port: 5432,
          username: "postgres",
          password: "postgres",
          database: "postgres"
        ]
      ]
    ])

    peer_options = [
      name: name,
      dir: "#{System.tmp_dir!()}/smolquery_ring_peer_#{:erlang.unique_integer([:positive])}",
      flush_interval_ms: 25,
      hot_server_port: 0,
      ring: [peer_node]
    ]

    :erpc.call(peer_node, Application, :put_env, [
      :smolquery,
      Smolquery.BufferService,
      peer_options
    ])

    peer_gen_rpc = [
      tcp_server_port: @peer_port,
      tcp_client_port: @peer_port,
      rpc_module_control: :whitelist,
      rpc_module_list: [Smolquery.BufferService.Endpoint]
    ]

    for {key, value} <- peer_gen_rpc do
      :erpc.call(peer_node, :application, :set_env, [:gen_rpc, key, value, [persistent: true]])
    end

    :erpc.call(peer_node, :application, :set_env, [
      :gen_rpc,
      :client_config_per_node,
      {:internal, %{node() => @primary_port}},
      [persistent: true]
    ])

    :application.set_env(
      :gen_rpc,
      :client_config_per_node,
      {:internal, %{peer_node => @peer_port}},
      persistent: true
    )

    # Started through the real application supervisor (not a bare
    # `:pg.start_link/1` erpc call, which dies the moment its transient
    # erpc-worker caller exits) — the same way `Smolquery.Cluster.children/0`
    # runs in production.
    {:ok, _started} = :erpc.call(peer_node, Application, :ensure_all_started, [:smolquery])
    :erpc.call(peer_node, Smolquery.BufferService.Routing, :forget, [name])

    two_node_ring = Ring.new!([node(), peer_node])
    peer_table = find_table_owned_by(two_node_ring, peer_node)
    primary_table = find_table_owned_by(two_node_ring, node())

    assert wait_until(fn ->
             Routing.resolve(name).ring |> Ring.nodes() == Enum.sort([node(), peer_node])
           end)

    assert Routing.resolve(name) |> Routing.owner(peer_table) == peer_node
    assert Routing.resolve(name) |> Routing.owner(primary_table) == node()

    assert {:ok, _ack} = Client.write_batch(name, peer_table, batch(1..3))
    assert {:ok, _ack} = Client.write_batch(name, primary_table, batch(1..3))

    safely_stop(peer)

    assert wait_until(fn -> Routing.resolve(name).ring |> Ring.nodes() == [node()] end)
    assert Routing.resolve(name) |> Routing.owner(peer_table) == node()

    assert {:ok, _ack} = Client.write_batch(name, peer_table, batch(4..6))
  end

  defp find_table_owned_by(ring, target_node) do
    Enum.find(1..1_000, fn i ->
      Ring.owner(ring, {"analytics", "t#{i}"}) == target_node
    end)
    |> then(&{"analytics", "t#{&1}"})
  end

  defp ensure_distributed do
    case Node.start(:"smolquery_ring_primary@127.0.0.1", :longnames) do
      {:ok, _pid} -> :ok
      {:error, {:already_started, _pid}} -> :ok
    end

    Node.set_cookie(:smolquery_test_cookie)
  end

  defp ensure_pg_scope! do
    case :pg.start_link(Cluster.pg_scope()) do
      {:ok, _pid} -> :ok
      {:error, {:already_started, _pid}} -> :ok
    end
  end

  defp start_peer do
    {:ok, peer, node} =
      :peer.start_link(%{
        name: :"ring_peer_#{:erlang.unique_integer([:positive])}",
        host: ~c"127.0.0.1",
        longnames: true,
        args: [~c"-setcookie", ~c"smolquery_test_cookie"]
      })

    on_exit(fn -> safely_stop(peer) end)

    {:ok, peer, node}
  end

  defp wait_until(fun, attempts \\ 100)
  defp wait_until(_fun, 0), do: false

  defp wait_until(fun, attempts) do
    if fun.() do
      true
    else
      Process.sleep(50)
      wait_until(fun, attempts - 1)
    end
  end

  defp safely_stop(peer) do
    :peer.stop(peer)
  catch
    :exit, _reason -> :ok
  end

  defp safely_stop_supervisor(pid) do
    Supervisor.stop(pid)
  catch
    :exit, _reason -> :ok
  end

  defp restore_buffer_env({:ok, config}),
    do: Application.put_env(:smolquery, Smolquery.BufferService, config)

  defp restore_buffer_env(:error),
    do: Application.delete_env(:smolquery, Smolquery.BufferService)

  defp restore_gen_rpc({:ok, value}),
    do: :application.set_env(:gen_rpc, :client_config_per_node, value, persistent: true)

  defp restore_gen_rpc(:undefined),
    do: :application.unset_env(:gen_rpc, :client_config_per_node, persistent: true)

  defp restore_cluster({:ok, value}), do: Application.put_env(:smolquery, Cluster, value)
  defp restore_cluster(:error), do: Application.delete_env(:smolquery, Cluster)
end
