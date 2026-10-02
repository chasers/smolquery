defmodule Smolquery.Cluster.RpcClientsTest do
  use ExUnit.Case, async: false

  alias Smolquery.Cluster.RpcClients
  alias Smolquery.Test.Eventually

  @moduletag :integration

  @peer_port 15_373
  @other_port 15_374

  setup do
    ensure_distributed()

    previous_per_node = :application.get_env(:gen_rpc, :client_config_per_node)
    on_exit(fn -> restore_per_node(previous_per_node) end)

    {os_pid, node} = start_peer(@peer_port)
    name = :"rpc_clients_#{:erlang.unique_integer([:positive])}"
    {:ok, _pid} = RpcClients.start_link(name: name, probe_timeout_ms: 200)

    %{name: name, node: node, os_pid: os_pid}
  end

  test "clients/1 lists every channel's client to a node", %{node: node} do
    open_channels(node)

    assert [_control, _bulk, _scatter] = RpcClients.clients(node)
  end

  test "drop/1 kills a node's clients and leaves other nodes' alone", %{node: node} do
    {_os_pid, other} = start_peer(@other_port)
    open_channels(node)
    open_channels(other)

    pids = RpcClients.clients(node)

    assert RpcClients.drop(node) == 3
    refute Enum.any?(pids, &Process.alive?/1)
    assert Eventually.until(fn -> RpcClients.clients(node) == [] end)
    assert [_control, _bulk, _scatter] = RpcClients.clients(other)
    assert {:ok, _node} = remote_node(other, :control)
  end

  test "keeps a node's clients on nodeup", %{name: name, node: node} do
    open_channels(node)
    pids = Enum.sort(RpcClients.clients(node))

    send(name, {:nodeup, node, []})
    :sys.get_state(name)

    assert Enum.sort(RpcClients.clients(node)) == pids
    assert Enum.all?(pids, &Process.alive?/1)
  end

  test "a frozen peer's stale client is dropped on nodedown and the next call after reconnect dials fresh",
       %{node: node, os_pid: os_pid} do
    open_channels(node)
    [stale | _rest] = RpcClients.clients(node)

    freeze(os_pid)
    on_exit(fn -> thaw(os_pid) end)

    assert {:badrpc, :timeout} = :gen_rpc.call({node, :control}, :erlang, :node, [], 200)
    assert Process.alive?(stale)

    true = Node.disconnect(node)

    assert Eventually.until(fn -> RpcClients.clients(node) == [] end)
    refute Process.alive?(stale)

    thaw(os_pid)
    true = Node.connect(node)

    started = System.monotonic_time(:millisecond)
    assert Eventually.until(fn -> remote_node(node, :control) == {:ok, node} end, 200, 10)
    assert System.monotonic_time(:millisecond) - started < 2_000
  end

  test "check/2 kills a channel whose probe goes unanswered and leaves the others",
       %{name: name, node: node, os_pid: os_pid} do
    open_channels(node)
    control = client({node, :control})

    freeze(os_pid)
    on_exit(fn -> thaw(os_pid) end)

    assert RpcClients.check({node, :control}, name) == :ok

    assert Eventually.until(fn -> client({node, :control}) == :undefined end)
    refute Process.alive?(control)
    assert is_pid(client({node, {:bulk, 1}}))
    assert is_pid(client({node, {:scatter, 1}}))
  end

  test "check/2 keeps a channel busy with a slow call", %{name: name, node: node} do
    open_channels(node)
    control = client({node, :control})

    slow = Task.async(fn -> :gen_rpc.call({node, :control}, :timer, :sleep, [3_000], 10_000) end)

    assert Eventually.until(fn -> sleeping?(node) end)

    assert RpcClients.check({node, :control}, name) == :ok

    assert Eventually.until(fn -> :sys.get_state(name).probes == %{} end)
    assert Task.yield(slow, 0) == nil
    assert client({node, :control}) == control
    assert Task.await(slow, 10_000) == :ok
  end

  test "check/2 never dials a destination with no client", %{name: name, node: node} do
    assert RpcClients.check({node, {:bulk, 9}}, name) == :ok

    assert Eventually.until(fn -> :sys.get_state(name).probes == %{} end)
    assert client({node, {:bulk, 9}}) == :undefined
  end

  test "check/2 runs one probe per destination at a time",
       %{name: name, node: node, os_pid: os_pid} do
    open_channels(node)
    freeze(os_pid)
    on_exit(fn -> thaw(os_pid) end)

    RpcClients.check({node, :control}, name)
    RpcClients.check({node, :control}, name)
    RpcClients.check({node, {:bulk, 1}}, name)

    assert map_size(:sys.get_state(name).probes) == 2
  end

  defp sleeping?(node) do
    node
    |> :erpc.call(:erlang, :processes, [])
    |> Enum.any?(fn pid ->
      :erpc.call(node, :erlang, :process_info, [pid, :current_function]) ==
        {:current_function, {:timer, :sleep, 1}}
    end)
  end

  defp client(destination), do: :gen_rpc_registry.whereis_name({:client, destination})

  defp open_channels(node) do
    for key <- [:control, {:bulk, 1}, {:scatter, 1}] do
      assert {:ok, ^node} = remote_node(node, key)
    end
  end

  defp remote_node(node, key) do
    case :gen_rpc.call({node, key}, :erlang, :node, [], 1_000) do
      ^node -> {:ok, node}
      other -> {:error, other}
    end
  end

  defp freeze(os_pid), do: {_out, 0} = System.cmd("kill", ["-STOP", os_pid])
  defp thaw(os_pid), do: System.cmd("kill", ["-CONT", os_pid])

  defp ensure_distributed do
    case Node.start(:"smolquery_primary@127.0.0.1", :longnames) do
      {:ok, _pid} -> :ok
      {:error, {:already_started, _pid}} -> :ok
    end

    Node.set_cookie(:smolquery_test_cookie)
  end

  defp start_peer(port) do
    {:ok, peer, node} =
      :peer.start_link(%{
        name: :"rpc_clients_peer_#{:erlang.unique_integer([:positive])}",
        host: ~c"127.0.0.1",
        longnames: true,
        connection: :standard_io,
        args: [~c"-setcookie", ~c"smolquery_test_cookie"]
      })

    on_exit(fn -> safely_stop(peer) end)

    :peer.call(peer, :code, :add_paths, [:code.get_path()])

    for {key, value} <- [
          tcp_server_port: port,
          tcp_client_port: port,
          rpc_module_control: :whitelist,
          rpc_module_list: [:erlang, :timer, Smolquery.Cluster.RpcProbe]
        ] do
      :peer.call(peer, :application, :set_env, [:gen_rpc, key, value, [persistent: true]])
    end

    {:ok, _started} = :peer.call(peer, :application, :ensure_all_started, [:gen_rpc])
    true = Node.connect(node)
    route(node, port)

    {:peer.call(peer, :os, :getpid, []) |> List.to_string(), node}
  end

  defp route(node, port) do
    routes =
      case :application.get_env(:gen_rpc, :client_config_per_node) do
        {:ok, {:internal, map}} -> map
        _unset -> %{}
      end

    :application.set_env(
      :gen_rpc,
      :client_config_per_node,
      {:internal, Map.put(routes, node, port)},
      persistent: true
    )
  end

  defp safely_stop(peer) do
    :peer.stop(peer)
  catch
    :exit, _reason -> :ok
  end

  defp restore_per_node({:ok, value}),
    do: :application.set_env(:gen_rpc, :client_config_per_node, value, persistent: true)

  defp restore_per_node(:undefined),
    do: :application.unset_env(:gen_rpc, :client_config_per_node, persistent: true)
end
