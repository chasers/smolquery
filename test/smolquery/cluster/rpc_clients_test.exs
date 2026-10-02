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
    {:ok, _pid} = RpcClients.start_link(name: name)

    %{name: name, node: node, os_pid: os_pid}
  end

  test "clients/1 lists every channel's client to a node", %{node: node} do
    open_channels(node)

    assert [_control, _bulk, _scatter] = RpcClients.clients(node)
  end

  test "drop/1 stops a node's clients and leaves other nodes' alone", %{node: node} do
    {_os_pid, other} = start_peer(@other_port)
    open_channels(node)
    open_channels(other)

    pids = RpcClients.clients(node)

    assert RpcClients.drop(node) == 3
    assert RpcClients.clients(node) == []
    refute Enum.any?(pids, &Process.alive?/1)
    assert [_control, _bulk, _scatter] = RpcClients.clients(other)
    assert {:ok, _node} = remote_node(other, :control)
  end

  test "drops a node's clients on nodeup", %{name: name, node: node} do
    open_channels(node)

    send(name, {:nodeup, node, []})

    assert Eventually.until(fn -> RpcClients.clients(node) == [] end)
    assert {:ok, ^node} = remote_node(node, :control)
  end

  test "a frozen peer's stale client is dropped on nodedown and the next call after nodeup dials fresh",
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
    assert Eventually.until(fn -> remote_node(node, :control) == {:ok, node} end)
    assert System.monotonic_time(:millisecond) - started < 2_000
  end

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
          rpc_module_list: [:erlang]
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
