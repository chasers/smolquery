defmodule Smolquery.QueryService.WorkerTransportPeerTest do
  @moduledoc """
  The remote half of `Smolquery.QueryService.WorkerTransport` against a real
  peer node (T-364): the call crosses gen_rpc, lands on the peer's
  `PartialWorker`, and the peer's allowlist admits it. The peer runs no
  query service, so the answer is the worker's own
  `:query_service_unavailable` — proof the call executed there rather than
  failing in transit.
  """

  use ExUnit.Case, async: false

  alias Smolquery.QueryService.WorkerTransport

  @moduletag :integration

  @primary_port 15_371
  @peer_port 15_373
  @request %{statements: [], partial_sql: "SELECT 1", allowed_paths: []}

  setup do
    ensure_distributed()
    {:ok, _peer, node} = start_peer()

    :erpc.call(node, :code, :add_paths, [:code.get_path()])

    peer_gen_rpc = [
      tcp_server_port: @peer_port,
      tcp_client_port: @peer_port,
      rpc_module_control: :whitelist,
      rpc_module_list: [Smolquery.QueryService.PartialWorker],
      client_config_per_node: {:internal, %{node() => @primary_port}}
    ]

    for {key, value} <- peer_gen_rpc do
      :erpc.call(node, :application, :set_env, [:gen_rpc, key, value, [persistent: true]])
    end

    {:ok, _started} = :erpc.call(node, Application, :ensure_all_started, [:gen_rpc])

    previous_per_node = :application.get_env(:gen_rpc, :client_config_per_node)

    :application.set_env(:gen_rpc, :client_config_per_node, {:internal, %{node => @peer_port}},
      persistent: true
    )

    on_exit(fn -> restore_per_node(previous_per_node) end)

    %{node: node}
  end

  test "reaches the peer's PartialWorker over gen_rpc", %{node: node} do
    assert node in Node.list()

    assert WorkerTransport.call(node, :never_started, @request, "job", 5_000) ==
             {:error, :query_service_unavailable}

    assert node in :gen_rpc.nodes()
  end

  test "a module outside the peer's allowlist is refused", %{node: node} do
    destination = WorkerTransport.destination(node, "job")

    assert {:badrpc, :unauthorized} = :gen_rpc.call(destination, Map, :new, [], 5_000)
  end

  defp ensure_distributed do
    case Node.start(:"smolquery_primary@127.0.0.1", :longnames) do
      {:ok, _pid} -> :ok
      {:error, {:already_started, _pid}} -> :ok
    end

    Node.set_cookie(:smolquery_test_cookie)
  end

  defp start_peer do
    {:ok, peer, node} =
      :peer.start_link(%{
        name: :"worker_peer_#{:erlang.unique_integer([:positive])}",
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

  defp restore_per_node({:ok, value}),
    do: :application.set_env(:gen_rpc, :client_config_per_node, value, persistent: true)

  defp restore_per_node(:undefined),
    do: :application.unset_env(:gen_rpc, :client_config_per_node, persistent: true)
end
