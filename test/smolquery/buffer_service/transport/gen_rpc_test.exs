defmodule Smolquery.BufferService.Transport.GenRpcTest do
  use ExUnit.Case, async: false

  alias Smolquery.BufferService.Client
  alias Smolquery.BufferService.Routing
  alias Smolquery.Schema

  @moduletag :integration
  @moduletag :tmp_dir

  @table {"analytics", "events"}
  @primary_port 15_371
  @peer_port 15_372

  defp batch(range) do
    %{schema: Schema.new!([{"id", :int64}]), rows: for(i <- range, do: %{"id" => i})}
  end

  setup context do
    ensure_distributed()

    name = :"buffer_#{:erlang.unique_integer([:positive])}"
    {:ok, peer, node} = start_peer()

    options = [
      name: name,
      dir: Path.join(context.tmp_dir, "buffer"),
      flush_interval_ms: 25,
      write_pool_size: 1,
      encode_concurrency: 1,
      ring: [node]
    ]

    :erpc.call(node, :code, :add_paths, [:code.get_path()])
    # Mix must run wherever its beams are loadable: phoenix (1.8.10+) reads
    # `Mix.Project.config()` at boot and dies on a Mix-less peer otherwise.
    {:ok, _mix} = :erpc.call(node, Application, :ensure_all_started, [:mix])
    :erpc.call(node, Application, :put_env, [:smolquery, :roles, [:buffer]])
    :erpc.call(node, Application, :put_env, [:smolquery, Smolquery.BufferService, options])

    peer_gen_rpc = [
      tcp_server_port: @peer_port,
      tcp_client_port: @peer_port,
      rpc_module_control: :whitelist,
      rpc_module_list: [Smolquery.BufferService.Endpoint]
    ]

    for {key, value} <- peer_gen_rpc do
      :erpc.call(node, :application, :set_env, [:gen_rpc, key, value, [persistent: true]])
    end

    :erpc.call(node, :application, :set_env, [
      :gen_rpc,
      :client_config_per_node,
      {:internal, %{node() => @primary_port}},
      [persistent: true]
    ])

    {:ok, _started} = :erpc.call(node, Application, :ensure_all_started, [:smolquery])

    previous_per_node = :application.get_env(:gen_rpc, :client_config_per_node)
    previous_buffer = Application.fetch_env(:smolquery, Smolquery.BufferService)

    :application.set_env(:gen_rpc, :client_config_per_node, {:internal, %{node => @peer_port}},
      persistent: true
    )

    Application.put_env(:smolquery, Smolquery.BufferService, options)
    Routing.forget(name)

    on_exit(fn ->
      Routing.forget(name)
      restore_buffer_env(previous_buffer)
      restore_per_node(previous_per_node)
    end)

    %{name: name, node: node, peer: peer}
  end

  test "routes a write to the owning node and acks from there", %{name: name, node: node} do
    assert Client.owner(name, @table) == node
    refute node == node()

    assert {:ok, ack} = Client.write_batch(name, @table, batch(1..3))
    assert ack.row_count == 3
    assert is_binary(ack.segment_id)
  end

  test "reads the hot manifest back off the owning node", %{name: name} do
    {:ok, ack} = Client.write_batch(name, @table, batch(1..2))

    assert {:ok, [entry]} = Client.hot_manifest(name, @table)
    assert entry.id == ack.segment_id
    assert entry.row_count == 2
    assert entry.stats["id"] == %{min: 1, max: 2, null_count: 0}
  end

  test "retires remotely, and the stamp is visible remotely", %{name: name} do
    {:ok, ack} = Client.write_batch(name, @table, batch(1..1))

    assert Client.retire(name, @table, [ack.segment_id], 42) == :ok
    assert {:ok, [entry]} = Client.hot_manifest(name, @table)
    assert entry.sealed_at == 42
  end

  test "leaves Erlang distribution unused for buffer traffic", %{name: name, node: node} do
    {:ok, _ack} = Client.write_batch(name, @table, batch(1..1))

    assert node in :gen_rpc.nodes()
  end

  test "reports an unreachable owner rather than raising", %{name: name} do
    Application.put_env(:smolquery, Smolquery.BufferService, ring: [:"gone@127.0.0.1"])
    Routing.forget(name)

    assert {:error, {kind, _reason}} = Client.write_batch(name, @table, batch(1..1))
    assert kind in [:badrpc, :badtcp]
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
        name: :"buffer_peer_#{:erlang.unique_integer([:positive])}",
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

  defp restore_buffer_env({:ok, config}),
    do: Application.put_env(:smolquery, Smolquery.BufferService, config)

  defp restore_buffer_env(:error),
    do: Application.delete_env(:smolquery, Smolquery.BufferService)

  defp restore_per_node({:ok, value}),
    do: :application.set_env(:gen_rpc, :client_config_per_node, value, persistent: true)

  defp restore_per_node(:undefined),
    do: :application.unset_env(:gen_rpc, :client_config_per_node, persistent: true)
end
