defmodule Smolquery.StorageService.MultiNodeSealOwnershipTest do
  @moduledoc """
  Milestone 8 L6 (PL-11 D6): two real StorageService nodes sharing one
  storage_ring never both act on the same table's seal signal.

  A genuine two-node proof, not a simulated one: a second node is a real
  `:peer`-booted BEAM running its own `Smolquery.StorageService.Supervisor`,
  reached over plain Erlang distribution the same way
  `Smolquery.StorageService.Sealer.seal_ready/4` reaches a remote owner in
  production. Both nodes are told to seal both tables — the worst case a
  stale or duplicated signal could produce — and only the owner named by the
  shared `ring` ever hands its table to `Smolquery.StorageService.Handoff`.
  """

  use ExUnit.Case, async: false

  alias Smolquery.BufferService.Ring
  alias Smolquery.StorageService.Runtime
  alias Smolquery.StorageService.Sealer
  alias Smolquery.StorageService.Supervisor, as: StorageSupervisor
  alias Smolquery.Test.HandoffProbe

  @moduletag :integration
  @moduletag :tmp_dir

  defp claim(ids), do: %{ids: ids, keys: ["analytics/events/sealed.parquet"]}

  setup context do
    ensure_distributed()

    name = :"seal_ownership_#{:erlang.unique_integer([:positive])}"
    {:ok, _peer, peer_node} = start_peer()

    ring = Enum.sort([node(), peer_node])
    test = self()

    {:ok, primary_sup} =
      StorageSupervisor.start_link(
        name: name,
        dir: Path.join(context.tmp_dir, "primary"),
        ring: ring,
        engine_extensions: [],
        catalog: [
          metadata: "sqlite:#{Path.join(context.tmp_dir, "primary_catalog.sqlite")}",
          data_path: Path.join(context.tmp_dir, "primary_ducklake")
        ],
        handoff: {HandoffProbe, {test, :ok}}
      )

    on_exit(fn -> safely_stop_supervisor(primary_sup) end)

    :erpc.call(peer_node, :code, :add_paths, [:code.get_path()])

    peer_dir =
      "#{System.tmp_dir!()}/smolquery_seal_ownership_peer_#{:erlang.unique_integer([:positive])}"

    peer_opts = [
      name: name,
      dir: Path.join(peer_dir, "storage"),
      ring: ring,
      engine_extensions: [],
      catalog: [
        metadata: "sqlite:#{Path.join(peer_dir, "catalog.sqlite")}",
        data_path: Path.join(peer_dir, "ducklake")
      ],
      handoff: {HandoffProbe, {test, :ok}}
    ]

    :erpc.call(peer_node, Application, :put_env, [:smolquery, :roles, [:storage]])

    :erpc.call(peer_node, Application, :put_env, [:smolquery, Smolquery.StorageService, peer_opts])

    # Started through the real application supervisor (not a bare
    # `Supervisor.start_link/2` erpc call, which dies the moment its transient
    # erpc-worker caller exits) — the same fix `multi_node_ring_test.exs`
    # documents for `:pg`.
    {:ok, _started} = :erpc.call(peer_node, Application, :ensure_all_started, [:smolquery])

    assert wait_until(fn -> peer_sealer_alive?(peer_node, name) end)

    two_node_ring = Ring.new!(ring)

    %{
      name: name,
      peer_node: peer_node,
      primary_table: find_table_owned_by(two_node_ring, node()),
      peer_table: find_table_owned_by(two_node_ring, peer_node)
    }
  end

  test "a table is only ever handed to its ring owner's handoff, never both",
       %{name: name, peer_node: peer_node, primary_table: primary_table, peer_table: peer_table} do
    # Both nodes are told about both tables — the worst case a stale or
    # duplicated signal could produce (PL-11 D6: a wrong transient owner is
    # safe by construction, not something the gate needs to prevent, but a
    # signal a genuine non-owner sees must still be ignored outright).
    Sealer.seal_ready(name, primary_table, claim(["a"]), node())
    Sealer.seal_ready(name, primary_table, claim(["a"]), peer_node)
    Sealer.seal_ready(name, peer_table, claim(["b"]), node())
    Sealer.seal_ready(name, peer_table, claim(["b"]), peer_node)

    assert_receive {:sealing, ^primary_table, %{ids: ["a"]}, primary_attempt}
    assert_receive {:sealing, ^peer_table, %{ids: ["b"]}, peer_attempt}

    assert node(primary_attempt) == node()
    assert node(peer_attempt) == peer_node

    HandoffProbe.release(primary_attempt)
    HandoffProbe.release(peer_attempt)

    refute_receive {:sealing, ^primary_table, _claim, _attempt}, 200
    refute_receive {:sealing, ^peer_table, _claim, _attempt}, 200
  end

  defp find_table_owned_by(ring, target_node) do
    Enum.find(1..1_000, fn i ->
      Ring.owner(ring, {"analytics", "t#{i}"}) == target_node
    end)
    |> then(&{"analytics", "t#{&1}"})
  end

  defp peer_sealer_alive?(peer_node, name) do
    case :erpc.call(peer_node, Process, :whereis, [Runtime.sealer(name)]) do
      pid when is_pid(pid) -> true
      _ -> false
    end
  end

  defp ensure_distributed do
    case Node.start(:"smolquery_seal_ownership_primary@127.0.0.1", :longnames) do
      {:ok, _pid} -> :ok
      {:error, {:already_started, _pid}} -> :ok
    end

    Node.set_cookie(:smolquery_test_cookie)
  end

  defp start_peer do
    {:ok, peer, node} =
      :peer.start_link(%{
        name: :"seal_ownership_peer_#{:erlang.unique_integer([:positive])}",
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
end
