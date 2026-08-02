defmodule Smolquery.BufferService.RingEpochTest do
  use ExUnit.Case, async: true

  alias Smolquery.BufferService
  alias Smolquery.BufferService.Client
  alias Smolquery.BufferService.Ring
  alias Smolquery.BufferService.RingEpoch
  alias Smolquery.BufferService.Runtime
  alias Smolquery.BufferService.TableBuffer
  alias Smolquery.Cluster.ConfigStore.Memory
  alias Smolquery.Schema
  alias Smolquery.Test.Eventually

  @other :"buffer2@nonexistent.invalid"
  @third :"buffer3@nonexistent.invalid"

  defp unique_name(prefix), do: :"#{prefix}_#{:erlang.unique_integer([:positive])}"

  defp start_store do
    name = unique_name(:epoch_store)
    start_supervised!({Memory, name: name}, id: name)

    name
  end

  defp start_epoch(name, store, members, opts \\ []) do
    spec =
      {RingEpoch,
       Keyword.merge(
         [name: name, static: [node()], store: {Memory, name: store}, members: members],
         opts
       )}

    start_supervised!(spec, id: {:epoch, name})
    RingEpoch.refresh(name)

    name
  end

  defp ref_owned_by(ring_nodes, owner) do
    ring = Ring.new!(ring_nodes)

    Enum.find(
      for(i <- 1..1_000, do: {"analytics", "table_#{i}"}),
      fn ref -> Ring.owner(ring, ref) == owner end
    )
  end

  defp scope(name), do: "buffer:#{name}"

  test "a node with no epoch keeper accepts everything" do
    assert RingEpoch.check_write(unique_name(:never_started), {"analytics", "events"}) == :ok
  end

  test "the sole member owns every table" do
    name = unique_name(:epoch)
    start_epoch(name, start_store(), fn -> [node()] end)

    assert RingEpoch.check_write(name, {"analytics", "events"}) == :ok
  end

  test "a table the configuration gives another node is refused" do
    name = unique_name(:epoch)
    start_epoch(name, start_store(), fn -> [@other] end)

    assert RingEpoch.check_write(name, {"analytics", "events"}) == {:error, :not_owner}
  end

  test "a fresh acquisition waits out the previous owner's lease, then accepts" do
    name = unique_name(:epoch)
    store = start_store()
    {:ok, _config} = Memory.ensure(store, scope(name), [@other])
    {:ok, _config} = Memory.advance(store, scope(name), 0, [node()])

    start_epoch(name, store, fn -> [node()] end, lease_ms: 150)

    ref = {"analytics", "events"}
    assert RingEpoch.check_write(name, ref) == {:error, :ownership_settling}
    assert Eventually.until(fn -> RingEpoch.check_write(name, ref) == :ok end)
  end

  test "an acquisition older than one lease is already settled" do
    name = unique_name(:epoch)
    store = start_store()
    {:ok, _config} = Memory.ensure(store, scope(name), [@other])
    {:ok, _config} = Memory.advance(store, scope(name), 0, [node()])
    Memory.age(store, scope(name), 60_000)

    start_epoch(name, store, fn -> [node()] end)

    assert RingEpoch.check_write(name, {"analytics", "events"}) == :ok
  end

  test "settling fences only the tables that changed hands" do
    name = unique_name(:epoch)
    store = start_store()
    {:ok, _config} = Memory.ensure(store, scope(name), [node(), @other])
    {:ok, _config} = Memory.advance(store, scope(name), 0, [node()])

    start_epoch(name, store, fn -> [node()] end, lease_ms: 60_000)

    kept = ref_owned_by([node(), @other], node())
    acquired = ref_owned_by([node(), @other], @other)

    assert RingEpoch.check_write(name, kept) == :ok
    assert RingEpoch.check_write(name, acquired) == {:error, :ownership_settling}
  end

  test "a second advance within the lease keeps the first advance's settle" do
    name = unique_name(:epoch)
    store = start_store()
    {:ok, _config} = Memory.ensure(store, scope(name), [@other])
    holder = start_supervised!({Agent, fn -> [node()] end}, id: {:members, name})

    start_epoch(name, store, fn -> Agent.get(holder, & &1) end, lease_ms: 500)

    ref = ref_owned_by([node(), @third], node())
    assert RingEpoch.check_write(name, ref) == {:error, :ownership_settling}

    Agent.update(holder, fn _members -> [node(), @third] end)
    RingEpoch.refresh(name)

    assert {:ok, %{epoch: 2}} = Memory.fetch(store, scope(name))
    assert RingEpoch.check_write(name, ref) == {:error, :ownership_settling}
    assert Eventually.until(fn -> RingEpoch.check_write(name, ref) == :ok end)
  end

  test "a membership change advances the epoch and fences the old view" do
    name = unique_name(:epoch)
    store = start_store()
    holder = start_supervised!({Agent, fn -> [node()] end}, id: {:members, name})

    start_epoch(name, store, fn -> Agent.get(holder, & &1) end)
    assert RingEpoch.check_write(name, {"analytics", "events"}) == :ok

    Agent.update(holder, fn _members -> [@other] end)
    RingEpoch.refresh(name)

    assert RingEpoch.check_write(name, {"analytics", "events"}) == {:error, :not_owner}
    assert {:ok, config} = Memory.fetch(store, scope(name))
    assert config.epoch == 1
    assert config.members == [@other]
    assert config.prev_members == [node()]
  end

  test "a keeper that stops verifying fails closed one lease later" do
    name = unique_name(:epoch)
    start_epoch(name, start_store(), fn -> [node()] end, lease_ms: 100, refresh_ms: 20)

    ref = {"analytics", "events"}
    assert RingEpoch.check_write(name, ref) == :ok

    stop_supervised!({:epoch, name})

    assert Eventually.until(fn ->
             RingEpoch.check_write(name, ref) == {:error, :ring_config_stale}
           end)
  end

  test "advance is compare-and-swap: a stale epoch conflicts" do
    store = start_store()
    {:ok, _config} = Memory.ensure(store, "buffer:cas", [node()])
    {:ok, _config} = Memory.advance(store, "buffer:cas", 0, [@other])

    assert Memory.advance(store, "buffer:cas", 0, [node()]) == {:error, :conflict}
    assert {:ok, %{epoch: 1}} = Memory.fetch(store, "buffer:cas")
  end

  test "advance on a missing scope conflicts, and the refetch answers not_found" do
    store = start_store()

    assert Memory.advance(store, "buffer:absent", 0, [node()]) == {:error, :conflict}
    assert Memory.fetch(store, "buffer:absent") == :not_found
  end

  describe "wired into a running buffer instance" do
    @describetag :tmp_dir

    defp start_buffer_service(context) do
      name = unique_name(:buffer)

      start_supervised!(
        {BufferService.Supervisor,
         name: name, dir: Path.join(context.tmp_dir, "buffer"), flush_interval_ms: 25},
        id: name
      )

      on_exit(fn -> Runtime.delete(name) end)

      name
    end

    defp batch(rows), do: %{schema: Schema.new!([{"id", :int64}]), rows: rows}

    test "the endpoint refuses a write for a table the fence denies", context do
      name = start_buffer_service(context)
      start_epoch(name, start_store(), fn -> [node(), @other] end)

      theirs = ref_owned_by([node(), @other], @other)
      mine = ref_owned_by([node(), @other], node())

      assert {:error, :not_owner} = Client.write_batch(name, theirs, batch([%{"id" => 1}]))
      assert {:ok, ack} = Client.write_batch(name, mine, batch([%{"id" => 1}]))
      assert ack.row_count == 1
    end

    test "the buffer's own mailbox re-checks the fence", context do
      name = start_buffer_service(context)
      holder = start_supervised!({Agent, fn -> [node()] end}, id: {:members, name})
      start_epoch(name, start_store(), fn -> Agent.get(holder, & &1) end)

      ref = {"analytics", "events"}
      assert {:ok, _ack} = Client.write_batch(name, ref, batch([%{"id" => 1}]))

      Agent.update(holder, fn _members -> [@other] end)
      RingEpoch.refresh(name)

      [{buffer, _load}] = Registry.lookup(Runtime.registry(name), ref)
      schema = Schema.new!([{"id", :int64}])

      assert {:error, :not_owner} = TableBuffer.write(buffer, schema, [%{"id" => 2}], 5_000)
    end
  end
end
