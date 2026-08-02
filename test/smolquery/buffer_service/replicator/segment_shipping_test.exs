defmodule Smolquery.BufferService.Replicator.SegmentShippingTest do
  @moduledoc """
  Two buffer instances in one BEAM play owner and follower (T-96): the
  owner's replicator targets the follower instance over `Transport.Local`,
  which is the same `Endpoint` surface gen_rpc reaches across a real
  cluster.
  """

  use ExUnit.Case, async: true

  alias Smolquery.BufferService
  alias Smolquery.BufferService.Client
  alias Smolquery.BufferService.Endpoint
  alias Smolquery.BufferService.HotManifest
  alias Smolquery.BufferService.Replicator.SegmentShipping
  alias Smolquery.BufferService.RingEpoch
  alias Smolquery.BufferService.Runtime
  alias Smolquery.BufferService.Transport
  alias Smolquery.Cluster.ConfigStore.Memory
  alias Smolquery.Schema
  alias Smolquery.Segments.Store
  alias Smolquery.Test.Eventually

  @moduletag :tmp_dir

  @table {"analytics", "events"}

  defp start_instance(context, opts \\ []) do
    opts =
      Keyword.merge(
        [name: :"ship_#{:erlang.unique_integer([:positive])}", flush_interval_ms: 25],
        opts
      )

    name = Keyword.fetch!(opts, :name)
    opts = Keyword.put_new(opts, :dir, Path.join(context.tmp_dir, "#{name}"))
    start_supervised!({BufferService.Supervisor, opts}, id: name)
    on_exit(fn -> Runtime.delete(name) end)

    name
  end

  defp start_pair(context) do
    follower = start_instance(context)

    owner =
      start_instance(context,
        replicator:
          {SegmentShipping,
           replication_factor: 2,
           targets: fn _name, _ref -> {:ok, [{Transport.Local, node(), follower}]} end}
      )

    {owner, follower}
  end

  defp batch(rows, batch_id) do
    %{schema: Schema.new!([{"id", :int64}]), rows: rows, batch_id: batch_id}
  end

  defp entries(name) do
    {:ok, entries} = Endpoint.hot_manifest(name, @table)

    entries
  end

  test "a write is on the follower's disk before the ack", context do
    {owner, follower} = start_pair(context)

    assert {:ok, ack} = Client.write_batch(owner, @table, batch([%{"id" => 1}], "b-1"))

    assert [entry] = entries(follower)
    assert entry.id == ack.segment_id
    assert entry.row_count == 1
    assert entries(owner) == [entry]

    {:ok, runtime} = Runtime.fetch(follower)
    {:ok, prefix} = Store.prefix(@table)
    {:ok, key} = Store.key(prefix, entry.id)
    assert File.exists?(Store.location(runtime.store, key))
  end

  test "an unreachable follower fails the flush and compensates the local commit", context do
    owner =
      start_instance(context,
        replicator:
          {SegmentShipping,
           replication_factor: 2,
           targets: fn _name, _ref ->
             {:ok, [{Transport.Local, node(), :no_such_instance}]}
           end}
      )

    assert {:error, {:replication_failed, _node, :buffer_service_unavailable}} =
             Client.write_batch(owner, @table, batch([%{"id" => 1}], "b-2"))

    assert entries(owner) == []
  end

  test "a ring smaller than the replication factor refuses every write", context do
    owner = start_instance(context, replicator: {SegmentShipping, replication_factor: 2})

    assert {:error, {:underreplicated, 1, 2}} =
             Client.write_batch(owner, @table, batch([%{"id" => 1}], "b-3"))
  end

  test "the follower answers a committed batch id after the owner is gone", context do
    {owner, follower} = start_pair(context)

    assert {:ok, ack} = Client.write_batch(owner, @table, batch([%{"id" => 7}], "b-4"))

    stop_supervised!(owner)

    assert {:ok, ^ack} = Endpoint.write_batch(follower, @table, batch([%{"id" => 7}], "b-4"))
    assert [_only_the_original] = entries(follower)
  end

  test "claims and retires replicate, keeping the follower's manifest in step", context do
    {owner, follower} = start_pair(context)

    assert {:ok, ack} = Client.write_batch(owner, @table, batch([%{"id" => 1}], "b-5"))

    :ok = Client.flush(owner, @table)
    {:ok, owner_runtime} = Runtime.fetch(owner)
    [{buffer, _load}] = Registry.lookup(Runtime.registry(owner), @table)
    :ok = GenServer.call(buffer, :force_seal)

    {:ok, follower_runtime} = Runtime.fetch(follower)
    assert {:ok, claim} = HotManifest.live_claim(follower_runtime.manifest, @table)
    assert ack.segment_id in claim.ids
    assert {:ok, ^claim} = HotManifest.live_claim(owner_runtime.manifest, @table)

    assert :ok = Client.retire(owner, @table, claim.ids, 42)
    assert [entry] = entries(follower)
    assert entry.sealed_at
  end

  test "an unreachable follower's compensation clears the applied copy", context do
    follower = start_instance(context)

    owner =
      start_instance(context,
        replicator:
          {SegmentShipping,
           replication_factor: 3,
           targets: fn _name, _ref ->
             {:ok,
              [
                {Transport.Local, node(), follower},
                {Transport.Local, node(), :no_such_instance}
              ]}
           end}
      )

    assert {:error, {:replication_failed, _node, :buffer_service_unavailable}} =
             Client.write_batch(owner, @table, batch([%{"id" => 1}], "b-7"))

    assert entries(owner) == []
    assert entries(follower) == []
  end

  test "mutations fan out past the followers to stale holders", context do
    follower = start_instance(context)
    stale = start_instance(context)

    owner =
      start_instance(context,
        replicator:
          {SegmentShipping,
           replication_factor: 2,
           targets: fn _name, _ref -> {:ok, [{Transport.Local, node(), follower}]} end,
           holders: fn _name -> [{Transport.Local, node(), stale}] end}
      )

    assert {:ok, ack} = Client.write_batch(owner, @table, batch([%{"id" => 1}], "b-8"))

    [entry] = entries(follower)
    assert :ok = Endpoint.accept_replica(stale, @table, entry, nil, nil)
    assert [_copy] = entries(stale)

    assert :ok = Client.retire(owner, @table, [ack.segment_id], 42)

    assert Eventually.until(fn ->
             match?([%{sealed_at: sealed_at}] when not is_nil(sealed_at), entries(stale))
           end)
  end

  test "a re-shipped claim is absorbed as ok, a different claim is refused", context do
    {owner, follower} = start_pair(context)

    assert {:ok, _ack} = Client.write_batch(owner, @table, batch([%{"id" => 1}], "b-10"))
    [entry] = entries(follower)
    claim = %{ids: [entry.id], keys: ["sealed-key"]}

    assert :ok = Endpoint.apply_replica_mutation(follower, @table, :claim, claim, nil)
    assert :ok = Endpoint.apply_replica_mutation(follower, @table, :claim, claim, nil)

    other = %{ids: [entry.id], keys: ["other-key"]}

    assert {:error, :claim_outstanding} =
             Endpoint.apply_replica_mutation(follower, @table, :claim, other, nil)
  end

  test "a mutation racing a queued accept_replica stays ordered behind it", context do
    {owner, follower} = start_pair(context)

    assert {:ok, _ack} = Client.write_batch(owner, @table, batch([%{"id" => 1}], "b-11"))
    [entry] = entries(follower)

    assert :ok = Endpoint.apply_replica_mutation(follower, @table, :drop, %{ids: [entry.id]}, nil)
    assert entries(follower) == []

    [{buffer, _load}] = Registry.lookup(Runtime.registry(follower), @table)
    :sys.suspend(buffer)

    accept = Task.async(fn -> Endpoint.accept_replica(follower, @table, entry, nil, nil) end)
    assert Eventually.until(fn -> queued(buffer) == 1 end)

    drop =
      Task.async(fn ->
        Endpoint.apply_replica_mutation(follower, @table, :drop, %{ids: [entry.id]}, nil)
      end)

    assert Eventually.until(fn -> queued(buffer) == 2 end)
    :sys.resume(buffer)

    assert Task.await(accept) == :ok
    assert Task.await(drop) == :ok
    assert entries(follower) == []
  end

  defp queued(pid) do
    {:message_queue_len, queued} = Process.info(pid, :message_queue_len)

    queued
  end

  test "a mutation for a table this node never held is free and starts nothing", context do
    follower = start_instance(context)

    assert :ok =
             Endpoint.apply_replica_mutation(follower, @table, :drop, %{ids: ["nothing"]}, nil)

    assert Registry.lookup(Runtime.registry(follower), @table) == []
  end

  test "a shipment from a stale epoch is refused", context do
    {owner, follower} = start_pair(context)

    assert {:ok, _ack} = Client.write_batch(owner, @table, batch([%{"id" => 1}], "b-6"))
    [entry] = entries(follower)

    store_name = :"ship_store_#{:erlang.unique_integer([:positive])}"
    start_supervised!({Memory, name: store_name}, id: store_name)
    {:ok, _config} = Memory.ensure(store_name, "buffer:#{follower}", [node()])
    {:ok, _config} = Memory.advance(store_name, "buffer:#{follower}", 0, [node()])

    start_supervised!(
      {RingEpoch,
       name: follower,
       static: [node()],
       store: {Memory, name: store_name},
       members: fn -> [node()] end},
      id: {:epoch, follower}
    )

    RingEpoch.refresh(follower)

    assert {:error, {:stale_epoch, 1}} =
             Endpoint.accept_replica(follower, @table, entry, nil, 0)
  end
end
