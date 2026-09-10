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

  # A standalone follower instance owns every table in its own one-node ring,
  # so it would claim the entries it holds on its own maintenance tick — the
  # valves an owner is started with are the follower's too, or a test that
  # outlives one tick races the follower's claim (seen on CI, T-450).
  defp start_pair(context, owner_opts \\ []) do
    follower =
      start_instance(
        context,
        Keyword.take(owner_opts, [:seal_max_files, :seal_max_bytes, :seal_max_age_ms])
      )

    owner =
      start_instance(
        context,
        [
          replicator:
            {SegmentShipping,
             replication_factor: 2,
             targets: fn _name, _ref -> {:ok, [{Transport.Local, node(), follower}]} end}
        ] ++ owner_opts
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

  test "a claim heals a follower that lost entries, by re-shipping them (T-289)", context do
    {owner, follower} = start_pair(context)

    {:ok, first} = Client.write_batch(owner, @table, batch([%{"id" => 1}], "b-12"))
    {:ok, second} = Client.write_batch(owner, @table, batch([%{"id" => 2}], "b-13"))

    :ok =
      Endpoint.apply_replica_mutation(follower, @table, :drop, %{ids: [second.segment_id]}, nil)

    {:ok, follower_runtime} = Runtime.fetch(follower)
    assert [_lone_survivor] = HotManifest.entries(follower_runtime.manifest, @table)

    [{buffer, _load}] = Registry.lookup(Runtime.registry(owner), @table)

    log =
      ExUnit.CaptureLog.capture_log(fn ->
        :ok = GenServer.call(buffer, :force_seal)
      end)

    assert log =~ "healed a partial claim"
    assert log =~ second.segment_id

    {:ok, owner_runtime} = Runtime.fetch(owner)
    assert {:ok, claim} = HotManifest.live_claim(owner_runtime.manifest, @table)
    assert Enum.sort(claim.ids) == Enum.sort([first.segment_id, second.segment_id])
    assert {:ok, ^claim} = HotManifest.live_claim(follower_runtime.manifest, @table)

    {:ok, entry} = HotManifest.entry(follower_runtime.manifest, @table, second.segment_id)
    {:ok, prefix} = Store.prefix(@table)
    {:ok, key} = Store.key(prefix, entry.id)
    assert File.exists?(Store.location(follower_runtime.store, key))
  end

  test "a claim heals a follower that holds none of its entries (T-344)", context do
    {owner, follower} = start_pair(context)

    {:ok, first} = Client.write_batch(owner, @table, batch([%{"id" => 1}], "b-44"))
    {:ok, second} = Client.write_batch(owner, @table, batch([%{"id" => 2}], "b-45"))
    ids = [first.segment_id, second.segment_id]

    :ok = Endpoint.apply_replica_mutation(follower, @table, :drop, %{ids: ids}, nil)

    {:ok, follower_runtime} = Runtime.fetch(follower)
    assert HotManifest.entries(follower_runtime.manifest, @table) == []

    [{buffer, _load}] = Registry.lookup(Runtime.registry(owner), @table)

    log =
      ExUnit.CaptureLog.capture_log(fn ->
        :ok = GenServer.call(buffer, :force_seal)
      end)

    assert log =~ "healed a partial claim"
    refute log =~ ":nothing_to_claim"

    {:ok, owner_runtime} = Runtime.fetch(owner)
    assert {:ok, claim} = HotManifest.live_claim(owner_runtime.manifest, @table)
    assert Enum.sort(claim.ids) == Enum.sort(ids)
    assert {:ok, ^claim} = HotManifest.live_claim(follower_runtime.manifest, @table)
    assert [_, _] = HotManifest.entries(follower_runtime.manifest, @table)
  end

  test "a heal larger than one batch converges across attempts", context do
    {owner, follower} =
      start_pair(context,
        seal_max_files: 1_000_000,
        seal_max_bytes: 1_000_000_000,
        seal_max_age_ms: 600_000
      )

    ids =
      for n <- 1..66 do
        {:ok, ack} = Client.write_batch(owner, @table, batch([%{"id" => n}], "b-batch-#{n}"))
        ack.segment_id
      end

    dropped = Enum.drop(ids, 1)
    :ok = Endpoint.apply_replica_mutation(follower, @table, :drop, %{ids: dropped}, nil)

    [{buffer, _load}] = Registry.lookup(Runtime.registry(owner), @table)

    first_attempt =
      ExUnit.CaptureLog.capture_log(fn ->
        :ok = GenServer.call(buffer, :force_seal)
      end)

    assert first_attempt =~ "heal on"
    assert first_attempt =~ "in progress"
    assert first_attempt =~ "re-shipped 64 missing entries"
    assert first_attempt =~ "1 remain"
    refute first_attempt =~ "consecutive"
    assert :sys.get_state(buffer).claim_backoff == nil

    {:ok, owner_runtime} = Runtime.fetch(owner)
    assert HotManifest.live_claim(owner_runtime.manifest, @table) == :error

    second_attempt =
      ExUnit.CaptureLog.capture_log(fn ->
        :ok = GenServer.call(buffer, :force_seal)
      end)

    assert second_attempt =~ "healed a partial claim"

    {:ok, follower_runtime} = Runtime.fetch(follower)
    assert {:ok, claim} = HotManifest.live_claim(owner_runtime.manifest, @table)
    assert Enum.sort(claim.ids) == Enum.sort(ids)
    assert {:ok, ^claim} = HotManifest.live_claim(follower_runtime.manifest, @table)
  end

  test "a claim over ids the follower already sealed fails loudly, naming them", context do
    {owner, follower} = start_pair(context)

    {:ok, first} = Client.write_batch(owner, @table, batch([%{"id" => 1}], "b-14"))
    {:ok, second} = Client.write_batch(owner, @table, batch([%{"id" => 2}], "b-15"))

    :ok =
      Endpoint.apply_replica_mutation(
        follower,
        @table,
        :retire,
        %{ids: [second.segment_id], snapshot: 9},
        nil
      )

    [{buffer, _load}] = Registry.lookup(Runtime.registry(owner), @table)

    log =
      ExUnit.CaptureLog.capture_log(fn ->
        :ok = GenServer.call(buffer, :force_seal)
      end)

    assert log =~ ":partial_claim"
    assert log =~ second.segment_id

    {:ok, owner_runtime} = Runtime.fetch(owner)
    {:ok, follower_runtime} = Runtime.fetch(follower)
    assert HotManifest.live_claim(owner_runtime.manifest, @table) == :error

    {:ok, entry} = HotManifest.entry(follower_runtime.manifest, @table, second.segment_id)
    assert entry.sealed_at == 9

    refute Enum.any?(
             HotManifest.entries(follower_runtime.manifest, @table),
             &(&1.id == first.segment_id and &1.claim_keys != [])
           )
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

  test "a release mutation clears the follower's live claim, idempotently", context do
    {owner, follower} = start_pair(context)

    assert {:ok, _ack} = Client.write_batch(owner, @table, batch([%{"id" => 1}], "b-16"))
    [entry] = entries(follower)
    claim = %{ids: [entry.id], keys: ["sealed-key"]}
    :ok = Endpoint.apply_replica_mutation(follower, @table, :claim, claim, nil)

    {:ok, follower_runtime} = Runtime.fetch(follower)
    assert {:ok, _live} = HotManifest.live_claim(follower_runtime.manifest, @table)

    assert :ok =
             Endpoint.apply_replica_mutation(follower, @table, :release, %{ids: [entry.id]}, nil)

    assert HotManifest.live_claim(follower_runtime.manifest, @table) == :error

    assert :ok =
             Endpoint.apply_replica_mutation(follower, @table, :release, %{ids: [entry.id]}, nil)
  end

  test "a release against a diverged follower claim heals it, then re-claims under the valves (T-297)",
       context do
    owner_name = :"t297_owner_#{:erlang.unique_integer([:positive])}"
    follower = start_instance(context)

    replicate_to_follower = [
      replicator:
        {SegmentShipping,
         replication_factor: 2,
         targets: fn _name, _ref -> {:ok, [{Transport.Local, node(), follower}]} end}
    ]

    owner =
      start_instance(
        context,
        [
          name: owner_name,
          seal_max_files: 1_000_000,
          seal_max_bytes: 1_000_000_000,
          seal_max_age_ms: 600_000
        ] ++ replicate_to_follower
      )

    ids =
      for n <- 1..18 do
        {:ok, ack} = Client.write_batch(owner, @table, batch([%{"id" => n}], "b-t297-#{n}"))
        ack.segment_id
      end

    {:ok, owner_runtime} = Runtime.fetch(owner)
    {:ok, prefix} = Store.prefix(@table)
    {:ok, old_key} = Store.key(prefix, "01KYWPEEGAM8FQVQS5S2QF26SV")
    {:ok, _oversized} = HotManifest.claim(owner_runtime.manifest, @table, ids, [old_key])

    diverged = %{ids: Enum.take(ids, 2), keys: ["diverged-key"]}
    :ok = Endpoint.apply_replica_mutation(follower, @table, :claim, diverged, nil)

    :ok = stop_supervised(owner)

    owner =
      start_instance(
        context,
        [
          name: owner_name,
          seal_max_files: 1,
          seal_max_bytes: 1_000_000_000,
          seal_max_age_ms: 600_000,
          seal_retry_ms: 1,
          maintenance_interval_ms: 600_000
        ] ++ replicate_to_follower
      )

    {:ok, owner_runtime} = Runtime.fetch(owner)

    assert Eventually.until(fn ->
             match?([{_buffer, _load}], Registry.lookup(Runtime.registry(owner), @table))
           end)

    [{buffer, _load}] = Registry.lookup(Runtime.registry(owner), @table)

    log =
      ExUnit.CaptureLog.capture_log(fn -> :ok = GenServer.call(buffer, :force_seal) end)

    assert log =~ "healed a diverged release"
    assert log =~ "released oversized claim"
    refute log =~ "releasing oversized claim on"

    assert {:ok, live} = HotManifest.live_claim(owner_runtime.manifest, @table)
    assert live.ids == Enum.take(ids, 16)
    refute live.keys == [old_key]

    {:ok, follower_runtime} = Runtime.fetch(follower)
    assert {:ok, ^live} = HotManifest.live_claim(follower_runtime.manifest, @table)
  end

  test "a diverged claim over ids the owner sealed refuses the heal, naming them (T-297)",
       context do
    owner_name = :"t297_guard_#{:erlang.unique_integer([:positive])}"
    follower = start_instance(context)

    replicate_to_follower = [
      replicator:
        {SegmentShipping,
         replication_factor: 2,
         targets: fn _name, _ref -> {:ok, [{Transport.Local, node(), follower}]} end}
    ]

    owner =
      start_instance(
        context,
        [
          name: owner_name,
          seal_max_files: 1_000_000,
          seal_max_bytes: 1_000_000_000,
          seal_max_age_ms: 600_000
        ] ++ replicate_to_follower
      )

    {:ok, retired} = Client.write_batch(owner, @table, batch([%{"id" => 0}], "b-t297-g-0"))

    ids =
      for n <- 1..18 do
        {:ok, ack} = Client.write_batch(owner, @table, batch([%{"id" => n}], "b-t297-g-#{n}"))
        ack.segment_id
      end

    {:ok, owner_runtime} = Runtime.fetch(owner)
    :ok = HotManifest.retire(owner_runtime.manifest, @table, [retired.segment_id], 42)

    {:ok, prefix} = Store.prefix(@table)
    {:ok, old_key} = Store.key(prefix, "01KYWPEEGAM8FQVQS5S2QF26SV")
    {:ok, _oversized} = HotManifest.claim(owner_runtime.manifest, @table, ids, [old_key])

    diverged = %{ids: [retired.segment_id, hd(ids)], keys: ["diverged-key"]}
    :ok = Endpoint.apply_replica_mutation(follower, @table, :claim, diverged, nil)

    :ok = stop_supervised(owner)

    owner =
      start_instance(
        context,
        [
          name: owner_name,
          seal_max_files: 1,
          seal_max_bytes: 1_000_000_000,
          seal_max_age_ms: 600_000,
          seal_retry_ms: 1,
          maintenance_interval_ms: 600_000
        ] ++ replicate_to_follower
      )

    {:ok, owner_runtime} = Runtime.fetch(owner)

    assert Eventually.until(fn ->
             match?([{_buffer, _load}], Registry.lookup(Runtime.registry(owner), @table))
           end)

    [{buffer, _load}] = Registry.lookup(Runtime.registry(owner), @table)

    log =
      ExUnit.CaptureLog.capture_log(fn -> :ok = GenServer.call(buffer, :force_seal) end)

    assert log =~ "releasing oversized claim"
    assert log =~ ":diverged_claim_sealed_on_owner"
    assert log =~ retired.segment_id
    refute log =~ "healed a diverged release"

    assert {:ok, live} = HotManifest.live_claim(owner_runtime.manifest, @table)
    assert live.keys == [old_key]

    {:ok, follower_runtime} = Runtime.fetch(follower)
    assert {:ok, diverged_live} = HotManifest.live_claim(follower_runtime.manifest, @table)
    assert Enum.sort(diverged_live.ids) == Enum.sort(diverged.ids)

    escalated =
      ExUnit.CaptureLog.capture_log(fn ->
        for _attempt <- 1..4, do: :ok = GenServer.call(buffer, :force_seal)
      end)

    assert escalated =~ "consecutive failures, sealing on this table is stalled (T-297)"
  end

  test "a re-shipped claim is absorbed as ok, a different claim is refused", context do
    {owner, follower} = start_pair(context)

    assert {:ok, _ack} = Client.write_batch(owner, @table, batch([%{"id" => 1}], "b-10"))
    [entry] = entries(follower)
    claim = %{ids: [entry.id], keys: ["sealed-key"]}

    assert :ok = Endpoint.apply_replica_mutation(follower, @table, :claim, claim, nil)
    assert :ok = Endpoint.apply_replica_mutation(follower, @table, :claim, claim, nil)

    other = %{ids: [entry.id], keys: ["other-key"]}

    assert {:error, {:partial_claim, %{claimed: [_id]}}} =
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

  describe "a diverged follower claim over the owner's ids (T-450)" do
    defp replicate_to(follower) do
      [
        replicator:
          {SegmentShipping,
           replication_factor: 2,
           targets: fn _name, _ref -> {:ok, [{Transport.Local, node(), follower}]} end}
      ]
    end

    # Writes under valves nothing crosses, so the owner claims nothing on its
    # own; the test then stages the follower's diverged claim and restarts the
    # owner under the valves the case needs, the way the T-297 tests do.
    defp diverged_pair(context) do
      owner_name = :"t450_owner_#{:erlang.unique_integer([:positive])}"
      follower = start_instance(context)

      owner =
        start_instance(
          context,
          [
            name: owner_name,
            seal_max_files: 1_000_000,
            seal_max_bytes: 1_000_000_000,
            seal_max_age_ms: 600_000
          ] ++ replicate_to(follower)
        )

      ids =
        for n <- 1..5 do
          {:ok, ack} = Client.write_batch(owner, @table, batch([%{"id" => n}], "b-t450-#{n}"))
          ack.segment_id
        end

      {owner, follower, ids}
    end

    defp restart_owner(context, owner, follower, opts) do
      {:ok, %{name: owner_name}} = Runtime.fetch(owner)
      :ok = stop_supervised(owner)

      owner =
        start_instance(
          context,
          [name: owner_name, seal_max_bytes: 1_000_000_000, seal_max_age_ms: 600_000] ++
            opts ++ replicate_to(follower)
        )

      assert Eventually.until(fn ->
               match?([{_buffer, _load}], Registry.lookup(Runtime.registry(owner), @table))
             end)

      [{buffer, _load}] = Registry.lookup(Runtime.registry(owner), @table)

      {owner, buffer}
    end

    defp stage_diverged(follower, ids, keys \\ ["diverged-key"]),
      do:
        :ok =
          Endpoint.apply_replica_mutation(follower, @table, :claim, %{ids: ids, keys: keys}, nil)

    defp live_claims(name) do
      {:ok, runtime} = Runtime.fetch(name)

      runtime.manifest
      |> HotManifest.live_claims(@table)
      |> Enum.map(&%{ids: Enum.sort(&1.ids), keys: &1.keys})
      |> Enum.sort()
    end

    test "heals by releasing the follower's claim, then applying the owner's", context do
      {owner, follower, ids} = diverged_pair(context)
      stage_diverged(follower, ids)

      {owner, buffer} =
        restart_owner(context, owner, follower,
          seal_max_files: 1_000_000,
          seal_retry_ms: 1,
          maintenance_interval_ms: 600_000
        )

      log = ExUnit.CaptureLog.capture_log(fn -> :ok = GenServer.call(buffer, :force_seal) end)

      assert log =~ "healed a diverged claim"
      assert log =~ "(T-450)"
      refute log =~ "claiming #{inspect(@table)} failed"

      assert [%{ids: live_ids, keys: keys}] = live_claims(owner)
      assert live_ids == Enum.sort(ids)
      refute keys == ["diverged-key"]
      assert live_claims(follower) == live_claims(owner)
    end

    test "a heal that uncovers the follower's own claim over the re-shipped entries converges in one attempt",
         context do
      {owner, follower, ids} = diverged_pair(context)
      [kept | dropped] = ids
      :ok = Endpoint.apply_replica_mutation(follower, @table, :drop, %{ids: dropped}, nil)
      stage_diverged(follower, [kept])

      {owner, buffer} =
        restart_owner(context, owner, follower,
          seal_max_files: 1_000_000,
          seal_retry_ms: 1,
          maintenance_interval_ms: 600_000
        )

      log = ExUnit.CaptureLog.capture_log(fn -> :ok = GenServer.call(buffer, :force_seal) end)

      assert log =~ "healed a diverged claim"
      assert log =~ "healed a partial claim"
      assert log =~ "re-shipped 4 missing entries"
      refute log =~ "claiming #{inspect(@table)} failed"

      assert [%{ids: live_ids}] = live_claims(owner)
      assert live_ids == Enum.sort(ids)
      assert live_claims(follower) == live_claims(owner)
    end

    test "a follower holding two claims over the owner's ids has each released, not their union",
         context do
      {owner, follower, ids} = diverged_pair(context)
      {first_two, rest} = Enum.split(ids, 2)
      stage_diverged(follower, first_two, ["diverged-key-1"])
      stage_diverged(follower, rest, ["diverged-key-2"])

      {owner, buffer} =
        restart_owner(context, owner, follower,
          seal_max_files: 1_000_000,
          seal_retry_ms: 1,
          maintenance_interval_ms: 600_000
        )

      log = ExUnit.CaptureLog.capture_log(fn -> :ok = GenServer.call(buffer, :force_seal) end)

      assert log =~ "healed a diverged claim"
      refute log =~ "claiming #{inspect(@table)} failed"

      assert [%{ids: live_ids}] = live_claims(owner)
      assert live_ids == Enum.sort(ids)
      assert live_claims(follower) == live_claims(owner)
    end

    test "a release heal releases only the follower claims holding the released ids", context do
      owner_name = :"t450_release_#{:erlang.unique_integer([:positive])}"
      follower = start_instance(context, seal_max_files: 1_000_000, seal_max_age_ms: 600_000)

      owner =
        start_instance(
          context,
          [
            name: owner_name,
            seal_max_files: 1_000_000,
            seal_max_bytes: 1_000_000_000,
            seal_max_age_ms: 600_000
          ] ++
            replicate_to(follower)
        )

      ids =
        for n <- 1..18 do
          {:ok, ack} = Client.write_batch(owner, @table, batch([%{"id" => n}], "b-t450r-#{n}"))
          ack.segment_id
        end

      {:ok, owner_runtime} = Runtime.fetch(owner)
      {:ok, prefix} = Store.prefix(@table)
      {:ok, old_key} = Store.key(prefix, "01KYWPEEGAM8FQVQS5S2QF26SV")
      {:ok, _oversized} = HotManifest.claim(owner_runtime.manifest, @table, ids, [old_key])

      # Two more entries the oversized claim does not cover, frozen on the
      # follower under a claim of its own: the release must not touch it.
      later =
        for n <- 19..20 do
          {:ok, ack} = Client.write_batch(owner, @table, batch([%{"id" => n}], "b-t450r-#{n}"))
          ack.segment_id
        end

      stage_diverged(follower, Enum.take(ids, 2), ["diverged-key"])
      stage_diverged(follower, later, ["later-key"])

      # Valves of 16: the 18-id claim is oversized, so a maintenance pass
      # releases it and re-claims the oldest 16 — never reaching `later`.
      {owner, buffer} =
        restart_owner(context, owner, follower,
          seal_max_files: 16,
          claim_valve_factor: 1,
          seal_retry_ms: 1,
          maintenance_interval_ms: 600_000
        )

      log = ExUnit.CaptureLog.capture_log(fn -> :ok = GenServer.call(buffer, :maintain) end)

      [healed] = log |> String.split("\n") |> Enum.filter(&(&1 =~ "healed a diverged release"))
      assert healed =~ inspect(ids |> Enum.take(2) |> Enum.sort())
      refute healed =~ hd(later)

      assert Enum.any?(
               live_claims(follower),
               &(&1.keys == ["later-key"] and &1.ids == Enum.sort(later))
             )

      assert [%{ids: live_ids}] = live_claims(owner)
      assert live_ids == ids |> Enum.take(16) |> Enum.sort()
    end

    test "a follower claim wider than the owner's is released whole", context do
      {owner, follower, ids} = diverged_pair(context)
      stage_diverged(follower, ids)

      {owner, buffer} =
        restart_owner(context, owner, follower,
          seal_max_files: 2,
          claim_valve_factor: 1,
          seal_retry_ms: 1,
          maintenance_interval_ms: 600_000
        )

      log = ExUnit.CaptureLog.capture_log(fn -> :ok = GenServer.call(buffer, :force_seal) end)

      assert log =~ "healed a diverged claim"
      assert log =~ inspect(Enum.sort(ids))
      refute log =~ "claiming #{inspect(@table)} failed"

      assert Enum.count(live_claims(owner)) > 1
      assert live_claims(follower) == live_claims(owner)
    end

    test "a follower claim holding an id the owner sealed refuses the heal, naming it",
         context do
      {owner, follower, ids} = diverged_pair(context)
      {:ok, owner_runtime} = Runtime.fetch(owner)
      [retired | rest] = ids
      :ok = HotManifest.retire(owner_runtime.manifest, @table, [retired], 42)
      stage_diverged(follower, ids)

      {owner, buffer} =
        restart_owner(context, owner, follower,
          seal_max_files: 1_000_000,
          seal_retry_ms: 1,
          maintenance_interval_ms: 600_000
        )

      log = ExUnit.CaptureLog.capture_log(fn -> :ok = GenServer.call(buffer, :force_seal) end)

      assert log =~ ":diverged_claim_sealed_on_owner"
      assert log =~ retired
      refute log =~ "healed a diverged claim"

      assert live_claims(owner) == []
      assert [%{ids: follower_ids}] = live_claims(follower)
      assert follower_ids == Enum.sort([retired | rest])
    end

    test "a claim that keeps failing backs off instead of retrying on every tick", context do
      {owner, follower, ids} = diverged_pair(context)
      {:ok, owner_runtime} = Runtime.fetch(owner)
      [retired | _rest] = ids
      :ok = HotManifest.retire(owner_runtime.manifest, @table, [retired], 42)
      stage_diverged(follower, ids)

      {owner, buffer} =
        restart_owner(context, owner, follower,
          seal_max_files: 1,
          seal_retry_ms: 60_000,
          maintenance_interval_ms: 600_000
        )

      log =
        ExUnit.CaptureLog.capture_log(fn ->
          for _tick <- 1..20, do: :ok = GenServer.call(buffer, :maintain)
        end)

      failures =
        log |> String.split("claiming #{inspect(@table)} failed") |> Enum.count() |> Kernel.-(1)

      assert failures == 1
      assert log =~ "1 consecutive, next attempt in 250 ms"

      state = :sys.get_state(buffer)
      assert %{consecutive: 1, retry_at: retry_at} = state.claim_backoff
      assert is_integer(retry_at)

      # Past the wait, the next tick tries again and the count grows.
      Process.sleep(300)
      :ok = GenServer.call(buffer, :maintain)
      assert %{consecutive: 2} = :sys.get_state(buffer).claim_backoff

      # A commit that lands reaches the same replicas, so it ends the backoff;
      # the maintenance pass right after it tries once more and fails afresh —
      # a count of 1, not 3.
      {:ok, _ack} = Client.write_batch(owner, @table, batch([%{"id" => 99}], "b-t450-reset"))

      assert Eventually.until(fn ->
               match?(%{consecutive: 1}, :sys.get_state(buffer).claim_backoff)
             end)
    end
  end
end
