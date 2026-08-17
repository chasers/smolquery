defmodule Smolquery.BufferService.SealingTest.FailingReleaseReplicator do
  @moduledoc """
  Replicates everything except a `:release`, the way a diverged follower
  answering `:claim_mismatch` looks to the owner (T-294 review fix).
  """

  @behaviour Smolquery.BufferService.Replicator

  @impl Smolquery.BufferService.Replicator
  def new(opts), do: opts

  @impl Smolquery.BufferService.Replicator
  def commit(_config, _commit), do: :ok

  @impl Smolquery.BufferService.Replicator
  def append(_config, %{op: :release}),
    do: {:error, {:replication_failed, :peer, :claim_mismatch}}

  def append(_config, _mutation), do: :ok

  @impl Smolquery.BufferService.Replicator
  def redundancy(_config), do: 0
end

defmodule Smolquery.BufferService.SealingTest do
  use ExUnit.Case, async: true

  alias Smolquery.BufferService
  alias Smolquery.BufferService.Client
  alias Smolquery.BufferService.HotManifest
  alias Smolquery.BufferService.Runtime
  alias Smolquery.BufferService.TableBuffer
  alias Smolquery.Schema
  alias Smolquery.Segments.Store
  alias Smolquery.Test.Eventually
  alias Smolquery.Test.SealCollector

  @moduletag :tmp_dir

  @table {"analytics", "events"}

  defp batch(range) do
    %{schema: Schema.new!([{"id", :int64}]), rows: for(i <- range, do: %{"id" => i})}
  end

  defp start_buffer_service(context, opts) do
    opts =
      Keyword.merge(
        [
          name: :"buffer_#{:erlang.unique_integer([:positive])}",
          dir: Path.join(context.tmp_dir, "buffer"),
          flush_interval_ms: 25,
          maintenance_interval_ms: 20,
          seal_consumer: {SealCollector, self()},
          seal_max_files: 1_000_000,
          seal_max_bytes: 1_000_000_000,
          seal_max_age_ms: 60_000,
          retire_grace_ms: 600_000
        ],
        opts
      )

    name = Keyword.fetch!(opts, :name)
    start_supervised!({BufferService.Supervisor, opts}, id: name)
    on_exit(fn -> Runtime.delete(name) end)

    %{name: name, runtime: Runtime.new(opts)}
  end

  defp buffer(name) do
    [{pid, _value}] = Registry.lookup(Runtime.registry(name), @table)

    pid
  end

  describe "seal_ready signalling" do
    test "stays quiet while a table is under every threshold", context do
      %{name: name} = start_buffer_service(context, [])

      {:ok, _ack} = Client.write_batch(name, @table, batch(1..2))

      refute_receive {:seal_ready, _table, _claim}, 100
    end

    test "signals when the file count crosses", context do
      %{name: name} = start_buffer_service(context, seal_max_files: 2)

      {:ok, first} = Client.write_batch(name, @table, batch(1..1))
      refute_receive {:seal_ready, _table, _claim}, 60

      {:ok, second} = Client.write_batch(name, @table, batch(2..2))

      assert_receive {:seal_ready, @table, claim}, 500
      assert Enum.sort(claim.ids) == Enum.sort([first.segment_id, second.segment_id])
    end

    test "signals when the byte total crosses", context do
      %{name: name} = start_buffer_service(context, seal_max_bytes: 1)

      {:ok, ack} = Client.write_batch(name, @table, batch(1..1))

      assert_receive {:seal_ready, @table, %{ids: [id]}}, 500
      assert id == ack.segment_id
    end

    test "signals on age alone, with no further writes", context do
      %{name: name} = start_buffer_service(context, seal_max_age_ms: 1)

      {:ok, ack} = Client.write_batch(name, @table, batch(1..1))

      assert_receive {:seal_ready, @table, %{ids: [id]}}, 500
      assert id == ack.segment_id
    end

    test "re-signals while a crossed table stays unretired", context do
      %{name: name} = start_buffer_service(context, seal_max_bytes: 1, seal_retry_ms: 1)

      {:ok, _ack} = Client.write_batch(name, @table, batch(1..1))

      assert_receive {:seal_ready, @table, first}, 500
      assert_receive {:seal_ready, @table, second}, 500
      assert first == second
    end

    test "does not re-signal before the retry interval", context do
      %{name: name} = start_buffer_service(context, seal_max_bytes: 1, seal_retry_ms: 60_000)

      {:ok, _ack} = Client.write_batch(name, @table, batch(1..1))

      assert_receive {:seal_ready, @table, _claim}, 500
      refute_receive {:seal_ready, _table, _claim}, 100
    end

    test "goes quiet once everything is retired", context do
      %{name: name} = start_buffer_service(context, seal_max_bytes: 1, seal_retry_ms: 1)

      {:ok, ack} = Client.write_batch(name, @table, batch(1..1))

      assert_receive {:seal_ready, @table, _claim}, 500

      :ok = Client.retire(name, @table, [ack.segment_id], 7)

      Process.sleep(60)
      flush_messages()

      refute_receive {:seal_ready, _table, _claim}, 100
    end

    test "freezes the claim, so writes after it wait for the next one", context do
      %{name: name} = start_buffer_service(context, seal_max_files: 2, seal_retry_ms: 1)

      {:ok, first} = Client.write_batch(name, @table, batch(1..1))
      {:ok, second} = Client.write_batch(name, @table, batch(2..2))

      assert_receive {:seal_ready, @table, claim}, 500
      assert Enum.sort(claim.ids) == Enum.sort([first.segment_id, second.segment_id])

      {:ok, third} = Client.write_batch(name, @table, batch(3..3))

      assert_receive {:seal_ready, @table, repeat}, 500
      assert repeat == claim
      refute third.segment_id in repeat.ids
    end

    test "a claim stops at the byte valve, oldest first", context do
      %{name: name} = start_buffer_service(context, seal_max_files: 100, seal_max_bytes: 1)

      {:ok, first} = Client.write_batch(name, @table, batch(1..1))
      {:ok, _second} = Client.write_batch(name, @table, batch(2..2))

      :ok = TableBuffer.force_seal(buffer(name))

      assert_receive {:seal_ready, @table, claim}, 500
      assert claim.ids == [first.segment_id]
    end

    test "a backlog past the count valve claims in valve-sized claims, oldest first", context do
      %{name: name} =
        start_buffer_service(context,
          seal_max_files: 1,
          seal_max_bytes: 1_000_000,
          seal_retry_ms: 1
        )

      {:ok, first} = Client.write_batch(name, @table, batch(0..0))

      assert_receive {:seal_ready, @table, live}, 500
      assert live.ids == [first.segment_id]

      backlog =
        for n <- 1..18 do
          {:ok, ack} = Client.write_batch(name, @table, batch(n..n))
          ack.segment_id
        end

      :ok = Client.retire(name, @table, live.ids, 1)
      flush_messages()

      assert_receive {:seal_ready, @table, claim}, 500
      assert claim.ids == Enum.take(backlog, 16)

      :ok = Client.retire(name, @table, claim.ids, 2)
      flush_messages()

      assert_receive {:seal_ready, @table, remainder}, 500
      assert remainder.ids == Enum.drop(backlog, 16)
    end

    test "an oversized claim frozen under old valves releases and drains (T-294)", context do
      name = :"resize_#{:erlang.unique_integer([:positive])}"

      %{name: ^name} = start_buffer_service(context, name: name, seal_max_files: 1_000_000)

      ids =
        for n <- 1..18 do
          {:ok, ack} = Client.write_batch(name, @table, batch(n..n))
          ack.segment_id
        end

      {:ok, runtime} = Runtime.fetch(name)
      {:ok, prefix} = Store.prefix(@table)
      {:ok, old_key} = Store.key(prefix, "01KYWPEEGAM8FQVQS5S2QF26SV")
      {:ok, oversized} = HotManifest.claim(runtime.manifest, @table, ids, [old_key])
      assert Enum.sort(oversized.ids) == Enum.sort(ids)

      :ok = stop_supervised(name)
      flush_messages()

      log =
        ExUnit.CaptureLog.capture_log(fn ->
          %{name: ^name} =
            start_buffer_service(context,
              name: name,
              seal_max_files: 1,
              seal_max_bytes: 1_000_000_000,
              seal_retry_ms: 1
            )

          assert_receive {:seal_ready, @table, claim}, 2_000
          assert claim.ids == Enum.take(ids, 16)
          refute claim.keys == [old_key]

          :ok = Client.retire(name, @table, claim.ids, 1)
          flush_messages()

          assert_receive {:seal_ready, @table, remainder}, 2_000
          assert remainder.ids == Enum.drop(ids, 16)
        end)

      assert log =~ "released oversized claim"
    end

    test "an oversized claim whose release cannot replicate re-signals, not stalls", context do
      name = :"stall_#{:erlang.unique_integer([:positive])}"

      %{name: ^name} = start_buffer_service(context, name: name, seal_max_files: 1_000_000)

      ids =
        for n <- 1..18 do
          {:ok, ack} = Client.write_batch(name, @table, batch(n..n))
          ack.segment_id
        end

      {:ok, runtime} = Runtime.fetch(name)
      {:ok, prefix} = Store.prefix(@table)
      {:ok, old_key} = Store.key(prefix, "01KYWPEEGAM8FQVQS5S2QF26SV")
      {:ok, _oversized} = HotManifest.claim(runtime.manifest, @table, ids, [old_key])

      :ok = stop_supervised(name)
      flush_messages()

      %{name: ^name} =
        start_buffer_service(context,
          name: name,
          seal_max_files: 1,
          seal_max_bytes: 1_000_000_000,
          seal_retry_ms: 1,
          replicator: {Smolquery.BufferService.SealingTest.FailingReleaseReplicator, []}
        )

      assert_receive {:seal_ready, @table, claim}, 2_000
      assert Enum.sort(claim.ids) == Enum.sort(ids)
      assert claim.keys == [old_key]
    end

    test "a claim freezes the entire unsealed backlog", context do
      %{name: name} = start_buffer_service(context, seal_max_files: 100)

      acks =
        for n <- 1..4 do
          {:ok, ack} = Client.write_batch(name, @table, batch(n..n))
          ack
        end

      :ok = TableBuffer.force_seal(buffer(name))

      assert_receive {:seal_ready, @table, claim}, 500
      assert Enum.sort(claim.ids) == acks |> Enum.map(& &1.segment_id) |> Enum.sort()
    end

    test "the next claim covers only what arrived after the last one retired", context do
      %{name: name} = start_buffer_service(context, seal_max_files: 2, seal_retry_ms: 1)

      {:ok, first} = Client.write_batch(name, @table, batch(1..1))
      {:ok, second} = Client.write_batch(name, @table, batch(2..2))

      assert_receive {:seal_ready, @table, claim}, 500

      :ok = Client.retire(name, @table, claim.ids, 3)
      flush_messages()

      {:ok, third} = Client.write_batch(name, @table, batch(3..3))
      {:ok, fourth} = Client.write_batch(name, @table, batch(4..4))

      assert_receive {:seal_ready, @table, next}, 500
      assert Enum.sort(next.ids) == Enum.sort([third.segment_id, fourth.segment_id])
      refute first.segment_id in next.ids
      refute second.segment_id in next.ids
      refute next.keys == claim.keys
    end

    test "the claim's key survives a buffer restart, so a retry seals into the same segment",
         context do
      %{name: name} = start_buffer_service(context, seal_max_bytes: 1, seal_retry_ms: 1)

      {:ok, _ack} = Client.write_batch(name, @table, batch(1..1))

      assert_receive {:seal_ready, @table, before}, 500

      Process.exit(buffer(name), :kill)
      flush_messages()

      assert_receive {:seal_ready, @table, replayed}, 1_000
      assert replayed == before
    end

    test "retiring one member of a claim retires the whole claim", context do
      %{name: name} = start_buffer_service(context, seal_max_files: 2, seal_retry_ms: 60_000)

      {:ok, first} = Client.write_batch(name, @table, batch(1..1))
      {:ok, second} = Client.write_batch(name, @table, batch(2..2))

      assert_receive {:seal_ready, @table, claim}, 500
      assert match?([_first, _second], claim.ids)

      :ok = Client.retire(name, @table, [first.segment_id], 3)

      assert {:ok, entries} = Client.hot_manifest(name, @table)
      assert Enum.all?(entries, &(&1.sealed_at == 3))

      assert entries |> Enum.map(& &1.id) |> Enum.sort() ==
               Enum.sort([first.segment_id, second.segment_id])
    end
  end

  describe "retire/4" do
    test "stamps the snapshot and keeps the segment readable", context do
      %{name: name, runtime: runtime} = start_buffer_service(context, [])

      {:ok, ack} = Client.write_batch(name, @table, batch(1..2))

      assert Client.retire(name, @table, [ack.segment_id], 42) == :ok

      assert {:ok, [entry]} = Client.hot_manifest(name, @table)
      assert entry.sealed_at == 42
      assert File.exists?(Store.location(runtime.store, entry.key))
    end

    test "is idempotent for a sealer retrying after a crash", context do
      %{name: name} = start_buffer_service(context, [])

      {:ok, ack} = Client.write_batch(name, @table, batch(1..1))

      assert Client.retire(name, @table, [ack.segment_id], 5) == :ok
      assert Client.retire(name, @table, [ack.segment_id], 9) == :ok

      assert {:ok, [entry]} = Client.hot_manifest(name, @table)
      assert entry.sealed_at == 5
    end

    test "is idempotent for ids the sweep already deleted", context do
      %{name: name} = start_buffer_service(context, retire_grace_ms: 0)

      {:ok, ack} = Client.write_batch(name, @table, batch(1..1))
      :ok = Client.retire(name, @table, [ack.segment_id], 5)

      assert Eventually.until(fn -> Client.hot_manifest(name, @table) == {:ok, []} end)
      assert Client.retire(name, @table, [ack.segment_id], 5) == :ok
    end

    test "is idempotent for an id the node never held", context do
      %{name: name} = start_buffer_service(context, [])

      {:ok, _ack} = Client.write_batch(name, @table, batch(1..1))

      assert Client.retire(name, @table, ["01KYWPEEGAM8FQVQS5S2QF26SV"], 5) == :ok
    end

    test "survives a restart, so a sealed segment is not re-sealed", context do
      %{name: name, runtime: runtime} = start_buffer_service(context, [])

      {:ok, ack} = Client.write_batch(name, @table, batch(1..1))
      :ok = Client.retire(name, @table, [ack.segment_id], 11)

      Process.exit(buffer(name), :kill)

      assert Eventually.until(fn ->
               match?(
                 [%{sealed_at: 11}],
                 HotManifest.entries(runtime.manifest, @table)
               )
             end)
    end

    test "routes to the owner rather than refusing", context do
      %{name: name} = start_buffer_service(context, ring: [:"buffer1@nonexistent.invalid"])

      assert Client.owner(name, @table) == :"buffer1@nonexistent.invalid"

      assert {:error, {kind, _reason}} =
               Client.retire(name, @table, ["01KYWPEEGAM8FQVQS5S2QF26SV"], 1)

      assert kind in [:badrpc, :badtcp]
    end
  end

  describe "grace-period sweep" do
    test "deletes a retired segment once its grace period expires", context do
      %{name: name, runtime: runtime} = start_buffer_service(context, retire_grace_ms: 0)

      {:ok, ack} = Client.write_batch(name, @table, batch(1..1))
      {:ok, [entry]} = Client.hot_manifest(name, @table)

      :ok = Client.retire(name, @table, [ack.segment_id], 3)

      assert Eventually.until(fn -> Client.hot_manifest(name, @table) == {:ok, []} end)
      refute File.exists?(Store.location(runtime.store, entry.key))
    end

    test "keeps a retired segment inside its grace period", context do
      %{name: name, runtime: runtime} = start_buffer_service(context, retire_grace_ms: 600_000)

      {:ok, ack} = Client.write_batch(name, @table, batch(1..1))
      :ok = Client.retire(name, @table, [ack.segment_id], 3)

      Process.sleep(80)

      assert {:ok, [entry]} = Client.hot_manifest(name, @table)
      assert File.exists?(Store.location(runtime.store, entry.key))
    end

    test "never deletes an unsealed segment", context do
      %{name: name} = start_buffer_service(context, retire_grace_ms: 0)

      {:ok, ack} = Client.write_batch(name, @table, batch(1..1))

      :ok = TableBuffer.maintain(buffer(name))

      assert {:ok, [entry]} = Client.hot_manifest(name, @table)
      assert entry.id == ack.segment_id
    end
  end

  defp flush_messages do
    receive do
      {:seal_ready, _table, _claim} -> flush_messages()
    after
      0 -> :ok
    end
  end
end
