defmodule Smolquery.BufferService.Replicator.FailedFlushZombieTest.LossyTransport do
  @moduledoc """
  A `Smolquery.BufferService.Transport` that loses exactly two replies, the
  way one network hiccup does: the first `accept_replica` is DELIVERED but
  its ack is lost (the owner sees a timeout after the follower fsynced), and
  the first `:drop` mutation — the compensation for that same flush — is
  lost outright. Everything after passes through `Transport.Local`.
  """

  @behaviour Smolquery.BufferService.Transport

  alias Smolquery.BufferService.Transport

  def script(test) do
    :ets.new(__MODULE__, [:named_table, :public])
    :ets.insert(__MODULE__, {:ack_lost, true})
    :ets.insert(__MODULE__, {:drop_lost, true})
    :ets.insert(__MODULE__, {:test, test})

    :ok
  end

  @impl Transport
  def invoke(node, channel, :accept_replica = function, args, timeout) do
    if take(:ack_lost) do
      Transport.Local.invoke(node, channel, function, args, timeout)

      {:error, {:badrpc, :timeout}}
    else
      Transport.Local.invoke(node, channel, function, args, timeout)
    end
  end

  @impl Transport
  def invoke(node, channel, :apply_replica_mutation = function, args, timeout) do
    if match?([_instance, _table_ref, :drop | _rest], args) and take(:drop_lost) do
      {:error, {:badrpc, :timeout}}
    else
      Transport.Local.invoke(node, channel, function, args, timeout)
    end
  end

  @impl Transport
  def invoke(node, channel, function, args, timeout),
    do: Transport.Local.invoke(node, channel, function, args, timeout)

  defp take(key) do
    case :ets.lookup(__MODULE__, key) do
      [{^key, true}] ->
        :ets.insert(__MODULE__, {key, false})

        true

      _spent ->
        false
    end
  end
end

defmodule Smolquery.BufferService.Replicator.FailedFlushZombieTest do
  @moduledoc """
  Replication of TLA+ finding F-2 (`tla/FINDINGS.md`,
  `tla/SegmentShipping.tla`): a failed flush's compensating drop is
  fire-and-forget, and the read side merges hot manifests from every member —
  so a follower that applied the entry and missed the drop serves rows the
  caller was told FAILED, with no crash and no promotion. A retry then
  double-counts them under a fresh entry id the planner's dedupe-by-id cannot
  tie back — and the same `batch_id` does not dedup it, because the
  compensating local drop also forgot the batch record
  (`hot_manifest.ex` `forget_batches`).

  One network hiccup produces it: the first ship is applied but its ack times
  out; the compensating drop is lost in the same window.

  A PASS of this test means the bug is present. Invert the assertions to the
  correct contract (zombie compensated, retry counted once) when F-2 is
  fixed.
  """

  use ExUnit.Case, async: true

  alias Smolquery.BufferService
  alias Smolquery.BufferService.Client
  alias Smolquery.BufferService.Endpoint
  alias Smolquery.BufferService.Replicator.FailedFlushZombieTest.LossyTransport
  alias Smolquery.BufferService.Replicator.SegmentShipping
  alias Smolquery.BufferService.Runtime
  alias Smolquery.Schema

  @moduletag :tmp_dir

  @table {"analytics", "events"}

  defp start_instance(context, opts \\ []) do
    opts =
      Keyword.merge(
        [name: :"zombie_#{:erlang.unique_integer([:positive])}", flush_interval_ms: 25],
        opts
      )

    name = Keyword.fetch!(opts, :name)
    opts = Keyword.put_new(opts, :dir, Path.join(context.tmp_dir, "#{name}"))
    start_supervised!({BufferService.Supervisor, opts}, id: name)
    on_exit(fn -> Runtime.delete(name) end)

    name
  end

  defp batch(rows, batch_id) do
    %{schema: Schema.new!([{"id", :int64}]), rows: rows, batch_id: batch_id}
  end

  defp entries(name) do
    {:ok, entries} = Endpoint.hot_manifest(name, @table)

    entries
  end

  defp planner_union(owner, follower) do
    (entries(owner) ++ entries(follower))
    |> Enum.uniq_by(& &1.id)
  end

  test "F-2: a lost drop strands a zombie the read side serves; a retry double-counts",
       context do
    :ok = LossyTransport.script(self())

    follower = start_instance(context)

    owner =
      start_instance(
        context,
        replicator:
          {SegmentShipping,
           replication_factor: 2,
           targets: fn _name, _ref -> {:ok, [{LossyTransport, node(), follower}]} end}
      )

    assert {:error, {:replication_failed, _node, _reason}} =
             Client.write_batch(owner, @table, batch([%{"id" => 1}], "b-zombie"))

    assert entries(owner) == []
    assert [zombie] = entries(follower)
    assert zombie.row_count == 1
    assert zombie.claim_keys == []

    assert [_zombie] = planner_union(owner, follower)

    assert {:ok, ack} = Client.write_batch(owner, @table, batch([%{"id" => 1}], "b-zombie"))
    refute ack.segment_id == zombie.id

    assert [_first, _second] = union = planner_union(owner, follower)
    assert Enum.sum(Enum.map(union, & &1.row_count)) == 2
  end
end
