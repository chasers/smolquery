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
  Regression test for TLA+ finding F-2 (`tla/FINDINGS.md`,
  `tla/SegmentShipping.tla`): a failed flush's compensating drop was
  fire-and-forget, and the read side merges hot manifests from every member —
  so a follower that applied the entry (ack lost) and missed the drop served
  rows the caller was told FAILED, with no crash and no promotion, and a
  retry double-counted them under a fresh entry id forever. The same
  `batch_id` did not dedup it, because the compensating local drop also
  forgot the batch record (`hot_manifest.ex` `forget_batches`).

  The fix (T-390) owes the drop durably: the committer records it with the
  compensation, and the owner re-ships it on the maintenance tick until the
  replicas ack (`SegmentShipping_durabledrop.cfg` is the model of this).

  One network hiccup drives the race: the first ship is applied but its ack
  times out; the compensating drop is lost in the same window. Everything
  else is the production path: the fault sits at the `Transport` behaviour —
  the same seam that picks gen_rpc over local calls in a cluster — the
  re-ship is the owner's own maintenance tick, and the read goes through
  `QueryService.Client.query/3` against the follower's hot server, the view
  the planner's every-member fan-out would merge in a cluster. The test
  asserts the corrected contract: the zombie clears on its own, the
  same-`batch_id` retry lands once, and no query ever again serves rows the
  caller was told failed.
  """

  use ExUnit.Case, async: false

  alias Smolquery.BufferService
  alias Smolquery.BufferService.Client
  alias Smolquery.BufferService.Endpoint
  alias Smolquery.BufferService.HotManifest
  alias Smolquery.BufferService.HotServer
  alias Smolquery.BufferService.Replicator.FailedFlushZombieTest.LossyTransport
  alias Smolquery.BufferService.Replicator.SegmentShipping
  alias Smolquery.BufferService.Runtime
  alias Smolquery.Catalog
  alias Smolquery.Catalog.DuckLake
  alias Smolquery.QueryService
  alias Smolquery.Schema
  alias Smolquery.Test.Eventually

  @moduletag :integration
  @moduletag :tmp_dir

  @table {"analytics", "events"}

  defp start_instance(context, opts \\ []) do
    opts =
      Keyword.merge(
        [
          name: :"zombie_#{:erlang.unique_integer([:positive])}",
          flush_interval_ms: 25,
          maintenance_interval_ms: 50,
          seal_retry_ms: 25
        ],
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

  defp start_follower_view(context, follower) do
    storage = :"zombie_catalog_#{:erlang.unique_integer([:positive])}"
    query = :"zombie_query_#{:erlang.unique_integer([:positive])}"
    metadata = "sqlite:#{Path.join(context.tmp_dir, "catalog.sqlite")}"
    data_path = Path.join(context.tmp_dir, "ducklake")

    start_supervised!(
      {DuckLake,
       name: Smolquery.StorageService.Runtime.catalog_engine(storage),
       metadata: metadata,
       data_path: data_path},
      id: storage
    )

    catalog = DuckLake.new(engine: Smolquery.StorageService.Runtime.catalog_engine(storage))
    :ok = Catalog.create_dataset(catalog, "analytics")
    :ok = Catalog.create_table(catalog, @table, Schema.new!([{"id", :int64}]))

    start_supervised!(
      {QueryService.Supervisor,
       name: query,
       catalog: catalog,
       buffer_name: follower,
       buffer_base_url: HotServer.base_url(follower),
       engine_extensions: [:httpfs],
       allowed_directories: [context.tmp_dir],
       job_bootstrap: [
         DuckLake.attach_statement(DuckLake.default_catalog(), metadata, data_path)
       ]},
      id: query
    )

    on_exit(fn -> QueryService.Runtime.delete(query) end)

    query
  end

  defp follower_view_ids(query) do
    case QueryService.Client.query(query, "SELECT id FROM analytics.events ORDER BY id") do
      {:ok, _job, %Explorer.DataFrame{} = frame} ->
        frame |> Explorer.DataFrame.to_columns() |> Map.get("id", [])

      {:ok, job, nil} ->
        {:query_failed, job.error}
    end
  end

  test "F-2: a lost drop is owed, re-shipped until the zombie clears, and a retry counts once",
       context do
    :ok = LossyTransport.script(self())

    follower = start_instance(context)
    view = start_follower_view(context, follower)

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

    {:ok, runtime} = Runtime.fetch(owner)

    assert entries(owner) == []
    assert [zombie] = entries(follower)
    assert zombie.claim_keys == []
    assert HotManifest.owed_drops(runtime.manifest, @table) == [zombie.id]

    assert Eventually.until(
             fn ->
               entries(follower) == [] and
                 HotManifest.owed_drops(runtime.manifest, @table) == []
             end,
             400,
             25
           )

    assert follower_view_ids(view) == []

    assert {:ok, ack} = Client.write_batch(owner, @table, batch([%{"id" => 1}], "b-zombie"))
    refute ack.segment_id == zombie.id

    assert [replica] = entries(follower)
    assert replica.id == ack.segment_id
    assert follower_view_ids(view) == [1]
  end
end
