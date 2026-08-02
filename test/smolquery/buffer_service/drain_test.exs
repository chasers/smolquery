defmodule Smolquery.BufferService.DrainTest do
  @moduledoc """
  Milestone 8 L4's drain sequence (PL-11 D4): force-seal, wait for
  retirement, leave the ring — exercised with `Smolquery.Test.SealCollector`
  standing in for a real `StorageService.Handoff`, the same way
  `sealing_test.exs` drives seal signalling without a sealer.
  """

  use ExUnit.Case, async: false

  alias Smolquery.BufferService
  alias Smolquery.BufferService.Client
  alias Smolquery.BufferService.Drain
  alias Smolquery.BufferService.Runtime
  alias Smolquery.Cluster
  alias Smolquery.Cluster.PgGroup
  alias Smolquery.Schema
  alias Smolquery.Test.SealCollector

  @moduletag :tmp_dir

  @table_a {"analytics", "events_a"}
  @table_b {"analytics", "events_b"}

  defp batch(range) do
    %{schema: Schema.new!([{"id", :int64}]), rows: for(i <- range, do: %{"id" => i})}
  end

  defp service_options(context, name) do
    [
      name: name,
      dir: Path.join(context.tmp_dir, "buffer"),
      flush_interval_ms: 25,
      maintenance_interval_ms: 20,
      seal_consumer: {SealCollector, self()},
      seal_max_files: 1_000_000,
      seal_max_bytes: 1_000_000_000,
      seal_max_age_ms: 60_000,
      retire_grace_ms: 600_000
    ]
  end

  defp start_buffer_service(context) do
    name = :"drain_#{:erlang.unique_integer([:positive])}"

    start_supervised!({BufferService.Supervisor, service_options(context, name)}, id: name)
    on_exit(fn -> Runtime.delete(name) end)

    name
  end

  defp respond_to_seals(name, tables), do: respond_loop(name, MapSet.new(tables))

  defp respond_loop(name, remaining) do
    if MapSet.size(remaining) == 0 do
      :ok
    else
      receive do
        {:seal_ready, table_ref, claim} ->
          :ok = Client.retire(name, table_ref, claim.ids, 1)
          respond_loop(name, MapSet.delete(remaining, table_ref))
      after
        5_000 -> flunk("never saw a seal_ready for #{inspect(MapSet.to_list(remaining))}")
      end
    end
  end

  test "drains: force-seals every owned table, waits for retirement, then leaves", context do
    name = start_buffer_service(context)

    {:ok, _ack} = Client.write_batch(name, @table_a, batch(1..2))
    {:ok, _ack} = Client.write_batch(name, @table_b, batch(1..2))

    refute Drain.draining?(name)

    drain = Task.async(fn -> Drain.drain(name, poll_ms: 10, timeout_ms: 2_000) end)

    respond_to_seals(name, [@table_a, @table_b])

    assert Task.await(drain) == :ok
    assert Drain.draining?(name)
  end

  test "rejects new writes for the whole instance once draining starts", context do
    name = start_buffer_service(context)

    {:ok, _ack} = Client.write_batch(name, @table_a, batch(1..1))

    drain = Task.async(fn -> Drain.drain(name, poll_ms: 10, timeout_ms: 2_000) end)

    respond_to_seals(name, [@table_a])

    assert Task.await(drain) == :ok

    assert Client.write_batch(name, @table_a, batch(2..2)) == {:error, :draining}
    assert Client.write_batch(name, @table_b, batch(1..1)) == {:error, :draining}
  end

  test "a table with nothing unsealed drains immediately", context do
    name = start_buffer_service(context)

    assert :ok = Drain.drain(name, poll_ms: 10, timeout_ms: 500)
  end

  test "times out, and does not leave the ring, if retirement never comes", context do
    name = start_buffer_service(context)
    {:ok, _ack} = Client.write_batch(name, @table_a, batch(1..1))

    drain = Task.async(fn -> Drain.drain(name, poll_ms: 10, timeout_ms: 300) end)

    assert_receive {:seal_ready, @table_a, _claim}, 1_000

    assert Task.await(drain) == {:error, {:drain_timeout, [@table_a]}}
    assert Drain.draining?(name)
  end

  test "drain leaves the ring on success, and a restarted subtree does not rejoin", context do
    previous = Application.fetch_env(:smolquery, Cluster)

    Application.put_env(:smolquery, Cluster,
      enabled: true,
      postgres: [hostname: "localhost", port: 5432, username: "postgres", database: "postgres"]
    )

    on_exit(fn -> restore_cluster(previous) end)

    ensure_pg_scope!()

    name = start_buffer_service(context)

    assert PgGroup.nodes(BufferService, name, []) == [node()]

    assert :ok = Drain.drain(name, poll_ms: 10, timeout_ms: 500)
    assert PgGroup.nodes(BufferService, name, []) == []

    stop_supervised!(name)
    start_supervised!({BufferService.Supervisor, service_options(context, name)}, id: {name, 2})

    assert PgGroup.nodes(BufferService, name, []) == []
  end

  test "handoff refuses new writes, flushes the tail, and seals nothing", context do
    name = start_buffer_service(context)

    assert {:ok, _ack} = Client.write_batch(name, @table_a, batch(1..10))
    assert :ok = Drain.handoff(name)

    assert Drain.draining?(name)
    assert {:error, :draining} = Client.write_batch(name, @table_a, batch(11..20))
    refute_receive {:seal_ready, @table_a, _claim}, 100
  end

  defp ensure_pg_scope! do
    case :pg.start_link(Cluster.pg_scope()) do
      {:ok, _pid} -> :ok
      {:error, {:already_started, _pid}} -> :ok
    end
  end

  defp restore_cluster({:ok, value}), do: Application.put_env(:smolquery, Cluster, value)
  defp restore_cluster(:error), do: Application.delete_env(:smolquery, Cluster)
end
