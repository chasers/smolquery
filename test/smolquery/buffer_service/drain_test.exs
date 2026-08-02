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
  alias Smolquery.BufferService.Membership
  alias Smolquery.BufferService.Runtime
  alias Smolquery.Cluster
  alias Smolquery.Schema
  alias Smolquery.Test.SealCollector

  @moduletag :tmp_dir

  @table_a {"analytics", "events_a"}
  @table_b {"analytics", "events_b"}

  defp batch(range) do
    %{schema: Schema.new!([{"id", :int64}]), rows: for(i <- range, do: %{"id" => i})}
  end

  defp start_buffer_service(context) do
    name = :"drain_#{:erlang.unique_integer([:positive])}"

    opts = [
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

    start_supervised!({BufferService.Supervisor, opts}, id: name)
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

  test "leave/2 is called with this instance's supervisor pid on success", context do
    name = start_buffer_service(context)
    supervisor = Process.whereis(Runtime.supervisor(name))

    previous = Application.fetch_env(:smolquery, Cluster)

    Application.put_env(:smolquery, Cluster,
      enabled: true,
      postgres: [hostname: "localhost", port: 5432, username: "postgres", database: "postgres"]
    )

    on_exit(fn -> restore_cluster(previous) end)

    :pg.start_link(Cluster.pg_scope())
    Membership.join(name, supervisor)

    assert Membership.nodes(name, []) == [node()]

    assert :ok = Drain.drain(name, poll_ms: 10, timeout_ms: 500)
    assert Membership.nodes(name, []) == []
  end

  defp restore_cluster({:ok, value}), do: Application.put_env(:smolquery, Cluster, value)
  defp restore_cluster(:error), do: Application.delete_env(:smolquery, Cluster)
end
