defmodule Smolquery.BufferService.ReplicatorTest do
  use ExUnit.Case, async: true

  alias Smolquery.BufferService
  alias Smolquery.BufferService.Client
  alias Smolquery.BufferService.Endpoint
  alias Smolquery.BufferService.Replicator
  alias Smolquery.BufferService.Runtime
  alias Smolquery.Schema

  @moduletag :tmp_dir

  @table {"analytics", "events"}

  defmodule Probe do
    @moduledoc """
    Reports every commit to the test and answers what the test configured,
    so a test can see exactly what crosses the seam and what a refusal does
    to the flush.
    """

    @behaviour Smolquery.BufferService.Replicator

    @impl Smolquery.BufferService.Replicator
    def new(opts), do: Map.new(opts)

    @impl Smolquery.BufferService.Replicator
    def commit(%{sink: sink, reply: reply}, commit) do
      send(sink, {:replicated, commit})

      reply
    end

    @impl Smolquery.BufferService.Replicator
    def append(%{sink: sink, reply: reply}, mutation) do
      send(sink, {:appended, mutation})

      reply
    end
  end

  defp start_buffer_service(context, opts) do
    opts =
      Keyword.merge(
        [
          name: :"replicated_#{:erlang.unique_integer([:positive])}",
          dir: Path.join(context.tmp_dir, "buffer"),
          flush_interval_ms: 25
        ],
        opts
      )

    name = Keyword.fetch!(opts, :name)
    start_supervised!({BufferService.Supervisor, opts}, id: name)
    on_exit(fn -> Runtime.delete(name) end)

    name
  end

  defp batch(rows, batch_id) do
    %{schema: Schema.new!([{"id", :int64}]), rows: rows, batch_id: batch_id}
  end

  test "None is the default and acks single-copy" do
    replicator = Replicator.new({Replicator.None, []})

    assert Replicator.commit(replicator, %{}) == :ok
    assert Runtime.new(name: :none_default).replicator.impl == Replicator.None
  end

  test "the commit crosses the seam after the local commit, before the ack", context do
    name = start_buffer_service(context, replicator: {Probe, sink: self(), reply: :ok})

    assert {:ok, ack} =
             Client.write_batch(name, @table, batch([%{"id" => 1}, %{"id" => 2}], "batch-1"))

    assert_receive {:replicated, commit}
    assert commit.name == name
    assert commit.table_ref == @table
    assert commit.entry.id == ack.segment_id
    assert commit.entry.row_count == 2
    assert commit.batch_ids == ["batch-1"]
    assert commit.segment.key =~ commit.entry.id
  end

  test "a refused commit fails the flush and compensates the local entry", context do
    name =
      start_buffer_service(context,
        replicator: {Probe, sink: self(), reply: {:error, :follower_down}}
      )

    assert {:error, :follower_down} =
             Client.write_batch(name, @table, batch([%{"id" => 1}], "batch-9"))

    assert_receive {:replicated, refused}
    assert Endpoint.hot_manifest(name, @table) == {:ok, []}

    assert {:error, :follower_down} =
             Client.write_batch(name, @table, batch([%{"id" => 1}], "batch-9"))

    assert_receive {:replicated, retried}
    assert retried.entry.id != refused.entry.id
  end
end
