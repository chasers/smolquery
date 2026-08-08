defmodule Smolquery.BufferService.NdjsonPassthroughTest do
  @moduledoc """
  The unparsed-body path, end to end through the buffer service.

  `writer_ndjson_test.exs` covers the DuckDB write on its own. This covers the
  seam that one does not: a batch carrying `:ndjson` reaching a real buffer,
  accumulating with others, and coming out of a group commit as one segment.
  Nothing tested that seam when it first shipped, and the missing
  `buffer_write/6` clause reached the rig as a 500 on every insert.
  """

  use ExUnit.Case, async: false

  alias Smolquery.BufferService
  alias Smolquery.BufferService.Client
  alias Smolquery.BufferService.HotManifest
  alias Smolquery.BufferService.Runtime
  alias Smolquery.Schema

  @moduletag :tmp_dir
  @table {"logs", "events"}

  defp schema do
    Schema.new!([{"id", :int64, nullable: false}, {"tenant", :string}])
  end

  defp ndjson_batch(range) do
    body =
      range
      |> Enum.map_join("\n", &JSON.encode!(%{"id" => &1, "tenant" => "t#{rem(&1, 3)}"}))
      |> Kernel.<>("\n")

    %{
      schema: schema(),
      ndjson: body,
      row_count: Enum.count(range),
      byte_size: byte_size(body)
    }
  end

  setup context do
    name = :"buffer_ndjson_#{:erlang.unique_integer([:positive])}"

    opts = [
      name: name,
      dir: Path.join(context.tmp_dir, "buffer"),
      flush_interval_ms: 25,
      flush_writer: :duckdb,
      write_pool_size: 1
    ]

    start_supervised!({BufferService.Supervisor, opts}, id: name)
    on_exit(fn -> Runtime.delete(name) end)

    %{name: name, runtime: Runtime.new(opts)}
  end

  test "acks an unparsed body once its segment is in the manifest", %{
    name: name,
    runtime: runtime
  } do
    assert {:ok, ack} = Client.write_batch(name, @table, ndjson_batch(1..3))

    assert ack.row_count == 3
    assert [entry] = HotManifest.entries(runtime.manifest, @table)
    assert entry.id == ack.segment_id
    assert entry.row_count == 3
  end

  test "merges several bodies into one segment", %{name: name, runtime: runtime} do
    tasks =
      for range <- [1..10, 11..20, 21..30] do
        Task.async(fn -> Client.write_batch(name, @table, ndjson_batch(range)) end)
      end

    acks = Task.await_many(tasks, 15_000)

    assert Enum.all?(acks, &match?({:ok, _ack}, &1))

    entries = HotManifest.entries(runtime.manifest, @table)

    assert Enum.sum(Enum.map(entries, & &1.row_count)) == 30
  end

  test "the row count comes from the Parquet footer, not the sender", %{
    name: name,
    runtime: runtime
  } do
    # A sender that miscounts must not scale the ack: the footer is authoritative.
    batch = %{ndjson_batch(1..5) | row_count: 99}

    assert {:ok, ack} = Client.write_batch(name, @table, batch)
    assert ack.row_count == 5

    assert [entry] = HotManifest.entries(runtime.manifest, @table)
    assert entry.row_count == 5
  end

  test "a value the schema cannot take fails the batch rather than acking it", %{name: name} do
    body = ~s({"id": "not-an-integer", "tenant": "t0"}\n)

    batch = %{
      schema: schema(),
      ndjson: body,
      row_count: 1,
      byte_size: byte_size(body)
    }

    assert {:error, _reason} = Client.write_batch(name, @table, batch)
  end
end
