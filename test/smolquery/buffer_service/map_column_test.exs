defmodule Smolquery.BufferService.MapColumnTest do
  @moduledoc """
  A `MAP(STRING, STRING)` schema through a real buffer, both batch shapes.

  Explorer cannot write a Parquet `MAP`, so a rows or frame batch against a map
  schema reaches DuckDB as NDJSON — the committer's re-encode — and lands as
  the same MAP segment an unparsed body does.
  """

  use ExUnit.Case, async: false

  alias Smolquery.BufferService
  alias Smolquery.BufferService.Client
  alias Smolquery.BufferService.HotManifest
  alias Smolquery.BufferService.Runtime
  alias Smolquery.Engine
  alias Smolquery.Schema

  @moduletag :tmp_dir
  @table {"logs", "events"}
  @engine __MODULE__.Engine

  defp schema do
    Schema.new!([{"id", :int64, nullable: false}, {"attrs", {:map, :string, :string}}])
  end

  defp rows_batch, do: %{schema: schema(), rows: rows()}

  defp rows do
    [
      %{"id" => 1, "attrs" => %{"host" => "h1", "pod" => "api-7"}},
      %{"id" => 2, "attrs" => %{"host" => "h2"}},
      %{"id" => 3}
    ]
  end

  defp ndjson_batch do
    body = Enum.map_join(rows(), "\n", &JSON.encode!/1) <> "\n"

    %{schema: schema(), ndjson: body, row_count: 3, byte_size: byte_size(body)}
  end

  defp start_buffer(context) do
    name = :"buffer_map_#{:erlang.unique_integer([:positive])}"

    opts = [
      name: name,
      dir: Path.join(context.tmp_dir, "buffer"),
      flush_interval_ms: 25,
      write_pool_size: 1
    ]

    start_supervised!({BufferService.Supervisor, opts}, id: name)
    on_exit(fn -> Runtime.delete(name) end)

    {name, Runtime.new(opts)}
  end

  defp hosts(runtime) do
    start_supervised!({Engine, name: @engine, extensions: []})

    [entry] = HotManifest.entries(runtime.manifest, @table)
    path = Path.join(runtime.store.config.dir, entry.key)

    {:ok, %{rows: rows}} =
      Engine.query(@engine, "SELECT id, attrs['host'] FROM read_parquet($1) ORDER BY id", [path])

    rows
  end

  describe "through the DuckDB writer" do
    test "a rows batch lands as a MAP segment, the same as an unparsed body", context do
      {name, runtime} = start_buffer(context)

      assert {:ok, ack} = Client.write_batch(name, @table, rows_batch())
      assert ack.row_count == 3

      assert hosts(runtime) == [[1, "h1"], [2, "h2"], [3, nil]]
    end

    test "a frame batch without the map column lands as a MAP segment too", context do
      {name, runtime} = start_buffer(context)
      frame = Explorer.DataFrame.new(id: Explorer.Series.from_list([1, 2, 3], dtype: {:s, 64}))
      batch = %{schema: schema(), frame: frame, byte_size: 24}

      assert {:ok, ack} = Client.write_batch(name, @table, batch)
      assert ack.row_count == 3

      assert hosts(runtime) == [[1, nil], [2, nil], [3, nil]]
    end

    test "an unparsed body lands as a MAP segment", context do
      {name, runtime} = start_buffer(context)

      assert {:ok, ack} = Client.write_batch(name, @table, ndjson_batch())
      assert ack.row_count == 3

      assert hosts(runtime) == [[1, "h1"], [2, "h2"], [3, nil]]
    end
  end
end
