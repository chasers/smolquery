defmodule Smolquery.Segments.WriterFieldIdsTest do
  @moduledoc """
  The writer stamps the catalog's column ids into every micro-segment it
  writes, as Parquet field ids (PL-62 IDS-1) — and stamps nothing when the
  schema carries none, so a file is never half-identified.
  """

  use ExUnit.Case, async: false

  alias Smolquery.Engine
  alias Smolquery.Schema
  alias Smolquery.Schema.Field
  alias Smolquery.Segments.Store
  alias Smolquery.Segments.Writer

  @moduletag :tmp_dir
  @engine __MODULE__.Engine

  setup do
    start_supervised!({Engine, name: @engine, extensions: []})
    :ok
  end

  defp spool(dir, rows) do
    path = Path.join(dir, "rows.ndjson")
    File.write!(path, Enum.map_join(rows, "\n", &JSON.encode!/1) <> "\n")
    path
  end

  defp field_ids_in(segment) do
    @engine
    |> Engine.query!(
      "SELECT name, field_id FROM parquet_schema($1) WHERE name != 'duckdb_schema'",
      [segment.path]
    )
    |> Map.fetch!(:rows)
    |> Map.new(fn [name, id] -> {name, id} end)
  end

  test "stamps every column with the id the catalog gave it", %{tmp_dir: dir} do
    schema =
      Schema.new!([
        Field.new!("tenant", :string, id: 3),
        Field.new!("id", :int64, id: 1),
        Field.new!("ratio", :float64, id: 7)
      ])

    path = spool(dir, [%{"tenant" => "a", "id" => 1, "ratio" => 0.5}])

    {:ok, segment} =
      Writer.write({:ndjson, [path]}, schema, store: Store.Local.new(dir: dir), engine: @engine)

    assert field_ids_in(segment) == %{"tenant" => 3, "id" => 1, "ratio" => 7}
  end

  test "stamps nothing when the schema has no ids, and nothing when it has only some",
       %{tmp_dir: dir} do
    path = spool(dir, [%{"tenant" => "a", "id" => 1}])
    store = Store.Local.new(dir: dir)

    anonymous = Schema.new!([{"tenant", :string}, {"id", :int64}])
    {:ok, segment} = Writer.write({:ndjson, [path]}, anonymous, store: store, engine: @engine)
    assert field_ids_in(segment) == %{"tenant" => nil, "id" => nil}

    partial = Schema.new!([Field.new!("tenant", :string, id: 1), Field.new!("id", :int64)])
    {:ok, segment} = Writer.write({:ndjson, [path]}, partial, store: store, engine: @engine)
    assert field_ids_in(segment) == %{"tenant" => nil, "id" => nil}
  end
end
