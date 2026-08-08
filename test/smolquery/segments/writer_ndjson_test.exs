defmodule Smolquery.Segments.WriterNdjsonTest do
  use ExUnit.Case, async: false

  alias Smolquery.Engine
  alias Smolquery.Schema
  alias Smolquery.Segments.Store
  alias Smolquery.Segments.Writer

  @moduletag :tmp_dir
  @engine __MODULE__.Engine

  setup do
    start_supervised!({Engine, name: @engine, extensions: []})

    :ok
  end

  defp store(dir), do: Store.Local.new(dir: dir)

  defp schema(clustering \\ []) do
    %{
      Schema.new!([
        {"tenant", :string},
        {"id", :int64},
        {"ratio", :float64}
      ])
      | clustering: clustering
    }
  end

  defp spool(dir, name, rows) do
    path = Path.join(dir, name)
    File.write!(path, Enum.map_join(rows, "\n", &JSON.encode!/1) <> "\n")

    path
  end

  test "writes one segment from several spooled bodies", %{tmp_dir: dir} do
    a = spool(dir, "a.ndjson", [%{"tenant" => "b", "id" => 2, "ratio" => 0.5}])
    b = spool(dir, "b.ndjson", [%{"tenant" => "a", "id" => 1, "ratio" => 1.5}])

    {:ok, segment} =
      Writer.write({:ndjson, [a, b]}, schema(), store: store(dir), engine: @engine)

    assert segment.row_count == 2
    assert segment.byte_size > 0
  end

  test "reads the row count from the Parquet footer, not the caller", %{tmp_dir: dir} do
    rows = for i <- 1..37, do: %{"tenant" => "t", "id" => i, "ratio" => i / 2}
    path = spool(dir, "many.ndjson", rows)

    {:ok, segment} = Writer.write({:ndjson, [path]}, schema(), store: store(dir), engine: @engine)

    assert segment.row_count == 37
  end

  test "carries min-max stats DuckDB can compare, strings included", %{tmp_dir: dir} do
    rows = [
      %{"tenant" => "c", "id" => 3, "ratio" => 3.0},
      %{"tenant" => "a", "id" => 1, "ratio" => 1.0}
    ]

    path = spool(dir, "stats.ndjson", rows)

    {:ok, segment} = Writer.write({:ndjson, [path]}, schema(), store: store(dir), engine: @engine)

    assert segment.stats["tenant"].min == "a"
    assert segment.stats["tenant"].max == "c"
    assert segment.stats["id"] == %{min: 1, max: 3, null_count: 0}
  end

  test "sorts on the clustering key", %{tmp_dir: dir} do
    rows = [
      %{"tenant" => "c", "id" => 3, "ratio" => 3.0},
      %{"tenant" => "a", "id" => 1, "ratio" => 1.0},
      %{"tenant" => "b", "id" => 2, "ratio" => 2.0}
    ]

    path = spool(dir, "sorted.ndjson", rows)

    {:ok, segment} =
      Writer.write({:ndjson, [path]}, schema(["tenant"]), store: store(dir), engine: @engine)

    {:ok, %{rows: read}} =
      Engine.query(@engine, "SELECT tenant FROM read_parquet($1)", [segment.path])

    assert List.flatten(read) == ~w(a b c)
  end

  test "refuses an empty path list rather than writing an empty segment", %{tmp_dir: dir} do
    assert {:error, :no_rows} =
             Writer.write({:ndjson, []}, schema(), store: store(dir), engine: @engine)
  end

  # The whole batch fails, not one row — the trade this path makes for never
  # building a term. The message names the file and the value, because a caller
  # who cannot be told which row failed must at least be told what did.
  test "reports a value the schema cannot take as a failed flush", %{tmp_dir: dir} do
    path = spool(dir, "bad.ndjson", [%{"tenant" => "a", "id" => "not-an-integer"}])

    assert {:error, {:put_failed, _key, {:ndjson_copy_failed, message}}} =
             Writer.write({:ndjson, [path]}, schema(), store: store(dir), engine: @engine)

    assert message =~ "not-an-integer"
  end
end
