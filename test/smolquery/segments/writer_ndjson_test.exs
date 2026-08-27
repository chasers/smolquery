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

  describe "a MAP(STRING, STRING) column" do
    defp map_schema do
      Schema.new!([{"id", :int64}, {"attrs", {:map, :string, :string}}])
    end

    test "is written as a Parquet MAP that DuckDB reads by key", %{tmp_dir: dir} do
      path =
        spool(dir, "map.ndjson", [
          %{"id" => 1, "attrs" => %{"host" => "h1", "pod" => "api-7"}},
          %{"id" => 2, "attrs" => %{"host" => "h2"}},
          %{"id" => 3},
          %{"id" => 4, "attrs" => %{}}
        ])

      {:ok, segment} =
        Writer.write({:ndjson, [path]}, map_schema(), store: store(dir), engine: @engine)

      {:ok, %{rows: rows}} =
        Engine.query(
          @engine,
          "SELECT id, attrs['host'], cardinality(attrs) FROM read_parquet($1) ORDER BY id",
          [segment.path]
        )

      assert rows == [[1, "h1", 2], [2, "h2", 1], [3, nil, nil], [4, nil, 0]]

      {:ok, %{rows: [[type]]}} =
        Engine.query(@engine, "SELECT typeof(attrs) FROM read_parquet($1) LIMIT 1", [
          segment.path
        ])

      assert type == "MAP(VARCHAR, VARCHAR)"
    end

    test "carries a null count but no bounds, so nothing prunes on it", %{tmp_dir: dir} do
      path =
        spool(dir, "map_stats.ndjson", [
          %{"id" => 1, "attrs" => %{"host" => "h1"}},
          %{"id" => 2}
        ])

      {:ok, segment} =
        Writer.write({:ndjson, [path]}, map_schema(), store: store(dir), engine: @engine)

      assert segment.stats["attrs"] == %{min: nil, max: nil, null_count: 1}
      assert segment.stats["id"] == %{min: 1, max: 2, null_count: 0}
    end

    test "stringifies a non-string value exactly as value_from_json/2 does", %{tmp_dir: dir} do
      attrs = %{
        "n" => 1,
        "ratio" => 1.5,
        "ok" => true,
        "tags" => ["a", "b"],
        "nested" => %{"k" => "v"},
        "none" => nil
      }

      path = spool(dir, "map_mixed.ndjson", [%{"id" => 1, "attrs" => attrs}])

      {:ok, segment} =
        Writer.write({:ndjson, [path]}, map_schema(), store: store(dir), engine: @engine)

      {:ok, %{rows: [[read]]}} =
        Engine.query(@engine, "SELECT attrs FROM read_parquet($1)", [segment.path])

      assert {:ok, coerced} = Schema.value_from_json({:map, :string, :string}, attrs)
      assert read == coerced
    end

    test "refuses a value that is not an object as a failed flush", %{tmp_dir: dir} do
      path = spool(dir, "map_bad.ndjson", [%{"id" => 1, "attrs" => "host=h1"}])

      assert {:error, {:put_failed, _key, {:ndjson_copy_failed, message}}} =
               Writer.write({:ndjson, [path]}, map_schema(), store: store(dir), engine: @engine)

      assert message =~ "OBJECT"
    end
  end

  describe "a VARIANT column" do
    defp variant_schema do
      Schema.new!([{"id", :int64}, {"attrs", :variant}])
    end

    test "is written as JSON text that reads as a VARIANT keeping each value's type", %{
      tmp_dir: dir
    } do
      path =
        spool(dir, "variant.ndjson", [
          %{"id" => 1, "attrs" => %{"host" => "h1", "n" => 1, "tags" => ["a", "b"]}},
          %{"id" => 2, "attrs" => %{"host" => "h2", "n" => "x"}},
          %{"id" => 3},
          %{"id" => 4, "attrs" => "just a string"},
          %{"id" => 5, "attrs" => [1, 2]}
        ])

      {:ok, segment} =
        Writer.write({:ndjson, [path]}, variant_schema(), store: store(dir), engine: @engine)

      {:ok, %{rows: rows}} =
        Engine.query(
          @engine,
          "SELECT id, attrs['host']::VARCHAR, TRY_CAST(attrs['n'] AS BIGINT), variant_typeof(attrs), " <>
            "attrs::JSON::VARCHAR FROM (SELECT id, attrs::VARIANT AS attrs FROM read_parquet($1)) ORDER BY id",
          [segment.path]
        )

      assert rows == [
               [1, "h1", 1, "OBJECT(host, n, tags)", ~s({"host":"h1","n":1,"tags":["a","b"]})],
               [2, "h2", nil, "OBJECT(host, n)", ~s({"host":"h2","n":"x"})],
               [3, nil, nil, "VARIANT_NULL", "null"],
               [4, nil, nil, "VARCHAR", ~s("just a string")],
               [5, nil, nil, "ARRAY(2)", "[1,2]"]
             ]

      {:ok, %{rows: [[type]]}} =
        Engine.query(@engine, "SELECT typeof(attrs) FROM read_parquet($1) LIMIT 1", [
          segment.path
        ])

      assert type == "JSON"
    end

    test "carries a null count but no bounds", %{tmp_dir: dir} do
      path =
        spool(dir, "variant_stats.ndjson", [%{"id" => 1, "attrs" => %{"a" => 1}}, %{"id" => 2}])

      {:ok, segment} =
        Writer.write({:ndjson, [path]}, variant_schema(), store: store(dir), engine: @engine)

      assert segment.stats["attrs"] == %{min: nil, max: nil, null_count: 1}
    end

    test "takes any JSON value, so readable_ndjson?/3 never refuses a shape", %{tmp_dir: dir} do
      path =
        spool(dir, "variant_ok.ndjson", [
          %{"id" => 1, "attrs" => %{"a" => [1]}},
          %{"id" => 2, "attrs" => "s"}
        ])

      assert Writer.readable_ndjson?(@engine, path, variant_schema())
    end
  end

  describe "ndjson_problem/3" do
    test "names what DuckDB refuses, in DuckDB's words", %{tmp_dir: dir} do
      path = spool(dir, "refused.ndjson", [%{"tenant" => "a", "id" => "not-an-integer"}])

      assert {:refused, message} = Writer.ndjson_problem(@engine, path, schema())
      assert message =~ "not-an-integer"
      refute Writer.readable_ndjson?(@engine, path, schema())

      fine = spool(dir, "fine.ndjson", [%{"tenant" => "a", "id" => 1, "ratio" => 1.0}])
      assert Writer.ndjson_problem(@engine, fine, schema()) == :ok
    end
  end
end
