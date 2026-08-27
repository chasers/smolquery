defmodule Smolquery.Test.SegmentFixtureTest do
  use ExUnit.Case, async: true

  alias Explorer.DataFrame
  alias Smolquery.Schema
  alias Smolquery.Segments.Id
  alias Smolquery.Segments.Segment
  alias Smolquery.Segments.Store
  alias Smolquery.Test.SegmentFixture

  @moduletag :tmp_dir

  defp store(dir), do: Store.Local.new(dir: dir)

  defp schema do
    Schema.new!([
      {"id", :int64, nullable: false},
      {"ts", :timestamp},
      {"name", :string},
      {"amount", {:numeric, 38, 2}},
      {"ratio", :float64},
      {"ok", :bool},
      {"day", :date}
    ])
  end

  defp clustered_schema(clustering), do: %{schema() | clustering: clustering}

  defp tenant_schema(clustering) do
    %{Schema.new!([{"tenant", :string}, {"id", :int64}]) | clustering: clustering}
  end

  defp rows(count) do
    for i <- 1..count do
      %{
        "id" => i,
        "ts" => NaiveDateTime.add(~N[2026-07-31 12:00:00], i, :second),
        "name" => "row-#{i}",
        "amount" => Decimal.new("#{i}.25"),
        "ratio" => i * 0.5,
        "ok" => rem(i, 2) == 0,
        "day" => Date.add(~D[2026-07-31], i)
      }
    end
  end

  describe "write/3" do
    test "writes a ULID-named segment and describes it", %{tmp_dir: dir} do
      assert {:ok, %Segment{} = segment} =
               SegmentFixture.write(rows(3), schema(), store: store(dir))

      assert Id.valid?(segment.id)
      assert segment.key == segment.id <> ".parquet"
      assert segment.path == Path.join(dir, segment.id <> ".parquet")
      assert File.exists?(segment.path)
      assert segment.row_count == 3
      assert segment.byte_size == File.stat!(segment.path).size
      assert segment.byte_size > 0
    end

    test "writes under a table's prefix", %{tmp_dir: dir} do
      {:ok, prefix} = Store.prefix({"analytics", "events"})

      assert {:ok, segment} =
               SegmentFixture.write(rows(1), schema(), store: store(dir), prefix: prefix)

      assert segment.key == "analytics/events/#{segment.id}.parquet"
      assert segment.path == Path.join(dir, segment.key)
      assert File.exists?(segment.path)
    end

    test "refuses a prefix that could climb out of the store", %{tmp_dir: dir} do
      assert SegmentFixture.write(rows(1), schema(), store: store(dir), prefix: "../escape") ==
               {:error, {:invalid_prefix, "../escape"}}
    end

    test "takes an explicit id", %{tmp_dir: dir} do
      id = Id.generate()

      assert {:ok, segment} = SegmentFixture.write(rows(1), schema(), store: store(dir), id: id)
      assert segment.id == id
      assert Path.basename(segment.path) == id <> ".parquet"
    end

    test "rejects an id that is not a ULID", %{tmp_dir: dir} do
      assert SegmentFixture.write(rows(1), schema(), store: store(dir), id: "../escape") ==
               {:error, {:invalid_segment_id, "../escape"}}
    end

    test "leaves nothing behind in the staging directory", %{tmp_dir: dir} do
      assert {:ok, _segment} = SegmentFixture.write(rows(2), schema(), store: store(dir))

      assert File.ls!(Path.join(dir, ".tmp")) == []
    end

    test "creates the staging directory when the target is fresh", %{tmp_dir: dir} do
      nested = Path.join(dir, "dataset/table")

      assert {:ok, segment} = SegmentFixture.write(rows(1), schema(), store: store(nested))
      assert File.exists?(segment.path)
    end

    test "refuses to write an empty segment", %{tmp_dir: dir} do
      assert SegmentFixture.write([], schema(), store: store(dir)) == {:error, :no_rows}
    end

    test "reports rows that do not fit the schema", %{tmp_dir: dir} do
      rows = [%{"id" => "not an integer"}]

      assert {:error, {:invalid_rows, message}} =
               SegmentFixture.write(rows, schema(), store: store(dir))

      assert is_binary(message)
    end

    test "writes a missing column as null", %{tmp_dir: dir} do
      assert {:ok, segment} = SegmentFixture.write([%{"id" => 1}], schema(), store: store(dir))

      assert segment.row_count == 1
      assert segment.stats["name"].null_count == 1
      assert segment.stats["id"].null_count == 0
    end

    test "round-trips every logical type through the written file", %{tmp_dir: dir} do
      {:ok, segment} = SegmentFixture.write(rows(2), schema(), store: store(dir))

      frame = DataFrame.from_parquet!(segment.path)

      assert DataFrame.names(frame) == ["id", "ts", "name", "amount", "ratio", "ok", "day"]

      assert DataFrame.dtypes(frame) == %{
               "id" => {:s, 64},
               "ts" => {:naive_datetime, :microsecond},
               "name" => :string,
               "amount" => {:decimal, 38, 2},
               "ratio" => {:f, 64},
               "ok" => :boolean,
               "day" => :date
             }
    end
  end

  describe "stats" do
    test "carries min-max for the orderable types pruning uses", %{tmp_dir: dir} do
      {:ok, segment} = SegmentFixture.write(rows(4), schema(), store: store(dir))

      assert segment.stats["id"] == %{min: 1, max: 4, null_count: 0}

      assert segment.stats["ts"] == %{
               min: ~N[2026-07-31 12:00:01.000000],
               max: ~N[2026-07-31 12:00:04.000000],
               null_count: 0
             }

      assert segment.stats["day"] == %{
               min: ~D[2026-08-01],
               max: ~D[2026-08-04],
               null_count: 0
             }

      assert segment.stats["ratio"] == %{min: 0.5, max: 2.0, null_count: 0}
      assert segment.stats["amount"].min == 1.25
      assert segment.stats["amount"].max == 4.25
    end

    test "counts nulls but skips min-max for types with no useful order", %{tmp_dir: dir} do
      {:ok, segment} = SegmentFixture.write(rows(2), schema(), store: store(dir))

      assert segment.stats["name"] == %{min: nil, max: nil, null_count: 0}
      assert segment.stats["ok"] == %{min: nil, max: nil, null_count: 0}
    end

    test "bounds a string column that leads the clustering key", %{tmp_dir: dir} do
      rows = [%{"tenant" => "c"}, %{"tenant" => "a"}, %{"tenant" => "b"}]

      {:ok, segment} = SegmentFixture.write(rows, tenant_schema(["tenant"]), store: store(dir))

      assert segment.stats["tenant"] == %{min: "a", max: "c", null_count: 0}
    end

    test "excludes trailing nulls from a leading string column's max", %{tmp_dir: dir} do
      rows = [%{"tenant" => "b"}, %{"tenant" => nil}, %{"tenant" => "a"}]

      {:ok, segment} = SegmentFixture.write(rows, tenant_schema(["tenant"]), store: store(dir))

      assert segment.stats["tenant"] == %{min: "a", max: "b", null_count: 1}
    end

    test "leaves an all-null leading string column unbounded", %{tmp_dir: dir} do
      rows = [%{"tenant" => nil}, %{"tenant" => nil}]

      {:ok, segment} = SegmentFixture.write(rows, tenant_schema(["tenant"]), store: store(dir))

      assert segment.stats["tenant"] == %{min: nil, max: nil, null_count: 2}
    end

    test "skips a string column the frame is not globally sorted by", %{tmp_dir: dir} do
      rows = [
        %{"id" => 2, "tenant" => "a"},
        %{"id" => 1, "tenant" => "z"},
        %{"id" => 1, "tenant" => "m"}
      ]

      {:ok, segment} =
        SegmentFixture.write(rows, tenant_schema(["id", "tenant"]), store: store(dir))

      assert segment.stats["tenant"] == %{min: nil, max: nil, null_count: 0}
      assert segment.stats["id"] == %{min: 1, max: 2, null_count: 0}
    end

    test "has an entry for every schema column", %{tmp_dir: dir} do
      {:ok, segment} = SegmentFixture.write(rows(1), schema(), store: store(dir))

      assert Map.keys(segment.stats) |> Enum.sort() == Schema.names(schema()) |> Enum.sort()
    end
  end

  describe "clustering" do
    test "leaves row order unchanged when clustering is empty", %{tmp_dir: dir} do
      rows = [
        %{"id" => 3, "ts" => ~N[2026-07-31 12:00:03]},
        %{"id" => 1, "ts" => ~N[2026-07-31 12:00:01]},
        %{"id" => 2, "ts" => ~N[2026-07-31 12:00:02]}
      ]

      schema = Schema.new!([{"id", :int64}, {"ts", :timestamp}])

      assert {:ok, segment} = SegmentFixture.write(rows, schema, store: store(dir))
      frame = DataFrame.from_parquet!(segment.path)
      assert DataFrame.to_columns(frame)["id"] == [3, 1, 2]
    end

    test "sorts list rows by clustering columns in declared order", %{tmp_dir: dir} do
      rows = [
        %{"id" => 2, "ts" => ~N[2026-07-31 12:00:02]},
        %{"id" => 1, "ts" => ~N[2026-07-31 12:00:03]},
        %{"id" => 1, "ts" => ~N[2026-07-31 12:00:01]}
      ]

      assert {:ok, segment} =
               SegmentFixture.write(rows, clustered_schema(["id", "ts"]), store: store(dir))

      frame = DataFrame.from_parquet!(segment.path)

      assert DataFrame.to_columns(frame)["id"] == [1, 1, 2]

      assert DataFrame.to_columns(frame)["ts"] == [
               ~N[2026-07-31 12:00:01.000000],
               ~N[2026-07-31 12:00:03.000000],
               ~N[2026-07-31 12:00:02.000000]
             ]
    end

    test "sorts timestamps chronologically, not by Erlang term order", %{tmp_dir: dir} do
      rows = [
        %{"id" => 1, "ts" => ~N[2026-02-01 00:00:00]},
        %{"id" => 1, "ts" => ~N[2026-01-31 23:59:59]},
        %{"id" => 1, "ts" => ~N[2026-01-31 12:00:30.000001]},
        %{"id" => 1, "ts" => ~N[2026-01-31 12:00:00.500000]}
      ]

      assert {:ok, segment} =
               SegmentFixture.write(rows, clustered_schema(["id", "ts"]), store: store(dir))

      frame = DataFrame.from_parquet!(segment.path)

      assert DataFrame.to_columns(frame)["ts"] == [
               ~N[2026-01-31 12:00:00.500000],
               ~N[2026-01-31 12:00:30.000001],
               ~N[2026-01-31 23:59:59.000000],
               ~N[2026-02-01 00:00:00.000000]
             ]
    end

    test "sorts dates chronologically across a month boundary", %{tmp_dir: dir} do
      rows = [
        %{"day" => ~D[2026-02-01]},
        %{"day" => ~D[2026-01-31]}
      ]

      schema = %{Schema.new!([{"day", :date}]) | clustering: ["day"]}

      assert {:ok, segment} = SegmentFixture.write(rows, schema, store: store(dir))
      frame = DataFrame.from_parquet!(segment.path)
      assert DataFrame.to_columns(frame)["day"] == [~D[2026-01-31], ~D[2026-02-01]]
    end

    test "places null clustering keys last", %{tmp_dir: dir} do
      rows = [
        %{"id" => nil},
        %{"id" => 2},
        %{"id" => 1}
      ]

      schema =
        %{Schema.new!([{"id", :int64}]) | clustering: ["id"]}

      assert {:ok, segment} = SegmentFixture.write(rows, schema, store: store(dir))
      frame = DataFrame.from_parquet!(segment.path)
      assert DataFrame.to_columns(frame)["id"] == [1, 2, nil]
    end

    test "sorts by the clustering columns the schema still has", %{tmp_dir: dir} do
      rows = [
        %{"id" => 2, "ts" => ~N[2026-07-31 12:00:02]},
        %{"id" => 1, "ts" => ~N[2026-07-31 12:00:01]}
      ]

      schema = %{
        Schema.new!([{"id", :int64}, {"ts", :timestamp}])
        | clustering: ["dropped", "id"]
      }

      assert {:ok, segment} = SegmentFixture.write(rows, schema, store: store(dir))
      frame = DataFrame.from_parquet!(segment.path)
      assert DataFrame.to_columns(frame)["id"] == [1, 2]
    end

    test "a key naming only dropped columns sorts nothing", %{tmp_dir: dir} do
      rows = [%{"id" => 2}, %{"id" => 1}]
      schema = %{Schema.new!([{"id", :int64}]) | clustering: ["dropped"]}

      assert {:ok, segment} = SegmentFixture.write(rows, schema, store: store(dir))
      frame = DataFrame.from_parquet!(segment.path)
      assert DataFrame.to_columns(frame)["id"] == [2, 1]
    end

    test "keeps equal clustering keys in stable input order", %{tmp_dir: dir} do
      rows = [
        %{"id" => 1, "name" => "first"},
        %{"id" => 1, "name" => "second"}
      ]

      schema =
        %{Schema.new!([{"id", :int64}, {"name", :string}]) | clustering: ["id"]}

      assert {:ok, segment} = SegmentFixture.write(rows, schema, store: store(dir))
      frame = DataFrame.from_parquet!(segment.path)
      assert DataFrame.to_columns(frame)["name"] == ["first", "second"]
    end
  end

  describe "durability" do
    test "a written segment is fsynced before it is observable", %{tmp_dir: dir} do
      assert {:ok, segment} = SegmentFixture.write(rows(1), schema(), store: store(dir))

      assert File.exists?(segment.path)
      assert File.ls!(Path.join(dir, ".tmp")) == []
    end
  end
end
