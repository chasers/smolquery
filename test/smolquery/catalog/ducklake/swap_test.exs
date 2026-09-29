defmodule Smolquery.Catalog.DuckLake.SwapTest do
  use ExUnit.Case, async: true

  alias Smolquery.Catalog.DuckLake.Swap

  @table [{1, "ts", nil, "TIMESTAMP"}, {2, "v", nil, "BIGINT"}, {4, "s", nil, "VARCHAR"}]
  @twin [{1, "ts", nil, "TIMESTAMP"}, {2, "v", nil, "BIGINT"}, {3, "s", nil, "VARCHAR"}]

  describe "matches?/2 and column_ids/2" do
    test "a twin matches its table by path and type, whatever the ids" do
      assert Swap.matches?(@twin, @table)
      refute Swap.matches?(@twin, [{5, "extra", nil, "BIGINT"} | @table])
      refute Swap.matches?([{1, "ts", nil, "TIMESTAMP_NS"} | tl(@twin)], @table)
    end

    test "maps the twin's ids to the table's by path, nested columns included" do
      twin = [{1, "attrs", nil, "STRUCT"}, {2, "a", 1, "VARCHAR"}]
      table = [{7, "attrs", nil, "STRUCT"}, {9, "a", 7, "VARCHAR"}]

      assert Swap.column_ids(@twin, @table) == {:ok, %{1 => 1, 2 => 2, 3 => 4}}
      assert Swap.column_ids(twin, table) == {:ok, %{1 => 7, 2 => 9}}
    end

    test "names a twin column the table has no column for" do
      assert Swap.column_ids([{9, "gone", nil, "BIGINT"} | @twin], @table) ==
               {:error, {:unmapped_columns, [["gone"]]}}
    end
  end

  describe "mapping/3" do
    @staged %{
      type: "map_by_name",
      rows: [{0, "ts", 1, nil, false}, {1, "v", 2, nil, false}, {2, "s", 3, nil, false}]
    }

    test "reuses the table's identical mapping once targets are the table's ids" do
      existing = %{
        type: "map_by_name",
        rows: [{2, "s", 4, nil, false}, {0, "ts", 1, nil, false}, {1, "v", 2, nil, false}]
      }

      assert Swap.mapping(@staged, %{0 => existing}, %{1 => 1, 2 => 2, 3 => 4}) ==
               {:existing, 0}
    end

    test "writes a new mapping when the table has none like it, and keeps none when none" do
      assert {:new, %{type: "map_by_name", rows: rows}} =
               Swap.mapping(@staged, %{}, %{1 => 1, 2 => 2, 3 => 4})

      assert {2, "s", 4, nil, false} in rows
      assert Swap.mapping(nil, %{}, %{}) == {:existing, nil}
    end
  end

  describe "statements/2" do
    defp plan(overrides) do
      Map.merge(
        %{
          snapshot: %{id: 11, schema_version: 3, next_catalog_id: 5, next_file_id: 40},
          table_id: 1,
          stage_table_id: 9,
          next_row_id: 100,
          staged: %{data_file_id: 39, rows: 20},
          retire: [7, 8],
          abandoned: [],
          column_ids: %{1 => 1, 3 => 4},
          mapping: {:existing, 0}
        },
        overrides
      )
    end

    test "inserts the next snapshot first and tags it a compaction of the table" do
      [first | _rest] = statements = Swap.statements(plan(%{}), "m")

      assert first =~ "INSERT INTO m.ducklake_snapshot"
      assert first =~ "VALUES (12, now(), 3, 5, 40)"
      assert List.last(statements) =~ "VALUES (12, 'merge_adjacent:1', NULL, NULL, NULL)"

      sql = Enum.join(statements, "\n")
      assert sql =~ "SET end_snapshot = 12 WHERE table_id = 1 AND end_snapshot IS NULL"
      assert sql =~ "AND data_file_id IN (7, 8)"

      assert sql =~
               "SET table_id = 1, begin_snapshot = 12, row_id_start = 100, mapping_id = 0 " <>
                 "WHERE data_file_id = 39"

      assert sql =~ "column_id = CASE column_id WHEN 1 THEN 1 WHEN 3 THEN 4 ELSE column_id END"
      assert sql =~ "SET next_row_id = next_row_id + 20 WHERE table_id = 1"
      refute sql =~ "record_count ="
      refute sql =~ "file_size_bytes ="
      refute sql =~ "deleted_from_table"
    end

    test "a new mapping takes the next file id, and abandoned staged files leave the twin" do
      mapping = {:new, %{type: "map_by_name", rows: [{0, "ts", 1, nil, false}]}}
      sql = Enum.join(Swap.statements(plan(%{mapping: mapping, abandoned: [30]}), "m"), "\n")

      assert sql =~ "VALUES (12, now(), 3, 5, 41)"

      assert sql =~
               "INSERT INTO m.ducklake_column_mapping (mapping_id, table_id, type) VALUES (40, 1"

      assert sql =~ "VALUES (40, 0, 'ts', 1, NULL, false)"
      assert sql =~ "mapping_id = 40 WHERE data_file_id = 39"
      assert sql =~ "WHERE table_id = 9 AND end_snapshot IS NULL AND data_file_id IN (30)"
      assert sql =~ "'merge_adjacent:1,deleted_from_table:9'"
    end
  end

  describe "rebase?/3" do
    test "inserts anywhere and changes to other tables leave the plan's reads valid" do
      assert Swap.rebase?(
               ["inserted_into_table:1", "inserted_into_table:9,merge_adjacent:4"],
               1,
               9
             )

      assert Swap.rebase?([], 1, 9)
    end

    test "any other change to the table or its twin, or one it cannot parse, redoes the swap" do
      refute Swap.rebase?(["merge_adjacent:1"], 1, 9)
      refute Swap.rebase?(["deleted_from_table:9"], 1, 9)
      refute Swap.rebase?([~s|created_table:"main"."t"|], 1, 9)
    end
  end

  describe "lost_snapshot?/1" do
    test "recognises a duplicate snapshot id from Postgres and SQLite metadata" do
      assert Swap.lost_snapshot?(%Adbc.Error{
               message:
                 ~s|ERROR:  duplicate key value violates unique constraint "ducklake_snapshot_pkey"|
             })

      assert Swap.lost_snapshot?(%Adbc.Error{
               message: "UNIQUE constraint failed: ducklake_snapshot.snapshot_id"
             })

      refute Swap.lost_snapshot?(%Adbc.Error{message: "database is locked"})
      refute Swap.lost_snapshot?(:closed)
    end
  end

  test "the twin lives in a hidden schema, named by table id" do
    assert Swap.stage_schema() == "__smolquery_stage"
    assert Swap.stage_table(42) == "t42"
  end
end
