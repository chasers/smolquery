defmodule SmolqueryApi.ClickHouseInsertTest do
  use ExUnit.Case, async: true

  alias SmolqueryApi.ClickHouseInsert

  describe "parse/1" do
    test "reads the statement Logflare sends" do
      assert ClickHouseInsert.parse(
               "INSERT INTO logflare.otel_logs_abc (id, source_uuid, timestamp) FORMAT RowBinary"
             ) ==
               {:ok,
                %{
                  database: "logflare",
                  table: "otel_logs_abc",
                  columns: ["id", "source_uuid", "timestamp"],
                  settings: %{},
                  format: "RowBinary"
                }}
    end

    test "takes lowercase keywords, TABLE, quoted names, SETTINGS and a trailing semicolon" do
      assert {:ok, insert} =
               ClickHouseInsert.parse(
                 ~S|insert into table `my db`."ev""ents" (`a b`, c) settings async_insert = 1, insert_deduplication_token = 'x\'y' format RowBinaryWithNamesAndTypes ;|
               )

      assert insert == %{
               database: "my db",
               table: ~S|ev"ents|,
               columns: ["a b", "c"],
               settings: %{"async_insert" => "1", "insert_deduplication_token" => "x'y"},
               format: "RowBinaryWithNamesAndTypes"
             }
    end

    test "a table with no database and no column list" do
      assert {:ok, %{database: nil, table: "events", columns: nil}} =
               ClickHouseInsert.parse("INSERT INTO events FORMAT RowBinary")
    end

    test "names what it expected and where" do
      assert ClickHouseInsert.parse("SELECT 1") == {:error, ~S|expected INSERT at "SELECT 1"|}

      assert ClickHouseInsert.parse("INSERT INTO t (a, b FORMAT RowBinary") ==
               {:error, ~S|expected , or ) in the column list at "FORMAT RowBinary"|}

      assert ClickHouseInsert.parse("INSERT INTO t VALUES (1)") ==
               {:error, ~S|expected FORMAT at "VALUES (1)"|}

      assert ClickHouseInsert.parse("INSERT INTO t FORMAT RowBinary\n\x01\x02") ==
               {:error, "unexpected text after the format name: <<1, 2>>"}

      assert ClickHouseInsert.parse("INSERT INTO `t FORMAT RowBinary") ==
               {:error, "a quoted name or value is never closed"}
    end
  end
end
