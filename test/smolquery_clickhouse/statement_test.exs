defmodule SmolqueryClickHouse.StatementTest do
  use ExUnit.Case, async: true

  alias SmolqueryClickHouse.Statement

  describe "parse/1" do
    test "reads the statement Logflare sends" do
      assert Statement.parse(
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
               Statement.parse(
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
               Statement.parse("INSERT INTO events FORMAT RowBinary")
    end

    test "names what it expected and where" do
      assert Statement.parse("SELECT 1") == {:error, ~S|expected INSERT at "SELECT 1"|}

      assert Statement.parse("INSERT INTO t (a, b FORMAT RowBinary") ==
               {:error, ~S|expected , or ) in the column list at "FORMAT RowBinary"|}

      assert Statement.parse("INSERT INTO t VALUES (1)") ==
               {:error, ~S|expected FORMAT at "VALUES (1)"|}

      assert Statement.parse("INSERT INTO t FORMAT RowBinary\n\x01\x02") ==
               {:error, "unexpected text after the format name: <<1, 2>>"}

      assert Statement.parse("INSERT INTO `t FORMAT RowBinary") ==
               {:error, "a quoted name or value is never closed"}
    end
  end

  describe "insert?/1" do
    test "reads the first keyword, in any case" do
      assert Statement.insert?("  insert into t FORMAT RowBinary")
      refute Statement.insert?("SELECT 1")
      refute Statement.insert?("INSERTED")
      refute Statement.insert?("")
    end
  end

  describe "split_format/1" do
    test "splits a trailing FORMAT clause and a final semicolon" do
      assert Statement.split_format("SELECT 1 FORMAT JSONEachRow ;\n") ==
               {"SELECT 1", "JSONEachRow"}

      assert Statement.split_format("select *\nfrom t\nformat TSV") == {"select *\nfrom t", "TSV"}
    end

    test "leaves a statement with no clause, or one only inside a string, whole" do
      assert Statement.split_format(" SELECT 1; ") == {"SELECT 1", nil}
      assert Statement.split_format("SELECT 'x FORMAT JSON'") == {"SELECT 'x FORMAT JSON'", nil}
    end

    test "takes a clause that follows a string literal" do
      assert Statement.split_format("SELECT * FROM t WHERE name = 'x' FORMAT JSONEachRow") ==
               {"SELECT * FROM t WHERE name = 'x'", "JSONEachRow"}

      assert Statement.split_format("SELECT 'it''s' FORMAT TSV;") == {"SELECT 'it''s'", "TSV"}
    end

    test "a column named format is not a clause" do
      assert Statement.split_format("SELECT a FROM t ORDER BY format desc") ==
               {"SELECT a FROM t ORDER BY format desc", nil}

      assert Statement.split_format("SELECT a FROM t ORDER BY format ASC") ==
               {"SELECT a FROM t ORDER BY format ASC", nil}
    end

    test "reads past a trailing comment, and never inside one" do
      assert Statement.split_format("SELECT 1 FORMAT JSON -- for the dashboard") ==
               {"SELECT 1 -- for the dashboard", "JSON"}

      assert Statement.split_format("SELECT 1 -- FORMAT JSON") == {"SELECT 1 -- FORMAT JSON", nil}
    end
  end
end
