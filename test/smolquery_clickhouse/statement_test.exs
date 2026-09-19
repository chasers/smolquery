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

  describe "split_settings/1" do
    test "a trailing clause is split off with its settings" do
      assert Statement.split_settings(
               "SELECT 1 SETTINGS max_execution_time = 15, short_circuit_function_evaluation = 'force_enable'"
             ) ==
               {"SELECT 1",
                %{
                  "max_execution_time" => "15",
                  "short_circuit_function_evaluation" => "force_enable"
                }}
    end

    test "a statement with no clause is left alone" do
      assert Statement.split_settings("SELECT 1") == {"SELECT 1", %{}}
    end

    test "a clause that ends a subquery is dropped, and the trailing one still applies" do
      sql =
        "WITH s AS (SELECT 1 AS n SETTINGS optimize_read_in_order = 0) SELECT n FROM s SETTINGS max_execution_time = 2"

      assert Statement.split_settings(sql) ==
               {"WITH s AS (SELECT 1 AS n ) SELECT n FROM s", %{"max_execution_time" => "2"}}
    end

    test "a table, a column, a literal and a comment named settings are not clauses" do
      for sql <- [
            "SELECT name, value FROM system.settings",
            "SELECT settings FROM t WHERE settings = 1",
            "SELECT * FROM settings WHERE name = 'x'",
            "SELECT 'SETTINGS a = 1'",
            "SELECT 1 -- SETTINGS a = 1"
          ] do
        assert Statement.split_settings(sql) == {sql, %{}}
      end
    end
  end

  describe "standard_quoting/1" do
    test "a backquoted identifier becomes a double-quoted one" do
      assert Statement.standard_quoting(~S|SELECT `Events.Name`, `a``b`, `c"d` FROM `t`|) ==
               ~S|SELECT "Events.Name", "a`b", "c""d" FROM "t"|
    end

    test "backslash escapes in a literal become the characters they stand for" do
      assert Statement.standard_quoting(~S|SELECT 'it\'s', 'a\tb', 'c\\d', '\x41'|) ==
               "SELECT 'it''s', 'a\tb', 'c\\d', 'A'"
    end

    test "an escape ClickHouse does not know keeps its backslash, as LIKE needs" do
      assert Statement.standard_quoting(~S|SELECT x LIKE '%a\_b\%'|) ==
               ~S|SELECT x LIKE '%a\_b\%'|
    end

    test "a doubled quote after an escaped one stays a quote" do
      assert Statement.standard_quoting(~S|SELECT '\'' 'a'|) == "SELECT '''' 'a'"
      assert Statement.standard_quoting(~S|SELECT '\'''a'|) == "SELECT '''''a'"
    end

    test "quoting without a backslash or a backtick is left byte for byte" do
      sql = ~S|SELECT 'it''s', "a""b" -- `c` 'd\'|

      assert Statement.standard_quoting(sql) == sql
    end
  end

  describe "unescape/1" do
    test "reads the escaped form a parameter's value arrives in" do
      assert Statement.unescape(~S|a\tb\nc\\d\'e\0|) == "a\tb\nc\\d'e" <> <<0>>
    end
  end
end
