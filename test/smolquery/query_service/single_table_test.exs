defmodule Smolquery.QueryService.SingleTableTest do
  use ExUnit.Case, async: false

  alias Smolquery.Engine
  alias Smolquery.Engine.Connection
  alias Smolquery.Engine.Result
  alias Smolquery.Identifier
  alias Smolquery.QueryService.SingleTable

  @engine __MODULE__.Parser
  @conn Engine.connection_name(@engine)

  setup_all do
    start_supervised!({Engine, name: @engine, extensions: []})
    :ok
  end

  defp statement(sql) do
    {:ok, result} =
      Connection.query(@conn, "SELECT json_serialize_sql(#{Identifier.sql_string(sql)})")

    %{"statements" => [statement]} = result |> Result.one!() |> JSON.decode!()

    statement
  end

  describe "table_reads/1" do
    test "counts every reference by table name, wherever it is and whatever its schema" do
      sql =
        "WITH recent AS (SELECT * FROM analytics.events) " <>
          "SELECT (SELECT count(*) FROM events) FROM recent JOIN analytics.users u USING (id) " <>
          "WHERE EXISTS (SELECT 1 FROM other.events)"

      assert SingleTable.table_reads(statement(sql)) == %{
               "events" => 3,
               "recent" => 1,
               "users" => 1
             }
    end

    test "a statement with no table reads none" do
      assert SingleTable.table_reads(statement("SELECT 1")) == %{}
    end

    test "a table function anywhere makes the count unknowable" do
      for sql <- [
            "SELECT * FROM analytics.events UNION ALL SELECT * FROM query_table('analytics.events')",
            "SELECT * FROM analytics.events WHERE id IN (SELECT * FROM range(3))"
          ] do
        assert SingleTable.table_reads(statement(sql)) == :unknowable, sql
      end
    end
  end

  describe "single_reference?/2" do
    test "one table and nothing excluded" do
      assert SingleTable.single_reference?(statement("SELECT id FROM analytics.events"), [])
    end

    test "a second reference, an excluded class, or a table function is not one" do
      refute SingleTable.single_reference?(
               statement("SELECT * FROM analytics.events a JOIN analytics.events b USING (id)"),
               []
             )

      refute SingleTable.single_reference?(
               statement("SELECT (SELECT 1) FROM analytics.events"),
               ["SUBQUERY"]
             )

      refute SingleTable.single_reference?(
               statement("SELECT * FROM analytics.events, range(3)"),
               []
             )
    end
  end
end
