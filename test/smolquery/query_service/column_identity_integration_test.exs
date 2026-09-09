defmodule Smolquery.QueryService.ColumnIdentityIntegrationTest do
  @moduledoc """
  The claim behind PL-62, as deployed (`Smolquery.Test.FullNode`): a column
  name dropped and given to a new column of another type, and a micro-segment
  written under the old column reads `NULL` in the new one — the hot tier
  projecting by id, not by name.

  The seal valves are wide open, so both micro-segments stay hot and the read
  is the planner's grouped hot read alone. The tombstone from T-430 is cleared
  by hand between the drop and the re-add, the way an operator would today;
  IDS-3 retires it.
  """

  use ExUnit.Case, async: false

  alias Smolquery.BufferService.Client
  alias Smolquery.Catalog
  alias Smolquery.Engine
  alias Smolquery.Engine.Frame
  alias Smolquery.QueryService
  alias Smolquery.Schema
  alias Smolquery.Schema.Field
  alias Smolquery.StorageService.Runtime, as: StorageRuntime
  alias Smolquery.Test.FullNode

  @moduletag :integration
  @moduletag :tmp_dir

  @table {"analytics", "events"}

  setup context do
    node =
      FullNode.start(context,
        schema: Schema.new!([{"id", :int64, nullable: false}]),
        seal_max_files: 1_000,
        seal_max_bytes: 1_000_000_000,
        seal_max_age_ms: 600_000
      )

    %{node: node}
  end

  defp rows(node, sql) do
    {:ok, job, %Explorer.DataFrame{} = frame} = QueryService.Client.query(node.query, sql)

    Frame.to_rows(frame, json_columns: job.json_columns)
  end

  defp clear_tombstones(node) do
    engine = StorageRuntime.catalog_engine(node.storage)

    [[catalog, schema]] =
      Engine.query!(
        engine,
        "SELECT table_catalog, table_schema FROM information_schema.tables " <>
          "WHERE table_name = 'smolquery_dropped_columns'"
      ).rows

    Engine.query!(engine, ~s|DELETE FROM "#{catalog}"."#{schema}".smolquery_dropped_columns|)
  end

  test "a micro-segment written under a dropped column reads NULL in its re-added namesake",
       %{node: node} do
    :ok = Catalog.alter_table(node.catalog, @table, {:add_column, Field.new!("ts_int", :int64)})
    {:ok, before} = Catalog.table_schema(node.catalog, @table)

    {:ok, _first} =
      Client.write_batch(node.buffer, @table, %{
        schema: before,
        rows: [%{"id" => 1, "ts_int" => 1_700_000_000}]
      })

    assert rows(node, "SELECT id, ts_int FROM analytics.events") == [
             %{"id" => 1, "ts_int" => 1_700_000_000}
           ]

    :ok = Catalog.alter_table(node.catalog, @table, {:drop_column, "ts_int"})
    clear_tombstones(node)
    :ok = Catalog.alter_table(node.catalog, @table, {:add_column, Field.new!("ts_int", :string)})
    {:ok, after_readd} = Catalog.table_schema(node.catalog, @table)

    {:ok, _second} =
      Client.write_batch(node.buffer, @table, %{
        schema: after_readd,
        rows: [%{"id" => 2, "ts_int" => "x"}]
      })

    assert rows(node, "SELECT id, ts_int FROM analytics.events ORDER BY id") == [
             %{"id" => 1, "ts_int" => nil},
             %{"id" => 2, "ts_int" => "x"}
           ]

    assert rows(node, "SELECT id FROM analytics.events WHERE ts_int = 'x'") == [%{"id" => 2}]
  end
end
