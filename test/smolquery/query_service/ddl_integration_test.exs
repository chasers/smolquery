defmodule Smolquery.QueryService.DdlIntegrationTest do
  @moduledoc """
  `ALTER TABLE` as deployed (`Smolquery.Test.FullNode`, PL-61 L3): the
  statement runs as a query job against the real DuckLake catalog, the next
  write carries the column, and the next read serves it.

  The first statement is a query, not the `ALTER`, for the reason the other
  alter-column proofs give: the node settles before the catalog is changed.
  """

  use ExUnit.Case, async: false

  alias Smolquery.BufferService.Client
  alias Smolquery.Catalog
  alias Smolquery.Engine.Frame
  alias Smolquery.QueryService
  alias Smolquery.Schema
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

  test "an ALTER TABLE job changes the lake, and the next write and read see it", %{node: node} do
    assert rows(node, "SELECT count(*) AS n FROM analytics.events") == [%{"n" => 0}]

    assert {:ok, %{state: :done, ddl: %{performed: true, column: "label"}}, nil} =
             QueryService.Client.query(
               node.query,
               "ALTER TABLE analytics.events ADD COLUMN label STRING"
             )

    {:ok, widened} = Catalog.table_schema(node.catalog, @table)
    assert Schema.names(widened) == ["id", "label"]

    {:ok, _ack} =
      Client.write_batch(node.buffer, @table, %{
        schema: widened,
        rows: [%{"id" => 1, "label" => "one"}]
      })

    assert rows(node, "SELECT id, label FROM analytics.events") == [
             %{"id" => 1, "label" => "one"}
           ]

    assert {:ok, %{state: :done, ddl: %{operation: :drop_column}}, nil} =
             QueryService.Client.query(
               node.query,
               "ALTER TABLE analytics.events DROP COLUMN label"
             )

    assert rows(node, "SELECT * FROM analytics.events") == [%{"id" => 1}]

    assert {:ok, %{state: :error, error: :last_column}, nil} =
             QueryService.Client.query(node.query, "ALTER TABLE analytics.events DROP COLUMN id")
  end
end
