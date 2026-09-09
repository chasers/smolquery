defmodule Smolquery.BufferService.StaleSchemaIntegrationTest do
  @moduledoc """
  A writer whose schema predates a `DROP` and `ADD` of the same name, as
  deployed (`Smolquery.Test.FullNode` with the buffer holding the node's
  catalog, T-439): the buffer refuses the batch instead of storing the row
  under the old column, the fresh schema lands, and the read is exact.

  The first statement is a query, for the reason the other alter-column
  proofs give.
  """

  use ExUnit.Case, async: false

  alias Smolquery.BufferService.Client
  alias Smolquery.Catalog
  alias Smolquery.Engine.Frame
  alias Smolquery.QueryService
  alias Smolquery.Schema
  alias Smolquery.Schema.Field
  alias Smolquery.Test.FullNode

  @moduletag :integration
  @moduletag :tmp_dir

  @table {"analytics", "events"}

  setup context do
    node =
      FullNode.start(context,
        schema: Schema.new!([{"id", :int64, nullable: false}, {"x", :string}]),
        catalog: :lake,
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

  test "a batch under the old id is refused, and the re-added column never reads the old data",
       %{node: node} do
    assert rows(node, "SELECT count(*) AS n FROM analytics.events") == [%{"n" => 0}]
    {:ok, before} = Catalog.table_schema(node.catalog, @table)

    {:ok, _one} =
      Client.write_batch(node.buffer, @table, %{schema: before, rows: [%{"id" => 1, "x" => "a"}]})

    :ok = Catalog.alter_table(node.catalog, @table, {:drop_column, "x"})
    :ok = Catalog.alter_table(node.catalog, @table, {:add_column, Field.new!("x", :int64)})
    {:ok, fresh} = Catalog.table_schema(node.catalog, @table)

    assert Client.write_batch(node.buffer, @table, %{
             schema: before,
             rows: [%{"id" => 2, "x" => "b"}]
           }) ==
             {:error, {:stale_schema, @table, ["x"]}}

    {:ok, _two} =
      Client.write_batch(node.buffer, @table, %{schema: fresh, rows: [%{"id" => 2, "x" => 7}]})

    assert rows(node, "SELECT id, x FROM analytics.events ORDER BY id") == [
             %{"id" => 1, "x" => nil},
             %{"id" => 2, "x" => 7}
           ]
  end
end
