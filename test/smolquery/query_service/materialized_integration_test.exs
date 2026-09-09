defmodule Smolquery.QueryService.MaterializedIntegrationTest do
  @moduledoc """
  The use case behind PL-61 L4, as deployed (`Smolquery.Test.FullNode`): a
  timestamp column materialized from an integer one. Defined by `ALTER TABLE`
  on the query path, computed by the buffer's writer as the rows land, read
  back through the planner from the hot tier and, after a seal, from the
  sealed tier — where the merge carries the value the writer computed.

  The first statement is a query, not the `ALTER`, for the reason the other
  alter-column proofs give.
  """

  use ExUnit.Case, async: false

  alias Smolquery.BufferService.Client
  alias Smolquery.Catalog
  alias Smolquery.Engine.Frame
  alias Smolquery.QueryService
  alias Smolquery.Schema
  alias Smolquery.Schema.Field
  alias Smolquery.Test.Eventually
  alias Smolquery.Test.FullNode

  @moduletag :integration
  @moduletag :tmp_dir

  @table {"analytics", "events"}

  setup context do
    node =
      FullNode.start(context,
        schema: Schema.new!([{"id", :int64, nullable: false}, {"ts_int", :int64}]),
        seal_max_files: 2,
        seal_max_bytes: 1_000_000_000,
        seal_max_age_ms: 600_000
      )

    %{node: node}
  end

  defp rows(node, sql) do
    {:ok, job, %Explorer.DataFrame{} = frame} = QueryService.Client.query(node.query, sql)

    Frame.to_rows(frame, json_columns: job.json_columns)
  end

  defp write(node, schema, rows),
    do: Client.write_batch(node.buffer, @table, %{schema: schema, rows: rows})

  test "a timestamp materialized from an integer, defined by DDL, lands computed in both tiers",
       %{node: node} do
    assert rows(node, "SELECT count(*) AS n FROM analytics.events") == [%{"n" => 0}]

    assert {:ok, %{state: :done, ddl: %{performed: true}}, nil} =
             QueryService.Client.query(
               node.query,
               "ALTER TABLE analytics.events ADD COLUMN ts TIMESTAMP MATERIALIZED epoch_ms(ts_int)"
             )

    {:ok, widened} = Catalog.table_schema(node.catalog, @table)

    assert {:ok,
            %Field{
              type: :timestamp,
              materialized: %{
                expression: "epoch_ms(ts_int)",
                canonical: "epoch_ms(ts_int)",
                sources: [2]
              }
            }} = Schema.field(widened, "ts")

    {:ok, _one} = write(node, widened, [%{"id" => 1, "ts_int" => 1_700_000_000_000}])

    assert rows(node, "SELECT id, ts FROM analytics.events") == [
             %{"id" => 1, "ts" => ~N[2023-11-14 22:13:20.000000]}
           ]

    {:ok, _two} = write(node, widened, [%{"id" => 2, "ts_int" => 1_700_000_060_000}])
    assert Eventually.until(fn -> FullNode.sealed_count(node) >= 1 end, 200, 25)

    assert rows(node, "SELECT id, ts FROM analytics.events ORDER BY id") == [
             %{"id" => 1, "ts" => ~N[2023-11-14 22:13:20.000000]},
             %{"id" => 2, "ts" => ~N[2023-11-14 22:14:20.000000]}
           ]

    assert rows(
             node,
             "SELECT id FROM analytics.events WHERE ts > TIMESTAMP '2023-11-14 22:14:00'"
           ) ==
             [%{"id" => 2}]

    assert {:ok, %{state: :error, error: {:materialized_source, "ts_int", "ts"}}, nil} =
             QueryService.Client.query(
               node.query,
               "ALTER TABLE analytics.events DROP COLUMN ts_int"
             )

    assert {:ok,
            %{state: :error, error: {:invalid_materialized, {:inconsistent_function, "now"}}},
            nil} =
             QueryService.Client.query(
               node.query,
               "ALTER TABLE analytics.events ADD COLUMN seen TIMESTAMP MATERIALIZED now()"
             )

    {:ok, unchanged} = Catalog.table_schema(node.catalog, @table)
    assert Schema.names(unchanged) == ["id", "ts_int", "ts"]
  end

  test "a row written before the column carries the value after its seal: the sealer recomputes (PL-61 L5)",
       %{node: node} do
    assert rows(node, "SELECT count(*) AS n FROM analytics.events") == [%{"n" => 0}]

    {:ok, plain} = Catalog.table_schema(node.catalog, @table)
    {:ok, _one} = write(node, plain, [%{"id" => 1, "ts_int" => 1_700_000_000_000}])

    assert {:ok, %{state: :done}, nil} =
             QueryService.Client.query(
               node.query,
               "ALTER TABLE analytics.events ADD COLUMN ts TIMESTAMP MATERIALIZED epoch_ms(ts_int)"
             )

    assert rows(node, "SELECT id, ts FROM analytics.events") == [%{"id" => 1, "ts" => nil}]

    {:ok, widened} = Catalog.table_schema(node.catalog, @table)
    {:ok, _two} = write(node, widened, [%{"id" => 2, "ts_int" => 1_700_000_060_000}])
    assert Eventually.until(fn -> FullNode.sealed_count(node) >= 1 end, 200, 25)

    assert rows(node, "SELECT id, ts FROM analytics.events ORDER BY id") == [
             %{"id" => 1, "ts" => ~N[2023-11-14 22:13:20.000000]},
             %{"id" => 2, "ts" => ~N[2023-11-14 22:14:20.000000]}
           ]
  end
end
