defmodule Smolquery.QueryService.ArrayColumnIntegrationTest do
  @moduledoc """
  An `ARRAY<T>` column end to end, as deployed (`Smolquery.Test.FullNode`):
  an unparsed body and a rows batch in through the buffer, a seal into
  DuckLake, and reads through the planner's hot ∪ sealed union (T-489).

  The seal valve is two files, as in the map test: the two unparsed bodies
  seal, the rows batch lands last and stays hot, so the union read covers a
  rows-written array on the hot side and a sealed Parquet `LIST` on the
  other.
  """

  use ExUnit.Case, async: false

  alias Smolquery.BufferService.Client
  alias Smolquery.Engine.Frame
  alias Smolquery.QueryService
  alias Smolquery.Schema
  alias Smolquery.Test.Eventually
  alias Smolquery.Test.FullNode

  @moduletag :integration
  @moduletag :tmp_dir

  @table {"analytics", "events"}

  defp schema do
    Schema.new!([
      {"id", :int64, nullable: false},
      {"counts", {:array, :int64}},
      {"bounds", {:array, :float64}},
      {"tags", {:array, :string}}
    ])
  end

  setup context do
    node =
      FullNode.start(context,
        schema: schema(),
        seal_max_files: 2,
        seal_max_bytes: 1_000_000_000,
        seal_max_age_ms: 600_000
      )

    %{node: node}
  end

  defp ndjson_batch(rows) do
    body = Enum.map_join(rows, "\n", &JSON.encode!/1) <> "\n"

    %{schema: schema(), ndjson: body, row_count: length(rows), byte_size: byte_size(body)}
  end

  defp query(node, sql) do
    case QueryService.Client.query(node.query, sql) do
      {:ok, _job, %Explorer.DataFrame{} = frame} -> Frame.to_rows(frame)
      {:ok, job, nil} -> {:query_failed, job.error}
    end
  end

  test "reads elements across the hot and sealed tiers, whichever shape wrote the row", %{
    node: node
  } do
    {:ok, _ack} =
      Client.write_batch(
        node.buffer,
        @table,
        ndjson_batch([
          %{"id" => 1, "counts" => [1, 2, 3], "bounds" => [0.5, 1.5], "tags" => ["a", nil]},
          %{"id" => 2, "counts" => [], "tags" => []}
        ])
      )

    sql =
      "SELECT id, counts, len(counts) AS n, counts[1] AS first, bounds, tags " <>
        "FROM analytics.events ORDER BY id"

    assert query(node, sql) == [
             %{
               "id" => 1,
               "counts" => [1, 2, 3],
               "n" => 3,
               "first" => 1,
               "bounds" => [0.5, 1.5],
               "tags" => ["a", nil]
             },
             %{"id" => 2, "counts" => [], "n" => 0, "first" => nil, "bounds" => nil, "tags" => []}
           ]

    {:ok, _ack} =
      Client.write_batch(node.buffer, @table, ndjson_batch([%{"id" => 5, "counts" => [50]}]))

    assert Eventually.until(fn -> FullNode.sealed_count(node) == 1 end, 200, 100)

    {:ok, _ack} =
      Client.write_batch(node.buffer, @table, %{
        schema: schema(),
        rows: [%{"id" => 3, "counts" => [30, 31], "bounds" => [3.0]}, %{"id" => 4}]
      })

    assert query(node, "SELECT id, counts, bounds FROM analytics.events ORDER BY id") == [
             %{"id" => 1, "counts" => [1, 2, 3], "bounds" => [0.5, 1.5]},
             %{"id" => 2, "counts" => [], "bounds" => nil},
             %{"id" => 3, "counts" => [30, 31], "bounds" => [3.0]},
             %{"id" => 4, "counts" => nil, "bounds" => nil},
             %{"id" => 5, "counts" => [50], "bounds" => nil}
           ]

    assert query(node, "SELECT id FROM analytics.events WHERE list_contains(counts, 31)") ==
             [%{"id" => 3}]

    assert query(
             node,
             "SELECT CAST(sum(list_sum(counts)) AS BIGINT) AS total FROM analytics.events " <>
               "WHERE len(counts) > 1"
           ) == [%{"total" => 67}]
  end
end
