defmodule Smolquery.QueryService.MapColumnIntegrationTest do
  @moduledoc """
  A `MAP(STRING, STRING)` column end to end, as deployed
  (`Smolquery.Test.FullNode`): an unparsed body and a rows batch in through the
  buffer, a seal into DuckLake, and reads through the planner's hot ∪ sealed
  union — the seam T-140 found broken for an Explorer-written map.
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
    Schema.new!([{"id", :int64, nullable: false}, {"attrs", {:map, :string, :string}}])
  end

  setup context do
    node =
      FullNode.start(context,
        schema: schema(),
        seal_max_files: 1,
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

  test "reads by key across the hot and sealed tiers, whichever shape wrote the row", %{
    node: node
  } do
    {:ok, _ack} =
      Client.write_batch(
        node.buffer,
        @table,
        ndjson_batch([
          %{"id" => 1, "attrs" => %{"host" => "h1", "pod" => "api-7"}},
          %{"id" => 2, "attrs" => %{"host" => "h2", "n" => 1}}
        ])
      )

    sql = "SELECT id, attrs['host'] AS host, attrs FROM analytics.events ORDER BY id"

    assert query(node, sql) == [
             %{"id" => 1, "host" => "h1", "attrs" => %{"host" => "h1", "pod" => "api-7"}},
             %{"id" => 2, "host" => "h2", "attrs" => %{"host" => "h2", "n" => "1"}}
           ]

    assert Eventually.until(fn -> FullNode.sealed_count(node) == 1 end, 200, 100)

    {:ok, _ack} =
      Client.write_batch(node.buffer, @table, %{
        schema: schema(),
        rows: [%{"id" => 3, "attrs" => %{"host" => "h3"}}, %{"id" => 4}]
      })

    assert query(node, sql) == [
             %{"id" => 1, "host" => "h1", "attrs" => %{"host" => "h1", "pod" => "api-7"}},
             %{"id" => 2, "host" => "h2", "attrs" => %{"host" => "h2", "n" => "1"}},
             %{"id" => 3, "host" => "h3", "attrs" => %{"host" => "h3"}},
             %{"id" => 4, "host" => nil, "attrs" => %{}}
           ]

    assert query(node, "SELECT id FROM analytics.events WHERE attrs['host'] = 'h3'") ==
             [%{"id" => 3}]

    assert query(node, "SELECT id FROM analytics.events WHERE map_contains(attrs, 'pod')") ==
             [%{"id" => 1}]
  end
end
