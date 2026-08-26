defmodule Smolquery.QueryService.VariantColumnIntegrationTest do
  @moduledoc """
  A `VARIANT` column end to end, as deployed (`Smolquery.Test.FullNode`): an
  unparsed body and a rows batch in, a seal into DuckLake, typed reads through
  the planner's hot ∪ sealed union, and the result crossing Arrow as JSON —
  rendered by the API's own page builder.

  The seal valve is two files, so no seal is committing while a read runs:
  a query job racing a seal for the DuckLake sqlite lock is a known transient
  (`database is locked`), not what this test is about.
  """

  use ExUnit.Case, async: false

  alias Smolquery.BufferService.Client
  alias Smolquery.Engine.Frame
  alias Smolquery.QueryService
  alias Smolquery.Schema
  alias Smolquery.Test.Eventually
  alias Smolquery.Test.FullNode
  alias SmolqueryApi.JobController

  @moduletag :integration
  @moduletag :tmp_dir

  @table {"analytics", "events"}

  defp schema do
    Schema.new!([{"id", :int64, nullable: false}, {"attrs", :variant}])
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
      {:ok, job, %Explorer.DataFrame{} = frame} -> {job, frame}
      {:ok, job, nil} -> {:query_failed, job.error}
    end
  end

  defp rows(node, sql) do
    case query(node, sql) do
      {:query_failed, error} -> {:query_failed, error}
      {job, frame} -> Frame.to_rows(frame, json_columns: job.json_columns)
    end
  end

  @doc_one %{"host" => "h1", "n" => 1, "tags" => ["a", "b"], "nested" => %{"k" => true}}

  test "typed access across tiers, and the document back out as nested JSON", %{node: node} do
    {:ok, _ack} =
      Client.write_batch(
        node.buffer,
        @table,
        ndjson_batch([
          %{"id" => 1, "attrs" => @doc_one},
          %{"id" => 2, "attrs" => %{"host" => "h2", "n" => "two"}}
        ])
      )

    typed =
      "SELECT id, attrs['host']::VARCHAR AS host, TRY_CAST(attrs['n'] AS BIGINT) AS n, " <>
        "variant_typeof(attrs['tags']) AS tags_type FROM analytics.events ORDER BY id"

    assert rows(node, typed) == [
             %{"id" => 1, "host" => "h1", "n" => 1, "tags_type" => "ARRAY(2)"},
             %{"id" => 2, "host" => "h2", "n" => nil, "tags_type" => "VARIANT_NULL"}
           ]

    {:ok, _ack} =
      Client.write_batch(node.buffer, @table, %{
        schema: schema(),
        rows: [%{"id" => 3, "attrs" => [1, "x"]}, %{"id" => 4}]
      })

    assert Eventually.until(fn -> FullNode.sealed_count(node) == 1 end, 200, 100)

    {:ok, _ack} =
      Client.write_batch(node.buffer, @table, ndjson_batch([%{"id" => 5, "attrs" => 5.5}]))

    {job, frame} = query(node, "SELECT id, attrs FROM analytics.events ORDER BY id")

    assert job.json_columns == ["attrs"]

    assert %{"rows" => rendered, "totalRows" => 5} = JobController.page_body(job, frame, 0, 10)

    assert rendered == [
             %{"id" => 1, "attrs" => @doc_one},
             %{"id" => 2, "attrs" => %{"host" => "h2", "n" => "two"}},
             %{"id" => 3, "attrs" => [1, "x"]},
             %{"id" => 4, "attrs" => nil},
             %{"id" => 5, "attrs" => 5.5}
           ]

    assert rows(node, "SELECT id FROM analytics.events WHERE TRY_CAST(attrs['n'] AS BIGINT) = 1") ==
             [%{"id" => 1}]

    assert rows(node, "SELECT id FROM analytics.events WHERE variant_typeof(attrs) = 'ARRAY(2)'") ==
             [%{"id" => 3}]
  end
end
