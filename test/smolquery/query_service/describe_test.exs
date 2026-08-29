defmodule Smolquery.QueryService.DescribeTest do
  use ExUnit.Case, async: false

  alias Explorer.DataFrame
  alias Smolquery.QueryService
  alias Smolquery.QueryService.Client
  alias Smolquery.QueryService.Runtime
  alias Smolquery.Test.FixedCatalog

  setup do
    name = :"query_describe_#{:erlang.unique_integer([:positive])}"

    start_supervised!(
      {QueryService.Supervisor,
       name: name, catalog: FixedCatalog.new(%{snapshot: 1, schemas: %{}, segments: %{}})},
      id: name
    )

    on_exit(fn -> Runtime.delete(name) end)

    %{name: name}
  end

  test "describe: true answers the query's columns without running it (PL-58)", %{name: name} do
    sql =
      "SELECT 1::BIGINT AS i, NULL::VARCHAR AS s, 12.5::DECIMAL(38,2) AS n WHERE error('never')"

    assert {:ok, job, frame} = Client.query(name, sql, describe: true)
    assert job.state == :done
    assert job.row_count == nil

    rows = DataFrame.to_rows(frame)

    assert Enum.map(rows, &{&1["column_name"], &1["column_type"]}) ==
             [{"i", "BIGINT"}, {"s", "VARCHAR"}, {"n", "DECIMAL(38,2)"}]
  end
end
