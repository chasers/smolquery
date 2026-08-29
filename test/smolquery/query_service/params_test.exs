defmodule Smolquery.QueryService.ParamsTest do
  use ExUnit.Case, async: false

  alias Explorer.DataFrame
  alias Smolquery.QueryService
  alias Smolquery.QueryService.Client
  alias Smolquery.QueryService.Runtime
  alias Smolquery.Test.FixedCatalog

  setup do
    name = :"query_params_#{:erlang.unique_integer([:positive])}"

    start_supervised!(
      {QueryService.Supervisor,
       name: name, catalog: FixedCatalog.new(%{snapshot: 1, schemas: %{}, segments: %{}})},
      id: name
    )

    on_exit(fn -> Runtime.delete(name) end)

    %{name: name}
  end

  test "params: bind positionally as engine parameters, never as SQL text (T-410)", %{name: name} do
    injection = "'; DROP TABLE x; --"

    assert {:ok, %{state: :done}, frame} =
             Client.query(name, "SELECT $1 + 1 AS n, $2 AS s, $3 AS d",
               params: [41, injection, ~D[2026-08-01]]
             )

    assert DataFrame.to_columns(frame) == %{
             "n" => [42],
             "s" => [injection],
             "d" => [~D[2026-08-01]]
           }
  end

  test "a timestamp binds as TIMESTAMP, and a decimal as DECIMAL (T-410)", %{name: name} do
    assert {:ok, %{state: :done}, frame} =
             Client.query(name, "SELECT typeof($1) AS t, typeof($2) AS d",
               params: [~N[2026-08-01 10:00:00], Decimal.new("12.50")]
             )

    assert %{"t" => ["TIMESTAMP"], "d" => [decimal]} = DataFrame.to_columns(frame)
    assert decimal =~ "DECIMAL"
  end

  test "describe: true binds the parameters it needs to name the columns (T-410)", %{name: name} do
    assert {:ok, %{state: :done}, frame} =
             Client.query(name, "SELECT $1 + 1 AS n", describe: true, params: [1])

    assert [%{"column_name" => "n", "column_type" => "BIGINT"}] = DataFrame.to_rows(frame)
  end

  test "explain: with params is refused: DuckDB cannot prepare an EXPLAIN (T-410)", %{name: name} do
    assert {:ok, %{state: :error, error: :explain_with_params}, nil} =
             Client.query(name, "SELECT $1 AS n", explain: :plan, params: [1])
  end
end
