defmodule SmolqueryClickHouse.PruneAndScatterTest do
  @moduledoc """
  Whether each statement a ClickHouse client sends prunes the hot tier and
  runs distributed, or the reason it does not (PL-68, T-535).

  A statement that stops pruning opens every hot micro-segment, and one that
  stops scattering runs on one engine. Neither fails anything: the page is
  slower, and nobody is told. So the statements are held here, HyperDX's own
  from the fixture corpus and the chart shapes beside them, each through the
  edge's real translate step and then the planner's two gates:

  - `prunes` is how many bounds `Smolquery.QueryService.Pruner` reads off
    the statement, with `Smolquery.QueryService.Fold`'s help as the planner
    gives it, `0` for none;
  - `scatters` is `:ok`, or `Smolquery.QueryService.Decomposer`'s refusal.

  A refusal listed here is a known one with a task against it. Flip the row
  when the task lands; a row that flips by itself is what this is for.
  """

  use ExUnit.Case, async: false

  alias Smolquery.Engine
  alias Smolquery.Engine.Connection
  alias Smolquery.Engine.Result
  alias Smolquery.Identifier
  alias Smolquery.QueryService.ClickHouseFunctions
  alias Smolquery.QueryService.Decomposer
  alias Smolquery.QueryService.Fold
  alias Smolquery.QueryService.Pruner
  alias SmolqueryClickHouse.Params
  alias SmolqueryClickHouse.Rewrite
  alias SmolqueryClickHouse.Statement

  @engine __MODULE__.Engine
  @conn Engine.connection_name(@engine)
  @events {"analytics", "events"}
  @columns ~w(Timestamp ServiceName SeverityText Body LogAttributes Duration)
  @fixture "test/support/fixtures/clickstack/hyperdx_search.json"

  @window "Timestamp >= fromUnixTimestamp64Milli(1789812660000) AND " <>
            "Timestamp <= fromUnixTimestamp64Milli(1789812780000)"
  @bucket "toStartOfInterval(toDateTime(Timestamp), INTERVAL 1 minute)"
  @from "FROM analytics.events WHERE (#{@window})"

  @hyperdx [
    {"rows", 2, :offset},
    {"histogram", 2, :ok},
    {"rows_term", 2, :offset},
    {"count_field_filters", 3, :ok},
    {"map_keys", 0, :cte},
    {"key_values", 2, :cte},
    {"rows_underscore_term", 2, :offset}
  ]

  @shapes [
    {"SELECT count(), ServiceName, #{@bucket} AS b #{@from} GROUP BY ServiceName, b ORDER BY b",
     2, :ok},
    {"SELECT avg(Duration), min(Duration), max(Duration), #{@bucket} AS b #{@from} GROUP BY b", 2,
     :ok},
    {"SELECT ServiceName, count() AS c #{@from} GROUP BY ServiceName ORDER BY c DESC LIMIT 10", 2,
     :ok},
    {"SELECT count(), LogAttributes['http.status'] AS s #{@from} GROUP BY s ORDER BY count() DESC LIMIT 10",
     2, :ok},
    {"SELECT countIf(SeverityText = 'error') AS errors, #{@bucket} AS b #{@from} GROUP BY b", 2,
     :ok},
    {"SELECT sumIf(Duration, SeverityText = 'error'), avgIf(Duration, ServiceName = 'api') #{@from}",
     2, :ok},
    {"SELECT ServiceName, count() AS c #{@from} GROUP BY ServiceName HAVING c > 10", 2, :having},
    {"SELECT DISTINCT ServiceName #{@from} LIMIT 100", 2,
     {:unsupported_modifier, "DISTINCT_MODIFIER"}},
    {"SELECT any(Body), argMax(Body, Timestamp), ServiceName #{@from} GROUP BY ServiceName", 2,
     {:ungrouped_expression, "any_value"}},
    {"SELECT groupUniqArray(20)(ServiceName) #{@from}", 2,
     {:ungrouped_expression, "groupuniqarray"}},
    {"SELECT groupArray(5)(Body), ServiceName #{@from} GROUP BY ServiceName", 2,
     {:ungrouped_expression, "grouparray"}},
    {"SELECT uniq(ServiceName), #{@bucket} AS b #{@from} GROUP BY b", 2,
     {:ungrouped_expression, "uniq"}},
    {"SELECT uniqExact(ServiceName) #{@from}", 2, {:ungrouped_expression, "uniqexact"}},
    {"SELECT count(DISTINCT ServiceName) #{@from}", 2, {:distinct_aggregate, "count"}},
    {"SELECT quantile(0.95)(Duration), #{@bucket} AS b #{@from} GROUP BY b", 2,
     {:ungrouped_expression, "quantile_cont"}},
    {"WITH sampledData AS (SELECT ServiceName AS param0 #{@from} LIMIT 100000) " <>
       "SELECT groupUniqArray(10000)(param0) AS param0 FROM sampledData", 2, :cte},
    {"SELECT count() FROM (SELECT ServiceName #{@from} LIMIT 100000)", 2, :from_not_a_base_table},
    {"SELECT count() FROM analytics.events WHERE Timestamp BETWEEN " <>
       "fromUnixTimestamp64Milli(1789812660000) AND fromUnixTimestamp64Milli(1789812780000)", 2,
     :ok},
    {"SELECT count() FROM analytics.events WHERE Timestamp >= toDateTime64('2026-09-19 10:11:00', 3)",
     1, :ok},
    {"SELECT count() FROM analytics.events WHERE Timestamp >= toDateTime('2026-09-19 10:11:00')",
     1, :ok},
    {"SELECT count() FROM analytics.events WHERE Timestamp >= " <>
       "parseDateTime64BestEffort('2026-09-19T10:11:00Z', 9)", 1, :ok},
    {"SELECT count() #{@from} AND Timestamp >= " <>
       "fromUnixTimestamp64Milli(1789812660000) - INTERVAL 1 HOUR", 3, :ok}
  ]

  setup_all do
    start_supervised!({Engine, name: @engine, extensions: []})
    Engine.query!(@engine, "CREATE SCHEMA analytics")

    Engine.query!(
      @engine,
      ~s|CREATE TABLE analytics.events ("Timestamp" TIMESTAMP_NS, "ServiceName" VARCHAR, | <>
        ~s|"SeverityText" VARCHAR, "Body" VARCHAR, "LogAttributes" MAP(VARCHAR, VARCHAR), | <>
        ~s|"Duration" BIGINT)|
    )

    for macro <- ClickHouseFunctions.statements(), do: Engine.query!(@engine, macro)

    steps = @fixture |> File.read!() |> JSON.decode!() |> Map.fetch!("steps")

    %{steps: Map.new(steps, &{&1["step"], &1})}
  end

  defp translated(sql, params \\ %{}) do
    url = Map.new(params, fn {name, value} -> {"param_" <> name, value} end)
    {:ok, filled} = sql |> Statement.standard_quoting() |> Params.substitute(url)

    Rewrite.call(filled)
  end

  defp bounds(sql) do
    {:ok, result} =
      Connection.query(@conn, "SELECT json_serialize_sql(#{Identifier.sql_string(sql)})")

    %{"statements" => [statement]} = result |> Result.one!() |> JSON.decode!()

    folded = Fold.bounds(@conn, Pruner.unread_bounds(statement, [@events]), true)

    statement |> Pruner.conjuncts([@events], [], folded) |> Map.get(@events, []) |> length()
  end

  defp scatters(sql) do
    {:ok, described} = Connection.query(@conn, "DESCRIBE " <> sql)
    outputs = Enum.map(described.rows, fn [name, type | _rest] -> {name, type} end)

    case Decomposer.decompose(@conn, sql, outputs, @columns) do
      {:ok, _decomposition} -> :ok
      {:error, reason} -> reason
    end
  end

  test "what HyperDX sends to open its Search page", %{steps: steps} do
    for {step, prunes, scatters} <- @hyperdx do
      %{"sql" => sql, "params" => params} = Map.fetch!(steps, step)
      sql = translated(sql, params)

      assert {step, bounds(sql), scatters(sql)} == {step, prunes, scatters}
    end
  end

  test "the chart and filter shapes a ClickHouse client writes" do
    for {sql, prunes, scatters} <- @shapes do
      sql = translated(sql)

      assert {sql, bounds(sql), scatters(sql)} == {sql, prunes, scatters}
    end
  end

  test "every data statement of the fixture is held here", %{steps: steps} do
    data =
      for {step, %{"sql" => sql}} <- steps,
          not (sql =~ ~r/system\.|\ADESCRIBE|version\(\)/),
          do: step

    assert Enum.sort(data) == @hyperdx |> Enum.map(&elem(&1, 0)) |> Enum.sort()
  end
end
