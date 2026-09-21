defmodule Smolquery.QueryService.DecomposerTest do
  @moduledoc """
  The split must be exact, so most tests execute it: run the partial per
  shard against a resharded view, merge the parquet partials with the
  final query, and compare against the same SQL over the whole view. The
  refusal tests pin the gate: every shape the decomposer cannot split
  exactly answers with a reason, never with wrong SQL.
  """

  use ExUnit.Case, async: false

  alias Smolquery.Engine
  alias Smolquery.QueryService.ClickHouseFunctions
  alias Smolquery.QueryService.Decomposer

  @moduletag :tmp_dir

  @engine __MODULE__.Engine
  @conn Engine.connection_name(@engine)
  @columns ~w(id name bucket ts value big tag)

  setup_all do
    start_supervised!({Engine, name: @engine, extensions: []})
    Engine.query!(@engine, "CREATE SCHEMA analytics")
    define_view("true")

    :ok
  end

  defp define_view(predicate) do
    Engine.query!(
      @engine,
      """
      CREATE OR REPLACE VIEW analytics.events AS
      SELECT n AS id,
             'u-' || (n % 7) AS name,
             CAST(n % 3 AS INTEGER) AS bucket,
             TIMESTAMP '2026-01-01 00:00:00' + INTERVAL (n) SECOND AS ts,
             CAST(n % 97 AS DOUBLE) / 7 AS value,
             9007199254740993 + n AS big,
             CASE WHEN n % 4 = 0 THEN NULL ELSE 't-' || (n % 5) END AS tag
      FROM range(1000) t(n)
      WHERE #{predicate}
      """
    )
  end

  defp describe(sql) do
    result = Engine.query!(@engine, "DESCRIBE " <> sql)

    Enum.map(result.rows, fn [name, type | _rest] -> {name, type} end)
  end

  defp round_trip(sql, tmp_dir) do
    expected = Engine.query!(@engine, sql)
    {:ok, decomposition} = Decomposer.decompose(@conn, sql, describe(sql), @columns)

    paths =
      try do
        for {predicate, index} <- Enum.with_index(["n % 2 = 0", "n % 2 = 1"]) do
          define_view(predicate)

          path =
            Path.join(tmp_dir, "partial-#{System.unique_integer([:positive])}-#{index}.parquet")

          Engine.query!(
            @engine,
            "COPY (#{decomposition.partial_sql}) TO " <>
              "#{Smolquery.Identifier.sql_string(path)} (FORMAT parquet)"
          )

          path
        end
      after
        define_view("true")
      end

    parquet = Enum.map_join(paths, ", ", &Smolquery.Identifier.sql_string/1)

    merged =
      Engine.query!(@engine, Decomposer.final_sql(decomposition, "read_parquet([#{parquet}])"))

    assert merged.columns == expected.columns
    assert_rows(expected.rows, merged.rows)

    decomposition
  end

  defp assert_rows(expected, merged) do
    assert length(expected) == length(merged)

    Enum.sort(expected)
    |> Enum.zip(Enum.sort(merged))
    |> Enum.each(fn {expected_row, merged_row} ->
      Enum.zip(expected_row, merged_row)
      |> Enum.each(fn {left, right} -> assert_value(left, right) end)
    end)
  end

  defp assert_value(left, right) when is_list(left) and is_list(right),
    do: assert(Enum.sort(left) == Enum.sort(right))

  defp assert_value(left, right) when is_float(left) and is_float(right) do
    assert abs(left - right) <= 1.0e-9 * max(1.0, max(abs(left), abs(right)))
  end

  defp assert_value(left, right), do: assert(left == right)

  describe "execution round trips" do
    test "global aggregates, avg included", %{tmp_dir: tmp_dir} do
      decomposition =
        round_trip(
          "SELECT count(*) AS c, count(value) AS cv, sum(value) AS s, " <>
            "avg(value) AS a, min(ts) AS lo, max(ts) AS hi FROM analytics.events",
          tmp_dir
        )

      assert decomposition.partial_sql =~ "__pq_a3_s"
      assert decomposition.partial_sql =~ "__pq_a3_c"
      refute decomposition.partial_sql =~ "avg"
    end

    test "unaliased aggregates keep DuckDB's own output names", %{tmp_dir: tmp_dir} do
      round_trip("SELECT count(*), sum(value), avg(value) FROM analytics.events", tmp_dir)
    end

    test "group-by with an ordered output", %{tmp_dir: tmp_dir} do
      round_trip(
        "SELECT name, count(*) AS n, sum(value) AS s FROM analytics.events " <>
          "GROUP BY name ORDER BY name",
        tmp_dir
      )
    end

    test "group-by on an aliased expression", %{tmp_dir: tmp_dir} do
      round_trip(
        "SELECT bucket + 1 AS b, count(*) AS n FROM analytics.events GROUP BY b ORDER BY b",
        tmp_dir
      )
    end

    test "top-k orders and limits only at the final step", %{tmp_dir: tmp_dir} do
      decomposition =
        round_trip(
          "SELECT name, sum(value) AS s FROM analytics.events " <>
            "GROUP BY name ORDER BY s DESC, name LIMIT 3",
          tmp_dir
        )

      refute decomposition.partial_sql =~ "LIMIT"
      assert decomposition.final_tail =~ "LIMIT 3"
    end

    test "ORDER BY an expression that is a select item orders by the column it became (T-536)",
         %{tmp_dir: tmp_dir} do
      by_aggregate =
        round_trip(
          "SELECT name, count(*) FROM analytics.events GROUP BY name " <>
            "ORDER BY count(*) DESC, name LIMIT 3",
          tmp_dir
        )

      assert by_aggregate.final_tail =~ ~s|ORDER BY "count_star()" DESC|

      by_key =
        round_trip(
          "SELECT count(*) AS n, date_trunc('minute', ts) AS b FROM analytics.events " <>
            "GROUP BY date_trunc('minute', ts) ORDER BY date_trunc('minute', ts) DESC LIMIT 4",
          tmp_dir
        )

      assert by_key.final_tail =~ ~s|ORDER BY "b" DESC|
    end

    test "a FILTER on an aggregate runs in the partial, and merges as the aggregate does (T-537)",
         %{tmp_dir: tmp_dir} do
      decomposition =
        round_trip(
          "SELECT bucket, count(*) FILTER (WHERE id % 5 = 0) AS fives, " <>
            "sum(value) FILTER (WHERE name = 'u-1') AS ones, " <>
            "avg(value) FILTER (WHERE id > 900) AS late, " <>
            "min(id) FILTER (WHERE id > 10) AS low, max(id) FILTER (WHERE id < 10) AS high, " <>
            "count(*) AS n FROM analytics.events GROUP BY bucket ORDER BY bucket",
          tmp_dir
        )

      assert decomposition.partial_sql =~ "FILTER"
    end

    test "a group no row of which passes the FILTER answers as the single engine does", %{
      tmp_dir: tmp_dir
    } do
      round_trip(
        "SELECT bucket, avg(value) FILTER (WHERE id < 0) AS none, " <>
          "sum(value) FILTER (WHERE id < 0) AS nothing, count(*) FILTER (WHERE id < 0) AS zero " <>
          "FROM analytics.events GROUP BY bucket ORDER BY bucket",
        tmp_dir
      )
    end

    test "ClickHouse's -If combinators split as the FILTER they are (T-537)", %{tmp_dir: tmp_dir} do
      for macro <- ClickHouseFunctions.statements_for("sumIf(x) avgIf(x) minIf(x) maxIf(x)"),
          do: Engine.query!(@engine, macro)

      decomposition =
        round_trip(
          "SELECT bucket, countIf(id % 5 = 0) AS fives, sumIf(value, name = 'u-1') AS ones, " <>
            "avgIf(value, id > 900) AS late, minIf(id, id > 10) AS low, maxIf(id, id < 10) AS high " <>
            "FROM analytics.events GROUP BY bucket ORDER BY bucket",
          tmp_dir
        )

      refute decomposition.partial_sql =~ ~r/sumif|avgif|minif|maxif/i
    end

    test "an -If form is an aggregate to GROUP BY ALL, and to the shapes that refuse one (review of T-537)",
         %{tmp_dir: tmp_dir} do
      for macro <- ClickHouseFunctions.statements_for("sumIf(x)"),
          do: Engine.query!(@engine, macro)

      round_trip(
        "SELECT bucket, sumIf(value, id > 500) AS late FROM analytics.events " <>
          "GROUP BY ALL ORDER BY bucket",
        tmp_dir
      )

      wrapped = "SELECT round(sumIf(value, id > 500)) FROM analytics.events"

      assert {:unsupported_aggregate_shape, "round"} = refused(wrapped, describe(wrapped))
    end

    test "HAVING filters the merged groups, not a shard's part of one (T-538)", %{
      tmp_dir: tmp_dir
    } do
      by_alias =
        round_trip(
          "SELECT name, count(*) AS n, sum(value) AS s FROM analytics.events " <>
            "GROUP BY name HAVING n > 142 AND s > 900 ORDER BY name",
          tmp_dir
        )

      refute by_alias.partial_sql =~ "HAVING"
      assert by_alias.final_having =~ "n > 142"

      round_trip(
        "SELECT name, count(*) AS n FROM analytics.events GROUP BY name " <>
          "HAVING count(*) > 142 OR name = 'u-0' ORDER BY n DESC, name LIMIT 2",
        tmp_dir
      )

      round_trip(
        "SELECT bucket, avg(value) AS mean FROM analytics.events GROUP BY bucket " <>
          "HAVING round(avg(value), 1) BETWEEN 6.0 AND 7.0 ORDER BY bucket",
        tmp_dir
      )

      round_trip(
        "SELECT bucket, count(*) AS n FROM analytics.events GROUP BY bucket HAVING n > 100000",
        tmp_dir
      )
    end

    test "SELECT DISTINCT is a GROUP BY of what it selects (T-538)", %{tmp_dir: tmp_dir} do
      decomposition =
        round_trip(
          "SELECT DISTINCT name, bucket FROM analytics.events WHERE id < 400 " <>
            "ORDER BY name, bucket LIMIT 9",
          tmp_dir
        )

      refute decomposition.partial_sql =~ "DISTINCT"
      assert decomposition.final_group =~ "GROUP BY"

      round_trip("SELECT DISTINCT bucket + 1 AS b FROM analytics.events ORDER BY b", tmp_dir)
    end

    test "count(DISTINCT x) is the distinct union of the shards' values, not the sum of their counts (T-539)",
         %{tmp_dir: tmp_dir} do
      for macro <- ClickHouseFunctions.statements_for("uniq(x) uniqExact(x)"),
          do: Engine.query!(@engine, macro)

      decomposition =
        round_trip(
          "SELECT bucket, count(DISTINCT name) AS names, uniq(tag) AS tags, " <>
            "uniqExact(id % 10) AS digits, count(*) AS n FROM analytics.events " <>
            "GROUP BY bucket ORDER BY bucket",
          tmp_dir
        )

      assert decomposition.value_lists
      refute decomposition.partial_sql =~ ~r/uniq/i

      round_trip("SELECT count(DISTINCT tag) FROM analytics.events WHERE id < 0", tmp_dir)
    end

    test "argMax and argMin carry the key they were taken at, over rows with a value (T-539)",
         %{tmp_dir: tmp_dir} do
      round_trip(
        "SELECT bucket, argMax(name, value + id / 1000.0) AS top, " <>
          "argMin(tag, id) AS first_tag, arg_max(tag, id) AS last_tag " <>
          "FROM analytics.events GROUP BY bucket ORDER BY bucket",
        tmp_dir
      )

      round_trip("SELECT arg_max(tag, id) FROM analytics.events WHERE id % 4 = 0", tmp_dir)
    end

    test "lists flatten, a distinct list keeps its NULL, and an n slices the final list (T-539)",
         %{tmp_dir: tmp_dir} do
      for macro <-
            ClickHouseFunctions.statements_for(
              "groupArray(x) groupUniqArray(x) groupUniqArrayArray(x)"
            ),
          do: Engine.query!(@engine, macro)

      round_trip(
        "SELECT bucket, groupUniqArray(tag) AS tags, list(DISTINCT name) AS names, " <>
          "groupUniqArray(name, 100) AS capped, groupUniqArrayArray([name, tag]) AS both " <>
          "FROM analytics.events GROUP BY bucket ORDER BY bucket",
        tmp_dir
      )

      round_trip(
        "SELECT bucket, groupArray(tag) AS every FROM analytics.events WHERE id < 40 " <>
          "GROUP BY bucket ORDER BY bucket",
        tmp_dir
      )

      sql = "SELECT groupUniqArray(name, 3) AS few FROM analytics.events"
      {:ok, decomposition} = Decomposer.decompose(@conn, sql, describe(sql), @columns)

      assert decomposition.final_select =~ "list_slice("
      assert decomposition.value_lists
    end

    test "a list over no rows is NULL on one engine and on many (review of T-539)", %{
      tmp_dir: tmp_dir
    } do
      for macro <-
            ClickHouseFunctions.statements_for(
              "groupArray(x) groupUniqArray(x) groupUniqArrayArray(x)"
            ),
          do: Engine.query!(@engine, macro)

      round_trip(
        "SELECT groupArray(name) AS every, groupUniqArray(name) AS each, " <>
          "list(DISTINCT tag) AS tags, groupUniqArrayArray([name, tag]) AS both, " <>
          "groupArray(name, 3) AS few, count(DISTINCT name) AS names " <>
          "FROM analytics.events WHERE id < 0",
        tmp_dir
      )
    end

    test "a shard ships n of a sliced list and the distinct elements of a flattened one, not every row (review of T-539)",
         %{tmp_dir: tmp_dir} do
      for macro <- ClickHouseFunctions.statements_for("groupArray(x) groupUniqArrayArray(x)"),
          do: Engine.query!(@engine, macro)

      sql =
        "SELECT bucket, groupArray(name, 2) AS two, groupUniqArrayArray([name, tag]) AS both " <>
          "FROM analytics.events GROUP BY bucket"

      {:ok, decomposition} = Decomposer.decompose(@conn, sql, describe(sql), @columns)

      assert decomposition.partial_sql =~ ~s|list_slice(list("name"), 1, 2)|
      assert decomposition.partial_sql =~ "list_distinct(flatten(list("
      refute decomposition.partial_sql =~ ":="

      round_trip(
        "SELECT bucket, groupArray(name, 2) AS two, groupUniqArrayArray([name, tag]) AS both " <>
          "FROM analytics.events WHERE id < 3 GROUP BY bucket ORDER BY bucket",
        tmp_dir
      )
    end

    test "a quantile is the engine's own over every value the shards hold (T-541)", %{
      tmp_dir: tmp_dir
    } do
      for macro <- ClickHouseFunctions.statements_for("quantileIf(x)"),
          do: Engine.query!(@engine, macro)

      decomposition =
        round_trip(
          "SELECT bucket, quantile_cont(value, 0.95) AS p95, quantile_disc(id, 0.5) AS mid, " <>
            "median(value) AS med, quantile_cont(value, [0.25, 0.75]) AS quartiles, " <>
            "quantileIf(value, id > 500, 0.9) AS late, " <>
            "quantile_cont(value, 0.5) FILTER (WHERE name = 'u-1') AS ones, " <>
            "quantile_cont(length(tag), 0.5) AS tagged, count(*) AS n " <>
            "FROM analytics.events GROUP BY bucket ORDER BY bucket",
          tmp_dir
        )

      assert decomposition.value_lists
      refute decomposition.partial_sql =~ ~r/quantile|median/i

      round_trip(
        "SELECT quantile_cont(value, 0.5) AS none FROM analytics.events WHERE id < 0",
        tmp_dir
      )
    end

    test "a sample under a LIMIT is rows from every shard, and the outer statement over LIMIT n of them (T-540)",
         %{tmp_dir: tmp_dir} do
      for macro <- ClickHouseFunctions.statements_for("groupUniqArray(x)"),
          do: Engine.query!(@engine, macro)

      as_cte =
        round_trip(
          "WITH sampled AS (SELECT name AS param0, tag AS param1 FROM analytics.events " <>
            "WHERE id >= 100 AND id < 900 LIMIT 100000) " <>
            "SELECT groupUniqArray(param0, 20) AS param0, groupUniqArray(param1, 20) AS param1 " <>
            "FROM sampled",
          tmp_dir
        )

      assert as_cte.rows.limit == 100_000
      assert as_cte.partial_sql =~ "LIMIT 100000"
      refute as_cte.partial_sql =~ ~r/groupuniqarray/i

      round_trip(
        "SELECT bucket, count(*) AS n, max(value) AS top FROM " <>
          "(SELECT bucket, value FROM analytics.events WHERE id < 500 LIMIT 5000) e " <>
          "GROUP BY bucket HAVING count(*) > 1 ORDER BY bucket",
        tmp_dir
      )

      round_trip(
        "SELECT count(*) FROM (SELECT * FROM analytics.events WHERE id < 0 LIMIT 10)",
        tmp_dir
      )
    end

    test "a sample smaller than the window is n rows of it, whichever they are (T-540)", %{
      tmp_dir: tmp_dir
    } do
      sql =
        "SELECT count(*) AS n, min(id) >= 100 AS inside FROM (SELECT id FROM analytics.events WHERE id >= 100 LIMIT 7)"

      {:ok, decomposition} = Decomposer.decompose(@conn, sql, describe(sql), @columns)

      parquet = Path.join(tmp_dir, "rows.parquet")

      Engine.query!(
        @engine,
        "COPY (SELECT * FROM (#{decomposition.partial_sql}) UNION ALL " <>
          "SELECT * FROM (#{decomposition.partial_sql})) TO " <>
          "#{Smolquery.Identifier.sql_string(parquet)} (FORMAT parquet)"
      )

      from = "read_parquet([#{Smolquery.Identifier.sql_string(parquet)}])"

      assert Engine.query!(@engine, Decomposer.final_sql(decomposition, from)).rows == [[7, true]]
    end

    test "any answers a value some shard holds, and ships no list (T-539)", %{tmp_dir: tmp_dir} do
      sql = "SELECT bucket, any_value(name) AS one FROM analytics.events GROUP BY bucket"
      {:ok, decomposition} = Decomposer.decompose(@conn, sql, describe(sql), @columns)

      refute decomposition.value_lists

      round_trip(
        "SELECT bucket, any_value(bucket * 2) AS twice FROM analytics.events " <>
          "GROUP BY bucket ORDER BY bucket",
        tmp_dir
      )
    end

    test "the WHERE clause runs in the partial", %{tmp_dir: tmp_dir} do
      decomposition =
        round_trip(
          "SELECT count(*) AS n, sum(value) AS s FROM analytics.events WHERE id > 500",
          tmp_dir
        )

      assert decomposition.partial_sql =~ "WHERE"
    end

    test "an unselected group key still shards exactly", %{tmp_dir: tmp_dir} do
      round_trip(
        "SELECT count(*) AS n FROM analytics.events GROUP BY name ORDER BY n DESC",
        tmp_dir
      )
    end

    test "GROUP BY ALL resolves its keys from the select list", %{tmp_dir: tmp_dir} do
      decomposition =
        round_trip(
          "SELECT count(name) AS count, name FROM analytics.events " <>
            "GROUP BY ALL ORDER BY count DESC, name LIMIT 3",
          tmp_dir
        )

      assert decomposition.partial_sql =~ "GROUP BY __pq_g0"
      refute decomposition.partial_sql =~ "ALL"
    end

    test "GROUP BY ALL with an expression key and an avg", %{tmp_dir: tmp_dir} do
      round_trip(
        "SELECT bucket + 1 AS b, avg(value) AS a, count(*) AS n FROM analytics.events " <>
          "GROUP BY ALL ORDER BY b",
        tmp_dir
      )
    end

    test "GROUP BY ALL over aggregates only groups nothing", %{tmp_dir: tmp_dir} do
      decomposition =
        round_trip(
          "SELECT count(*) AS n, sum(value) AS s FROM analytics.events GROUP BY ALL",
          tmp_dir
        )

      refute decomposition.partial_sql =~ "GROUP BY"
    end

    test "integer sums past 2^53 stay exact through parquet partials", %{tmp_dir: tmp_dir} do
      decomposition =
        round_trip("SELECT sum(big) AS s, avg(big) AS a FROM analytics.events", tmp_dir)

      assert decomposition.partial_sql =~ "DECIMAL(38,0)"
    end
  end

  describe "the gate refuses what it cannot split exactly" do
    defp refused(sql, outputs \\ [], columns \\ @columns) do
      assert {:error, reason} = Decomposer.decompose(@conn, sql, outputs, columns)

      reason
    end

    test "SELECT star" do
      assert {:unsupported_expression, "STAR"} = refused("SELECT * FROM analytics.events")
    end

    test "joins" do
      assert :from_not_a_base_table =
               refused("SELECT count(*) FROM analytics.events e JOIN analytics.events f ON true")
    end

    test "CTEs" do
      assert :cte =
               refused("WITH x AS (SELECT 1 AS n) SELECT count(*) FROM analytics.events, x")

      assert :from_not_a_base_table = refused("WITH x AS (SELECT 1 AS n) SELECT count(*) FROM x")
    end

    test "HAVING over an aggregate or a column that is no select item" do
      hidden = "SELECT name, count(*) FROM analytics.events GROUP BY name HAVING sum(value) > 1"
      assert {:having_aggregate, "sum"} = refused(hidden, describe(hidden))

      unselected = "SELECT count(*) AS n FROM analytics.events GROUP BY name HAVING name = 'u-1'"
      assert {:having_reference, "name"} = refused(unselected, describe(unselected))

      subquery =
        "SELECT name, count(*) AS n FROM analytics.events GROUP BY name " <>
          "HAVING n > (SELECT 5)"

      assert {:unsupported_expression, "SUBQUERY"} = refused(subquery, describe(subquery))

      shadowed =
        "SELECT bucket + 1 AS bucket, count(*) AS n FROM analytics.events GROUP BY ALL " <>
          "HAVING bucket > 1"

      assert {:having_ambiguous, "bucket"} = refused(shadowed, describe(shadowed))

      unknown =
        "SELECT bucket, count(*) AS n FROM analytics.events GROUP BY bucket HAVING bool_or(bucket > 1)"

      assert {:having_aggregate, "bool_or"} = refused(unknown, describe(unknown))

      for macro <- ClickHouseFunctions.statements_for("uniqExact(x)"),
          do: Engine.query!(@engine, macro)

      macro =
        "SELECT bucket, count(*) AS n FROM analytics.events GROUP BY bucket HAVING uniqExact(bucket) > 0"

      assert {:having_aggregate, "uniqexact"} = refused(macro, describe(macro))

      bound =
        "SELECT name, count(*) AS n FROM analytics.events GROUP BY name HAVING count(*) > $1"

      assert {:error, :parameter_in_having} =
               Decomposer.decompose(
                 @conn,
                 bound,
                 [{"name", "VARCHAR"}, {"n", "BIGINT"}],
                 @columns,
                 [
                   5
                 ]
               )

      volatile =
        "SELECT name, count(*) AS n FROM analytics.events GROUP BY name HAVING n > random()"

      assert {:volatile_function, "random"} = refused(volatile, describe(volatile))
    end

    test "DISTINCT ON, and DISTINCT over an aggregate" do
      on = "SELECT DISTINCT ON (name) name, id FROM analytics.events"
      assert :distinct_on = refused(on, describe(on))

      over = "SELECT DISTINCT count(*) FROM analytics.events GROUP BY name"
      assert :distinct_over_aggregates = refused(over, describe(over))
    end

    test "a bare count(*) is answered from metadata, not a scan (T-448)" do
      assert :metadata_only = refused("SELECT count(*) FROM analytics.events")
      assert :metadata_only = refused("SELECT count(*) AS n, count(*) AS m FROM analytics.events")
      assert :metadata_only = refused("SELECT count(*) FROM analytics.events GROUP BY ALL")
    end

    test "a count that scans still decomposes", %{tmp_dir: tmp_dir} do
      round_trip("SELECT count(name) AS n FROM analytics.events", tmp_dir)
      round_trip("SELECT count(*) AS n FROM analytics.events WHERE id > 10", tmp_dir)
      round_trip("SELECT bucket, count(*) AS n FROM analytics.events GROUP BY bucket", tmp_dir)
    end

    test "a sampled subquery in a shape this does not split (T-540)" do
      for {sql, reason} <- [
            {"SELECT count(*) FROM (SELECT id FROM analytics.events)", :unbounded_rows},
            {"SELECT count(*) FROM (SELECT id FROM analytics.events ORDER BY id LIMIT 5)",
             :sampled_order},
            {"SELECT count(*) FROM (SELECT id FROM analytics.events LIMIT 5 OFFSET 2)",
             :sampled_order},
            {"SELECT count(*) FROM (SELECT DISTINCT id FROM analytics.events LIMIT 5)",
             :sampled_order},
            {"SELECT sum(n) FROM (SELECT count(*) AS n FROM analytics.events GROUP BY name LIMIT 5)",
             :sampled_groups},
            {"SELECT count(*) FROM (SELECT id FROM analytics.events WHERE random() < 0.5 LIMIT 5)",
             {:volatile_function, "random"}},
            {"SELECT count(*) FROM (SELECT id, name AS id FROM analytics.events LIMIT 5)",
             :duplicate_row_columns},
            {"SELECT count(*) FROM (SELECT big + CAST(1 AS HUGEINT) AS wide FROM analytics.events LIMIT 5)",
             :inexact_row_column},
            {"SELECT count(*), (SELECT max(id) FROM analytics.events) FROM " <>
               "(SELECT id FROM analytics.events LIMIT 5)",
             {:unsupported_expression, "SUBQUERY"}},
            {"WITH a AS (SELECT id FROM analytics.events LIMIT 5), b AS (SELECT 1) " <>
               "SELECT count(*) FROM a", :cte},
            {"SELECT count(*) FROM (SELECT id FROM analytics.events LIMIT 5) x " <>
               "JOIN (SELECT id FROM analytics.events LIMIT 5) y USING (id)",
             :from_not_a_base_table}
          ] do
        assert reason == refused(sql, describe(sql)), sql
      end
    end

    test "a value aggregate in a shape this does not split" do
      for macro <- ClickHouseFunctions.statements_for("groupArray(x)"),
          do: Engine.query!(@engine, macro)

      for {sql, reason} <- [
            {"SELECT list(name ORDER BY id) FROM analytics.events", {:ordered_aggregate, "list"}},
            {"SELECT count(DISTINCT name) FILTER (WHERE id > 5) FROM analytics.events",
             {:filtered_aggregate, "count"}},
            {"SELECT arg_max(name, id) FILTER (WHERE id > 5) FROM analytics.events",
             {:filtered_aggregate, "arg_max"}},
            {"SELECT quantile_cont(value, 1 - 0.5) FROM analytics.events",
             {:unsupported_aggregate_shape, "quantile_cont"}},
            {"SELECT count(DISTINCT big + CAST(1 AS HUGEINT)) FROM analytics.events",
             {:inexact_partial_column, "HUGEINT[]"}},
            {"SELECT groupArray(name, 1 + 1) FROM analytics.events",
             {:unsupported_aggregate_shape, "grouparray"}}
          ] do
        assert reason == refused(sql, describe(sql)), sql
      end
    end

    test "a DISTINCT aggregate" do
      assert {:distinct_aggregate, "sum"} =
               refused("SELECT sum(DISTINCT value) FROM analytics.events")
    end

    test "an aggregate inside a FILTER" do
      assert {:nested_aggregate, "count_star"} =
               refused("SELECT count(*) FILTER (WHERE id > (max(id))) FROM analytics.events")
    end

    test "an aggregate that does not merge" do
      assert {:ungrouped_expression, "stddev_samp"} =
               refused("SELECT stddev_samp(value) FROM analytics.events")
    end

    test "arithmetic over an aggregate" do
      assert {:unsupported_aggregate_shape, "+"} =
               refused("SELECT sum(value) + 1 FROM analytics.events")
    end

    test "arithmetic over an aggregate under GROUP BY ALL is not a key" do
      assert {:unsupported_aggregate_shape, "+"} =
               refused("SELECT name, sum(value) + 1 FROM analytics.events GROUP BY ALL")
    end

    test "window functions" do
      assert {:unsupported_expression, "WINDOW"} =
               refused("SELECT sum(value) OVER () FROM analytics.events")
    end

    test "subqueries" do
      assert {:unsupported_expression, "SUBQUERY"} =
               refused("SELECT (SELECT 1) FROM analytics.events")
    end

    test "OFFSET" do
      assert :offset = refused("SELECT count(*) FROM analytics.events LIMIT 5 OFFSET 5")
    end

    test "ORDER BY on an expression that is no select item, or one whose name is another's too" do
      sql = "SELECT name, count(*) FROM analytics.events GROUP BY name ORDER BY sum(value)"

      assert :order_by_expression = refused(sql, describe(sql))

      twice =
        "SELECT count(*) AS n, sum(value) AS n FROM analytics.events GROUP BY name " <>
          "ORDER BY sum(value)"

      assert :order_by_expression = refused(twice, describe(twice))
    end

    test "ORDER BY a constant is a position, not the select item that is the same literal" do
      sql =
        "SELECT 2 AS two, name, count(*) FROM analytics.events GROUP BY 2, name ORDER BY 2 LIMIT 3"

      assert :order_by_position = refused(sql, describe(sql))
    end

    test "an ungrouped column" do
      assert :ungrouped_expression = refused("SELECT name, count(*) FROM analytics.events")
    end

    test "a group reference that resolves nowhere" do
      assert {:unknown_group_reference, "nope"} =
               refused("SELECT count(*) FROM analytics.events GROUP BY nope")
    end

    test "table columns colliding with the generated aliases" do
      assert :reserved_column_prefix =
               refused("SELECT count(*) FROM analytics.events", [], ["__pq_g0"])
    end

    test "anything but a single SELECT" do
      assert :not_a_single_select = refused("INSERT INTO analytics.events VALUES (1)")
    end

    test "volatile functions" do
      assert {:volatile_function, _now} =
               refused(
                 "SELECT count(*) FROM analytics.events WHERE ts >= now() - INTERVAL 1 HOUR"
               )

      for name <- ~w(rand rand32 rand64 randCanonical) do
        assert {:volatile_function, _rand} =
                 refused("SELECT count(*) FROM analytics.events WHERE #{name}() > 0.5")
      end

      assert {:volatile_function, "rand"} =
               refused(
                 "SELECT count(*) FROM analytics.events WHERE cityHash64(ts, rand()) % 2 = 0"
               )

      assert {:volatile_function, "random"} =
               refused("SELECT count(*) FROM analytics.events WHERE value > random()")

      assert {:volatile_function, "now64"} =
               refused(
                 "SELECT k, count(*) FROM analytics.events WHERE ts > now64(3) - INTERVAL 1 HOUR GROUP BY k"
               )
    end
  end
end
