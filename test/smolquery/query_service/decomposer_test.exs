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
  alias Smolquery.QueryService.Decomposer

  @moduletag :tmp_dir

  @engine __MODULE__.Engine
  @conn Engine.connection_name(@engine)
  @columns ~w(id name bucket ts value big)

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
             9007199254740993 + n AS big
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
      assert :cte = refused("WITH x AS (SELECT 1 AS n) SELECT count(*) FROM x")
    end

    test "HAVING" do
      assert :having =
               refused(
                 "SELECT name, count(*) FROM analytics.events GROUP BY name HAVING count(*) > 1"
               )
    end

    test "DISTINCT" do
      assert {:unsupported_modifier, _modifier} =
               refused("SELECT DISTINCT name FROM analytics.events")
    end

    test "a DISTINCT aggregate" do
      assert {:distinct_aggregate, "count"} =
               refused("SELECT count(DISTINCT name) FROM analytics.events")
    end

    test "a FILTER clause" do
      assert {:filtered_aggregate, "count_star"} =
               refused("SELECT count(*) FILTER (WHERE id > 1) FROM analytics.events")
    end

    test "an aggregate that does not merge" do
      assert {:ungrouped_expression, "median"} =
               refused("SELECT median(value) FROM analytics.events")
    end

    test "arithmetic over an aggregate" do
      assert {:unsupported_aggregate_shape, "+"} =
               refused("SELECT sum(value) + 1 FROM analytics.events")
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

    test "ORDER BY on an expression" do
      sql = "SELECT name, count(*) FROM analytics.events GROUP BY name ORDER BY sum(value)"

      assert :order_by_expression = refused(sql, describe(sql))
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

      assert {:volatile_function, "random"} =
               refused("SELECT count(*) FROM analytics.events WHERE value > random()")
    end
  end
end
