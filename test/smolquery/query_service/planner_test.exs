defmodule Smolquery.QueryService.PlannerTest do
  use ExUnit.Case, async: false

  alias Smolquery.Engine
  alias Smolquery.QueryService.Plan
  alias Smolquery.QueryService.Planner
  alias Smolquery.QueryService.Runtime
  alias Smolquery.Schema
  alias Smolquery.Test.FixedCatalog
  alias Smolquery.Test.ManifestServer

  @engine __MODULE__.Parser
  @table {"analytics", "events"}
  @snapshot 7

  setup_all do
    start_supervised!({Engine, name: @engine, extensions: []})
    :ok
  end

  defp entry(id, overrides \\ %{}) do
    Map.merge(
      %{
        "id" => id,
        "row_count" => 1,
        "byte_size" => 10,
        "added_at" => 0,
        "sealed_at" => nil,
        "retired_at" => nil,
        "claim_keys" => [],
        "stats" => %{},
        "url" => "http://hot.test/#{id}.parquet"
      },
      overrides
    )
  end

  defp runtime(entries, opts \\ []) do
    agent = start_supervised!({Agent, fn -> entries end}, id: make_ref())
    server = start_supervised!(ManifestServer.bandit_spec(agent), id: make_ref())

    answers =
      Map.merge(
        %{
          snapshot: @snapshot,
          schemas: %{@table => Schema.new!([{"id", :int64}, {"name", :string}])},
          segments: %{}
        },
        Map.new(Keyword.get(opts, :answers, []))
      )

    Runtime.new(
      name: :"planner_#{:erlang.unique_integer([:positive])}",
      catalog: FixedCatalog.new(answers),
      buffer_base_url: ManifestServer.base_url(server),
      buffer_timeout_ms: 2_000
    )
  end

  describe "table_refs/2" do
    test "finds dataset-qualified references through joins, subqueries, and CTE bodies" do
      sql = """
      WITH recent AS (SELECT * FROM analytics.events WHERE id > 10)
      SELECT r.id, u.name
        FROM recent r
        JOIN analytics.users u ON u.id = r.id
       WHERE u.id IN (SELECT id FROM analytics.blocked)
      """

      assert Planner.table_refs(@engine, sql) ==
               {:ok, [{"analytics", "events"}, {"analytics", "users"}, {"analytics", "blocked"}]}
    end

    test "a CTE name is not a table reference" do
      sql = "WITH c AS (SELECT 1 AS a) SELECT * FROM c"

      assert Planner.table_refs(@engine, sql) == {:ok, []}
    end

    test "a repeated reference appears once" do
      sql = "SELECT * FROM analytics.events UNION ALL SELECT * FROM analytics.events"

      assert Planner.table_refs(@engine, sql) == {:ok, [{"analytics", "events"}]}
    end

    test "a bare name that is no CTE is unknown, not guessed at" do
      assert Planner.table_refs(@engine, "SELECT * FROM events") ==
               {:error, {:unknown_table, "events"}}
    end

    test "a catalog-qualified reference is refused" do
      assert Planner.table_refs(@engine, "SELECT * FROM lake.analytics.events") ==
               {:error, {:catalog_qualified_reference, "lake.events"}}
    end

    test "a reference carrying its own AT clause is refused" do
      assert Planner.table_refs(
               @engine,
               "SELECT * FROM analytics.events AT (VERSION => 3)"
             ) ==
               {:error, {:unsupported_at_clause, "events"}}
    end

    test "DML and DDL fail the read-only gate" do
      assert {:error, {:invalid_query, message}} =
               Planner.table_refs(@engine, "INSERT INTO analytics.events VALUES (1)")

      assert message =~ "Only SELECT"

      assert {:error, {:invalid_query, _message}} =
               Planner.table_refs(@engine, "DROP TABLE analytics.events")
    end

    test "unparseable SQL is an invalid query, not a crash" do
      assert {:error, {:invalid_query, _message}} =
               Planner.table_refs(@engine, "SELECT FROM WHERE")
    end

    test "two statements are refused" do
      assert Planner.table_refs(@engine, "SELECT 1; SELECT 2") ==
               {:error, :multiple_statements}
    end

    test "a table function is not a table reference" do
      assert Planner.table_refs(@engine, "SELECT * FROM range(10)") == {:ok, []}
    end

    test "a dataset name that is not an identifier is refused" do
      assert Planner.table_refs(@engine, ~s|SELECT * FROM "bad ds"."t"|) ==
               {:error, {:invalid_identifier, "bad ds"}}
    end
  end

  describe "plan/3" do
    test "pins the snapshot and reads the sealed side AT that version" do
      runtime = runtime([entry("01A")])

      assert {:ok, %Plan{} = plan} =
               Planner.plan(runtime, @engine, "SELECT * FROM analytics.events")

      assert plan.snapshot == @snapshot
      assert plan.tables == [@table]

      assert [schema_stmt, view_stmt] = plan.statements
      assert schema_stmt == ~s|CREATE SCHEMA IF NOT EXISTS "analytics"|
      assert view_stmt =~ ~s|CREATE VIEW "analytics"."events" AS SELECT "id", "name" FROM|
      assert view_stmt =~ ~s|FROM "lake"."analytics"."events" AT (VERSION => #{@snapshot})|
    end

    test "unclaimed micro-segments join the union" do
      runtime = runtime([entry("01A"), entry("01B")])

      assert {:ok, plan} = Planner.plan(runtime, @engine, "SELECT * FROM analytics.events")

      [_schema, view] = plan.statements
      assert view =~ "UNION ALL BY NAME"

      assert view =~
               ~s|read_parquet(['http://hot.test/01A.parquet', 'http://hot.test/01B.parquet'], union_by_name := true)|

      assert [%{"id" => "01A"}, %{"id" => "01B"}] = plan.hot[@table]
    end

    test "a claimed entry whose sealed keys are all present at the snapshot is excluded" do
      sealed_path = "/data/sealed/analytics/events/01SEALED.parquet"

      runtime =
        runtime(
          [entry("01A", %{"claim_keys" => ["analytics/events/01SEALED.parquet"]})],
          answers: [segments: %{{@table, @snapshot} => [sealed_path]}]
        )

      assert {:ok, plan} = Planner.plan(runtime, @engine, "SELECT * FROM analytics.events")

      assert plan.hot[@table] == []
      [_schema, view] = plan.statements
      refute view =~ "read_parquet"
    end

    test "a claimed entry whose commit has not landed at the snapshot is included" do
      runtime =
        runtime([entry("01A", %{"claim_keys" => ["analytics/events/01PENDING.parquet"]})])

      assert {:ok, plan} = Planner.plan(runtime, @engine, "SELECT * FROM analytics.events")

      assert [%{"id" => "01A"}] = plan.hot[@table]
    end

    test "a claim is excluded only when every one of its keys is present" do
      sealed_path = "/data/sealed/analytics/events/01ONE.parquet"

      runtime =
        runtime(
          [
            entry("01A", %{
              "claim_keys" => [
                "analytics/events/01ONE.parquet",
                "analytics/events/01TWO.parquet"
              ]
            })
          ],
          answers: [segments: %{{@table, @snapshot} => [sealed_path]}]
        )

      assert {:ok, plan} = Planner.plan(runtime, @engine, "SELECT * FROM analytics.events")

      assert [%{"id" => "01A"}] = plan.hot[@table]
    end

    test "a query touching no tables plans no views" do
      runtime = runtime([])

      assert {:ok, plan} = Planner.plan(runtime, @engine, "SELECT 1 + 1")

      assert plan.tables == []
      assert plan.statements == []
    end

    test "an unknown table fails at plan time" do
      runtime = runtime([])

      assert Planner.plan(runtime, @engine, "SELECT * FROM analytics.missing") ==
               {:error, {:unknown_table, {"analytics", "missing"}}}
    end

    test "an unreachable hot tier fails the plan, not the sealed half of an answer" do
      runtime =
        Runtime.new(
          name: :"planner_down_#{:erlang.unique_integer([:positive])}",
          catalog:
            FixedCatalog.new(%{
              snapshot: @snapshot,
              schemas: %{@table => Schema.new!([{"id", :int64}])},
              segments: %{}
            }),
          buffer_base_url: "http://127.0.0.1:1",
          buffer_timeout_ms: 500
        )

      assert {:error, {:hot_tier_unavailable, @table, _reason}} =
               Planner.plan(runtime, @engine, "SELECT * FROM analytics.events")
    end
  end
end
