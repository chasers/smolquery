defmodule Smolquery.QueryService.PlannerTest do
  use ExUnit.Case, async: false

  alias Smolquery.Engine
  alias Smolquery.QueryService.Plan
  alias Smolquery.QueryService.Planner
  alias Smolquery.QueryService.Runtime
  alias Smolquery.QueryService.Statistics
  alias Smolquery.Schema
  alias Smolquery.Test.FixedCatalog
  alias Smolquery.Test.ManifestServer

  @engine __MODULE__.Parser
  @conn Engine.connection_name(@engine)
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

      assert Planner.table_refs(@conn, sql) ==
               {:ok, [{"analytics", "events"}, {"analytics", "users"}, {"analytics", "blocked"}]}
    end

    test "a CTE name is not a table reference" do
      sql = "WITH c AS (SELECT 1 AS a) SELECT * FROM c"

      assert Planner.table_refs(@conn, sql) == {:ok, []}
    end

    test "a repeated reference appears once" do
      sql = "SELECT * FROM analytics.events UNION ALL SELECT * FROM analytics.events"

      assert Planner.table_refs(@conn, sql) == {:ok, [{"analytics", "events"}]}
    end

    test "a bare name that is no CTE is unknown, not guessed at" do
      assert Planner.table_refs(@conn, "SELECT * FROM events") ==
               {:error, {:unknown_table, "events"}}
    end

    test "a catalog-qualified reference is refused" do
      assert Planner.table_refs(@conn, "SELECT * FROM lake.analytics.events") ==
               {:error, {:catalog_qualified_reference, "lake.events"}}
    end

    test "a reference carrying its own AT clause is refused" do
      assert Planner.table_refs(
               @conn,
               "SELECT * FROM analytics.events AT (VERSION => 3)"
             ) ==
               {:error, {:unsupported_at_clause, "events"}}
    end

    test "DML and DDL fail the read-only gate" do
      assert {:error, {:invalid_query, message}} =
               Planner.table_refs(@conn, "INSERT INTO analytics.events VALUES (1)")

      assert message =~ "Only SELECT"

      assert {:error, {:invalid_query, _message}} =
               Planner.table_refs(@conn, "DROP TABLE analytics.events")
    end

    test "unparseable SQL is an invalid query, not a crash" do
      assert {:error, {:invalid_query, _message}} =
               Planner.table_refs(@conn, "SELECT FROM WHERE")
    end

    test "two statements are refused" do
      assert Planner.table_refs(@conn, "SELECT 1; SELECT 2") ==
               {:error, :multiple_statements}
    end

    test "a table function is not a table reference" do
      assert Planner.table_refs(@conn, "SELECT * FROM range(10)") == {:ok, []}
    end

    test "a dataset name that is not an identifier is refused" do
      assert Planner.table_refs(@conn, ~s|SELECT * FROM "bad ds"."t"|) ==
               {:error, {:invalid_identifier, "bad ds"}}
    end
  end

  describe "table function allowlist (T-321)" do
    test "postgres_scan is refused before it can open a connection" do
      sql = "SELECT * FROM postgres_scan('host=10.0.0.1 user=u password=p', 'public', 't')"

      assert Planner.table_refs(@conn, sql) ==
               {:error, {:unsupported_table_function, "postgres_scan"}}
    end

    test "postgres_scan_pushdown is refused too" do
      sql = "SELECT * FROM postgres_scan_pushdown('host=10.0.0.1', 'public', 't')"

      assert Planner.table_refs(@conn, sql) ==
               {:error, {:unsupported_table_function, "postgres_scan_pushdown"}}
    end

    test "duckdb_databases is refused: it returns the catalog's connection string" do
      assert Planner.table_refs(@conn, "SELECT path FROM duckdb_databases()") ==
               {:error, {:unsupported_table_function, "duckdb_databases"}}
    end

    test "a file reader is refused here, not left to lockdown" do
      assert Planner.table_refs(@conn, "SELECT * FROM read_csv('/etc/passwd')") ==
               {:error, {:unsupported_table_function, "read_csv"}}

      assert Planner.table_refs(@conn, "SELECT * FROM read_parquet('/data/x.parquet')") ==
               {:error, {:unsupported_table_function, "read_parquet"}}
    end

    test "the allowed generators pass" do
      assert Planner.table_refs(@conn, "SELECT * FROM range(10)") == {:ok, []}
      assert Planner.table_refs(@conn, "SELECT * FROM generate_series(1, 10)") == {:ok, []}
      assert Planner.table_refs(@conn, "SELECT * FROM repeat('a', 3)") == {:ok, []}
    end

    test "unnest passes despite the list_value function under its arguments" do
      assert Planner.table_refs(@conn, "SELECT * FROM unnest([1, 2, 3])") == {:ok, []}
    end

    test "a refused function hidden in a CTE or subquery is still refused" do
      cte = "WITH c AS (SELECT * FROM duckdb_databases()) SELECT * FROM c"

      assert Planner.table_refs(@conn, cte) ==
               {:error, {:unsupported_table_function, "duckdb_databases"}}

      subquery = "SELECT * FROM (SELECT path FROM duckdb_databases()) d"

      assert Planner.table_refs(@conn, subquery) ==
               {:error, {:unsupported_table_function, "duckdb_databases"}}
    end

    test "a refused function joined against a real table is refused" do
      sql = """
      SELECT e.id
        FROM analytics.events e
        JOIN postgres_scan('host=10.0.0.1', 'public', 'u') u ON u.id = e.id
      """

      assert Planner.table_refs(@conn, sql) ==
               {:error, {:unsupported_table_function, "postgres_scan"}}
    end

    test "plan/3 refuses it as well, so no job engine ever runs it" do
      runtime = runtime([])

      assert Planner.plan(runtime, @conn, "SELECT path FROM duckdb_databases()") ==
               {:error, {:unsupported_table_function, "duckdb_databases"}}
    end

    test "a scalar function in the projection is untouched" do
      assert Planner.table_refs(@conn, "SELECT upper(name) FROM analytics.events") ==
               {:ok, [{"analytics", "events"}]}
    end
  end

  describe "plan/3" do
    test "pins the snapshot and reads the sealed side AT that version" do
      runtime = runtime([entry("01A")])

      assert {:ok, %Plan{} = plan} =
               Planner.plan(runtime, @conn, "SELECT * FROM analytics.events")

      assert plan.snapshot == @snapshot
      assert plan.tables == [@table]

      assert [schema_stmt, view_stmt] = plan.statements
      assert schema_stmt == ~s|CREATE SCHEMA IF NOT EXISTS "analytics"|
      assert view_stmt =~ ~s|CREATE VIEW "analytics"."events" AS SELECT "id", "name" FROM|
      assert view_stmt =~ ~s|FROM "lake"."analytics"."events" AT (VERSION => #{@snapshot})|
    end

    test "unclaimed micro-segments join the union" do
      runtime = runtime([entry("01A"), entry("01B")])

      assert {:ok, plan} = Planner.plan(runtime, @conn, "SELECT * FROM analytics.events")

      [_schema, view] = plan.statements
      assert view =~ "UNION ALL BY NAME"

      assert view =~
               ~s|read_parquet(['http://hot.test/01A.parquet', 'http://hot.test/01B.parquet'], union_by_name := true)|

      assert [%{"id" => "01A"}, %{"id" => "01B"}] = plan.hot[@table]
    end

    test "the same segment id arriving twice is read once, not counted per copy" do
      runtime = runtime([entry("01A"), entry("01B"), entry("01A")])

      assert {:ok, plan} = Planner.plan(runtime, @conn, "SELECT * FROM analytics.events")

      assert [%{"id" => "01A"}, %{"id" => "01B"}] = plan.hot[@table]

      [_schema, view] = plan.statements

      assert view =~
               ~s|read_parquet(['http://hot.test/01A.parquet', 'http://hot.test/01B.parquet'], union_by_name := true)|
    end

    test "of two copies of a segment, the one further along the seal handoff wins" do
      sealed_path = "/data/sealed/analytics/events/01SEALED.parquet"
      claim = %{"claim_keys" => ["analytics/events/01SEALED.parquet"]}

      runtime =
        runtime(
          [entry("01A"), entry("01A", claim)],
          answers: [segments: %{{@table, @snapshot} => [sealed_path]}]
        )

      assert {:ok, plan} = Planner.plan(runtime, @conn, "SELECT * FROM analytics.events")

      assert plan.hot[@table] == []
    end

    test "a claimed entry whose sealed keys are all present at the snapshot is excluded" do
      sealed_path = "/data/sealed/analytics/events/01SEALED.parquet"

      runtime =
        runtime(
          [entry("01A", %{"claim_keys" => ["analytics/events/01SEALED.parquet"]})],
          answers: [segments: %{{@table, @snapshot} => [sealed_path]}]
        )

      assert {:ok, plan} = Planner.plan(runtime, @conn, "SELECT * FROM analytics.events")

      assert plan.hot[@table] == []
      [_schema, view] = plan.statements
      refute view =~ "read_parquet"
    end

    test "a claimed entry whose commit has not landed at the snapshot is included" do
      runtime =
        runtime([entry("01A", %{"claim_keys" => ["analytics/events/01PENDING.parquet"]})])

      assert {:ok, plan} = Planner.plan(runtime, @conn, "SELECT * FROM analytics.events")

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

      assert {:ok, plan} = Planner.plan(runtime, @conn, "SELECT * FROM analytics.events")

      assert [%{"id" => "01A"}] = plan.hot[@table]
    end

    test "a query touching no tables plans no views" do
      runtime = runtime([])

      assert {:ok, plan} = Planner.plan(runtime, @conn, "SELECT 1 + 1")

      assert plan.tables == []
      assert plan.statements == []
    end

    test "an unknown table fails at plan time" do
      runtime = runtime([])

      assert Planner.plan(runtime, @conn, "SELECT * FROM analytics.missing") ==
               {:error, {:unknown_table, {"analytics", "missing"}}}
    end

    test "an entry whose stats cannot match the WHERE is pruned from the union" do
      stats = %{"id" => %{"min" => 1, "max" => 10, "null_count" => 0}}
      runtime = runtime([entry("01A", %{"stats" => stats}), entry("01B")])

      assert {:ok, plan} =
               Planner.plan(runtime, @conn, "SELECT * FROM analytics.events WHERE id > 100")

      assert [%{"id" => "01B"}] = plan.hot[@table]
      [_schema, view] = plan.statements
      assert view =~ "01B.parquet"
      refute view =~ "01A.parquet"
    end

    test "stats that leave a chance keep their entry" do
      stats = %{"id" => %{"min" => 1, "max" => 200, "null_count" => 0}}
      runtime = runtime([entry("01A", %{"stats" => stats})])

      assert {:ok, plan} =
               Planner.plan(runtime, @conn, "SELECT * FROM analytics.events WHERE id > 100")

      assert [%{"id" => "01A"}] = plan.hot[@table]
    end

    test "statistics count both tiers, with pruning reflected in the hot tier" do
      stats = %{"id" => %{"min" => 1, "max" => 10, "null_count" => 0}}

      runtime =
        runtime(
          [
            entry("01A", %{"stats" => stats, "row_count" => 5, "byte_size" => 50}),
            entry("01B", %{"row_count" => 7, "byte_size" => 70})
          ],
          answers: [stats: %{{@table, @snapshot} => %{files: 3, rows: 1_000, bytes: 4_096}}]
        )

      assert {:ok, plan} =
               Planner.plan(runtime, @conn, "SELECT * FROM analytics.events WHERE id > 100")

      assert plan.statistics.hot == %{
               files_total: 2,
               files_scanned: 1,
               rows_scanned: 7,
               bytes_scanned: 70
             }

      assert plan.statistics.sealed == %{
               files_total: 3,
               files_scanned: 3,
               rows_scanned: 1_000,
               bytes_scanned: 4_096
             }
    end

    test "a membership-excluded entry is not counted as considered" do
      sealed_path = "/data/sealed/analytics/events/01SEALED.parquet"

      runtime =
        runtime(
          [entry("01A", %{"claim_keys" => ["analytics/events/01SEALED.parquet"]})],
          answers: [segments: %{{@table, @snapshot} => [sealed_path]}]
        )

      assert {:ok, plan} = Planner.plan(runtime, @conn, "SELECT * FROM analytics.events")

      assert plan.statistics.hot == %{
               files_total: 0,
               files_scanned: 0,
               rows_scanned: 0,
               bytes_scanned: 0
             }
    end

    test "a stats read failing degrades statistics to nil, not the query" do
      runtime =
        runtime(
          [entry("01A")],
          answers: [stats: %{{@table, @snapshot} => {:error, :metadata_locked}}]
        )

      assert {:ok, plan} = Planner.plan(runtime, @conn, "SELECT * FROM analytics.events")

      assert plan.statistics == nil
      assert [%{"id" => "01A"}] = plan.hot[@table]
    end

    test "the plan carries the statement's canonical text" do
      runtime = runtime([])

      assert {:ok, plan} = Planner.plan(runtime, @conn, "SELECT 1 AS n; -- trailing")

      assert plan.canonical_sql == "SELECT 1 AS n"
    end

    test "a query touching no tables weighs nothing" do
      runtime = runtime([])

      assert {:ok, plan} = Planner.plan(runtime, @conn, "SELECT 1 + 1")

      assert Statistics.files_total(plan.statistics) == 0
      assert Statistics.bytes_scanned(plan.statistics) == 0
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
               Planner.plan(runtime, @conn, "SELECT * FROM analytics.events")
    end
  end
end
