defmodule Smolquery.QueryService.PlannerTest do
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog

  alias Smolquery.BufferService.HotManifest.Entry
  alias Smolquery.Catalog.Connection
  alias Smolquery.Engine
  alias Smolquery.QueryService.Plan
  alias Smolquery.QueryService.Planner
  alias Smolquery.QueryService.Runtime
  alias Smolquery.QueryService.Statistics
  alias Smolquery.QueryService.Trace
  alias Smolquery.Schema
  alias Smolquery.Segments.Id
  alias Smolquery.Segments.Store
  alias Smolquery.Test.FixedCatalog
  alias Smolquery.Test.ManifestServer
  alias Smolquery.Test.SegmentFixture

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

  defp ids(entries), do: Enum.map(entries, & &1["id"])

  defp runtime(entries_or_agent, opts \\ [])

  defp runtime(entries, opts) when is_list(entries),
    do: runtime(start_supervised!({Agent, fn -> entries end}, id: make_ref()), opts)

  defp runtime(agent, opts) when is_pid(agent) do
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
      Keyword.merge(
        [
          name: :"planner_#{:erlang.unique_integer([:positive])}",
          catalog: FixedCatalog.new(answers),
          buffer_base_url: ManifestServer.base_url(server),
          buffer_timeout_ms: 2_000
        ],
        Keyword.get(opts, :runtime, [])
      )
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

  describe "federated references (T-324)" do
    setup do
      previous = Application.get_env(:smolquery, :credential_key)

      Application.put_env(
        :smolquery,
        :credential_key,
        Base.encode64(:crypto.strong_rand_bytes(32))
      )

      on_exit(fn ->
        if previous do
          Application.put_env(:smolquery, :credential_key, previous)
        else
          Application.delete_env(:smolquery, :credential_key)
        end
      end)

      :ok
    end

    defp federated_runtime(names) do
      connections =
        Map.new(names, fn name ->
          {:ok, connection} =
            Connection.new(%{
              "name" => name,
              "host" => "db.internal",
              "database" => "app",
              "username" => "reader",
              "password" => "hunter2"
            })

          {name, connection}
        end)

      runtime([], answers: [connections: connections])
    end

    test "a reference to a registered connection attaches it" do
      runtime = federated_runtime(["warehouse"])

      assert {:ok, plan} =
               Planner.plan(runtime, @conn, "SELECT * FROM warehouse.public.users")

      assert plan.federated
      assert [attach] = plan.statements
      assert attach =~ ~s|AS "warehouse"|
      assert attach =~ "READ_ONLY"
      assert plan.tables == []
    end

    test "the attach carries the opened password, and the plan says it federates" do
      runtime = federated_runtime(["warehouse"])

      {:ok, plan} = Planner.plan(runtime, @conn, "SELECT * FROM warehouse.public.users")

      assert hd(plan.statements) =~ "password=hunter2"
    end

    test "an unregistered catalog is still refused, naming the reference" do
      runtime = federated_runtime(["warehouse"])

      assert Planner.plan(runtime, @conn, "SELECT * FROM lake.public.users") ==
               {:error, {:catalog_qualified_reference, "lake.users"}}
    end

    test "a catalog that stores no connections refuses every catalog-qualified name" do
      runtime = runtime([])

      assert Planner.plan(runtime, @conn, "SELECT * FROM warehouse.public.users") ==
               {:error, {:catalog_qualified_reference, "warehouse.users"}}
    end

    test "each connection attaches once, however many tables it names" do
      runtime = federated_runtime(["warehouse"])

      sql = """
      SELECT * FROM warehouse.public.users u
        JOIN warehouse.public.orders o ON o.user_id = u.id
      """

      {:ok, plan} = Planner.plan(runtime, @conn, sql)

      assert [_one] = plan.statements
    end

    test "two connections both attach" do
      runtime = federated_runtime(["warehouse", "billing"])

      sql = """
      SELECT * FROM warehouse.public.users u
        JOIN billing.public.invoices i ON i.user_id = u.id
      """

      {:ok, plan} = Planner.plan(runtime, @conn, sql)

      assert [_first, _second] = plan.statements
    end

    test "a plan touching no connection does not federate" do
      runtime = runtime([])

      {:ok, plan} = Planner.plan(runtime, @conn, "SELECT * FROM analytics.events")

      refute plan.federated
    end

    test "a name that is not an identifier is refused before any catalog read" do
      runtime = federated_runtime([])

      assert {:error, {:catalog_qualified_reference, _reference}} =
               Planner.plan(runtime, @conn, ~s|SELECT * FROM "bad cat"."public"."users"|)
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

      assert view_stmt =~
               ~s|CREATE OR REPLACE VIEW "analytics"."events" AS SELECT "id", "name" FROM|

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

    test "hot_members is the membership before pruning, so a later WHERE prunes from the whole set (T-418)" do
      stats = %{"id" => %{"min" => 1, "max" => 10, "null_count" => 0}}
      runtime = runtime([entry("01A", %{"stats" => stats}), entry("01B")])

      assert {:ok, plan} =
               Planner.plan(runtime, @conn, "SELECT * FROM analytics.events WHERE id > 100")

      assert ids(plan.hot[@table]) == ["01B"]
      assert plan.hot_members == %{@table => ["01A", "01B"]}
    end

    test "hot_ids: reads a table by id, so a segment stamped before the bound that lands later stays out (T-418)" do
      sql = "SELECT * FROM analytics.events"
      bound = System.system_time(:millisecond)
      first = Id.generate(bound - 10)
      agent = start_supervised!({Agent, fn -> [entry(first)] end}, id: make_ref())
      runtime = runtime(agent)

      assert {:ok, first_touch} = Planner.plan(runtime, @conn, sql, hot_before_ms: bound)
      assert first_touch.hot_members == %{@table => [first]}

      skewed = Id.generate(bound - 5)
      Agent.update(agent, &(&1 ++ [entry(skewed)]))

      assert {:ok, by_time} = Planner.plan(runtime, @conn, sql, hot_before_ms: bound)
      assert ids(by_time.hot[@table]) == [first, skewed]

      assert {:ok, by_id} =
               Planner.plan(runtime, @conn, sql,
                 hot_before_ms: bound,
                 hot_ids: first_touch.hot_members
               )

      assert ids(by_id.hot[@table]) == [first]
      assert by_id.hot_members == first_touch.hot_members
    end

    test "a pin older than hot_pin_max_age_ms is refused before any manifest is read (T-418)" do
      runtime = runtime([entry("01A")], runtime: [hot_pin_max_age_ms: 1_000])
      stale = System.system_time(:millisecond) - 5_000

      assert {:error, {:pinned_hot_expired, age_ms, 1_000}} =
               Planner.plan(runtime, @conn, "SELECT * FROM analytics.events",
                 hot_before_ms: stale
               )

      assert age_ms >= 5_000

      assert {:ok, _plan} =
               Planner.plan(runtime, @conn, "SELECT * FROM analytics.events",
                 hot_before_ms: System.system_time(:millisecond)
               )
    end

    test "hot_members ids are copies, not slices of the manifest page (T-418)" do
      runtime = runtime([entry("01A")])

      assert {:ok, plan} = Planner.plan(runtime, @conn, "SELECT * FROM analytics.events")
      assert [id] = plan.hot_members[@table]
      assert :binary.referenced_byte_size(id) == byte_size(id)
    end

    test "a pinned id the manifest no longer holds fails the plan rather than reading elsewhere (T-418)" do
      runtime = runtime([entry("01A")])

      assert {:error, {:pinned_hot_retired, @table, ["01GONE"]}} =
               Planner.plan(runtime, @conn, "SELECT * FROM analytics.events",
                 hot_ids: %{@table => ["01A", "01GONE"]}
               )
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

  describe "the Top-N bound (T-400)" do
    @hot_schema Schema.new!([{"id", :int64}, {"project", :string}, {"ts", :timestamp}])
    @tail "SELECT * FROM analytics.events WHERE project = 'a' ORDER BY ts DESC LIMIT 5"

    defp segment(dir, rows, schema \\ @hot_schema) do
      {:ok, prefix} = Store.prefix(@table)

      {:ok, segment} =
        SegmentFixture.write(rows, schema, store: Store.Local.new(dir: dir), prefix: prefix)

      segment |> Entry.from_segment(0) |> Entry.to_manifest() |> Map.put("url", segment.path)
    end

    defp minute(k, row) do
      %{
        "id" => k * 100 + row,
        "project" => project(row),
        "ts" => NaiveDateTime.add(~N[2026-08-27 10:00:00], k * 60 + row, :second)
      }
    end

    defp project(0), do: "c"
    defp project(row) when rem(row, 2) == 0, do: "a"
    defp project(_odd), do: "b"

    defp hot_entries(dir, count \\ 10),
      do: for(k <- 1..count, do: segment(dir, for(row <- 0..19, do: minute(k, row))))

    defp hot_runtime(entries, opts \\ []) do
      runtime(entries, Keyword.merge([answers: [schemas: %{@table => @hot_schema}]], opts))
    end

    defp at(entries, positions), do: for(k <- positions, do: Enum.at(entries, k - 1)["id"])

    @tag :tmp_dir
    test "a last-N query reads only the entries that can hold one of its rows", ctx do
      entries = hot_entries(ctx.tmp_dir)

      assert {:ok, plan} = Planner.plan(hot_runtime(entries), @conn, @tail)

      assert ids(plan.hot[@table]) == at(entries, [10])
      [_schema, view] = plan.statements
      assert view =~ Enum.at(entries, 9)["url"]
      refute view =~ Enum.at(entries, 8)["url"]
      assert view =~ "AT (VERSION => #{@snapshot})"
    end

    @tag :tmp_dir
    test "a rare match falls through to the second round's budget", ctx do
      entries = hot_entries(ctx.tmp_dir)
      runtime = hot_runtime(entries, runtime: [top_n_probe_rows: 60])
      sql = "SELECT * FROM analytics.events WHERE project = 'c' ORDER BY ts DESC LIMIT 3"

      assert {:ok, plan} = Planner.plan(runtime, @conn, sql)

      assert ids(plan.hot[@table]) == at(entries, [8, 9, 10])
    end

    @tag :tmp_dir
    test "too few matches in both rounds keeps every entry", ctx do
      entries = hot_entries(ctx.tmp_dir)
      runtime = hot_runtime(entries, runtime: [top_n_probe_rows: 60])
      sql = "SELECT * FROM analytics.events WHERE project = 'c' ORDER BY ts DESC LIMIT 4"

      assert {:ok, plan} = Planner.plan(runtime, @conn, sql)

      assert ids(plan.hot[@table]) == ids(entries)
    end

    @tag :tmp_dir
    test "ASC bounds by the oldest entries", ctx do
      entries = hot_entries(ctx.tmp_dir)
      sql = "SELECT id FROM analytics.events ORDER BY ts LIMIT 5"

      assert {:ok, plan} = Planner.plan(hot_runtime(entries), @conn, sql)

      assert ids(plan.hot[@table]) == at(entries, [1])
    end

    @tag :tmp_dir
    test "a candidate that predates a column still binds the WHERE", ctx do
      entries = hot_entries(ctx.tmp_dir, 9)
      old_schema = Schema.new!([{"id", :int64}, {"ts", :timestamp}])
      rows = for row <- 0..19, do: minute(10, row) |> Map.delete("project")
      entries = entries ++ [segment(ctx.tmp_dir, rows, old_schema)]
      runtime = hot_runtime(entries, runtime: [top_n_probe_rows: 40])

      assert {:ok, plan} = Planner.plan(runtime, @conn, @tail)

      assert ids(plan.hot[@table]) == at(entries, [9, 10])
    end

    @tag :tmp_dir
    test "the trace carries the outcome", ctx do
      entries = hot_entries(ctx.tmp_dir)
      collector = Trace.attach("top-n-#{System.unique_integer([:positive])}", self())

      assert {:ok, _plan} = Planner.plan(hot_runtime(entries), @conn, @tail)

      assert %{meta: %{bounded: true, rounds: 1, candidates: 1}} =
               collector |> Trace.stop() |> Enum.find(&(&1.name == :top_n))
    end

    @tag :tmp_dir
    test "a budget of zero turns the bound off", ctx do
      entries = hot_entries(ctx.tmp_dir)
      runtime = hot_runtime(entries, runtime: [top_n_probe_rows: 0])

      assert {:ok, plan} = Planner.plan(runtime, @conn, @tail)

      assert ids(plan.hot[@table]) == ids(entries)
    end

    @tag :tmp_dir
    test "fewer entries than the probe is worth are read as they are", ctx do
      entries = hot_entries(ctx.tmp_dir, 3)

      assert {:ok, plan} = Planner.plan(hot_runtime(entries), @conn, @tail)

      assert ids(plan.hot[@table]) == ids(entries)
    end

    @tag :tmp_dir
    test "a query the bound does not apply to reads every entry", ctx do
      entries = hot_entries(ctx.tmp_dir)
      sql = "SELECT * FROM analytics.events WHERE project = 'a' ORDER BY ts DESC"

      assert {:ok, plan} = Planner.plan(hot_runtime(entries), @conn, sql)

      assert ids(plan.hot[@table]) == ids(entries)
    end

    @tag :tmp_dir
    test "a WHERE that draws a volatile function is not probed", ctx do
      entries = hot_entries(ctx.tmp_dir)
      sql = "SELECT * FROM analytics.events WHERE random() < 0.5 ORDER BY ts DESC LIMIT 5"
      collector = Trace.attach("top-n-#{System.unique_integer([:positive])}", self())

      log =
        capture_log(fn ->
          assert {:ok, plan} = Planner.plan(hot_runtime(entries), @conn, sql)
          assert ids(plan.hot[@table]) == ids(entries)
        end)

      refute log =~ "top-n probe failed"

      assert %{meta: %{bounded: false, rounds: 0}} =
               collector |> Trace.stop() |> Enum.find(&(&1.name == :top_n))
    end

    @tag :tmp_dir
    test "a clock-relative window is probed: the later clock only adds newer rows", ctx do
      entries = hot_entries(ctx.tmp_dir)

      sql =
        "SELECT * FROM analytics.events WHERE ts > now()::TIMESTAMP - INTERVAL 100 YEAR ORDER BY ts DESC LIMIT 5"

      assert {:ok, plan} = Planner.plan(hot_runtime(entries), @conn, sql)

      assert ids(plan.hot[@table]) == at(entries, [10])
    end

    @tag :tmp_dir
    test "an ordering column the manifest cannot bound is not probed", ctx do
      schema = Schema.new!([{"id", :int64}, {"amount", {:numeric, 10, 2}}, {"ts", :timestamp}])

      entries =
        for k <- 1..10 do
          rows =
            for row <- 0..19,
                do: %{
                  "id" => k * 100 + row,
                  "amount" => Decimal.new(k * 100 + row),
                  "ts" => minute(k, row)["ts"]
                }

          segment(ctx.tmp_dir, rows, schema)
        end

      runtime = runtime(entries, answers: [schemas: %{@table => schema}])
      sql = "SELECT * FROM analytics.events ORDER BY amount DESC LIMIT 5"

      assert {:ok, plan} = Planner.plan(runtime, @conn, sql)

      assert ids(plan.hot[@table]) == ids(entries)
    end

    @tag :tmp_dir
    test "under lockdown the probe turns extension autoload off first", ctx do
      entries = hot_entries(ctx.tmp_dir)

      assert {:ok, _plan} = Planner.plan(hot_runtime(entries), @conn, @tail)

      {:ok, result} =
        Smolquery.Engine.Connection.query(
          @conn,
          "SELECT current_setting('autoload_known_extensions'), current_setting('autoinstall_known_extensions')"
        )

      assert result.rows == [[false, false]]
    end

    @tag :tmp_dir
    test "a probe that cannot read its candidates keeps every entry and says so", ctx do
      stats = fn k ->
        %{
          "ts" => %{
            "min" => %{"type" => "naive_datetime", "value" => "2026-08-27T10:#{k}:00"},
            "max" => %{"type" => "naive_datetime", "value" => "2026-08-27T10:#{k}:19"},
            "null_count" => 0
          }
        }
      end

      entries =
        for k <- 10..19 do
          entry("01#{k}", %{
            "row_count" => 20,
            "stats" => stats.(k),
            "url" => Path.join(ctx.tmp_dir, "missing-#{k}.parquet")
          })
        end

      runtime = hot_runtime(entries)

      log =
        capture_log(fn ->
          assert {:ok, plan} = Planner.plan(runtime, @conn, @tail)
          assert ids(plan.hot[@table]) == ids(entries)
        end)

      assert log =~ "top-n probe failed, reading every hot entry"
    end
  end
end
