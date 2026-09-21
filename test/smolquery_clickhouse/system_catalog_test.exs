defmodule SmolqueryClickHouse.SystemCatalogTest do
  use ExUnit.Case, async: false

  @moduletag :capture_log

  import Plug.Conn, only: [put_req_header: 3, get_resp_header: 2]
  import Plug.Test

  alias Smolquery.Catalog
  alias Smolquery.QueryService
  alias Smolquery.Schema
  alias Smolquery.Schema.Field
  alias Smolquery.Test.ExitingCatalog
  alias Smolquery.Test.FixedCatalog
  alias Smolquery.Test.MapCatalog
  alias SmolqueryClickHouse.Router
  alias SmolqueryClickHouse.Runtime
  alias SmolqueryClickHouse.SystemCatalog

  @password "system-catalog-password"

  setup do
    unique = :erlang.unique_integer([:positive])
    query = :"ch_system_query_#{unique}"
    name = :"ch_system_edge_#{unique}"

    catalog = MapCatalog.new()
    :ok = Catalog.create_dataset(catalog, "default")
    :ok = Catalog.create_dataset(catalog, "empty")

    schema =
      Schema.new!([
        {"Timestamp", :timestamp_ns, nullable: false},
        {"ServiceName", :string},
        {"Body", :string},
        {"LogAttributes", {:map, :string, :string}},
        {"pod", :string, materialized: "LogAttributes['k8s.pod.name']"}
      ])

    :ok = Catalog.create_table(catalog, {"default", "otel_logs"}, schema)
    :ok = Catalog.put_clustering(catalog, {"default", "otel_logs"}, ["ServiceName", "Timestamp"])

    start_supervised!(
      {QueryService.Supervisor,
       name: query, catalog: FixedCatalog.new(%{snapshot: 1, schemas: %{}, segments: %{}})},
      id: query
    )

    on_exit(fn -> QueryService.Runtime.delete(query) end)

    start_supervised!(
      {SmolqueryClickHouse.Supervisor,
       name: name,
       password: @password,
       query_name: query,
       port: 0,
       catalog: catalog,
       total_rows_max_tables: 32},
      id: name
    )

    on_exit(fn -> Runtime.delete(name) end)

    %{name: name, catalog: catalog, query: query}
  end

  defp post(name, sql, query \\ %{}) do
    conn(:post, "/?" <> URI.encode_query(query), sql)
    |> put_req_header("x-clickhouse-key", @password)
    |> Router.call(name)
  end

  defp data(response) do
    assert response.status == 200, response.resp_body

    JSON.decode!(response.resp_body)["data"]
  end

  describe "what HyperDX asks before its first query" do
    test "system.settings answers no rows, not a failure, and still names its columns (T-509)", %{
      name: name
    } do
      response = post(name, "SELECT name, value FROM system.settings FORMAT JSON")

      assert data(response) == []

      assert [%{"name" => "name"}, %{"name" => "value"}] =
               JSON.decode!(response.resp_body)["meta"]
    end

    test "DESCRIBE of a system table lists the columns a SELECT from it answers (T-512)", %{
      name: name
    } do
      for response <- [
            post(name, "DESCRIBE system.tables FORMAT JSON"),
            post(name, "DESCRIBE TABLE {db:Identifier}.{tbl:Identifier} FORMAT JSON", %{
              "param_db" => "system",
              "param_tbl" => "tables"
            })
          ] do
        assert response.status == 200, response.resp_body

        described = Map.new(data(response), &{&1["name"], &1["type"]})

        assert %{
                 "database" => "String",
                 "name" => "String",
                 "table" => "String",
                 "is_temporary" => "UInt8",
                 "sorting_key" => "String",
                 "total_rows" => "Nullable(UInt64)",
                 "total_bytes" => "Nullable(UInt64)"
               } = described

        selected = post(name, "SELECT * FROM system.tables LIMIT 0 FORMAT JSON")

        assert Enum.map(JSON.decode!(selected.resp_body)["meta"], & &1["name"]) ==
                 Enum.map(data(response), & &1["name"])
      end

      assert ["database", "table", "name", "type" | _rest] =
               name
               |> post("DESCRIBE system.columns FORMAT JSON")
               |> data()
               |> Enum.map(& &1["name"])

      missing = post(name, "DESCRIBE system.no_such_table FORMAT JSON")
      assert missing.status == 404
      assert missing.resp_body =~ "Table system.no_such_table does not exist"
    end

    test "DESCRIBE with Identifier parameters lists columns and their ClickHouse types", %{
      name: name
    } do
      response =
        post(name, "DESCRIBE {db:Identifier}.{t:Identifier} FORMAT JSON", %{
          "param_db" => "default",
          "param_t" => "otel_logs"
        })

      body = JSON.decode!(response.resp_body)

      assert Enum.map(body["meta"], & &1["name"]) ==
               ~w(name type default_type default_expression comment codec_expression ttl_expression)

      assert Enum.map(body["data"], &{&1["name"], &1["type"], &1["default_type"]}) == [
               {"Timestamp", "DateTime64(9)", ""},
               {"ServiceName", "Nullable(String)", ""},
               {"Body", "Nullable(String)", ""},
               {"LogAttributes", "Map(String, String)", ""},
               {"pod", "Nullable(String)", "MATERIALIZED"}
             ]

      assert List.last(body["data"])["default_expression"] == "LogAttributes['k8s.pod.name']"
    end

    test "system.tables answers the row HyperDX reads its sorting key from", %{name: name} do
      sql =
        "SELECT * FROM system.tables WHERE database = {d:String} AND name = {n:String} LIMIT 1 FORMAT JSON"

      assert [row] = data(post(name, sql, %{"param_d" => "default", "param_n" => "otel_logs"}))

      assert %{
               "engine" => "MergeTree",
               "sorting_key" => "ServiceName, Timestamp",
               "primary_key" => "ServiceName, Timestamp",
               "partition_key" => "",
               "engine_full" => "MergeTree ORDER BY (ServiceName, Timestamp)",
               "total_rows" => nil
             } = row

      assert row["create_table_query"] =~ ~s|"Timestamp" DateTime64(9)|
    end

    test "system.data_skipping_indices is read by its table column, a reserved word to the engine",
         %{name: name} do
      sql =
        "SELECT name, type, type_full as typeFull, expr as expression, granularity " <>
          "FROM system.data_skipping_indices WHERE database = 'default' AND table = 'otel_logs' FORMAT JSON"

      assert data(post(name, sql)) == []
    end

    test "the cloud probe answers, and finds no SharedMergeTree", %{name: name} do
      sql =
        "SELECT count() > 0 AS is_cloud FROM system.table_engines WHERE name = 'SharedMergeTree' FORMAT JSON"

      assert data(post(name, sql)) == [%{"is_cloud" => false}]
    end
  end

  describe "SHOW, EXISTS and DESCRIBE" do
    test "SHOW DATABASES lists every dataset and system", %{name: name} do
      assert post(name, "SHOW DATABASES").resp_body == "default\nempty\nsystem\n"
    end

    test "SHOW TABLES reads FROM, else the request's database", %{name: name} do
      assert post(name, "SHOW TABLES FROM default").resp_body == "otel_logs\n"
      assert post(name, "SHOW TABLES", %{"database" => "default"}).resp_body == "otel_logs\n"
      assert post(name, "SHOW TABLES FROM empty").resp_body == ""
    end

    test "EXISTS answers 1 or 0", %{name: name} do
      assert post(name, "EXISTS TABLE default.otel_logs").resp_body == "1\n"
      assert post(name, "EXISTS default.nothing").resp_body == "0\n"
    end

    test "DESCRIBE TABLE with quoted names, as clickhouse-go writes it", %{name: name} do
      response = post(name, ~s|DESCRIBE TABLE "default"."otel_logs"|)

      assert response.status == 200
      assert [first | _rest] = String.split(response.resp_body, "\n")
      assert String.starts_with?(first, "Timestamp\tDateTime64(9)\t")
    end

    test "DESCRIBE of a table that is not there is code 60", %{name: name} do
      response = post(name, "DESC default.nothing")

      assert response.status == 404
      assert get_resp_header(response, "x-clickhouse-exception-code") == ["60"]
      assert response.resp_body =~ "default.nothing"
    end

    test "a GET may read the catalog", %{name: name} do
      response =
        conn(:get, "/?" <> URI.encode_query(%{"query" => "SHOW DATABASES"}))
        |> put_req_header("x-clickhouse-key", @password)
        |> Router.call(name)

      assert response.status == 200
    end
  end

  describe "which statements are the catalog's" do
    test "a table created after the last read appears once the emulation refreshes",
         %{name: name, catalog: catalog} do
      assert post(name, "EXISTS default.later").resp_body == "0\n"

      :ok = Catalog.create_table(catalog, {"default", "later"}, Schema.new!([{"id", :int64}]))
      Process.sleep(1_100)

      assert post(name, "EXISTS default.later").resp_body == "1\n"
    end

    test "system.columns and system.databases join like tables", %{name: name} do
      sql =
        "SELECT c.name FROM system.columns c JOIN system.tables t ON t.name = c.table " <>
          "WHERE c.is_in_sorting_key = 1 ORDER BY c.position FORMAT JSONCompact"

      assert data(post(name, sql)) == [["Timestamp"], ["ServiceName"]]
    end

    test "a system table that is not emulated is code 60, by its ClickHouse name", %{name: name} do
      response = post(name, "SELECT * FROM system.parts")

      assert response.status == 404
      assert response.resp_body =~ "system.parts"
    end

    test "a statement that reads a user's table beside a system one is the query service's",
         %{name: name} do
      response = post(name, "SELECT * FROM system.tables, default.otel_logs")

      assert get_resp_header(response, "x-clickhouse-exception-code") != []
      refute response.resp_body =~ "system_tables"
    end

    test "the word system in a literal asks nothing of the catalog", %{name: name} do
      assert post(name, "SELECT 'system.tables' AS s").resp_body == "system.tables\n"
    end
  end

  describe "what a catalog statement costs (T-529)" do
    defp edge(catalog, query, opts \\ []) do
      name = :"ch_system_cost_#{:erlang.unique_integer([:positive])}"

      start_supervised!(
        {SmolqueryClickHouse.Supervisor,
         [name: name, password: @password, query_name: query, port: 0, catalog: catalog] ++ opts},
        id: name
      )

      on_exit(fn -> Runtime.delete(name) end)

      name
    end

    defp refreshes(name) do
      test = self()
      handler = "catalog-refresh-#{name}"

      :telemetry.attach(
        handler,
        [:smolquery, :clickhouse, :catalog_refresh],
        fn _event, measurements, meta, nil ->
          if meta.name == name, do: send(test, {:refresh, meta.result, measurements})
        end,
        nil
      )

      on_exit(fn -> :telemetry.detach(handler) end)
    end

    test "a statement over tables the lake does not fill answers without reading the lake",
         %{catalog: catalog, query: query} do
      unreadable = ExitingCatalog.new(catalog, [:schema_version, :list_datasets, :list_tables])
      name = edge(unreadable, query)
      refreshes(name)

      assert data(post(name, "SELECT name, value FROM system.settings FORMAT JSONCompact")) == []

      assert data(
               post(
                 name,
                 "SELECT count() > 0 FROM system.table_engines WHERE name = 'MergeTree' FORMAT JSONCompact"
               )
             ) == [[true]]

      assert data(post(name, "SELECT name FROM system.data_skipping_indices FORMAT JSONCompact")) ==
               []

      assert post(name, "SELECT dummy FROM system.one").resp_body == "0\n"
      assert post(name, "DESCRIBE system.settings").resp_body =~ "name\tString"

      refute_received {:refresh, _result, _measurements}

      assert post(name, "EXISTS default.otel_logs").status == 503
      assert_received {:refresh, :error, _measurements}
    end

    test "a schema version that has not moved is one read, and one that has is a rebuild",
         %{catalog: catalog, query: query} do
      name = edge(catalog, query)
      refreshes(name)

      assert post(name, "EXISTS default.later").resp_body == "0\n"
      assert_received {:refresh, :rebuilt, %{duration_us: _us}}

      assert post(name, "EXISTS default.later").resp_body == "0\n"
      refute_received {:refresh, _result, _measurements}

      Process.sleep(1_100)
      assert post(name, "EXISTS default.later").resp_body == "0\n"
      assert_received {:refresh, :unchanged, _measurements}

      :ok = Catalog.create_table(catalog, {"default", "later"}, Schema.new!([{"id", :int64}]))
      Process.sleep(1_100)

      assert post(name, "EXISTS default.later").resp_body == "1\n"
      assert_received {:refresh, :rebuilt, _measurements}
    end

    test "a clustering key moves no version, and is seen once catalog_rebuild_ms has passed",
         %{catalog: catalog, query: query} do
      sorting_key =
        "SELECT sorting_key FROM system.tables WHERE name = 'otel_logs' FORMAT JSONCompact"

      patient = edge(catalog, query)
      eager = edge(catalog, query, catalog_rebuild_ms: 0)

      assert data(post(patient, sorting_key)) == [["ServiceName, Timestamp"]]
      assert data(post(eager, sorting_key)) == [["ServiceName, Timestamp"]]

      :ok = Catalog.put_clustering(catalog, {"default", "otel_logs"}, ["Timestamp"])
      Process.sleep(1_100)

      assert data(post(patient, sorting_key)) == [["ServiceName, Timestamp"]]
      assert data(post(eager, sorting_key)) == [["Timestamp"]]
    end
  end

  test "column_type/1 is the type a RowBinary insert reads the column as" do
    assert SystemCatalog.column_type(%Field{name: "a", type: :int64, nullable: false}) == "Int64"
    assert SystemCatalog.column_type(%Field{name: "a", type: :bool}) == "Nullable(Bool)"

    assert SystemCatalog.column_type(%Field{name: "a", type: {:numeric, 38, 2}}) ==
             "Nullable(Decimal(38, 2))"

    assert SystemCatalog.column_type(%Field{name: "a", type: :variant}) == "JSON"
    assert SystemCatalog.column_type(%Field{name: "a", type: :date}) == "Nullable(Date32)"
  end

  describe "the catalog's engine answers the catalog and nothing else (review of T-482)" do
    test "a table function beside a system table does not read the host", %{name: name} do
      File.write!(Path.join(System.tmp_dir!(), "smolquery-catalog-probe.txt"), "host-secret")
      path = Path.join(System.tmp_dir!(), "smolquery-catalog-probe.txt")

      for sql <- [
            "SELECT * FROM system.one, read_text('#{path}')",
            "SELECT * FROM system.one WHERE (SELECT count(*) FROM read_text('#{path}')) > 0",
            "SELECT * FROM system.one, read_csv('#{path}')"
          ] do
        response = post(name, sql)

        assert response.status != 200, sql
        refute response.resp_body =~ "host-secret"
      end
    end

    test "the engine itself refuses the file system, whatever reaches it", %{name: name} do
      engine = Runtime.catalog_engine(name)

      assert {:error, error} =
               Smolquery.Engine.query(engine, "SELECT * FROM read_text('/etc/hostname')")

      assert Exception.message(error) =~ ~r/disabled|permission/i
    end

    test "a generator and a recursive query are not the catalog's to run", %{name: name} do
      for sql <- [
            "SELECT count(*) FROM system.columns a, range(100000000000) r",
            "WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM r) SELECT n FROM r, system.one"
          ] do
        response = post(name, sql)

        assert response.status != 200, sql
      end

      assert post(name, "SELECT dummy FROM system.one").resp_body == "0\n"
    end

    test "an answer is capped", %{name: name} do
      sql =
        "SELECT a.name FROM system.columns a, system.columns b, system.columns c, system.columns d, system.columns e, system.columns f, system.columns g"

      rows = post(name, sql).resp_body |> String.split("\n", trim: true)

      assert Enum.count(rows) == 10_000
    end
  end

  test "a function the catalog's engine lacks is code 46, and lands in the unanswered log (review)",
       %{name: name} do
    import ExUnit.CaptureLog

    log =
      capture_log(fn ->
        response = post(name, "SELECT currentDatabase() FROM system.one")

        assert response.status == 404
        assert get_resp_header(response, "x-clickhouse-exception-code") == ["46"]
      end)

    assert log =~ "could not answer: code=46"
  end

  test "EXPLAIN ESTIMATE of a catalog statement is the catalog's, and reads nothing (review of T-506)",
       %{name: name} do
    response = post(name, "EXPLAIN ESTIMATE SELECT name FROM system.tables FORMAT JSONEachRow")

    assert response.status == 200, response.resp_body
    assert %{"rows" => "0", "parts" => "0"} = JSON.decode!(String.trim(response.resp_body))
  end

  describe "a system table named with Identifier parameters (T-507)" do
    test "is the catalog's whether its name arrives quoted, half quoted or bare", %{name: name} do
      for from <- [
            ~s|"system"."tables"|,
            ~s|system."tables"|,
            ~s|"system".tables|,
            "system.tables"
          ] do
        sql = "SELECT name FROM #{from} WHERE database = 'default' FORMAT JSONCompact"

        assert data(post(name, sql)) == [["otel_logs"]], from
      end
    end

    test "the table column, as ClickHouse names it, filters like name", %{name: name} do
      sql =
        "SELECT count() AS n FROM {d:Identifier}.{t:Identifier} " <>
          "WHERE ((table = 'otel_logs' AND database = 'default')) FORMAT JSON"

      response = post(name, sql, %{"param_d" => "system", "param_t" => "tables"})

      assert response.status == 200, response.resp_body
      assert [%{"n" => "1"}] = JSON.decode!(response.resp_body)["data"]
    end

    test "a quoted name that only looks like it is left to the query service", %{name: name} do
      response = post(name, ~s|SELECT * FROM "system"."tables", default.otel_logs|)

      refute response.status == 200
    end
  end

  describe "a count that cannot be had (review of T-507)" do
    test "is a retryable refusal, not a NULL the checklist reads as no data", %{name: name} do
      {:ok, runtime} = Runtime.fetch(name)

      Runtime.put(%{
        runtime
        | query_name: :"no_such_query_service_#{:erlang.unique_integer([:positive])}"
      })

      response =
        post(name, "SELECT sum(total_rows) AS n FROM system.tables WHERE name = 'otel_logs'")

      assert response.status == 503
      assert get_resp_header(response, "retry-after") != []
      assert response.resp_body =~ "default.otel_logs"
    end

    test "a statement that reads total_rows from more tables than it may count says so", %{
      name: name,
      catalog: catalog
    } do
      for i <- 1..33,
          do:
            :ok = Catalog.create_table(catalog, {"empty", "t#{i}"}, Schema.new!([{"id", :int64}]))

      Process.sleep(1_100)

      response =
        post(name, "SELECT sum(total_rows) AS n FROM system.tables WHERE database = 'empty'")

      assert response.status == 400
      assert response.resp_body =~ "at most 32 tables"
      assert response.resp_body =~ "SMOLQUERY_CLICKHOUSE_TOTAL_ROWS_MAX_TABLES"
    end

    test "the counts share one deadline, the statement's own, and running out is code 159 (review of T-513)",
         %{name: name} do
      sql = "SELECT sum(total_rows) AS n FROM system.tables WHERE name = 'otel_logs'"

      assert {:error, {500, 159, "TIMEOUT_EXCEEDED", message, nil}} =
               SystemCatalog.answer(name, sql, "default", timeout_ms: 0)

      assert message =~ "name the tables"
    end

    test "the cap is the runtime's, 256 unless set: HyperDX sums every table with no WHERE (T-513)" do
      assert Runtime.new(name: :cap_default, password: "p").total_rows_max_tables == 256

      assert Runtime.new(name: :cap_set, password: "p", total_rows_max_tables: 1_000).total_rows_max_tables ==
               1_000
    end
  end
end
