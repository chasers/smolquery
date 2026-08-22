defmodule SmolqueryWeb.TableLiveTest do
  use SmolqueryWeb.ConnCase, async: false

  alias Smolquery.Catalog
  alias Smolquery.Schema
  alias Smolquery.Test.Eventually

  defp seed(runtime) do
    :ok = Catalog.create_dataset(runtime.catalog, "analytics")

    schema =
      Schema.new!([
        {"id", :int64, nullable: false},
        {"ts", :timestamp},
        {"name", :string}
      ])

    :ok = Catalog.create_table(runtime.catalog, {"analytics", "events"}, schema)

    schema
  end

  describe "index" do
    test "lists datasets with their tables", %{conn: conn} do
      runtime = start_web!()
      seed(runtime)

      {:ok, _lv, html} = live(conn, ~p"/tables")

      assert html =~ "analytics"
      assert html =~ "events"
    end

    test "also serves the root path", %{conn: conn} do
      start_web!()

      {:ok, _lv, html} = live(conn, ~p"/")

      assert html =~ "No datasets yet"
    end

    test "creates a dataset", %{conn: conn} do
      runtime = start_web!()

      {:ok, lv, _html} = live(conn, ~p"/tables")

      html = render_submit(lv, "create_dataset", %{"dataset" => %{"name" => "fresh"}})

      assert html =~ "fresh"
      assert Catalog.list_datasets(runtime.catalog) == {:ok, ["fresh"]}
    end

    test "rejects a blank dataset name", %{conn: conn} do
      start_web!()

      {:ok, lv, _html} = live(conn, ~p"/tables")

      html = render_submit(lv, "create_dataset", %{"dataset" => %{"name" => "  "}})

      assert html =~ "Dataset name is required"
    end

    test "creates a table and navigates to it", %{conn: conn} do
      runtime = start_web!()
      seed(runtime)

      {:ok, lv, _html} = live(conn, ~p"/tables")

      render_submit(lv, "create_table", %{
        "table" => %{
          "dataset" => "analytics",
          "name" => "clicks",
          "fields" => %{
            "0" => %{"name" => "id", "type" => "INT64", "nullable" => "false"},
            "1" => %{"name" => "ts", "type" => "TIMESTAMP", "nullable" => "true"}
          }
        }
      })

      assert_redirect(lv, "/tables/analytics/clicks")

      assert {:ok, schema} = Catalog.table_schema(runtime.catalog, {"analytics", "clicks"})
      assert Schema.names(schema) == ["id", "ts"]
    end

    test "a table needs at least one named field", %{conn: conn} do
      runtime = start_web!()
      seed(runtime)

      {:ok, lv, _html} = live(conn, ~p"/tables")

      html =
        render_submit(lv, "create_table", %{
          "table" => %{
            "dataset" => "analytics",
            "name" => "clicks",
            "fields" => %{"0" => %{"name" => "  ", "type" => "INT64", "nullable" => "true"}}
          }
        })

      assert html =~ "at least one field"
    end

    test "adds and removes schema field rows", %{conn: conn} do
      start_web!()

      {:ok, lv, _html} = live(conn, ~p"/tables")

      html = render_click(lv, "add_field")
      assert html =~ "table[fields][1][name]"

      html = render_click(lv, "remove_field", %{"index" => "0"})
      refute html =~ "table[fields][0][name]"
    end
  end

  describe "show" do
    test "renders the schema and an absent retention policy", %{conn: conn} do
      runtime = start_web!()
      seed(runtime)

      {:ok, lv, html} = live(conn, ~p"/tables/analytics/events")

      assert html =~ "analytics.events"
      assert html =~ "INT64"
      assert html =~ "TIMESTAMP"
      assert html =~ "No retention policy"
      assert html =~ "No clustering key"

      assert render_async(lv) =~ "query service is not running"
    end

    test "renders the clustering key", %{conn: conn} do
      runtime = start_web!()
      seed(runtime)
      :ok = Catalog.put_clustering(runtime.catalog, {"analytics", "events"}, ["name", "ts"])

      {:ok, _lv, html} = live(conn, ~p"/tables/analytics/events")

      assert html =~ "sort writes by"
      assert html =~ ~r/name.*ts/s
      refute html =~ "No clustering key"
    end

    test "saves and clears clustering", %{conn: conn} do
      runtime = start_web!()
      seed(runtime)

      {:ok, lv, _html} = live(conn, ~p"/tables/analytics/events")

      html =
        lv
        |> form("#clustering", clustering: %{columns: "ts, id"})
        |> render_submit()

      assert html =~ "Clustering saved"
      assert html =~ ~r/ts.*id/s
      assert {:ok, ["ts", "id"]} = Catalog.clustering(runtime.catalog, {"analytics", "events"})

      html = lv |> element("button", "Clear") |> render_click()

      assert html =~ "Clustering cleared"
      assert html =~ "No clustering key"
      assert {:ok, []} = Catalog.clustering(runtime.catalog, {"analytics", "events"})
    end

    test "rejects an unknown or repeated clustering column", %{conn: conn} do
      runtime = start_web!()
      seed(runtime)

      {:ok, lv, _html} = live(conn, ~p"/tables/analytics/events")

      html =
        lv
        |> form("#clustering", clustering: %{columns: "ts, nope"})
        |> render_submit()

      assert html =~ "unknown_clustering_column"
      assert html =~ "No clustering key"

      html =
        lv
        |> form("#clustering", clustering: %{columns: "ts, ts"})
        |> render_submit()

      assert html =~ "repeated_clustering_column"
      assert {:ok, []} = Catalog.clustering(runtime.catalog, {"analytics", "events"})
    end

    test "saves and clears retention", %{conn: conn} do
      runtime = start_web!()
      seed(runtime)

      {:ok, lv, _html} = live(conn, ~p"/tables/analytics/events")

      html =
        render_submit(lv, "save_retention", %{
          "retention" => %{"column" => "ts", "ttl_ms" => "86400000"}
        })

      assert html =~ "older than 86400000 ms"

      assert Catalog.retention(runtime.catalog, {"analytics", "events"}) ==
               {:ok, %{column: "ts", ttl_ms: 86_400_000}}

      html = render_click(lv, "clear_retention")

      assert html =~ "No retention policy"
      assert Catalog.retention(runtime.catalog, {"analytics", "events"}) == {:ok, nil}
    end

    test "rejects an unparseable ttl", %{conn: conn} do
      runtime = start_web!()
      seed(runtime)

      {:ok, lv, _html} = live(conn, ~p"/tables/analytics/events")

      html =
        render_submit(lv, "save_retention", %{
          "retention" => %{"column" => "ts", "ttl_ms" => "soon"}
        })

      assert html =~ "Could not save retention"
    end

    test "an unknown table redirects back to the listing", %{conn: conn} do
      start_web!()

      assert {:error, {:live_redirect, %{to: "/tables"}}} =
               live(conn, ~p"/tables/nope/missing")
    end
  end

  describe "lifecycle (T-295)" do
    defp seed_lifeview(runtime) do
      :ok = Catalog.create_dataset(runtime.catalog, "lifeview")

      :ok =
        Catalog.create_table(
          runtime.catalog,
          {"lifeview", "events"},
          Schema.new!([{"id", :int64, nullable: false}])
        )
    end

    defp seal_event(result, overrides \\ %{}) do
      Map.merge(
        %{
          kind: :seal,
          table_ref: {"lifeview", "events"},
          node: node(),
          result: result,
          measurements: %{duration_us: 1_200_000, segments: 16},
          at: System.system_time(:millisecond)
        },
        overrides
      )
    end

    test "renders the hot and sealed tiers on load", %{conn: conn} do
      runtime = start_web!()
      seed_lifeview(runtime)

      {:ok, lv, _html} = live(conn, ~p"/tables/lifeview/events")

      html = render_async(lv)
      assert html =~ "Lifecycle"
      assert html =~ "hot tier unreachable"
      assert html =~ "Catalog stats unavailable"
      assert html =~ "Waiting for commits, seals, and compactions"
    end

    test "a lifecycle event lands in the feed and a failure streak shows", %{conn: conn} do
      runtime = start_web!()
      seed_lifeview(runtime)

      {:ok, lv, _html} = live(conn, ~p"/tables/lifeview/events")
      render_async(lv)

      send(lv.pid, {:lifecycle, seal_event(:ok)})
      html = render(lv)
      assert html =~ "seal ok · 16 segments"

      send(lv.pid, {:lifecycle, seal_event(:error)})
      send(lv.pid, {:lifecycle, seal_event(:error)})
      html = render(lv)
      assert html =~ "2 failed seals"

      send(lv.pid, {:lifecycle, seal_event(:ok)})
      refute render(lv) =~ "failed seals"
    end

    test "the last seal stays pinned after commits push it out of the feed", %{conn: conn} do
      runtime = start_web!()
      seed_lifeview(runtime)

      {:ok, lv, _html} = live(conn, ~p"/tables/lifeview/events")
      render_async(lv)

      send(lv.pid, {:lifecycle, seal_event(:ok)})

      for _flood <- 1..9 do
        send(
          lv.pid,
          {:lifecycle,
           %{
             kind: :commit,
             table_ref: {"lifeview", "events"},
             node: node(),
             result: :ok,
             measurements: %{rows: 200, bytes: 11_700},
             at: System.system_time(:millisecond)
           }}
        )
      end

      html = render(lv)
      assert html =~ "seal ok · 16 segments"
      assert html =~ "last"
    end

    test "a broadcast through the bridge reaches the page", %{conn: conn} do
      runtime = start_web!()
      seed_lifeview(runtime)

      {:ok, lv, _html} = live(conn, ~p"/tables/lifeview/events")
      render_async(lv)

      :telemetry.execute(
        [:smolquery, :compact, :swap],
        %{replaced: 5, duration_us: 2_000_000},
        %{result: :ok, table_ref: {"lifeview", "events"}}
      )

      assert Eventually.until(fn ->
               render(lv) =~ "compaction ok · 5 replaced"
             end)
    end
  end
end
