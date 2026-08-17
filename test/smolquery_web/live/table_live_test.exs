defmodule SmolqueryWeb.TableLiveTest do
  use SmolqueryWeb.ConnCase, async: false

  alias Smolquery.Catalog
  alias Smolquery.Schema

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

      assert render_async(lv) =~ "query service is not running"
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
    defp seal_event(result, overrides \\ %{}) do
      Map.merge(
        %{
          kind: :seal,
          table_ref: {"analytics", "events"},
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
      seed(runtime)

      {:ok, lv, _html} = live(conn, ~p"/tables/analytics/events")

      html = render_async(lv)
      assert html =~ "Lifecycle"
      assert html =~ "hot tier unreachable"
      assert html =~ "Catalog stats unavailable"
      assert html =~ "Waiting for commits, seals, and compactions"
    end

    test "a lifecycle event lands in the feed and a failure streak shows", %{conn: conn} do
      runtime = start_web!()
      seed(runtime)

      {:ok, lv, _html} = live(conn, ~p"/tables/analytics/events")
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
      seed(runtime)

      {:ok, lv, _html} = live(conn, ~p"/tables/analytics/events")
      render_async(lv)

      send(lv.pid, {:lifecycle, seal_event(:ok)})

      for _flood <- 1..9 do
        send(
          lv.pid,
          {:lifecycle,
           %{
             kind: :commit,
             table_ref: {"analytics", "events"},
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
      seed(runtime)

      {:ok, lv, _html} = live(conn, ~p"/tables/analytics/events")
      render_async(lv)

      :telemetry.execute(
        [:smolquery, :compact, :swap],
        %{replaced: 5, duration_us: 2_000_000},
        %{result: :ok, table_ref: {"analytics", "events"}}
      )

      assert render_eventually(lv, "compaction ok · 5 replaced")
    end

    defp render_eventually(lv, needle, attempts \\ 50)

    defp render_eventually(_lv, _needle, 0), do: false

    defp render_eventually(lv, needle, attempts) do
      if render(lv) =~ needle do
        true
      else
        Process.sleep(20)
        render_eventually(lv, needle, attempts - 1)
      end
    end
  end
end
