defmodule SmolqueryWeb.QueryLiveTest do
  use SmolqueryWeb.ConnCase, async: false

  alias Smolquery.QueryService
  alias Smolquery.Test.Eventually
  alias Smolquery.Test.FixedCatalog

  describe "without a query service" do
    test "renders the editor", %{conn: conn} do
      start_web!()

      {:ok, _lv, html} = live(conn, ~p"/query")

      assert html =~ "Query"
      assert html =~ "SELECT"
    end

    test "run reports the service as unavailable", %{conn: conn} do
      start_web!()

      {:ok, lv, _html} = live(conn, ~p"/query")

      html = render_submit(lv, "run", %{"query" => %{"sql" => "SELECT 1"}})

      assert html =~ "query service is not running"
    end

    test "run rejects blank SQL", %{conn: conn} do
      start_web!()

      {:ok, lv, _html} = live(conn, ~p"/query")

      html = render_submit(lv, "run", %{"query" => %{"sql" => "   "}})

      assert html =~ "Write some SQL first"
    end
  end

  describe "with a query service" do
    setup do
      query = :"web_query_#{:erlang.unique_integer([:positive])}"

      start_supervised!(
        {QueryService.Supervisor,
         name: query, catalog: FixedCatalog.new(%{snapshot: 1, schemas: %{}, segments: %{}})},
        id: query
      )

      on_exit(fn -> QueryService.Runtime.delete(query) end)

      %{query: query}
    end

    test "runs a query to done and renders the rows", %{conn: conn, query: query} do
      start_web!(query_name: query)

      {:ok, lv, _html} = live(conn, ~p"/query")

      render_submit(lv, "run", %{"query" => %{"sql" => "SELECT 42 AS answer"}})

      assert Eventually.until(fn -> render(lv) =~ "done" end)

      html = render(lv)
      assert html =~ "answer"
      assert html =~ "42"
    end

    test "cancel is safe once the job is already terminal", %{conn: conn, query: query} do
      start_web!(query_name: query)

      {:ok, lv, _html} = live(conn, ~p"/query")

      render_submit(lv, "run", %{"query" => %{"sql" => "SELECT 1"}})

      assert Eventually.until(fn -> render(lv) =~ "done" end)

      assert render_click(lv, "cancel") =~ "done"
    end

    test "a run renders the scan statistics line", %{conn: conn, query: query} do
      start_web!(query_name: query)

      {:ok, lv, _html} = live(conn, ~p"/query")

      render_submit(lv, "run", %{"query" => %{"sql" => "SELECT 1 AS n"}})

      assert Eventually.until(fn -> render(lv) =~ "done" end)

      assert render(lv) =~ "0/0 files"
    end

    test "explain renders the engine's plan instead of rows", %{conn: conn, query: query} do
      start_web!(query_name: query)

      {:ok, lv, _html} = live(conn, ~p"/query")

      render_change(lv, "sql_changed", %{"query" => %{"sql" => "SELECT 1 AS n"}})
      render_click(lv, "explain", %{"mode" => "plan"})

      assert Eventually.until(fn -> render(lv) =~ "done" end)

      html = render(lv)
      assert html =~ "PROJECTION"
      refute html =~ "data_table"
    end

    test "explain rejects blank SQL", %{conn: conn, query: query} do
      start_web!(query_name: query)

      {:ok, lv, _html} = live(conn, ~p"/query")

      assert render_click(lv, "explain", %{"mode" => "plan"}) =~ "Write some SQL first"
    end

    test "the trace toggle renders a waterfall", %{conn: conn, query: query} do
      start_web!(query_name: query)

      {:ok, lv, _html} = live(conn, ~p"/query")

      render_submit(lv, "run", %{"query" => %{"sql" => "SELECT 1 AS n", "trace" => "true"}})

      assert Eventually.until(fn -> render(lv) =~ "done" end)

      html = render(lv)
      assert html =~ "Trace"
      assert html =~ "engine_start"
      assert html =~ "execute"
    end

    test "without the toggle no waterfall renders", %{conn: conn, query: query} do
      start_web!(query_name: query)

      {:ok, lv, _html} = live(conn, ~p"/query")

      render_submit(lv, "run", %{"query" => %{"sql" => "SELECT 1 AS n"}})

      assert Eventually.until(fn -> render(lv) =~ "done" end)

      refute render(lv) =~ "engine_start"
    end

    test "a broken query surfaces the job error", %{conn: conn, query: query} do
      start_web!(query_name: query)

      {:ok, lv, _html} = live(conn, ~p"/query")

      render_submit(lv, "run", %{"query" => %{"sql" => "SELECT FROM WHERE"}})

      assert Eventually.until(fn -> render(lv) =~ "badge-error" end)
    end
  end
end
