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

    test "a broken query surfaces the job error", %{conn: conn, query: query} do
      start_web!(query_name: query)

      {:ok, lv, _html} = live(conn, ~p"/query")

      render_submit(lv, "run", %{"query" => %{"sql" => "SELECT FROM WHERE"}})

      assert Eventually.until(fn -> render(lv) =~ "badge-error" end)
    end
  end
end
