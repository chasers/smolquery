defmodule SmolqueryWeb.ClusterLiveTest do
  use SmolqueryWeb.ConnCase, async: false

  alias Smolquery.Cluster.Pods

  describe "index" do
    test "renders this node as the only, alive row", %{conn: conn} do
      start_web!()

      {:ok, _lv, html} = live(conn, ~p"/cluster")

      assert html =~ "Cluster"
      assert html =~ to_string(node())
      assert html =~ "up"
    end

    test "renders the node name in a truncating cell with the full name as a title", %{
      conn: conn
    } do
      start_web!()

      {:ok, _lv, html} = live(conn, ~p"/cluster")

      assert html =~ ~s|class="block max-w-56 truncate" title="#{node()}">#{node()}</span>|
    end

    test "lists every role this node runs in one Roles column", %{conn: conn} do
      start_web!()

      {:ok, _lv, html} = live(conn, ~p"/cluster")

      assert html =~ "<th>Roles</th>"
      refute html =~ "<th>Buffer</th>"
      refute html =~ "<th>Storage</th>"

      for role <- Smolquery.Roles.enabled() do
        assert html =~ ~s|badge-outline badge-sm mr-1">#{role}</span>|
      end
    end

    test "disables the kill and restart buttons without a kind cluster", %{conn: conn} do
      start_web!()

      {:ok, lv, html} = live(conn, ~p"/cluster")

      assert html =~ "No local kind cluster detected"
      assert has_element?(lv, "button[disabled]", "Kill")
      assert has_element?(lv, "button[disabled]", "Restart")
    end

    test "kill is a no-op with a flash when no kind cluster is available", %{conn: conn} do
      start_web!()

      {:ok, lv, _html} = live(conn, ~p"/cluster")

      html = render_click(lv, "kill", %{"pod" => Pods.pod_of_node(node())})

      assert html =~ "No kind cluster detected"
    end

    test "restart is a no-op with a flash when no kind cluster is available", %{conn: conn} do
      start_web!()

      {:ok, lv, _html} = live(conn, ~p"/cluster")

      html = render_click(lv, "restart", %{"pod" => Pods.pod_of_node(node())})

      assert html =~ "No kind cluster detected"
    end

    test "kill of a pod outside the fleet flashes instead of deleting it", %{conn: conn} do
      start_web!()

      {:ok, lv, _html} = live(conn, ~p"/cluster")

      html = render_click(lv, "kill", %{"pod" => "smolquery-postgres-0"})

      assert html =~ "smolquery-postgres-0 is not part of the fleet"
    end

    test "drain of an unknown node flashes instead of crashing", %{conn: conn} do
      start_web!()

      {:ok, lv, _html} = live(conn, ~p"/cluster")

      html = render_click(lv, "drain", %{"node" => "ghost@nowhere"})

      assert html =~ "ghost@nowhere is not part of the fleet"
    end

    test "no drain button for a node that isn't a buffer member", %{conn: conn} do
      start_web!()

      {:ok, lv, _html} = live(conn, ~p"/cluster")

      refute has_element?(lv, "button", "Drain")
    end

    test "drain runs async over :rpc and flashes the result", %{conn: conn} do
      start_web!()

      {:ok, lv, _html} = live(conn, ~p"/cluster")

      render_click(lv, "drain", %{"node" => to_string(node())})
      html = render_async(lv)

      assert html =~ "buffer_service_unavailable"
    end
  end
end
