defmodule SmolqueryWeb.ConnectionLiveTest do
  use SmolqueryWeb.ConnCase, async: false

  alias Smolquery.Catalog
  alias Smolquery.Catalog.Connection

  setup do
    previous = Application.get_env(:smolquery, :credential_key)
    Application.put_env(:smolquery, :credential_key, Base.encode64(:crypto.strong_rand_bytes(32)))

    on_exit(fn ->
      if previous do
        Application.put_env(:smolquery, :credential_key, previous)
      else
        Application.delete_env(:smolquery, :credential_key)
      end
    end)

    :ok
  end

  defp seed(runtime, overrides \\ %{}) do
    {:ok, connection} =
      Map.merge(
        %{
          "name" => "warehouse",
          "host" => "db.internal",
          "database" => "app",
          "username" => "reader",
          "password" => "hunter2"
        },
        overrides
      )
      |> Connection.new()

    :ok = Catalog.put_connection(runtime.catalog, connection)

    connection
  end

  defp edit_params(overrides), do: overrides |> form_params() |> Map.delete("name")

  defp form_params(overrides \\ %{}) do
    Map.merge(
      %{
        "name" => "warehouse",
        "host" => "db.internal",
        "port" => "5432",
        "database" => "app",
        "username" => "reader",
        "password" => "hunter2",
        "sslmode" => "require"
      },
      overrides
    )
  end

  describe "index" do
    test "says so when there are none", %{conn: conn} do
      start_web!()

      {:ok, _lv, html} = live(conn, ~p"/connections")

      assert html =~ "No connections yet"
    end

    test "lists a registered connection without its password", %{conn: conn} do
      runtime = start_web!()
      seed(runtime)

      {:ok, _lv, html} = live(conn, ~p"/connections")

      assert html =~ "warehouse"
      assert html =~ "db.internal"
      assert html =~ "reader"
      refute html =~ "hunter2"
    end

    test "the page is reachable from the navbar", %{conn: conn} do
      start_web!()

      {:ok, _lv, html} = live(conn, ~p"/tables")

      assert html =~ ~s|href="/connections"|
    end

    test "warns when the node holds no credential key", %{conn: conn} do
      Application.delete_env(:smolquery, :credential_key)
      start_web!()

      {:ok, _lv, html} = live(conn, ~p"/connections")

      assert html =~ "SMOLQUERY_CREDENTIAL_KEY"
    end
  end

  describe "registering" do
    test "creates a connection and lists it", %{conn: conn} do
      runtime = start_web!()

      {:ok, lv, _html} = live(conn, ~p"/connections")

      html =
        lv
        |> form("#connection-form", connection: form_params())
        |> render_submit()

      assert html =~ "Connection warehouse saved"
      assert {:ok, [stored]} = Catalog.list_connections(runtime.catalog)
      assert stored.name == "warehouse"
      assert stored.host == "db.internal"
    end

    test "the submitted password never renders back", %{conn: conn} do
      start_web!()

      {:ok, lv, _html} = live(conn, ~p"/connections")

      html =
        lv
        |> form("#connection-form", connection: form_params())
        |> render_submit()

      refute html =~ "hunter2"
    end

    test "a name that is not an identifier is refused with a reason", %{conn: conn} do
      start_web!()

      {:ok, lv, _html} = live(conn, ~p"/connections")

      html =
        lv
        |> form("#connection-form", connection: form_params(%{"name" => "bad name"}))
        |> render_submit()

      assert html =~ "not a valid name"
    end

    test "a missing field names itself", %{conn: conn} do
      start_web!()

      {:ok, lv, _html} = live(conn, ~p"/connections")

      html =
        lv
        |> form("#connection-form", connection: form_params(%{"host" => ""}))
        |> render_submit()

      assert html =~ "host is required"
    end
  end

  describe "editing" do
    test "loads the connection into the form with an empty password", %{conn: conn} do
      runtime = start_web!()
      seed(runtime)

      {:ok, lv, _html} = live(conn, ~p"/connections")

      html = lv |> element("button[phx-click=edit][phx-value-name=warehouse]") |> render_click()

      assert html =~ "Edit warehouse"
      assert html =~ "blank keeps the stored one"
      refute html =~ "hunter2"
    end

    test "a blank password keeps the stored one", %{conn: conn} do
      runtime = start_web!()
      original = seed(runtime)

      {:ok, lv, _html} = live(conn, ~p"/connections")

      lv |> element("button[phx-click=edit][phx-value-name=warehouse]") |> render_click()

      lv
      |> form("#connection-form",
        connection: edit_params(%{"host" => "db2.internal", "password" => ""})
      )
      |> render_submit()

      assert {:ok, stored} = Catalog.connection(runtime.catalog, "warehouse")
      assert stored.host == "db2.internal"
      assert stored.secret == original.secret
    end

    test "a typed password replaces the stored one", %{conn: conn} do
      runtime = start_web!()
      original = seed(runtime)

      {:ok, lv, _html} = live(conn, ~p"/connections")

      lv |> element("button[phx-click=edit][phx-value-name=warehouse]") |> render_click()

      lv
      |> form("#connection-form", connection: edit_params(%{"password" => "correcthorse"}))
      |> render_submit()

      assert {:ok, stored} = Catalog.connection(runtime.catalog, "warehouse")
      refute stored.secret == original.secret
      assert {:ok, string} = Connection.connection_string(stored)
      assert string =~ "password=correcthorse"
    end

    test "cancel returns to the new-connection form", %{conn: conn} do
      runtime = start_web!()
      seed(runtime)

      {:ok, lv, _html} = live(conn, ~p"/connections")

      lv |> element("button[phx-click=edit][phx-value-name=warehouse]") |> render_click()
      html = lv |> element("button[phx-click=cancel]") |> render_click()

      assert html =~ "New connection"
    end
  end

  describe "removing" do
    test "removes the connection", %{conn: conn} do
      runtime = start_web!()
      seed(runtime)

      {:ok, lv, _html} = live(conn, ~p"/connections")

      html = lv |> element("button[phx-click=delete][phx-value-name=warehouse]") |> render_click()

      assert html =~ "Connection warehouse removed"
      assert {:ok, []} = Catalog.list_connections(runtime.catalog)
    end
  end

  describe "testing" do
    @tag :integration
    test "an unreachable database reports a failure without the password", %{conn: conn} do
      runtime = start_web!()
      seed(runtime, %{"host" => "127.0.0.1", "port" => 1, "password" => "sup3rsecret"})

      {:ok, lv, _html} = live(conn, ~p"/connections")

      html = lv |> element("button[phx-click=test][phx-value-name=warehouse]") |> render_click()

      assert html =~ "Could not open connection warehouse"
      refute html =~ "sup3rsecret"
    end
  end
end
