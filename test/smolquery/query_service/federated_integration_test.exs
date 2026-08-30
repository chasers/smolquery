defmodule Smolquery.QueryService.FederatedIntegrationTest do
  @moduledoc """
  A federated join, end to end (T-324): rows in a smolquery table, rows in a
  real Postgres, and one query that joins them.

  This is the test that proves the feature rather than its parts. It needs the
  same Postgres the DuckLake suite uses (`Smolquery.Test.Postgres`, which
  creates the database on demand), and it exercises the whole chain — a connection
  registered in the catalog, the planner resolving a catalog-qualified
  reference against it, the runner loading the `postgres` extension and
  running the `ATTACH`, and lockdown still applying afterwards.
  """

  use ExUnit.Case, async: false

  alias Explorer.DataFrame
  alias Smolquery.BufferService
  alias Smolquery.BufferService.HotServer
  alias Smolquery.Catalog
  alias Smolquery.Catalog.Connection
  alias Smolquery.Catalog.DuckLake
  alias Smolquery.Federation
  alias Smolquery.QueryService
  alias Smolquery.QueryService.Client
  alias Smolquery.Schema
  alias Smolquery.Test.Postgres

  @moduletag :integration
  @moduletag :tmp_dir

  @lake __MODULE__.Lake
  @table {"analytics", "events"}
  @remote_table "smolquery_federation_users"

  setup context do
    previous = Application.get_env(:smolquery, :credential_key)
    Application.put_env(:smolquery, :credential_key, Base.encode64(:crypto.strong_rand_bytes(32)))

    on_exit(fn ->
      if previous do
        Application.put_env(:smolquery, :credential_key, previous)
      else
        Application.delete_env(:smolquery, :credential_key)
      end
    end)

    seed_remote!()

    metadata = "sqlite:#{Path.join(context.tmp_dir, "catalog.sqlite")}"
    data_path = Path.join(context.tmp_dir, "data")

    start_supervised!({DuckLake, name: @lake, metadata: metadata, data_path: data_path})

    catalog = DuckLake.new(engine: @lake)
    :ok = Catalog.create_dataset(catalog, "analytics")
    :ok = Catalog.create_table(catalog, @table, schema())
    :ok = Catalog.put_connection(catalog, warehouse())

    buffer = :"fed_buffer_#{:erlang.unique_integer([:positive])}"

    start_supervised!(
      {BufferService.Supervisor,
       name: buffer, dir: Path.join(context.tmp_dir, "buffer"), flush_interval_ms: 25},
      id: buffer
    )

    on_exit(fn -> BufferService.Runtime.delete(buffer) end)

    query = :"fed_query_#{:erlang.unique_integer([:positive])}"

    start_supervised!(
      {QueryService.Supervisor,
       name: query,
       catalog: catalog,
       buffer_base_url: HotServer.base_url(buffer),
       engine_extensions: [:httpfs],
       allowed_directories: [context.tmp_dir],
       job_bootstrap: [
         DuckLake.attach_statement(DuckLake.default_catalog(), metadata, data_path)
       ]},
      id: query
    )

    on_exit(fn -> QueryService.Runtime.delete(query) end)

    %{catalog: catalog, buffer: buffer, query: query}
  end

  test "a federated table answers on its own", %{query: query} do
    assert {:ok, job, frame} =
             Client.query(query, "SELECT count(*) AS n FROM warehouse.public.#{@remote_table}")

    assert job.state == :done
    assert DataFrame.to_columns(frame)["n"] == [3]
  end

  test "a smolquery table joins a federated one", %{buffer: buffer, query: query} do
    rows = [
      %{"id" => 1, "name" => "first"},
      %{"id" => 2, "name" => "second"},
      %{"id" => 9, "name" => "unmatched"}
    ]

    {:ok, _ack} =
      BufferService.Client.write_batch(buffer, @table, %{schema: schema(), rows: rows})

    sql = """
    SELECT e.name AS event_name, u.email AS email
      FROM analytics.events e
      JOIN warehouse.public.#{@remote_table} u ON u.id = e.id
     ORDER BY e.id
    """

    assert {:ok, job, frame} = Client.query(query, sql)

    assert job.state == :done

    columns = DataFrame.to_columns(frame)
    assert columns["event_name"] == ["first", "second"]
    assert columns["email"] == ["one@example.test", "two@example.test"]
  end

  test "the UI's discovery query ranks the remote user tables", %{query: query} do
    sql = Federation.discovery_query("warehouse")

    assert {:ok, job, frame} = Client.query(query, sql)

    assert job.state == :done
    assert @remote_table in DataFrame.to_columns(frame)["relname"]
  end

  test "an unregistered catalog is still refused", %{query: query} do
    assert {:ok, job, nil} = Client.query(query, "SELECT * FROM nosuch.public.users")

    assert job.state == :error
    assert job.error == {:catalog_qualified_reference, "nosuch.users"}
  end

  test "lockdown still denies the filesystem in a federated query", %{query: query} do
    assert {:ok, job, nil} =
             Client.query(
               query,
               "SELECT * FROM warehouse.public.#{@remote_table}, read_csv('/etc/passwd')"
             )

    assert job.state == :error
    assert job.error == {:unsupported_table_function, "read_csv"}
  end

  test "the attachment is read-only", %{query: query} do
    assert {:ok, job, nil} =
             Client.query(query, "INSERT INTO warehouse.public.#{@remote_table} VALUES (4, 'x')")

    assert job.state == :error
  end

  defp schema do
    Schema.new!([{"id", :int64, nullable: false}, {"name", :string}])
  end

  defp warehouse do
    options = Postgres.connection()

    {:ok, connection} =
      Connection.new(%{
        "name" => "warehouse",
        "host" => options[:hostname],
        "port" => options[:port],
        "database" => options[:database],
        "username" => options[:username],
        "password" => options[:password],
        "sslmode" => "disable"
      })

    connection
  end

  defp seed_remote! do
    {:ok, conn} = Postgres.ensure_database!() |> Postgrex.start_link()

    {:ok, _dropped} = Postgrex.query(conn, "DROP TABLE IF EXISTS #{@remote_table}", [])

    {:ok, _created} =
      Postgrex.query(conn, "CREATE TABLE #{@remote_table} (id BIGINT, email TEXT)", [])

    {:ok, _inserted} =
      Postgrex.query(
        conn,
        "INSERT INTO #{@remote_table} VALUES (1, 'one@example.test'), " <>
          "(2, 'two@example.test'), (3, 'three@example.test')",
        []
      )

    GenServer.stop(conn)
  end
end
