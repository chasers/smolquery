defmodule Smolquery.QueryService.FederatedDucklakeIntegrationTest do
  @moduledoc """
  A federated DuckLake, end to end (T-610): another lake, its catalog in its
  own Postgres database and its files in their own directory, registered as a
  connection and queried through smolquery, alone and joined with a
  smolquery table.

  The remote lake's files sit outside the query engine's
  `allowed_directories`, and the engine still reads them after lockdown:
  DuckLake reads an attached lake's files itself, which the lockdown does not
  restrict, so a DuckLake connection needs no widening of it. That is also
  the trust boundary `Smolquery.Federation` documents.
  """

  use ExUnit.Case, async: false

  alias Explorer.DataFrame
  alias Smolquery.BufferService
  alias Smolquery.BufferService.HotServer
  alias Smolquery.Catalog
  alias Smolquery.Catalog.Connection
  alias Smolquery.Catalog.DuckLake
  alias Smolquery.Engine
  alias Smolquery.Federation
  alias Smolquery.QueryService
  alias Smolquery.QueryService.Client
  alias Smolquery.Schema
  alias Smolquery.Test.Postgres

  @moduletag :integration
  @moduletag :tmp_dir

  @lake __MODULE__.Lake
  @remote_database "smolquery_remote_lake"
  @table {"analytics", "events"}

  setup context do
    previous_key = Application.get_env(:smolquery, :credential_key)
    previous_federation = Application.get_env(:smolquery, Federation)
    Application.put_env(:smolquery, :credential_key, Base.encode64(:crypto.strong_rand_bytes(32)))

    remote_dir =
      Path.join(System.tmp_dir!(), "smolquery_remote_lake_#{System.unique_integer([:positive])}")

    Application.put_env(:smolquery, Federation, local_roots: [remote_dir])

    on_exit(fn ->
      restore(:credential_key, previous_key)
      restore(Federation, previous_federation)
      File.rm_rf!(remote_dir)
    end)

    data_path = Path.join(remote_dir, "data") <> "/"
    seed_remote_lake!(data_path)

    metadata = "sqlite:#{Path.join(context.tmp_dir, "catalog.sqlite")}"
    local_data = Path.join(context.tmp_dir, "data")
    start_supervised!({DuckLake, name: @lake, metadata: metadata, data_path: local_data})

    catalog = DuckLake.new(engine: @lake)
    :ok = Catalog.create_dataset(catalog, "analytics")
    :ok = Catalog.create_table(catalog, @table, Schema.new!([{"id", :int64}, {"name", :string}]))
    connection = remote(data_path)
    :ok = Catalog.put_connection(catalog, connection)

    buffer = :"fed_lake_buffer_#{:erlang.unique_integer([:positive])}"

    start_supervised!(
      {BufferService.Supervisor,
       name: buffer, dir: Path.join(context.tmp_dir, "buffer"), flush_interval_ms: 25},
      id: buffer
    )

    on_exit(fn -> BufferService.Runtime.delete(buffer) end)

    query = :"fed_lake_query_#{:erlang.unique_integer([:positive])}"

    start_supervised!(
      {QueryService.Supervisor,
       name: query,
       catalog: catalog,
       buffer_base_url: HotServer.base_url(buffer),
       engine_extensions: [:httpfs],
       allowed_directories: [context.tmp_dir],
       job_bootstrap: [
         DuckLake.attach_statement(DuckLake.default_catalog(), metadata, local_data)
       ]},
      id: query
    )

    on_exit(fn -> QueryService.Runtime.delete(query) end)

    %{buffer: buffer, query: query, connection: connection, data_path: data_path}
  end

  test "a federated DuckLake answers under lockdown, its files outside allowed directories",
       %{query: query} do
    assert {:ok, job, frame} =
             Client.query(
               query,
               "SELECT count(*) AS n, sum(amount) AS total FROM remote.sales.orders"
             )

    assert job.state == :done
    assert DataFrame.to_columns(frame)["n"] == [5]
  end

  test "a smolquery table joins a federated DuckLake one", %{buffer: buffer, query: query} do
    rows = [%{"id" => 1, "name" => "first"}, %{"id" => 3, "name" => "third"}]
    schema = Schema.new!([{"id", :int64}, {"name", :string}])
    {:ok, _ack} = BufferService.Client.write_batch(buffer, @table, %{schema: schema, rows: rows})

    sql = """
    SELECT e.name, o.amount
      FROM analytics.events e
      JOIN remote.sales.orders o ON o.id = e.id
     ORDER BY e.id
    """

    assert {:ok, job, frame} = Client.query(query, sql)
    assert job.state == :done
    assert DataFrame.to_columns(frame)["name"] == ["first", "third"]
  end

  test "the attachment is read-only", %{query: query} do
    assert {:ok, job, nil} =
             Client.query(query, "INSERT INTO remote.sales.orders VALUES (9, 1.0)")

    assert job.state == :error
  end

  test "probe/1 reads through the lake, and tables/1 lists it", %{connection: connection} do
    assert Federation.probe(connection) == :ok
    assert Federation.tables(connection) == {:ok, [{"sales", "orders"}]}
  end

  test "a data path the lake does not live at fails the probe, and names only the connection",
       %{connection: connection, data_path: data_path} do
    elsewhere = String.replace(data_path, "/data/", "/elsewhere/")
    File.mkdir_p!(elsewhere)

    assert {:ok, moved} = Connection.update(connection, %{"data_path" => elsewhere})
    assert {:error, {:federation_error, "remote", reason}} = Federation.probe(moved)
    refute reason =~ "password=postgres"
  end

  defp remote(data_path) do
    options = Postgres.connection()

    {:ok, connection} =
      Connection.new(%{
        "name" => "remote",
        "kind" => "ducklake",
        "host" => options[:hostname],
        "port" => options[:port],
        "database" => @remote_database,
        "username" => options[:username],
        "password" => options[:password],
        "sslmode" => "disable",
        "data_path" => data_path
      })

    connection
  end

  defp seed_remote_lake!(data_path) do
    options = Postgres.connection()
    {:ok, admin} = Postgrex.start_link(Keyword.put(options, :database, "postgres"))
    Postgrex.query!(admin, ~s|DROP DATABASE IF EXISTS "#{@remote_database}" WITH (FORCE)|, [])
    Postgrex.query!(admin, ~s|CREATE DATABASE "#{@remote_database}"|, [])
    GenServer.stop(admin)

    owner = __MODULE__.Owner

    metadata =
      "ducklake:postgres:dbname=#{@remote_database} host=#{options[:hostname]} " <>
        "port=#{options[:port]} user=#{options[:username]} password=#{options[:password]}"

    start_supervised!(
      {Engine,
       name: owner,
       extensions: [:postgres, :ducklake],
       statements: ["ATTACH '#{metadata}' AS owner (DATA_PATH '#{data_path}')"]},
      id: owner
    )

    Engine.query!(owner, "CREATE SCHEMA owner.sales")

    Engine.query!(
      owner,
      "CREATE TABLE owner.sales.orders AS SELECT range AS id, range * 1.5 AS amount FROM range(5)"
    )

    stop_supervised!(owner)
  end

  defp restore(key, nil), do: Application.delete_env(:smolquery, key)
  defp restore(key, value), do: Application.put_env(:smolquery, key, value)
end
