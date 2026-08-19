defmodule Smolquery.Catalog.DuckLakeConnectionsTest do
  @moduledoc """
  Federated connections stored in a real DuckLake catalog (T-322).

  Tagged `:integration` for the same reason `Smolquery.Catalog.DuckLakeTest`
  is: it downloads the `ducklake` extension and writes a catalog database to
  disk. What it pins is that the side table survives a round trip through
  DuckDB's own types, that a password never reaches the metadata database in
  the clear, and that a name collision replaces rather than duplicates.

  `Connectionless` is defined here rather than reached for in `test/support`
  precisely because the four callbacks are optional: a shared double is free
  to grow them later, and a test asserting the *absence* of an optional
  callback must not depend on that. One that did broke the moment
  `Smolquery.Test.FixedCatalog` gained connection reads.
  """

  defmodule Connectionless do
    @moduledoc false

    @behaviour Smolquery.Catalog

    def create_dataset(_config, _dataset), do: {:error, :connectionless}
    def list_datasets(_config), do: {:error, :connectionless}
    def create_table(_config, _table, _schema), do: {:error, :connectionless}
    def list_tables(_config, _dataset), do: {:error, :connectionless}
    def table_schema(_config, _table), do: {:error, :connectionless}
    def register_segments(_config, _table, _segments), do: {:error, :connectionless}
    def segments(_config, _table, _snapshot), do: {:error, :connectionless}
    def registered_through(_config, _table, _snapshot), do: {:error, :connectionless}
    def segment_stats(_config, _table, _snapshot), do: {:error, :connectionless}
    def segment_files(_config, _table, _snapshot), do: {:error, :connectionless}
    def drop_segments(_config, _table, _paths), do: {:error, :connectionless}
    def replace_segments(_config, _table, _segments, _paths), do: {:error, :connectionless}
    def current_snapshot(_config), do: {:error, :connectionless}
    def known_segments(_config), do: {:error, :connectionless}
    def put_retention(_config, _table, _policy), do: {:error, :connectionless}
    def retention(_config, _table), do: {:error, :connectionless}
    def put_clustering(_config, _table, _columns), do: {:error, :connectionless}
    def clustering(_config, _table), do: {:error, :connectionless}
    def put_partitions(_config, _table, _count), do: {:error, :connectionless}
    def partitions(_config, _table), do: {:error, :connectionless}
    def expire_snapshots(_config, _older_than_ms), do: {:error, :connectionless}
  end

  use ExUnit.Case, async: false

  alias Smolquery.Catalog
  alias Smolquery.Catalog.Connection
  alias Smolquery.Catalog.DuckLake
  alias Smolquery.Engine
  alias Smolquery.Identifier

  @moduletag :integration
  @moduletag :tmp_dir

  @engine __MODULE__.Lake

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

    start_supervised!(
      {DuckLake,
       name: @engine,
       metadata: "sqlite:#{Path.join(context.tmp_dir, "catalog.sqlite")}",
       data_path: Path.join(context.tmp_dir, "data")}
    )

    %{catalog: DuckLake.new(engine: @engine)}
  end

  defp connection(overrides \\ %{}) do
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

    connection
  end

  test "a connection round-trips through the catalog", %{catalog: catalog} do
    original = connection(%{"port" => 6543, "sslmode" => "verify-full"})

    assert :ok = Catalog.put_connection(catalog, original)
    assert {:ok, stored} = Catalog.connection(catalog, "warehouse")

    assert stored.name == "warehouse"
    assert stored.host == "db.internal"
    assert stored.port == 6543
    assert stored.database == "app"
    assert stored.username == "reader"
    assert stored.sslmode == "verify-full"
    assert stored.secret == original.secret
    assert is_integer(stored.created_at)
    assert is_integer(stored.updated_at)
  end

  test "the stored password opens back to the plaintext", %{catalog: catalog} do
    :ok = Catalog.put_connection(catalog, connection())

    {:ok, stored} = Catalog.connection(catalog, "warehouse")

    assert {:ok, string} = Connection.connection_string(stored)
    assert string =~ "password=hunter2"
  end

  test "the metadata database holds no plaintext password", %{catalog: catalog} do
    :ok = Catalog.put_connection(catalog, connection())

    schema = Identifier.quote_name!("__ducklake_metadata_" <> DuckLake.default_catalog())

    {:ok, result} = Engine.query(@engine, "SELECT * FROM #{schema}.smolquery_connections")

    refute inspect(result.rows) =~ "hunter2"
  end

  test "an unknown connection names itself", %{catalog: catalog} do
    assert Catalog.connection(catalog, "nope") == {:error, {:unknown_connection, "nope"}}
  end

  test "list_connections/1 orders by name and starts empty", %{catalog: catalog} do
    assert {:ok, []} = Catalog.list_connections(catalog)

    :ok = Catalog.put_connection(catalog, connection(%{"name" => "zeta"}))
    :ok = Catalog.put_connection(catalog, connection(%{"name" => "alpha"}))

    assert {:ok, connections} = Catalog.list_connections(catalog)
    assert Enum.map(connections, & &1.name) == ["alpha", "zeta"]
  end

  test "putting the same name twice replaces rather than duplicates", %{catalog: catalog} do
    :ok = Catalog.put_connection(catalog, connection())
    :ok = Catalog.put_connection(catalog, connection(%{"host" => "db2.internal"}))

    assert {:ok, [stored]} = Catalog.list_connections(catalog)
    assert stored.host == "db2.internal"
  end

  test "a replacement keeps the original created_at", %{catalog: catalog} do
    :ok = Catalog.put_connection(catalog, connection())
    {:ok, first} = Catalog.connection(catalog, "warehouse")

    :ok = Catalog.put_connection(catalog, %{first | host: "db2.internal"})
    {:ok, second} = Catalog.connection(catalog, "warehouse")

    assert second.created_at == first.created_at
  end

  test "delete_connection/2 removes it, and removing an absent one is ok", %{catalog: catalog} do
    :ok = Catalog.put_connection(catalog, connection())

    assert :ok = Catalog.delete_connection(catalog, "warehouse")
    assert {:ok, []} = Catalog.list_connections(catalog)
    assert :ok = Catalog.delete_connection(catalog, "warehouse")
  end

  test "a catalog that stores no connections says so" do
    catalog = %Catalog{impl: Connectionless, config: nil}

    assert Catalog.list_connections(catalog) == {:error, :connections_unsupported}
    assert Catalog.connection(catalog, "warehouse") == {:error, :connections_unsupported}
    assert Catalog.delete_connection(catalog, "warehouse") == {:error, :connections_unsupported}
  end
end
