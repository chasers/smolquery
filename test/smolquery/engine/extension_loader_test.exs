defmodule Smolquery.Engine.ExtensionLoaderTest do
  use ExUnit.Case, async: false

  alias Smolquery.DuckDB
  alias Smolquery.Engine.Connection
  alias Smolquery.Engine.ExtensionLoader

  @database __MODULE__.Database

  setup do
    start_supervised!({DuckDB, process_options: [name: @database]})
    {:ok, adbc} = Adbc.Connection.start_link(database: @database)
    %{adbc: adbc}
  end

  describe "load/3" do
    test "installs and loads an extension through the loader", %{adbc: adbc} do
      loader = start_supervised!({ExtensionLoader, name: __MODULE__.Loader})

      assert ExtensionLoader.load(loader, adbc, [:sqlite]) == :ok

      assert {:ok, %Adbc.Result{}} =
               Adbc.Connection.query(
                 adbc,
                 "SELECT 1 FROM duckdb_extensions() WHERE extension_name = 'sqlite' AND loaded"
               )
    end

    test "loads inline when no loader is running, as a script does", %{adbc: adbc} do
      assert ExtensionLoader.load(__MODULE__.Absent, adbc, [:sqlite]) == :ok
    end

    test "nothing to load returns at once", %{adbc: adbc} do
      assert ExtensionLoader.load(__MODULE__.Absent, adbc, []) == :ok
    end

    test "a connection that dies under the loader is an error, not a crash" do
      loader = start_supervised!({ExtensionLoader, name: __MODULE__.Dying})
      {:ok, database} = DuckDB.start_link(process_options: [name: __MODULE__.DyingDatabase])
      {:ok, adbc} = Adbc.Connection.start_link(database: database)
      Process.unlink(adbc)
      Process.exit(adbc, :kill)

      assert {:error, {:extension_failed, "sqlite", {:connection_exited, _reason}}} =
               ExtensionLoader.load(loader, adbc, [:sqlite])

      assert Process.alive?(loader)
    end
  end

  describe "through Smolquery.Engine.Connection (T-415)" do
    test "a burst of connections loading the same extension all start" do
      starts =
        for _slot <- 1..6 do
          Task.async(fn -> Connection.start_link(database: @database, extensions: [:sqlite]) end)
        end

      for start <- starts do
        assert {:ok, conn} = Task.await(start, 30_000)
        assert {:ok, _answered} = Connection.query(conn, "SELECT 1")
      end
    end

    test "load_extensions/2 loads on a running connection, for a federated query" do
      {:ok, conn} = Connection.start_link(database: @database)

      assert Connection.load_extensions(conn, [:sqlite]) == :ok
      assert {:ok, _answered} = Connection.query(conn, "SELECT 1")
    end

    @tag :integration
    test "load_extensions/2 names the extension that does not exist" do
      {:ok, conn} = Connection.start_link(database: @database)

      assert {:error, {:extension_failed, "definitely_not_an_extension", _}} =
               Connection.load_extensions(conn, [:definitely_not_an_extension])
    end
  end
end
