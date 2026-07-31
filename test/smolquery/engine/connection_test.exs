defmodule Smolquery.Engine.ConnectionTest do
  use ExUnit.Case, async: false

  alias Smolquery.Engine.Connection
  alias Smolquery.Engine.Result

  @database __MODULE__.Database

  setup do
    start_supervised!({Adbc.Database, driver: :duckdb, process_options: [name: @database]})
    :ok
  end

  describe "start_link/1" do
    test "starts unnamed when no name is given" do
      assert {:ok, pid} = Connection.start_link(database: @database)
      assert is_pid(pid)
    end

    test "registers under the given name" do
      start_supervised!({Connection, database: @database, name: __MODULE__.Conn})

      assert is_pid(Process.whereis(__MODULE__.Conn))
    end

    test "refuses to start when a session setting is rejected" do
      Process.flag(:trap_exit, true)

      assert {:error, {:setting_failed, :not_a_real_setting, %Adbc.Error{}}} =
               Connection.start_link(database: @database, settings: [not_a_real_setting: 1])
    end

    test "applies settings before returning" do
      {:ok, conn} = Connection.start_link(database: @database, settings: [threads: 3])

      assert {:ok, result} = Connection.query(conn, "SELECT current_setting('threads')")
      assert Result.one!(result) == 3
    end
  end

  describe "query/4" do
    setup do
      {:ok, conn} = Connection.start_link(database: @database)
      %{conn: conn}
    end

    test "returns a neutral result", %{conn: conn} do
      assert {:ok, %Result{columns: ["n"], rows: [[1]]}} = Connection.query(conn, "SELECT 1 AS n")
    end

    test "binds positional parameters", %{conn: conn} do
      assert {:ok, %Result{rows: [["hi"]]}} = Connection.query(conn, "SELECT $1", ["hi"])
    end

    test "surfaces query errors without dying", %{conn: conn} do
      assert {:error, %Adbc.Error{}} = Connection.query(conn, "SELECT nope()")
      assert Process.alive?(conn)
    end
  end

  describe "adbc_connection/1" do
    test "exposes the underlying ADBC connection for direct use" do
      {:ok, conn} = Connection.start_link(database: @database)

      adbc = Connection.adbc_connection(conn)

      assert is_pid(adbc)
      assert {:ok, %Adbc.Result{}} = Adbc.Connection.query(adbc, "SELECT 1")
    end
  end

  describe "extensions" do
    @tag :integration
    test "fails to start on an unknown extension" do
      Process.flag(:trap_exit, true)

      assert {:error, {:extension_failed, "definitely_not_an_extension", _}} =
               Connection.start_link(
                 database: @database,
                 extensions: [:definitely_not_an_extension]
               )
    end
  end
end
