defmodule Smolquery.Engine.ConnectionTest do
  use ExUnit.Case, async: false

  alias Smolquery.DuckDB
  alias Smolquery.Engine.Connection
  alias Smolquery.Engine.Result

  @database __MODULE__.Database

  setup do
    start_supervised!({DuckDB, process_options: [name: @database]})
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

    @tag :tmp_dir
    test "points DuckDB at the configured extension directory first", %{tmp_dir: tmp_dir} do
      Application.put_env(:smolquery, :extension_directory, tmp_dir)
      on_exit(fn -> Application.delete_env(:smolquery, :extension_directory) end)

      {:ok, conn} = Connection.start_link(database: @database)

      assert {:ok, result} =
               Connection.query(conn, "SELECT current_setting('extension_directory')")

      assert Result.one!(result) == tmp_dir
    end
  end

  describe "bootstrap statements" do
    test "runs them in order before the connection is reachable" do
      {:ok, conn} =
        Connection.start_link(
          database: @database,
          statements: [
            "CREATE TABLE bootstrapped (n INTEGER)",
            "INSERT INTO bootstrapped VALUES (1), (2)"
          ]
        )

      assert {:ok, result} = Connection.query(conn, "SELECT n FROM bootstrapped ORDER BY n")
      assert result.rows == [[1], [2]]
    end

    test "refuses to start when a statement fails" do
      Process.flag(:trap_exit, true)

      assert {:error, {:statement_failed, "SELECT nope()", %Adbc.Error{}}} =
               Connection.start_link(database: @database, statements: ["SELECT nope()"])
    end

    test "runs after settings, so a statement may depend on one" do
      {:ok, conn} =
        Connection.start_link(
          database: @database,
          settings: [threads: 3],
          statements: ["CREATE TABLE threads AS SELECT current_setting('threads') AS n"]
        )

      assert {:ok, result} = Connection.query(conn, "SELECT n FROM threads")
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

  describe "fatal?/1" do
    test "recognises the errors that invalidate a DuckDB database" do
      assert Connection.fatal?(%Adbc.Error{
               message:
                 "FATAL Error: Failed: database has been invalidated because of a " <>
                   "previous fatal error. The database must be restarted prior to being used again."
             })

      assert Connection.fatal?(%Adbc.Error{
               message: "INTERNAL Error: Attempted to dereference shared_ptr that is NULL!"
             })
    end

    test "leaves ordinary query errors alone" do
      refute Connection.fatal?(%Adbc.Error{
               message: "Catalog Error: Table with name x does not exist"
             })

      refute Connection.fatal?(%Adbc.Error{message: "Parser Error: syntax error at or near"})

      refute Connection.fatal?(%Adbc.Error{
               message:
                 "TransactionContext Error: Failed to commit: Failed to commit DuckLake transaction."
             })
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
