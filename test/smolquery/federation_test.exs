defmodule Smolquery.FederationTest do
  @moduledoc """
  The DuckDB side of a federated connection (T-323).

  The probe tests are `:integration`: they load the `postgres` extension, and
  the reachable case needs the same Postgres the DuckLake suite uses
  (`Smolquery.Test.Postgres`, which creates the database on demand — CI's
  Postgres service starts with only the default databases).
  """

  use ExUnit.Case, async: false

  alias Smolquery.Catalog.Connection
  alias Smolquery.Federation
  alias Smolquery.Test.Postgres

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

  defp connection(overrides \\ %{}) do
    {:ok, connection} =
      Map.merge(
        %{
          "name" => "warehouse",
          "host" => "db.internal",
          "database" => "app",
          "username" => "reader",
          "password" => "hunter2",
          "sslmode" => "disable"
        },
        overrides
      )
      |> Connection.new()

    connection
  end

  describe "attach_statement/1" do
    test "attaches under the connection's own name, read-only" do
      assert {:ok, statement} = Federation.attach_statement(connection())

      assert statement =~ ~s|AS "warehouse"|
      assert statement =~ "TYPE postgres"
      assert statement =~ "READ_ONLY"
    end

    test "carries the opened password, since DuckDB needs it to connect" do
      assert {:ok, statement} = Federation.attach_statement(connection())

      assert statement =~ "password=hunter2"
    end

    test "a secret that does not open produces no statement" do
      conn = connection()

      Application.put_env(
        :smolquery,
        :credential_key,
        Base.encode64(:crypto.strong_rand_bytes(32))
      )

      assert Federation.attach_statement(conn) == {:error, :invalid_secret}
    end
  end

  describe "scrub/2" do
    test "replaces the connection string wherever it appears" do
      conn = connection()
      {:ok, string} = Connection.connection_string(conn)

      reason = %{message: ~s|Failed to attach "#{string}": connection refused|}

      assert {:federation_error, "warehouse", scrubbed} = Federation.scrub(reason, conn)
      assert scrubbed =~ "<redacted>"
      assert scrubbed =~ "connection refused"
      refute scrubbed =~ "hunter2"
    end

    test "names the connection even when the secret cannot open" do
      conn = connection()

      Application.put_env(
        :smolquery,
        :credential_key,
        Base.encode64(:crypto.strong_rand_bytes(32))
      )

      assert Federation.scrub(:anything, conn) == {:federation_error, "warehouse", :unavailable}
    end
  end

  describe "probe/1" do
    @tag :integration
    test "an unreachable host is an error that never quotes the password" do
      conn = connection(%{"host" => "127.0.0.1", "port" => 1, "password" => "sup3rsecret"})

      assert {:error, {:federation_error, "warehouse", reason}} = Federation.probe(conn)
      refute inspect(reason) =~ "sup3rsecret"
    end

    @tag :integration
    test "a reachable database opens" do
      options = Postgres.ensure_database!()

      conn =
        connection(%{
          "host" => options[:hostname],
          "port" => options[:port],
          "database" => options[:database],
          "username" => options[:username],
          "password" => options[:password]
        })

      assert Federation.probe(conn) == :ok
    end
  end
end
