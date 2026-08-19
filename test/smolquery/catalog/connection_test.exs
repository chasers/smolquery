defmodule Smolquery.Catalog.ConnectionTest do
  use ExUnit.Case, async: false

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

  defp params(overrides \\ %{}) do
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
  end

  describe "new/1" do
    test "seals the password and keeps no plaintext" do
      assert {:ok, connection} = Connection.new(params())

      refute connection.secret == "hunter2"
      refute inspect(connection) =~ "hunter2"
      refute inspect(connection) =~ connection.secret
    end

    test "defaults the port and requires TLS by default" do
      assert {:ok, connection} = Connection.new(params())

      assert connection.port == 5432
      assert connection.sslmode == "require"
    end

    test "accepts an explicit port and sslmode" do
      assert {:ok, connection} =
               Connection.new(params(%{"port" => 6543, "sslmode" => "verify-full"}))

      assert connection.port == 6543
      assert connection.sslmode == "verify-full"
    end

    test "a name that is not an identifier is refused: it becomes a catalog alias" do
      assert Connection.new(params(%{"name" => "bad name"})) ==
               {:error, {:invalid_identifier, "bad name"}}
    end

    test "every required field is named when missing" do
      for field <- ~w(name host database username password) do
        assert Connection.new(Map.delete(params(), field)) == {:error, {:missing_field, field}}
        assert Connection.new(params(%{field => ""})) == {:error, {:missing_field, field}}
      end
    end

    test "an out-of-range port and an unknown sslmode are refused" do
      assert Connection.new(params(%{"port" => 0})) == {:error, {:invalid_param, "port"}}
      assert Connection.new(params(%{"port" => 99_999})) == {:error, {:invalid_param, "port"}}
      assert Connection.new(params(%{"port" => "5432"})) == {:error, {:invalid_param, "port"}}

      assert Connection.new(params(%{"sslmode" => "sometimes"})) ==
               {:error, {:invalid_param, "sslmode"}}
    end

    test "without a credential key the password cannot be sealed" do
      Application.delete_env(:smolquery, :credential_key)

      assert Connection.new(params()) == {:error, :no_credential_key}
    end
  end

  describe "update/2" do
    test "an absent password leaves the stored secret untouched" do
      {:ok, connection} = Connection.new(params())

      assert {:ok, updated} = Connection.update(connection, %{"host" => "db2.internal"})

      assert updated.host == "db2.internal"
      assert updated.secret == connection.secret
    end

    test "a present password replaces the secret" do
      {:ok, connection} = Connection.new(params())

      assert {:ok, updated} = Connection.update(connection, %{"password" => "correcthorse"})

      refute updated.secret == connection.secret
      assert {:ok, string} = Connection.connection_string(updated)
      assert string =~ "password=correcthorse"
    end

    test "fields not named keep their values" do
      {:ok, connection} = Connection.new(params())

      assert {:ok, updated} = Connection.update(connection, %{})

      assert updated == connection
    end

    test "an invalid value is refused rather than partly applied" do
      {:ok, connection} = Connection.new(params())

      assert Connection.update(connection, %{"port" => 0}) == {:error, {:invalid_param, "port"}}
      assert Connection.update(connection, %{"host" => ""}) == {:error, {:missing_field, "host"}}
    end
  end

  describe "connection_string/1" do
    test "builds the libpq string DuckDB's ATTACH takes" do
      {:ok, connection} = Connection.new(params())

      assert {:ok, string} = Connection.connection_string(connection)

      assert string ==
               "dbname=app host=db.internal port=5432 user=reader " <>
                 "password=hunter2 sslmode=require"
    end

    test "quotes and escapes a value that would otherwise break the string" do
      {:ok, connection} = Connection.new(params(%{"password" => "pass word'with\\slash"}))

      assert {:ok, string} = Connection.connection_string(connection)
      assert string =~ ~S|password='pass word\'with\\slash'|
    end

    test "a secret sealed under another key does not open" do
      {:ok, connection} = Connection.new(params())

      Application.put_env(
        :smolquery,
        :credential_key,
        Base.encode64(:crypto.strong_rand_bytes(32))
      )

      assert Connection.connection_string(connection) == {:error, :invalid_secret}
    end
  end

  describe "to_json/1" do
    test "names every field except the secret" do
      {:ok, connection} = Connection.new(params())

      json = Connection.to_json(connection)

      assert Map.keys(json) |> Enum.sort() ==
               ~w(createdAt database host name port sslmode updatedAt username)

      refute json |> inspect() =~ connection.secret
    end
  end
end
