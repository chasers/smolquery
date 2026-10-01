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
               ~w(createdAt database host kind name port sslmode updatedAt username)

      refute json |> inspect() =~ connection.secret
    end
  end

  describe "a DuckLake connection (T-610)" do
    defp lake(overrides \\ %{}) do
      params(Map.merge(%{"kind" => "ducklake", "data_path" => "s3://lakes/sales/"}, overrides))
    end

    test "is the lake's metadata database plus its data path" do
      assert {:ok, connection} = Connection.new(lake())

      assert connection.kind == "ducklake"
      assert connection.data_path == "s3://lakes/sales/"
      assert connection.storage == %{}
      assert connection.storage_secret == nil
    end

    test "a Postgres connection is the default kind and carries no lake fields" do
      assert {:ok, connection} = Connection.new(params(%{"data_path" => "s3://ignored/"}))

      assert connection.kind == "postgres"
      assert connection.data_path == nil
      assert Connection.options(connection) == %{}
    end

    test "requires a data path, and refuses a kind it does not know" do
      assert Connection.new(Map.delete(lake(), "data_path")) ==
               {:error, {:missing_field, "data_path"}}

      assert Connection.new(params(%{"kind" => "mysql"})) == {:error, {:invalid_param, "kind"}}
    end

    test "refuses a local data path outside the configured roots, and takes one inside" do
      previous = Application.get_env(:smolquery, Smolquery.Federation)
      on_exit(fn -> restore_federation(previous) end)

      assert Connection.new(lake(%{"data_path" => "/etc"})) ==
               {:error, {:invalid_param, "data_path"}}

      Application.put_env(:smolquery, Smolquery.Federation, local_roots: ["/srv/lakes"])

      assert {:ok, _connection} = Connection.new(lake(%{"data_path" => "/srv/lakes/sales/"}))

      assert Connection.new(lake(%{"data_path" => "/srv/lakes-other"})) ==
               {:error, {:invalid_param, "data_path"}}

      Application.put_env(:smolquery, Smolquery.Federation, local_roots: ["/"])
      assert {:ok, _connection} = Connection.new(lake(%{"data_path" => "/data/lake/"}))
    end

    test "seals the S3 secret, needs the key id beside it, and never returns it" do
      assert {:ok, connection} =
               Connection.new(
                 lake(%{
                   "s3_key_id" => "AKIA1",
                   "s3_secret" => "s3cret",
                   "s3_region" => "eu-west-1",
                   "s3_endpoint" => "http://minio:9000",
                   "s3_url_style" => "path"
                 })
               )

      refute connection.storage_secret == "s3cret"
      refute inspect(connection) =~ "s3cret"
      assert Connection.storage_secret(connection) == {:ok, "s3cret"}

      json = Connection.to_json(connection)
      refute inspect(json) =~ "s3cret"

      assert json["s3"] == %{
               "keyId" => "AKIA1",
               "region" => "eu-west-1",
               "endpoint" => "http://minio:9000",
               "urlStyle" => "path",
               "hasSecret" => true
             }

      assert Connection.new(lake(%{"s3_key_id" => "AKIA1"})) ==
               {:error, {:invalid_param, "s3_secret"}}

      assert Connection.new(lake(%{"s3_url_style" => "sideways"})) ==
               {:error, {:invalid_param, "s3_url_style"}}
    end

    test "options/1 and with_options/3 round-trip what the catalog stores" do
      {:ok, connection} =
        Connection.new(
          lake(%{"s3_key_id" => "AKIA1", "s3_secret" => "s3cret", "s3_region" => "us-east-1"})
        )

      options = Connection.options(connection)
      refute inspect(options) =~ "s3cret"

      read_back =
        Connection.with_options(
          %{connection | data_path: nil, storage: %{}, storage_secret: nil},
          Jason.decode!(Jason.encode!(options)),
          connection.storage_secret
        )

      assert read_back == connection
    end

    test "update/2 keeps the kind and a blank-free secret, and edits the lake fields" do
      {:ok, connection} = Connection.new(lake(%{"s3_key_id" => "AKIA1", "s3_secret" => "s3cret"}))

      assert {:ok, updated} =
               Connection.update(connection, %{
                 "kind" => "postgres",
                 "data_path" => "s3://lakes/other/"
               })

      assert updated.kind == "ducklake"
      assert updated.data_path == "s3://lakes/other/"
      assert updated.storage_secret == connection.storage_secret

      assert {:ok, cleared} =
               Connection.update(connection, %{"s3_key_id" => "", "s3_secret" => ""})

      assert cleared.storage == %{}
      assert cleared.storage_secret == nil
    end
  end

  test "kinds/0 lists Postgres and DuckLake" do
    assert Connection.kinds() == ["postgres", "ducklake"]
  end

  defp restore_federation(nil), do: Application.delete_env(:smolquery, Smolquery.Federation)
  defp restore_federation(value), do: Application.put_env(:smolquery, Smolquery.Federation, value)
end
