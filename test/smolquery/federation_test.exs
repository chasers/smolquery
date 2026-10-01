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

  describe "a DuckLake connection (T-610)" do
    defp lake(overrides \\ %{}) do
      {:ok, connection} =
        Connection.new(
          Map.merge(
            %{
              "name" => "sales",
              "kind" => "ducklake",
              "host" => "catalog.internal",
              "database" => "lake",
              "username" => "reader",
              "password" => "hunter2",
              "sslmode" => "disable",
              "data_path" => "s3://lakes/sales/"
            },
            overrides
          )
        )

      connection
    end

    test "attaches the lake through its Postgres metadata, read-only, at its data path" do
      assert {:ok, statement} = Federation.attach_statement(lake())

      assert statement =~ "ATTACH 'ducklake:postgres:dbname=lake host=catalog.internal"
      assert statement =~ ~s|AS "sales" (READ_ONLY, DATA_PATH 's3://lakes/sales/')|
    end

    test "statements/1 opens with an S3 secret scoped to the data path, when it has one" do
      assert {:ok, [attach]} = Federation.statements(lake())
      assert attach =~ "ATTACH"

      with_secret =
        lake(%{
          "s3_key_id" => "AKIA1",
          "s3_secret" => "s3cret",
          "s3_region" => "eu-west-1",
          "s3_endpoint" => "http://minio:9000"
        })

      assert {:ok, [secret, _attach]} = Federation.statements(with_secret)
      assert secret =~ ~s|CREATE OR REPLACE TEMPORARY SECRET "federated_sales" (TYPE s3|
      assert secret =~ "KEY_ID 'AKIA1', SECRET 's3cret'"
      assert secret =~ "SCOPE 's3://lakes/sales/'"
      assert secret =~ "REGION 'eu-west-1'"
      assert secret =~ "ENDPOINT 'minio:9000', URL_STYLE 'path', USE_SSL false"
    end

    test "extensions/1 adds ducklake to postgres" do
      assert Federation.extensions(lake()) == [:postgres, :ducklake]
      assert Federation.extensions(connection()) == [:postgres]
    end

    test "redact_statement/2 strips the metadata password and the S3 secret" do
      with_secret = lake(%{"s3_key_id" => "AKIA1", "s3_secret" => "s3cret"})
      {:ok, [secret, attach]} = Federation.statements(with_secret)

      refute Federation.redact_statement({:failed, "quoted s3cret"}, secret) =~ "s3cret"

      {:ok, string} = Connection.connection_string(with_secret)

      refute Federation.redact_statement({:failed, "unable to connect: #{string}"}, attach) =~
               "hunter2"
    end

    test "scrub/2 strips the S3 secret too" do
      with_secret = lake(%{"s3_key_id" => "AKIA1", "s3_secret" => "s3cret"})

      assert {:federation_error, "sales", reason} =
               Federation.scrub({:failed, "s3cret and hunter2"}, with_secret)

      refute reason =~ "s3cret"
    end

    test "check/2 refuses a data path in the sealed tier's bucket, and takes one elsewhere" do
      sealed = ["s3://acme/"]

      assert Federation.check(lake(%{"data_path" => "s3://acme/"}), sealed) ==
               {:error, {:federated_path_in_sealed_bucket, "sales"}}

      assert Federation.check(lake(%{"data_path" => "s3://acme/lakes/sales"}), sealed) ==
               {:error, {:federated_path_in_sealed_bucket, "sales"}}

      assert Federation.check(lake(%{"data_path" => "s3://acme-lakes/sales/"}), sealed) == :ok
      assert Federation.check(lake(), []) == :ok
      assert Federation.check(connection(), sealed) == :ok
    end

    test "a DuckLake connection read back without a data path is an error, not a crash" do
      broken = %{lake() | data_path: nil}

      assert Federation.statements(broken) == {:error, {:missing_data_path, "sales"}}
      assert Federation.attach_statement(broken) == {:error, {:missing_data_path, "sales"}}
    end

    test "redact_statement/2 finds the secret past a key id that mimics it" do
      crafted = lake(%{"s3_key_id" => "x, SECRET 'y", "s3_secret" => "realsecret"})
      {:ok, [secret, _attach]} = Federation.statements(crafted)

      redacted = Federation.redact_statement({:failed, "said realsecret"}, secret)
      refute redacted =~ "realsecret"
      assert redacted =~ "said <redacted>"
    end

    test "table_query/2 reads the first rows of a table" do
      assert Federation.table_query("sales", {"main", "orders"}) ==
               ~s|select *\nfrom "sales"."main"."orders"\nlimit 100;\n|
    end
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
