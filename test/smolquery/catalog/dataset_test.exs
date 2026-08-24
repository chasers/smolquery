defmodule Smolquery.Catalog.DatasetTest do
  use ExUnit.Case, async: false

  alias Smolquery.Catalog.Dataset

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

  defp catalog_params(overrides \\ %{}) do
    Map.merge(
      %{
        "host" => "db.abc.supabase.co",
        "database" => "postgres",
        "username" => "postgres",
        "password" => "hunter2"
      },
      overrides
    )
  end

  defp storage_params(overrides \\ %{}) do
    Map.merge(
      %{
        "bucket" => "lake",
        "prefix" => "analytics",
        "endpoint" => "https://abc.storage.supabase.co/storage/v1/s3",
        "region" => "eu-west-1",
        "url_style" => "path",
        "access_key_id" => "AKIA",
        "secret_access_key" => "shh"
      },
      overrides
    )
  end

  defp params(overrides \\ %{}) do
    Map.merge(
      %{"name" => "analytics", "catalog" => catalog_params(), "storage" => storage_params()},
      overrides
    )
  end

  describe "default/1" do
    test "is a dataset on the deployment defaults" do
      assert {:ok, %Dataset{name: "analytics", catalog: nil, storage: nil}} =
               Dataset.default("analytics")
    end

    test "validates the name as an identifier" do
      assert Dataset.default("bad name") == {:error, {:invalid_identifier, "bad name"}}
    end
  end

  describe "new/1" do
    test "a name alone is a dataset on the defaults" do
      assert {:ok, %Dataset{name: "analytics", catalog: nil, storage: nil}} =
               Dataset.new(%{"name" => "analytics"})
    end

    test "seals both secrets and keeps no plaintext" do
      assert {:ok, dataset} = Dataset.new(params())

      refute dataset.catalog.secret == "hunter2"
      refute dataset.storage.secret == "shh"
      refute inspect(dataset) =~ "hunter2"
      refute inspect(dataset) =~ "shh"
      refute inspect(dataset) =~ dataset.catalog.secret
      refute inspect(dataset) =~ dataset.storage.secret
    end

    test "defaults the catalog port, sslmode, and the storage region and prefix" do
      assert {:ok, dataset} =
               Dataset.new(%{
                 "name" => "analytics",
                 "catalog" => catalog_params(),
                 "storage" => %{"bucket" => "lake"}
               })

      assert dataset.catalog.port == 5432
      assert dataset.catalog.sslmode == "require"
      assert dataset.catalog.version == "1.0"
      assert dataset.storage.prefix == ""
      assert dataset.storage.region == "us-east-1"
      assert dataset.storage.access_key_id == nil
      assert dataset.storage.secret == nil
    end

    test "the catalog version is pinned from the request or the deployment default" do
      assert {:ok, pinned} =
               Dataset.new(%{"name" => "a", "catalog" => catalog_params(%{"version" => "1.1"})})

      assert pinned.catalog.version == "1.1"

      Application.put_env(:smolquery, :ducklake_version, "1.2")
      on_exit(fn -> Application.delete_env(:smolquery, :ducklake_version) end)

      assert Dataset.default_version() == "1.2"
      assert {:ok, defaulted} = Dataset.new(%{"name" => "a", "catalog" => catalog_params()})
      assert defaulted.catalog.version == "1.2"

      assert Dataset.new(%{"name" => "a", "catalog" => catalog_params(%{"version" => "latest"})}) ==
               {:error, {:invalid_param, "catalog.version"}}
    end

    test "each axis is independent" do
      assert {:ok, %Dataset{catalog: %Dataset.Catalog{}, storage: nil}} =
               Dataset.new(%{"name" => "a", "catalog" => catalog_params()})

      assert {:ok, %Dataset{catalog: nil, storage: %Dataset.Storage{}}} =
               Dataset.new(%{"name" => "a", "storage" => storage_params()})
    end

    test "trims the slashes a prefix arrives with" do
      assert {:ok, dataset} =
               Dataset.new(%{"name" => "a", "storage" => storage_params(%{"prefix" => "/x/y/"})})

      assert dataset.storage.prefix == "x/y"
    end

    test "every required field is named, with its axis" do
      for field <- ~w(host database username password) do
        assert Dataset.new(params(%{"catalog" => Map.delete(catalog_params(), field)})) ==
                 {:error, {:missing_field, "catalog." <> field}}
      end

      assert Dataset.new(params(%{"storage" => Map.delete(storage_params(), "bucket")})) ==
               {:error, {:missing_field, "storage.bucket"}}

      assert Dataset.new(Map.delete(params(), "name")) == {:error, {:missing_field, "name"}}
    end

    test "half a storage key pair is refused, not a credential chain" do
      assert Dataset.new(params(%{"storage" => Map.delete(storage_params(), "access_key_id")})) ==
               {:error, {:missing_field, "storage.access_key_id"}}

      assert Dataset.new(
               params(%{"storage" => Map.delete(storage_params(), "secret_access_key")})
             ) ==
               {:error, {:missing_field, "storage.secret_access_key"}}
    end

    test "invalid values are refused by field" do
      assert Dataset.new(params(%{"catalog" => catalog_params(%{"port" => 70_000})})) ==
               {:error, {:invalid_param, "catalog.port"}}

      assert Dataset.new(params(%{"catalog" => catalog_params(%{"sslmode" => "maybe"})})) ==
               {:error, {:invalid_param, "catalog.sslmode"}}

      assert Dataset.new(params(%{"storage" => storage_params(%{"bucket" => "Bad Bucket"})})) ==
               {:error, {:invalid_param, "storage.bucket"}}

      assert Dataset.new(params(%{"storage" => storage_params(%{"prefix" => "../x"})})) ==
               {:error, {:invalid_param, "storage.prefix"}}

      assert Dataset.new(params(%{"storage" => storage_params(%{"url_style" => "weird"})})) ==
               {:error, {:invalid_param, "storage.url_style"}}

      assert Dataset.new(params(%{"catalog" => "not a map"})) ==
               {:error, {:invalid_param, "catalog"}}
    end

    test "a name that is not an identifier is refused: it becomes a lake alias" do
      assert Dataset.new(params(%{"name" => "bad name"})) ==
               {:error, {:invalid_identifier, "bad name"}}
    end
  end

  describe "update/2" do
    test "rotates the catalog and storage credentials" do
      {:ok, dataset} = Dataset.new(params())

      assert {:ok, updated} =
               Dataset.update(dataset, %{
                 "catalog" => %{"username" => "reader", "password" => "new"},
                 "storage" => %{"access_key_id" => "AKIB", "secret_access_key" => "newer"}
               })

      assert updated.catalog.username == "reader"
      refute updated.catalog.secret == dataset.catalog.secret
      assert updated.storage.access_key_id == "AKIB"
      refute updated.storage.secret == dataset.storage.secret
      assert {:ok, metadata} = Dataset.metadata(updated)
      assert metadata =~ "password='new'"
    end

    test "an absent credential keeps the stored secret" do
      {:ok, dataset} = Dataset.new(params())

      assert {:ok, updated} = Dataset.update(dataset, %{"catalog" => %{"username" => "reader"}})

      assert updated.catalog.secret == dataset.catalog.secret
      assert updated.storage == dataset.storage
    end

    test "every other field is immutable" do
      {:ok, dataset} = Dataset.new(params())

      assert Dataset.update(dataset, %{"name" => "other"}) ==
               {:error, {:immutable_field, "name"}}

      assert Dataset.update(dataset, %{"catalog" => %{"host" => "elsewhere"}}) ==
               {:error, {:immutable_field, "catalog.host"}}

      assert Dataset.update(dataset, %{"catalog" => %{"version" => "1.1"}}) ==
               {:error, {:immutable_field, "catalog.version"}}

      assert Dataset.update(dataset, %{"storage" => %{"bucket" => "other"}}) ==
               {:error, {:immutable_field, "storage.bucket"}}
    end

    test "an axis on the defaults cannot gain credentials" do
      {:ok, dataset} = Dataset.default("analytics")

      assert Dataset.update(dataset, %{"catalog" => %{"password" => "x"}}) ==
               {:error, {:immutable_field, "catalog"}}

      assert Dataset.update(dataset, %{
               "storage" => %{"access_key_id" => "a", "secret_access_key" => "b"}
             }) ==
               {:error, {:immutable_field, "storage"}}
    end

    test "a storage key pair must still arrive whole" do
      {:ok, dataset} = Dataset.new(params())

      assert Dataset.update(dataset, %{"storage" => %{"access_key_id" => "AKIB"}}) ==
               {:error, {:missing_field, "storage.secret_access_key"}}
    end
  end

  describe "lake/1" do
    test "is ds_<name> for a dataset with its own catalog, nil otherwise" do
      {:ok, own} = Dataset.new(%{"name" => "analytics", "catalog" => catalog_params()})
      {:ok, default} = Dataset.default("analytics")

      assert Dataset.lake(own) == "ds_analytics"
      assert Dataset.lake(default) == nil
    end
  end

  describe "metadata/1" do
    test "is the libpq metadata string with the password opened" do
      {:ok, dataset} =
        Dataset.new(%{
          "name" => "analytics",
          "catalog" => catalog_params(%{"port" => 6543, "password" => "it's secret"})
        })

      assert {:ok, metadata} = Dataset.metadata(dataset)

      assert metadata =~ "postgres:dbname='postgres'"
      assert metadata =~ "host='db.abc.supabase.co'"
      assert metadata =~ "port='6543'"
      assert metadata =~ "user='postgres'"
      assert metadata =~ "password='it\\'s secret'"
      assert metadata =~ "sslmode='require'"
    end

    test "is nil for a dataset on the default catalog" do
      {:ok, dataset} = Dataset.default("analytics")

      assert Dataset.metadata(dataset) == {:ok, nil}
    end
  end

  describe "store_options/1" do
    test "are the S3 store options with the secret key opened" do
      {:ok, dataset} = Dataset.new(%{"name" => "analytics", "storage" => storage_params()})

      assert Dataset.store_options(dataset) ==
               {:ok,
                [
                  bucket: "lake",
                  prefix: "analytics",
                  endpoint: "https://abc.storage.supabase.co/storage/v1/s3",
                  region: "eu-west-1",
                  url_style: "path",
                  secret_name: "smolquery_ds_analytics",
                  access_key_id: "AKIA",
                  secret_access_key: "shh"
                ]}
    end

    test "carry no keys for a credential-chain store" do
      {:ok, dataset} = Dataset.new(%{"name" => "analytics", "storage" => %{"bucket" => "lake"}})

      assert Dataset.store_options(dataset) ==
               {:ok,
                [
                  bucket: "lake",
                  prefix: "",
                  region: "us-east-1",
                  secret_name: "smolquery_ds_analytics"
                ]}
    end

    test "are nil for a dataset on the default store" do
      {:ok, dataset} = Dataset.default("analytics")

      assert Dataset.store_options(dataset) == {:ok, nil}
    end
  end

  describe "same_settings?/2" do
    test "ignores credentials and timestamps" do
      {:ok, left} = Dataset.new(params())
      {:ok, right} = Dataset.new(params())

      {:ok, rotated} =
        Dataset.update(right, %{"catalog" => %{"username" => "other", "password" => "new"}})

      assert Dataset.same_settings?(left, %{right | created_at: 1, updated_at: 2})
      assert Dataset.same_settings?(left, rotated)
    end

    test "sees a different bucket, host, or name" do
      {:ok, left} = Dataset.new(params())

      {:ok, other_bucket} =
        Dataset.new(params(%{"storage" => storage_params(%{"bucket" => "bkt"})}))

      {:ok, other_host} = Dataset.new(params(%{"catalog" => catalog_params(%{"host" => "h"})}))

      {:ok, other_version} =
        Dataset.new(params(%{"catalog" => catalog_params(%{"version" => "1.1"})}))

      {:ok, default} = Dataset.default("analytics")

      refute Dataset.same_settings?(left, other_bucket)
      refute Dataset.same_settings?(left, other_host)
      refute Dataset.same_settings?(left, other_version)
      refute Dataset.same_settings?(left, default)
    end
  end

  describe "to_json/1" do
    test "names every field a client may see and no secret" do
      {:ok, dataset} = Dataset.new(params())

      json = Dataset.to_json(%{dataset | created_at: 1, updated_at: 2})

      assert json == %{
               "name" => "analytics",
               "catalog" => %{
                 "host" => "db.abc.supabase.co",
                 "port" => 5432,
                 "database" => "postgres",
                 "username" => "postgres",
                 "sslmode" => "require",
                 "version" => "1.0"
               },
               "storage" => %{
                 "bucket" => "lake",
                 "prefix" => "analytics",
                 "endpoint" => "https://abc.storage.supabase.co/storage/v1/s3",
                 "region" => "eu-west-1",
                 "url_style" => "path",
                 "access_key_id" => "AKIA"
               },
               "createdAt" => 1,
               "updatedAt" => 2
             }

      refute inspect(json) =~ "hunter2"
      refute inspect(json) =~ "shh"
    end

    test "a default dataset has nil axes" do
      {:ok, dataset} = Dataset.default("analytics")

      assert %{"catalog" => nil, "storage" => nil} = Dataset.to_json(dataset)
    end
  end
end
