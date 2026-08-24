defmodule Smolquery.Catalog.DuckLakeDatasetsTest do
  @moduledoc """
  Dataset records stored in a real DuckLake catalog (PL-51 L1).

  Tagged `:integration` for the reason `Smolquery.Catalog.DuckLakeTest` is.
  What it pins: a dataset on the defaults still creates a schema and reads
  back as one; a dataset with its own catalog or storage round-trips through
  the side table with no secret in the clear; re-creating with the same
  settings is a no-op and with different settings a conflict; a schema from
  before the side table existed still answers as a dataset; and one Postgres
  database serves one dataset.
  """

  use ExUnit.Case, async: false

  alias Smolquery.Catalog
  alias Smolquery.Catalog.Dataset
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

  defp dataset(overrides \\ %{}) do
    {:ok, dataset} =
      %{"name" => "analytics", "catalog" => catalog_params(), "storage" => storage_params()}
      |> Map.merge(overrides)
      |> Dataset.new()

    dataset
  end

  defp metadata_table(table) do
    schema = Identifier.quote_name!("__ducklake_metadata_" <> DuckLake.default_catalog())

    "#{schema}.#{table}"
  end

  test "a dataset on the defaults creates a schema and reads back as one", %{catalog: catalog} do
    assert :ok = Catalog.create_dataset(catalog, "plain")

    assert {:ok, ["main", "plain"]} = Catalog.list_datasets(catalog)

    assert {:ok, %Dataset{name: "plain", catalog: nil, storage: nil} = stored} =
             Catalog.dataset(catalog, "plain")

    assert is_integer(stored.created_at)

    assert :ok =
             Catalog.create_table(
               catalog,
               {"plain", "events"},
               Smolquery.Schema.new!([{"id", :int64}])
             )
  end

  test "a dataset with its own catalog and storage round-trips", %{catalog: catalog} do
    original = dataset()

    assert :ok = Catalog.create_dataset(catalog, original)
    assert {:ok, stored} = Catalog.dataset(catalog, "analytics")

    assert stored.name == "analytics"
    assert stored.catalog.host == "db.abc.supabase.co"
    assert stored.catalog.port == 5432
    assert stored.catalog.database == "postgres"
    assert stored.catalog.username == "postgres"
    assert stored.catalog.sslmode == "require"
    assert stored.catalog.version == "1.0"
    assert stored.catalog.secret == original.catalog.secret
    assert stored.storage.bucket == "lake"
    assert stored.storage.prefix == "analytics"
    assert stored.storage.endpoint == "https://abc.storage.supabase.co/storage/v1/s3"
    assert stored.storage.region == "eu-west-1"
    assert stored.storage.url_style == "path"
    assert stored.storage.access_key_id == "AKIA"
    assert stored.storage.secret == original.storage.secret
    assert is_integer(stored.created_at)
    assert stored.updated_at == stored.created_at

    assert {:ok, metadata} = Dataset.metadata(stored)
    assert metadata =~ "password='hunter2'"
    assert {:ok, options} = Dataset.store_options(stored)
    assert options[:secret_access_key] == "shh"
  end

  test "a dataset with its own catalog lists without a schema in the default lake",
       %{catalog: catalog} do
    :ok = Catalog.create_dataset(catalog, dataset(%{"storage" => nil}))
    :ok = Catalog.create_dataset(catalog, "plain")

    assert {:ok, ["analytics", "main", "plain"]} = Catalog.list_datasets(catalog)

    {:ok, schemata} =
      Engine.query(
        @engine,
        "SELECT schema_name FROM information_schema.schemata WHERE catalog_name = $1",
        [DuckLake.default_catalog()]
      )

    refute ["analytics"] in schemata.rows
  end

  test "a dataset with only its own storage still creates a schema", %{catalog: catalog} do
    :ok = Catalog.create_dataset(catalog, dataset(%{"catalog" => nil}))

    assert {:ok, %Dataset{catalog: nil, storage: %Dataset.Storage{bucket: "lake"}}} =
             Catalog.dataset(catalog, "analytics")

    assert :ok =
             Catalog.create_table(
               catalog,
               {"analytics", "events"},
               Smolquery.Schema.new!([{"id", :int64}])
             )
  end

  test "the metadata database holds no plaintext secret", %{catalog: catalog} do
    :ok = Catalog.create_dataset(catalog, dataset())

    {:ok, result} = Engine.query(@engine, "SELECT * FROM #{metadata_table("smolquery_datasets")}")

    refute inspect(result.rows) =~ "hunter2"
    refute inspect(result.rows) =~ "shh"
  end

  test "re-creating with the same settings is a no-op", %{catalog: catalog} do
    :ok = Catalog.create_dataset(catalog, dataset())
    {:ok, first} = Catalog.dataset(catalog, "analytics")

    assert :ok =
             Catalog.create_dataset(
               catalog,
               dataset(%{"catalog" => catalog_params(%{"password" => "other"})})
             )

    assert {:ok, ^first} = Catalog.dataset(catalog, "analytics")

    assert :ok = Catalog.create_dataset(catalog, "plain")
    assert :ok = Catalog.create_dataset(catalog, "plain")
    assert {:ok, ["analytics", "main", "plain"]} = Catalog.list_datasets(catalog)
  end

  test "re-creating with different settings is a conflict", %{catalog: catalog} do
    :ok = Catalog.create_dataset(catalog, dataset())

    assert Catalog.create_dataset(
             catalog,
             dataset(%{"storage" => storage_params(%{"bucket" => "bkt"})})
           ) ==
             {:error, {:dataset_exists, "analytics"}}

    assert Catalog.create_dataset(catalog, "analytics") ==
             {:error, {:dataset_exists, "analytics"}}

    :ok = Catalog.create_dataset(catalog, "plain")

    assert Catalog.create_dataset(catalog, dataset(%{"name" => "plain"})) ==
             {:error, {:dataset_exists, "plain"}}
  end

  test "one Postgres database serves one dataset", %{catalog: catalog} do
    :ok = Catalog.create_dataset(catalog, dataset())

    assert Catalog.create_dataset(catalog, dataset(%{"name" => "other"})) ==
             {:error, {:catalog_in_use, "analytics"}}

    assert :ok =
             Catalog.create_dataset(
               catalog,
               dataset(%{
                 "name" => "other",
                 "catalog" => catalog_params(%{"database" => "second"})
               })
             )
  end

  test "a schema from before the side table answers as a default dataset", %{catalog: catalog} do
    {:ok, _result} = Engine.query(@engine, "CREATE SCHEMA #{DuckLake.default_catalog()}.legacy")

    assert {:ok, ["legacy", "main"]} = Catalog.list_datasets(catalog)

    assert {:ok, %Dataset{name: "legacy", catalog: nil, storage: nil, created_at: nil}} =
             Catalog.dataset(catalog, "legacy")

    assert :ok = Catalog.create_dataset(catalog, "legacy")
    assert {:ok, %Dataset{created_at: at}} = Catalog.dataset(catalog, "legacy")
    assert is_integer(at)
  end

  test "an unknown dataset names itself", %{catalog: catalog} do
    assert Catalog.dataset(catalog, "nope") == {:error, {:unknown_dataset, "nope"}}
  end

  test "update_dataset/2 rotates credentials and keeps created_at", %{catalog: catalog} do
    :ok = Catalog.create_dataset(catalog, dataset())
    {:ok, stored} = Catalog.dataset(catalog, "analytics")

    {:ok, rotated} =
      Dataset.update(stored, %{
        "catalog" => %{"username" => "reader", "password" => "new"},
        "storage" => %{"access_key_id" => "AKIB", "secret_access_key" => "newer"}
      })

    Process.sleep(2)
    assert :ok = Catalog.update_dataset(catalog, rotated)
    assert {:ok, after_update} = Catalog.dataset(catalog, "analytics")

    assert after_update.catalog.username == "reader"
    assert after_update.storage.access_key_id == "AKIB"
    assert after_update.created_at == stored.created_at
    assert after_update.updated_at > stored.updated_at
    assert {:ok, metadata} = Dataset.metadata(after_update)
    assert metadata =~ "password='new'"
    assert {:ok, options} = Dataset.store_options(after_update)
    assert options[:secret_access_key] == "newer"
  end

  test "a name that is not an identifier is refused", %{catalog: catalog} do
    assert Catalog.create_dataset(catalog, "bad; DROP TABLE t") ==
             {:error, {:invalid_identifier, "bad; DROP TABLE t"}}
  end
end
