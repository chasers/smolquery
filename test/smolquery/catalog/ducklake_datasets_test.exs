defmodule Smolquery.Catalog.DuckLakeDatasetsTest do
  @moduledoc """
  Dataset records stored in a real DuckLake catalog (PL-51 L1), and, since
  L3, a dataset's own lake attached on first touch.

  Tagged `:integration` for the reason `Smolquery.Catalog.DuckLakeTest` is,
  and it also needs the suite's Postgres (`Smolquery.Test.Postgres`): a
  dataset with its own catalog is attached at creation, so a fake host is
  refused, which is the behavior an operator with a typo in a Supabase host
  should get. Storage-only datasets need no store to answer: a `CREATE
  SECRET` connects to nothing.

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
  alias Smolquery.Test.Postgres

  @moduletag :integration
  @moduletag :tmp_dir

  @engine __MODULE__.Lake
  @database "smolquery_test"
  @second_database "smolquery_test_dataset2"

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

    Postgres.ensure_database!(@database)
    Postgres.reset_lake!(@database)

    start_supervised!(
      {DuckLake,
       name: @engine,
       metadata: "sqlite:#{Path.join(context.tmp_dir, "catalog.sqlite")}",
       data_path: Path.join(context.tmp_dir, "data")}
    )

    %{catalog: DuckLake.new(engine: @engine)}
  end

  defp catalog_params(overrides \\ []), do: Postgres.catalog_params(overrides)

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

  defp schemata(lake) do
    {:ok, result} =
      Engine.query(
        @engine,
        "SELECT schema_name FROM information_schema.schemata WHERE catalog_name = $1",
        [lake]
      )

    List.flatten(result.rows)
  end

  defp original_password, do: catalog_params()["password"]

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
    assert stored.catalog.host == original.catalog.host
    assert stored.catalog.port == original.catalog.port
    assert stored.catalog.database == @database
    assert stored.catalog.username == original.catalog.username
    assert stored.catalog.sslmode == "disable"
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
    assert metadata =~ "password='#{original_password()}'"
    assert {:ok, options} = Dataset.store_options(stored)
    assert options[:secret_access_key] == "shh"
  end

  test "a dataset with its own catalog is attached and lives in its own lake",
       %{catalog: catalog} do
    :ok = Catalog.create_dataset(catalog, dataset(%{"storage" => nil}))
    :ok = Catalog.create_dataset(catalog, "plain")

    assert {:ok, ["analytics", "main", "plain"]} = Catalog.list_datasets(catalog)
    assert DuckLake.Attachments.attached(@engine) == ["analytics"]
    refute "analytics" in schemata(DuckLake.default_catalog())
    assert "analytics" in schemata("ds_analytics")
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

    assert "analytics" in schemata(DuckLake.default_catalog())
    assert DuckLake.Attachments.attached(@engine) == ["analytics"]
  end

  test "a catalog that cannot be reached refuses the create", %{catalog: catalog} do
    unreachable = dataset(%{"catalog" => catalog_params(port: 1), "storage" => nil})

    assert {:error, _reason} = Catalog.create_dataset(catalog, unreachable)
    assert Catalog.dataset(catalog, "analytics") == {:error, {:unknown_dataset, "analytics"}}
  end

  test "the metadata database holds no plaintext secret", %{catalog: catalog} do
    :ok = Catalog.create_dataset(catalog, dataset())

    {:ok, result} =
      Engine.query(
        @engine,
        "SELECT catalog_secret, storage_secret FROM #{metadata_table("smolquery_datasets")}"
      )

    assert [[catalog_secret, storage_secret]] = result.rows
    refute catalog_secret == original_password()
    refute catalog_secret =~ original_password()
    refute storage_secret == "shh"
    refute storage_secret =~ "shh"
  end

  test "re-creating with the same settings is a no-op", %{catalog: catalog} do
    :ok = Catalog.create_dataset(catalog, dataset())
    {:ok, first} = Catalog.dataset(catalog, "analytics")

    assert :ok = Catalog.create_dataset(catalog, dataset())
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

    Postgres.ensure_database!(@second_database)
    Postgres.reset_lake!(@second_database)

    assert :ok =
             Catalog.create_dataset(
               catalog,
               dataset(%{
                 "name" => "other",
                 "catalog" => catalog_params(database: @second_database)
               })
             )

    assert DuckLake.Attachments.attached(@engine) == ["other", "analytics"]
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
