defmodule Smolquery.Catalog.DuckLake.AttachmentsTest do
  @moduledoc """
  The per-engine record of attached dataset lakes and its bound (PL-51 D6).

  Needs the suite's Postgres: a dataset with its own catalog attaches to a
  real database. Two databases give two lakes, which is what an eviction
  needs to prove anything.
  """

  use ExUnit.Case, async: false

  alias Smolquery.Catalog
  alias Smolquery.Catalog.Dataset
  alias Smolquery.Catalog.DuckLake
  alias Smolquery.Catalog.DuckLake.Attachments
  alias Smolquery.Engine
  alias Smolquery.Test.Postgres

  @moduletag :integration
  @moduletag :tmp_dir

  @engine __MODULE__.Lake
  @databases ["smolquery_test", "smolquery_test_dataset2"]

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

    for database <- @databases do
      Postgres.ensure_database!(database)
      Postgres.reset_lake!(database)
    end

    start_supervised!(
      {DuckLake,
       name: @engine,
       metadata: "sqlite:#{Path.join(context.tmp_dir, "catalog.sqlite")}",
       data_path: Path.join(context.tmp_dir, "data"),
       attach_limit: 1}
    )

    %{catalog: DuckLake.new(engine: @engine)}
  end

  defp dataset(name, database) do
    {:ok, dataset} =
      Dataset.new(%{"name" => name, "catalog" => Postgres.catalog_params(database: database)})

    dataset
  end

  defp attached_databases do
    {:ok, result} =
      Engine.query(@engine, "SELECT database_name FROM duckdb_databases() ORDER BY 1")

    List.flatten(result.rows)
  end

  test "ensure/2 attaches once and is a no-op after that" do
    [database | _rest] = @databases
    dataset = dataset("one", database)

    assert :ok = Attachments.ensure(@engine, dataset)
    assert :ok = Attachments.ensure(@engine, dataset)
    assert Attachments.attached(@engine) == ["one"]
    assert "ds_one" in attached_databases()
  end

  test "a dataset on the defaults needs nothing" do
    {:ok, dataset} = Dataset.default("plain")

    assert :ok = Attachments.ensure(@engine, dataset)
    assert Attachments.attached(@engine) == []
  end

  test "past the limit the least recently used lake is detached" do
    [first_db, second_db] = @databases
    first = dataset("first", first_db)
    second = dataset("second", second_db)

    :ok = Attachments.ensure(@engine, first)
    assert "ds_first" in attached_databases()

    :ok = Attachments.ensure(@engine, second)
    assert Attachments.attached(@engine) == ["second"]
    assert "ds_second" in attached_databases()
    refute "ds_first" in attached_databases()

    :ok = Attachments.ensure(@engine, first)
    assert Attachments.attached(@engine) == ["first"]
    assert "ds_first" in attached_databases()
    refute "ds_second" in attached_databases()
  end

  test "a lake that cannot be reached leaves nothing attached" do
    unreachable = dataset("gone", "smolquery_test_missing_database")

    assert {:error, _reason} = Attachments.ensure(@engine, unreachable)
    assert Attachments.attached(@engine) == []
    refute "ds_gone" in attached_databases()
  end

  test "an evicted lake re-attaches on the next catalog operation", %{catalog: catalog} do
    [first_db, second_db] = @databases
    schema = Smolquery.Schema.new!([{"id", :int64}])

    :ok = Catalog.create_dataset(catalog, dataset("first", first_db))
    :ok = Catalog.create_table(catalog, {"first", "events"}, schema)
    :ok = Catalog.create_dataset(catalog, dataset("second", second_db))
    assert Attachments.attached(@engine) == ["second"]

    assert {:ok, ["events"]} = Catalog.list_tables(catalog, "first")
    assert Attachments.attached(@engine) == ["first"]
  end
end
