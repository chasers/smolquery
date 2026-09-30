defmodule Smolquery.Catalog.RepoTest do
  use ExUnit.Case, async: true

  alias Smolquery.Catalog.Repo

  test "init/2 takes the advisory lock and smolquery's own migrations table, whatever it is given" do
    assert {:ok, config} = Repo.init(:runtime, migration_lock: :table_lock, hostname: "db")

    assert config[:migration_lock] == :pg_advisory_lock
    assert config[:migration_source] == "smolquery_schema_migrations"
    assert config[:migration_default_prefix] == "public"
    assert config[:hostname] == "db"
  end
end
