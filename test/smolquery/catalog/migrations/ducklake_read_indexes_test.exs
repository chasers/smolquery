defmodule Smolquery.Catalog.Migrations.DucklakeReadIndexesTest do
  @moduledoc """
  What the index migration declares; building the indexes, and rebuilding an
  invalid one, is `Smolquery.Catalog.MigratorTest`'s, over a real Postgres.
  """

  use ExUnit.Case, async: true

  alias Smolquery.Catalog.Migrations.DucklakeReadIndexes

  test "runs outside a DDL transaction, as a CONCURRENTLY build must" do
    assert DucklakeReadIndexes.__migration__()[:disable_ddl_transaction]
  end

  test "keeps the migration lock, which the repo's advisory lock allows" do
    refute DucklakeReadIndexes.__migration__()[:disable_migration_lock]
  end
end
