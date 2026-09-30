defmodule Smolquery.Catalog.Migrations.SmolqueryTablesTest do
  @moduledoc """
  What the tables migration declares; creating the tables on a fresh database
  and only recording one the old bootstrap made is
  `Smolquery.Catalog.MigratorTest`'s, over a real Postgres.
  """

  use ExUnit.Case, async: true

  alias Smolquery.Catalog.Migrations.SmolqueryTables

  test "runs in a transaction, so a failure leaves no table half-made" do
    refute SmolqueryTables.__migration__()[:disable_ddl_transaction]
  end

  test "can be reverted" do
    Code.ensure_loaded!(SmolqueryTables)
    assert function_exported?(SmolqueryTables, :down, 0)
  end
end
