defmodule Smolquery.Catalog.Migrations.ConnectionKindsTest do
  @moduledoc """
  What the connection-kinds migration declares; adding the columns on a real
  database, old rows reading as Postgres, is `Smolquery.Catalog.MigratorTest`'s
  and `Smolquery.Catalog.DuckLakeConnectionsTest`'s.
  """

  use ExUnit.Case, async: true

  alias Smolquery.Catalog.Migrations.ConnectionKinds

  test "runs in a transaction, and can be reverted" do
    refute ConnectionKinds.__migration__()[:disable_ddl_transaction]
    Code.ensure_loaded!(ConnectionKinds)
    assert function_exported?(ConnectionKinds, :down, 0)
  end
end
