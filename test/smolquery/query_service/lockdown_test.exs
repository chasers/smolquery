defmodule Smolquery.QueryService.LockdownTest do
  use ExUnit.Case, async: true

  alias Smolquery.Catalog.DuckLake
  alias Smolquery.QueryService.Lockdown

  describe "statements/3" do
    test "lists first, external access off, DuckLake's options exempted, then the lock" do
      assert Lockdown.statements(["/data", "s3://sealed/"], ["http://hot/a.parquet"]) == [
               "SET allowed_directories = ['/data', 's3://sealed/']",
               "SET allowed_paths = ['http://hot/a.parquet']",
               "SET enable_external_access = false",
               DuckLake.allowed_configs_statement(),
               "SET lock_configuration = true"
             ]
    end

    test "keeps external access on when asked, and still exempts before locking" do
      statements = Lockdown.statements([], [], external_access: true)

      refute "SET enable_external_access = false" in statements

      assert Enum.take(statements, -2) == [
               DuckLake.allowed_configs_statement(),
               "SET lock_configuration = true"
             ]
    end
  end
end
