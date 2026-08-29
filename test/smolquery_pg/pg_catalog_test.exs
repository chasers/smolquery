defmodule SmolqueryPg.PgCatalogTest do
  use ExUnit.Case, async: true

  alias SmolqueryPg.PgCatalog

  describe "fixture_dir/0" do
    test "resolves under the running application's priv directory" do
      assert PgCatalog.fixture_dir() == Application.app_dir(:smolquery, "priv/pg_catalog")
    end

    test "holds every static catalog fixture the edge loads at boot" do
      for file <- ~w(pg_type.csv pg_range.csv pg_collation.csv catalog_shapes.csv) do
        assert File.regular?(Path.join(PgCatalog.fixture_dir(), file)), "#{file} is missing"
      end
    end
  end
end
