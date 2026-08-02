defmodule Smolquery.EngineSecretsTest do
  use ExUnit.Case, async: false

  alias Smolquery.Cluster
  alias Smolquery.EngineSecrets
  alias Smolquery.Segments.Store

  setup do
    previous = Application.fetch_env(:smolquery, Cluster)
    on_exit(fn -> restore(previous) end)
  end

  describe "hot_tier/2" do
    test "is empty without httpfs — nothing reads the hot tier over HTTP" do
      assert EngineSecrets.hot_tier([:json], "http://127.0.0.1:4001") == []
    end

    test "single-node, scopes the secret to the one configured address" do
      assert [statement] = EngineSecrets.hot_tier([:httpfs], "http://127.0.0.1:4001")
      assert statement =~ "SCOPE 'http://127.0.0.1:4001'"
    end

    test "clustered, widens the scope to reach node-derived HotServer URLs" do
      Application.put_env(:smolquery, Cluster, enabled: true)

      assert [statement] = EngineSecrets.hot_tier([:httpfs], "http://127.0.0.1:4001")
      assert statement =~ "SCOPE 'http://'"
    end
  end

  describe "sealed_tier/1" do
    test "an S3 store gets its CREATE SECRET" do
      store =
        Store.S3.new(
          bucket: "sealed",
          access_key_id: "id",
          secret_access_key: "secret",
          staging_dir: "/tmp/staging"
        )

      assert [statement] = EngineSecrets.sealed_tier(store)
      assert statement =~ "CREATE SECRET IF NOT EXISTS smolquery_sealed_tier"
    end

    test "a local store needs no credential" do
      assert EngineSecrets.sealed_tier(Store.Local.new(dir: "/tmp/sealed")) == []
    end

    test "no store, no secret" do
      assert EngineSecrets.sealed_tier(nil) == []
    end
  end

  defp restore({:ok, value}), do: Application.put_env(:smolquery, Cluster, value)
  defp restore(:error), do: Application.delete_env(:smolquery, Cluster)
end
