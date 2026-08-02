defmodule Smolquery.Cluster.RingCacheTest do
  use ExUnit.Case, async: false

  alias Smolquery.Cluster
  alias Smolquery.Cluster.RingCache

  setup do
    previous = Application.fetch_env(:smolquery, Cluster)
    on_exit(fn -> restore(previous) end)
  end

  describe "clustering off" do
    test "builds once and caches the result" do
      key = unique_key()
      {:ok, counter} = Agent.start_link(fn -> 0 end)

      build = fn -> Agent.get_and_update(counter, &{&1 + 1, &1 + 1}) end

      assert RingCache.resolve(key, build) == 1
      assert RingCache.resolve(key, build) == 1
    end

    test "forget/1 drops the cache so the next resolve rebuilds" do
      key = unique_key()
      {:ok, counter} = Agent.start_link(fn -> 0 end)

      build = fn -> Agent.get_and_update(counter, &{&1 + 1, &1 + 1}) end

      assert RingCache.resolve(key, build) == 1
      assert RingCache.forget(key)
      assert RingCache.resolve(key, build) == 2
    end
  end

  describe "clustering on" do
    setup do
      Application.put_env(:smolquery, Cluster,
        enabled: true,
        postgres: [hostname: "localhost", port: 5432, username: "postgres", database: "postgres"]
      )
    end

    test "rebuilds fresh on every resolve, never caching" do
      key = unique_key()
      {:ok, counter} = Agent.start_link(fn -> 0 end)

      build = fn -> Agent.get_and_update(counter, &{&1 + 1, &1 + 1}) end

      assert RingCache.resolve(key, build) == 1
      assert RingCache.resolve(key, build) == 2
    end
  end

  defp unique_key, do: {__MODULE__, :erlang.unique_integer([:positive])}

  defp restore({:ok, value}), do: Application.put_env(:smolquery, Cluster, value)
  defp restore(:error), do: Application.delete_env(:smolquery, Cluster)
end
