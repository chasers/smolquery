defmodule Smolquery.Cluster.RingCacheTest do
  use ExUnit.Case, async: true

  alias Smolquery.Cluster.RingCache

  test "builds once and caches the result while the fingerprint holds" do
    key = unique_key()
    build = counter()

    assert RingCache.resolve(key, [:a@host], build) == 1
    assert RingCache.resolve(key, [:a@host], build) == 1
  end

  test "a changed fingerprint rebuilds" do
    key = unique_key()
    build = counter()

    assert RingCache.resolve(key, [:a@host], build) == 1
    assert RingCache.resolve(key, [:a@host, :b@host], build) == 2
    assert RingCache.resolve(key, [:a@host, :b@host], build) == 2
  end

  test "forget/1 drops the cache so the next resolve rebuilds" do
    key = unique_key()
    build = counter()

    assert RingCache.resolve(key, [:a@host], build) == 1
    assert RingCache.forget(key)
    assert RingCache.resolve(key, [:a@host], build) == 2
  end

  defp counter do
    {:ok, counter} = Agent.start_link(fn -> 0 end)

    fn -> Agent.get_and_update(counter, &{&1 + 1, &1 + 1}) end
  end

  defp unique_key, do: {__MODULE__, :erlang.unique_integer([:positive])}
end
