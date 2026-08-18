defmodule Smolquery.Segments.Store.LocalTest do
  use ExUnit.Case, async: true

  alias Smolquery.Segments.Store
  alias Smolquery.Segments.Store.Local
  alias Smolquery.Test.FakeParquet

  @moduletag :tmp_dir

  defp write(contents), do: fn path -> File.write(path, FakeParquet.bytes(contents)) end

  describe "put/3" do
    test "commits the encoded bytes at the key and reports them", %{tmp_dir: dir} do
      store = Local.new(dir: dir)

      assert {:ok, put} = Store.put(store, "a/b/one.parquet", write("hello"))

      assert put.location == Path.join(dir, "a/b/one.parquet")
      assert put.byte_size == byte_size(FakeParquet.bytes("hello"))
      assert File.read!(put.location) == FakeParquet.bytes("hello")
    end

    test "creates the key's directories", %{tmp_dir: dir} do
      store = Local.new(dir: Path.join(dir, "fresh"))

      assert {:ok, put} = Store.put(store, "deeply/nested/one.parquet", write("x"))
      assert File.exists?(put.location)
    end

    test "leaves nothing staged behind", %{tmp_dir: dir} do
      store = Local.new(dir: dir)

      {:ok, _put} = Store.put(store, "one.parquet", write("x"))

      assert File.ls!(Path.join(dir, ".tmp")) == []
    end

    test "stages nothing at the key until the encode succeeds", %{tmp_dir: dir} do
      store = Local.new(dir: dir)
      failing = fn _path -> {:error, :enospc} end

      assert Store.put(store, "one.parquet", failing) ==
               {:error, {:put_failed, "one.parquet", :enospc}}

      refute File.exists?(Path.join(dir, "one.parquet"))
      assert File.ls!(Path.join(dir, ".tmp")) == []
    end

    test "cleans up a partially written staged file", %{tmp_dir: dir} do
      store = Local.new(dir: dir)

      half_written = fn path ->
        File.write!(path, "partial")
        {:error, :closed}
      end

      assert {:error, {:put_failed, _key, :closed}} =
               Store.put(store, "one.parquet", half_written)

      assert File.ls!(Path.join(dir, ".tmp")) == []
      refute File.exists?(Path.join(dir, "one.parquet"))
    end

    test "concurrent puts of the same key do not collide in staging", %{tmp_dir: dir} do
      store = Local.new(dir: dir)

      results =
        1..8
        |> Task.async_stream(fn i -> Store.put(store, "one.parquet", write("body-#{i}")) end)
        |> Enum.map(fn {:ok, result} -> result end)

      assert Enum.all?(results, &match?({:ok, _put}, &1))
      assert File.ls!(Path.join(dir, ".tmp")) == []
      assert File.read!(Path.join(dir, "one.parquet")) =~ "body-"
    end

    test "writes without fsync when asked", %{tmp_dir: dir} do
      store = Local.new(dir: dir, fsync: false)

      assert {:ok, put} = Store.put(store, "one.parquet", write("x"))
      assert File.read!(put.location) == FakeParquet.bytes("x")
    end

    test "cleans up after an encoder that raises, and lets the raise through", %{tmp_dir: dir} do
      store = Local.new(dir: dir)

      raising = fn path ->
        File.write!(path, "partial")
        raise ArgumentError, "bad column"
      end

      assert_raise ArgumentError, fn -> Store.put(store, "one.parquet", raising) end

      assert File.ls!(Path.join(dir, ".tmp")) == []
      refute File.exists?(Path.join(dir, "one.parquet"))
    end
  end

  describe "put/3 write-once guarantee" do
    test "a retry to a committed key never overwrites it", %{tmp_dir: dir} do
      store = Local.new(dir: dir)

      assert {:ok, first} = Store.put(store, "one.parquet", write("original"))
      assert {:ok, retry} = Store.put(store, "one.parquet", write("truncated-retry"))

      assert retry.location == first.location
      assert File.read!(first.location) == FakeParquet.bytes("original")
    end

    test "a retry to a committed key reports the committed size, not its own", %{tmp_dir: dir} do
      store = Local.new(dir: dir)

      assert {:ok, first} = Store.put(store, "one.parquet", write("original"))
      assert {:ok, retry} = Store.put(store, "one.parquet", write("a-much-longer-retry-body"))

      assert first.byte_size == byte_size(FakeParquet.bytes("original"))
      assert retry.byte_size == first.byte_size
    end

    test "a retry to a committed key leaves nothing staged behind", %{tmp_dir: dir} do
      store = Local.new(dir: dir)
      {:ok, _first} = Store.put(store, "one.parquet", write("original"))

      {:ok, _retry} = Store.put(store, "one.parquet", write("truncated-retry"))

      assert File.ls!(Path.join(dir, ".tmp")) == []
    end
  end

  describe "key validation" do
    test "every operation refuses a key that could climb out of the store", %{tmp_dir: dir} do
      store = Local.new(dir: dir)

      for key <- ["../escape.parquet", "a/../../etc/passwd", "/absolute.parquet", ""] do
        assert Store.put(store, key, write("x")) == {:error, {:invalid_key, key}}
        assert Store.delete(store, key) == {:error, {:invalid_key, key}}
        assert_raise ArgumentError, fn -> Store.location(store, key) end
      end

      assert File.ls!(dir) == []
    end

    test "the staging namespace is reserved", %{tmp_dir: dir} do
      store = Local.new(dir: dir)

      assert Store.put(store, ".tmp/one.parquet", write("x")) ==
               {:error, {:invalid_key, ".tmp/one.parquet"}}

      assert Store.list(store, ".tmp") == {:error, {:invalid_prefix, ".tmp"}}
    end

    test "list refuses a traversing prefix", %{tmp_dir: dir} do
      store = Local.new(dir: dir)

      assert Store.list(store, "../elsewhere") == {:error, {:invalid_prefix, "../elsewhere"}}
    end
  end

  describe "location/2" do
    test "resolves a key beneath the store root", %{tmp_dir: dir} do
      store = Local.new(dir: dir)

      assert Store.location(store, "a/b/one.parquet") == Path.join(dir, "a/b/one.parquet")
    end
  end

  describe "list/2" do
    setup %{tmp_dir: dir} do
      store = Local.new(dir: dir)

      for key <- ["ds/one/a.parquet", "ds/one/b.parquet", "ds/two/c.parquet", "root.parquet"] do
        {:ok, _put} = Store.put(store, key, write("x"))
      end

      %{store: store}
    end

    test "lists the keys under a prefix", %{store: store} do
      assert Store.list(store, "ds/one") == {:ok, ["ds/one/a.parquet", "ds/one/b.parquet"]}
      assert Store.list(store, "ds/two") == {:ok, ["ds/two/c.parquet"]}
    end

    test "lists the whole store from the root prefix", %{store: store} do
      assert Store.list(store, "") ==
               {:ok,
                [
                  "ds/one/a.parquet",
                  "ds/one/b.parquet",
                  "ds/two/c.parquet",
                  "root.parquet"
                ]}
    end

    test "never reports a staged file as a segment", %{store: store, tmp_dir: dir} do
      File.write!(Path.join(dir, ".tmp/leftover.parquet"), "x")

      assert {:ok, keys} = Store.list(store, "")
      refute Enum.any?(keys, &String.starts_with?(&1, ".tmp"))
    end

    test "reports an empty list for a prefix the store does not hold", %{store: store} do
      assert Store.list(store, "ds/missing") == {:ok, []}
    end
  end

  describe "delete/2" do
    test "removes the segment at a key", %{tmp_dir: dir} do
      store = Local.new(dir: dir)
      {:ok, put} = Store.put(store, "one.parquet", write("x"))

      assert Store.delete(store, "one.parquet") == :ok
      refute File.exists?(put.location)
    end

    test "is idempotent, so retirement and GC can retry", %{tmp_dir: dir} do
      store = Local.new(dir: dir)
      {:ok, _put} = Store.put(store, "one.parquet", write("x"))

      assert Store.delete(store, "one.parquet") == :ok
      assert Store.delete(store, "one.parquet") == :ok
      assert Store.delete(store, "never/existed.parquet") == :ok
    end
  end

  describe "shared?/1" do
    test "is not shared — one node's disk needs HotServer to serve it", %{tmp_dir: dir} do
      refute Store.shared?(Local.new(dir: dir))
    end
  end

  describe "sweep_staging/2" do
    defp stage(dir, name) do
      staging = Path.join(dir, ".tmp")
      File.mkdir_p!(staging)
      path = Path.join(staging, name)
      File.write!(path, "half-encoded")

      path
    end

    test "deletes a staged file a killed encoder left behind", %{tmp_dir: dir} do
      store = Local.new(dir: dir)
      path = stage(dir, "leaked.parquet.123")

      assert Store.sweep_staging(store, 0) == {:ok, ["leaked.parquet.123"]}
      refute File.exists?(path)
    end

    test "spares a staged file younger than the age guard", %{tmp_dir: dir} do
      store = Local.new(dir: dir)
      path = stage(dir, "in-flight.parquet.456")

      assert Store.sweep_staging(store, 60_000) == {:ok, []}
      assert File.exists?(path)
    end

    test "never touches committed segments", %{tmp_dir: dir} do
      store = Local.new(dir: dir)
      {:ok, put} = Store.put(store, "a/b/one.parquet", write("kept"))

      assert Store.sweep_staging(store, 0) == {:ok, []}
      assert File.exists?(put.location)
    end

    test "an empty or absent staging directory sweeps nothing", %{tmp_dir: dir} do
      assert Store.sweep_staging(Local.new(dir: dir), 0) == {:ok, []}
    end
  end
end
