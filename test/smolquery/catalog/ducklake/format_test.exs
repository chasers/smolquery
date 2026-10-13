defmodule Smolquery.Catalog.DuckLake.FormatTest do
  use ExUnit.Case, async: true

  alias Smolquery.Catalog.DuckLake
  alias Smolquery.Catalog.DuckLake.Format
  alias Smolquery.Engine

  test "version/0 is the DuckLake format the hand-written SQL targets" do
    assert Format.version() == "1.0"
  end

  test "version_sql/1 reads the version key under the prefix" do
    assert Format.version_sql("m") ==
             "SELECT value FROM m.ducklake_metadata WHERE key = 'version'"
  end

  test "version_in/1 reads the version out of version_sql/1's rows" do
    assert Format.version_in([["1.0"]]) == "1.0"
    assert Format.version_in([]) == nil
  end

  describe "the DuckLake extension" do
    @describetag :integration
    @describetag :tmp_dir

    test "creates a catalog at version/0, the format a driver bump must keep (T-651)", context do
      engine = Module.concat(__MODULE__, "Lake#{System.unique_integer([:positive])}")

      start_supervised!(
        {DuckLake,
         name: engine,
         metadata: "sqlite:#{Path.join(context.tmp_dir, "catalog.sqlite")}",
         data_path: Path.join(context.tmp_dir, "data")}
      )

      result = Engine.query!(engine, Format.version_sql(~s("__ducklake_metadata_lake")))

      assert Format.version_in(result.rows) == Format.version()
    end
  end

  test "check/1 accepts only the version, naming both otherwise" do
    assert Format.check("1.0") == :ok
    assert Format.check("1.1-dev1") == {:error, {:catalog_format, "1.1-dev1", "1.0"}}
    assert Format.check(nil) == {:error, {:catalog_format, nil, "1.0"}}
  end

  describe "guard_statements/2" do
    setup do
      engine = Module.concat(__MODULE__, "Engine#{System.unique_integer([:positive])}")
      start_supervised!({Engine, name: engine, extensions: []})

      Engine.query!(engine, "CREATE SCHEMA m")
      Engine.query!(engine, "CREATE TABLE m.ducklake_metadata (key VARCHAR, value VARCHAR)")
      Engine.query!(engine, "INSERT INTO m.ducklake_metadata VALUES ('version', '1.0')")

      %{engine: engine}
    end

    test "pass at the version, and fail with an error mismatch/1 reads back otherwise",
         %{engine: engine} do
      assert Engine.try_transaction(engine, Format.guard_statements("m")) == :ok

      Engine.query!(engine, "UPDATE m.ducklake_metadata SET value = '1.1-dev1'")

      assert {:error, error} = Engine.try_transaction(engine, Format.guard_statements("m"))
      assert Format.mismatch(error) == {:ok, {:catalog_format, "1.1-dev1", "1.0"}}
    end

    test "hold the version row first only when asked to lock" do
      assert [lock, _check] = Format.guard_statements("m", lock: true)
      assert lock == "SELECT 1 FROM m.ducklake_metadata WHERE key = 'version' FOR SHARE"
      assert [check] = Format.guard_statements("m")
      refute check =~ "FOR SHARE"
    end
  end

  test "mismatch/1 is :error for anything a guard did not raise" do
    assert Format.mismatch(RuntimeError.exception("duplicate key ducklake_snapshot_pkey")) ==
             :error

    assert Format.mismatch(:timeout) == :error
  end

  test "mismatch/1 reads the version, not the guard's own text an error quotes" do
    [check] = Format.guard_statements("m")
    quoted = RuntimeError.exception("Failed to execute query \"#{check}\": no connection")

    assert Format.mismatch(quoted) == :error
  end
end
