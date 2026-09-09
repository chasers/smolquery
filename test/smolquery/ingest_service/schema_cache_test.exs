defmodule Smolquery.IngestService.SchemaCacheTest do
  use ExUnit.Case, async: true

  alias Smolquery.IngestService.Runtime
  alias Smolquery.IngestService.SchemaCache
  alias Smolquery.Test.Eventually
  alias Smolquery.Test.StubCatalog

  @table {"analytics", "events"}

  defp start_cache(opts \\ []) do
    name = :"ingest_cache_#{:erlang.unique_integer([:positive])}"

    runtime =
      Runtime.new(
        Keyword.merge([name: name, catalog: StubCatalog.new(self()), buffer_name: :none], opts)
      )

    start_supervised!({SchemaCache, runtime})

    runtime
  end

  test "a hit costs the catalog nothing" do
    runtime = start_cache()

    assert {:ok, schema} = SchemaCache.fetch(runtime, @table)
    assert schema == StubCatalog.schema()
    assert_received {:called, :table_schema, [@table]}

    assert {:ok, ^schema} = SchemaCache.fetch(runtime, @table)
    refute_received {:called, :table_schema, _args}
  end

  test "an expired entry reads through again" do
    runtime = start_cache(schema_cache_ttl_ms: 1)

    {:ok, _schema} = SchemaCache.fetch(runtime, @table)
    Process.sleep(5)
    {:ok, _schema} = SchemaCache.fetch(runtime, @table)

    assert_received {:called, :table_schema, [@table]}
    assert_received {:called, :table_schema, [@table]}
  end

  test "a schema change broadcast drops the entry, another table's does not (PL-61 L3)" do
    runtime = start_cache()

    {:ok, _schema} = SchemaCache.fetch(runtime, @table)
    assert_received {:called, :table_schema, [@table]}

    :telemetry.execute(
      [:smolquery, :catalog, :schema_change],
      %{count: 1},
      %{result: :ok, table_ref: {"analytics", "other"}, change: :add_column, column: "x"}
    )

    :telemetry.execute(
      [:smolquery, :catalog, :schema_change],
      %{count: 1},
      %{result: :ok, table_ref: @table, change: :add_column, column: "label"}
    )

    assert Eventually.until(
             fn ->
               {:ok, _schema} = SchemaCache.fetch(runtime, @table)
               received_read_through?()
             end,
             100,
             10
           )
  end

  defp received_read_through? do
    receive do
      {:called, :table_schema, [@table]} -> true
    after
      0 -> false
    end
  end

  test "invalidation drops the entry" do
    runtime = start_cache()

    {:ok, _schema} = SchemaCache.fetch(runtime, @table)
    :ok = SchemaCache.invalidate(runtime, @table)
    {:ok, _schema} = SchemaCache.fetch(runtime, @table)

    assert_received {:called, :table_schema, [@table]}
    assert_received {:called, :table_schema, [@table]}
  end
end
