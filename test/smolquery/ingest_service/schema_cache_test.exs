defmodule Smolquery.IngestService.SchemaCacheTest do
  use ExUnit.Case, async: true

  alias Smolquery.IngestService.Runtime
  alias Smolquery.IngestService.SchemaCache
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

  test "invalidation drops the entry" do
    runtime = start_cache()

    {:ok, _schema} = SchemaCache.fetch(runtime, @table)
    :ok = SchemaCache.invalidate(runtime, @table)
    {:ok, _schema} = SchemaCache.fetch(runtime, @table)

    assert_received {:called, :table_schema, [@table]}
    assert_received {:called, :table_schema, [@table]}
  end
end
