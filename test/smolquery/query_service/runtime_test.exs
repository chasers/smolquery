defmodule Smolquery.QueryService.RuntimeTest do
  use ExUnit.Case, async: true

  alias Smolquery.Catalog
  alias Smolquery.QueryService.Runtime
  alias Smolquery.Test.StubCatalog

  describe "new/1" do
    test "opts override application config" do
      runtime = Runtime.new(name: __MODULE__.Overridden, max_concurrent_jobs: 3)

      assert runtime.max_concurrent_jobs == 3
    end

    test "inherits application config for what opts leave out" do
      configured = Application.get_env(:smolquery, Smolquery.QueryService, [])
      runtime = Runtime.new(name: __MODULE__.Inherited)

      assert runtime.buffer_base_url == Keyword.fetch!(configured, :buffer_base_url)
      assert runtime.result_ttl_ms == Keyword.fetch!(configured, :result_ttl_ms)
    end

    test "defaults the instance name" do
      assert Runtime.new().name == Smolquery.QueryService
    end

    test "wires a DuckLake catalog to this instance's own engine" do
      runtime = Runtime.new(name: __MODULE__.Lake)

      assert %Catalog{impl: Catalog.DuckLake, config: %{engine: engine}} = runtime.catalog
      assert engine == Runtime.catalog_engine(__MODULE__.Lake)
      assert runtime.catalog_opts == []
    end

    test "passes catalog options through for the supervisor to start" do
      opts = [metadata: "sqlite:/tmp/c.sqlite", data_path: "/tmp/lake"]
      runtime = Runtime.new(name: __MODULE__.LakeOpts, catalog: opts)

      assert runtime.catalog_opts == opts
    end

    test "takes a catalog handle outright, and then starts none" do
      catalog = StubCatalog.new(self())
      runtime = Runtime.new(name: __MODULE__.Given, catalog: catalog)

      assert runtime.catalog == catalog
      assert runtime.catalog_opts == nil
    end

    test "defaults the buffer instance ownership questions go to" do
      assert Runtime.new(name: __MODULE__.Buffered).buffer_name == Smolquery.BufferService
    end
  end

  describe "put/1, fetch/1 and delete/1" do
    test "round-trips a runtime through persistent_term" do
      runtime = Runtime.new(name: __MODULE__.Published)

      assert Runtime.put(runtime) == :ok
      assert Runtime.fetch(__MODULE__.Published) == {:ok, runtime}
      assert Runtime.delete(__MODULE__.Published)
      assert Runtime.fetch(__MODULE__.Published) == :error
    end

    test "fetching an unpublished instance is an error, not a raise" do
      assert Runtime.fetch(__MODULE__.NeverPublished) == :error
    end
  end

  describe "naming" do
    test "derives every process name from the instance name" do
      assert Runtime.supervisor(Query) == Query.Supervisor
      assert Runtime.catalog_engine(Query) == Query.Catalog
      assert Runtime.registry(Query) == Query.Registry
      assert Runtime.runners(Query) == Query.Runners
    end
  end
end
