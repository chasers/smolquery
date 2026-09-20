defmodule SmolqueryClickHouse.RuntimeTest do
  use ExUnit.Case, async: false

  alias SmolqueryClickHouse.Runtime

  setup do
    api = Application.get_env(:smolquery, SmolqueryApi)
    edge = Application.get_env(:smolquery, SmolqueryClickHouse)

    on_exit(fn ->
      restore(SmolqueryApi, api)
      restore(SmolqueryClickHouse, edge)
    end)
  end

  defp restore(key, nil), do: Application.delete_env(:smolquery, key)
  defp restore(key, value), do: Application.put_env(:smolquery, key, value)

  test "resolves the configured listener with a given password" do
    runtime = Runtime.new(password: "given")

    assert runtime.name == SmolqueryClickHouse
    assert runtime.password == "given"
    assert runtime.ip == {127, 0, 0, 1}
    assert runtime.port == 0
  end

  test "the password defaults to the API key" do
    Application.put_env(:smolquery, SmolqueryApi, api_key: "the-api-key")

    assert Runtime.new().password == "the-api-key"
  end

  test "the body limits default to the API's, and derive the in-flight limit as the API does" do
    Application.put_env(:smolquery, SmolqueryApi, max_ndjson_bytes: 1_234)

    runtime = Runtime.new(password: "given")

    assert runtime.max_ndjson_bytes == 1_234
    assert SmolqueryApi.Runtime.insert_max_in_flight_bytes(runtime, {:ok, 40_000}) == 10_000

    assert Runtime.new(password: "given", insert_max_in_flight_bytes: 99).insert_max_in_flight_bytes ==
             99
  end

  test "refuses to boot without a password or an API key" do
    Application.put_env(:smolquery, SmolqueryApi, [])

    assert_raise ArgumentError, ~r/SMOLQUERY_CLICKHOUSE_PASSWORD.*:clickhouse role/, fn ->
      Runtime.new()
    end
  end

  test "never shows the password when inspected" do
    refute inspect(Runtime.new(password: "hidden-password")) =~ "hidden-password"
  end

  test "names its supervisor, listener, catalog server and engines after the instance" do
    assert Runtime.supervisor(Edge) == Edge.Supervisor
    assert Runtime.listener(Edge) == Edge.Listener
    assert Runtime.system_catalog(Edge) == Edge.SystemCatalog
    assert Runtime.catalog_engine(Edge) == Edge.CatalogEngine
    assert Runtime.lake_engine(Edge) == Edge.Lake
  end
end
