defmodule SmolqueryVictoriaMetrics.RuntimeTest do
  use ExUnit.Case, async: false

  alias SmolqueryVictoriaMetrics.Runtime

  setup do
    api = Application.get_env(:smolquery, SmolqueryApi)
    edge = Application.get_env(:smolquery, SmolqueryVictoriaMetrics)

    on_exit(fn ->
      restore(SmolqueryApi, api)
      restore(SmolqueryVictoriaMetrics, edge)
    end)
  end

  defp restore(key, nil), do: Application.delete_env(:smolquery, key)
  defp restore(key, value), do: Application.put_env(:smolquery, key, value)

  test "resolves the configured listener and the defaults with a given password" do
    runtime = Runtime.new(password: "given")

    assert runtime.name == SmolqueryVictoriaMetrics
    assert runtime.password == "given"
    assert runtime.ip == {127, 0, 0, 1}
    assert runtime.port == 0
    assert runtime.table == {"metrics", "samples"}
    assert runtime.lookback_ms == 300_000
    assert runtime.max_series == 10_000
    assert runtime.max_samples == 20_000_000
    assert runtime.max_points_per_series == 30_000
    assert runtime.max_decoded_bytes == 33_554_432
    assert runtime.ingest_name == Smolquery.IngestService
    assert runtime.query_name == Smolquery.QueryService
    assert runtime.catalog_opts == []
  end

  test "takes the query ceilings" do
    runtime =
      Runtime.new(password: "given", max_series: 5, max_samples: 50, max_points_per_series: 500)

    assert {runtime.max_series, runtime.max_samples, runtime.max_points_per_series} ==
             {5, 50, 500}
  end

  test "takes the decoded-body bound" do
    assert Runtime.new(password: "given", max_decoded_bytes: 1_024).max_decoded_bytes == 1_024
  end

  test "the password defaults to the API key" do
    Application.put_env(:smolquery, SmolqueryApi, api_key: "the-api-key")

    assert Runtime.new().password == "the-api-key"
  end

  test "the body limits default to the API's" do
    Application.put_env(:smolquery, SmolqueryApi, max_ndjson_bytes: 1_234)

    runtime = Runtime.new(password: "given")

    assert runtime.max_ndjson_bytes == 1_234
    assert runtime.insert_max_in_flight_bytes == nil

    assert Runtime.new(password: "given", insert_max_in_flight_bytes: 99).insert_max_in_flight_bytes ==
             99
  end

  test "refuses to boot without a password or an API key" do
    Application.put_env(:smolquery, SmolqueryApi, [])

    assert_raise ArgumentError,
                 ~r/SMOLQUERY_VICTORIAMETRICS_PASSWORD.*:victoriametrics role/,
                 fn ->
                   Runtime.new()
                 end

    assert_raise ArgumentError, ~r/refuses to boot/, fn -> Runtime.new(password: "") end
  end

  test "takes the table as dataset.table text or as a tuple" do
    assert Runtime.new(password: "given", table: "prom.points").table == {"prom", "points"}
    assert Runtime.new(password: "given", table: {"prom", "points"}).table == {"prom", "points"}
  end

  test "refuses to boot on a table that is not two identifiers" do
    for table <- ["samples", "a.b.c", "metrics.", "1metrics.samples", "metrics.sam-ples", 42] do
      assert_raise ArgumentError, ~r/SMOLQUERY_VICTORIAMETRICS_TABLE/, fn ->
        Runtime.new(password: "given", table: table)
      end
    end
  end

  test "parse_table/1 answers a tagged tuple" do
    assert Runtime.parse_table("metrics.samples") == {:ok, {"metrics", "samples"}}
    assert Runtime.parse_table("metrics") == {:error, {:invalid_table, "metrics"}}

    assert Runtime.parse_table({"metrics", "x y"}) ==
             {:error, {:invalid_table, {"metrics", "x y"}}}
  end

  test "never shows the password when inspected" do
    refute inspect(Runtime.new(password: "hidden-password")) =~ "hidden-password"
  end

  test "publishes, fetches and withdraws a runtime by instance name" do
    runtime = Runtime.new(name: :vm_runtime_publish_test, password: "given")

    assert Runtime.put(runtime) == :ok
    assert Runtime.fetch(:vm_runtime_publish_test) == {:ok, runtime}
    assert Runtime.delete(:vm_runtime_publish_test)
    assert Runtime.fetch(:vm_runtime_publish_test) == :error
  end

  test "names its supervisor, listener and lake engine after the instance" do
    assert Runtime.supervisor(Edge) == Edge.Supervisor
    assert Runtime.listener(Edge) == Edge.Listener
    assert Runtime.lake_engine(Edge) == Edge.Lake
  end
end
