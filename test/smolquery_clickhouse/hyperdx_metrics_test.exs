defmodule SmolqueryClickHouse.HyperdxMetricsTest do
  @moduledoc """
  What HyperDX 2.41.0 sends for its metrics screens, run against real rows
  (T-501): the metric catalog and name search, a metric's unit, attributes
  and attribute values, and every chart shape its `translateMetricChartConfig`
  and multi-series composer emit for gauge, sum and histogram metrics.

  The statements are the fixture's, verbatim from HyperDX's own query code
  (`scripts/hyperdx-probe/` says how they were captured), with its settings
  and its `param_*` values in the URL and the `FORMAT` clause clickhouse-js
  appends. Three `otel_metrics_*` tables are created through the catalog and
  filled through the buffer, so every step crosses the whole edge:
  parameters, quoting, the rewrite, the emulated catalog, the macros, the
  planner and the formats. The assertions are on what HyperDX reads: the
  `meta` types its chart code classifies, and the values.
  """

  use ExUnit.Case, async: false

  import Plug.Conn, only: [put_req_header: 3]
  import Plug.Test

  alias Smolquery.BufferService
  alias Smolquery.Catalog
  alias Smolquery.Schema
  alias Smolquery.Test.FullNode
  alias SmolqueryClickHouse.Router
  alias SmolqueryClickHouse.Runtime

  @moduletag :integration
  @moduletag :tmp_dir

  @password "hyperdx-metrics-password"
  @fixture "test/support/fixtures/clickstack/hyperdx_metrics.json"
  @first ~N[2026-10-15 11:00:00]
  @services ["api", "worker", "web"]
  @bounds [5.0, 10.0, 25.0, 50.0, 100.0, 250.0, 500.0, 1000.0]

  @common [
    {"ResourceAttributes", {:map, :string, :string}},
    {"ResourceSchemaUrl", :string},
    {"ScopeName", :string},
    {"ScopeVersion", :string},
    {"ScopeAttributes", {:map, :string, :string}},
    {"ScopeDroppedAttrCount", :int64},
    {"ScopeSchemaUrl", :string},
    {"ServiceName", :string},
    {"MetricName", :string},
    {"MetricDescription", :string},
    {"MetricUnit", :string},
    {"Attributes", {:map, :string, :string}},
    {"StartTimeUnix", :timestamp},
    {"TimeUnix", :timestamp, nullable: false},
    {"Flags", :int64}
  ]

  @tables %{
    "otel_metrics_gauge" => [{"Value", :float64}],
    "otel_metrics_sum" => [
      {"Value", :float64},
      {"AggregationTemporality", :int64},
      {"IsMonotonic", :bool}
    ],
    "otel_metrics_histogram" => [
      {"Count", :int64},
      {"Sum", :float64},
      {"BucketCounts", {:array, :int64}},
      {"ExplicitBounds", {:array, :float64}},
      {"Min", :float64},
      {"Max", :float64},
      {"AggregationTemporality", :int64}
    ]
  }

  setup context do
    schemas = Map.new(@tables, fn {name, extra} -> {name, Schema.new!(@common ++ extra)} end)

    node =
      FullNode.start(context,
        schema: schemas["otel_metrics_gauge"],
        seal_max_files: 1_000,
        seal_max_age_ms: 600_000
      )

    :ok = Catalog.create_dataset(node.catalog, "default")

    for {name, schema} <- schemas do
      table = {"default", name}
      :ok = Catalog.create_table(node.catalog, table, schema)
      :ok = Catalog.put_clustering(node.catalog, table, ["ServiceName", "MetricName", "TimeUnix"])

      {:ok, _ack} =
        BufferService.Client.write_batch(node.buffer, table, %{schema: schema, rows: rows(name)})
    end

    name = :"ch_hyperdx_metrics_#{:erlang.unique_integer([:positive])}"

    start_supervised!(
      {SmolqueryClickHouse.Supervisor,
       name: name, password: @password, query_name: node.query, port: 0, catalog: node.catalog},
      id: name
    )

    on_exit(fn -> Runtime.delete(name) end)

    %{name: name, fixture: @fixture |> File.read!() |> JSON.decode!()}
  end

  defp at(minute), do: NaiveDateTime.add(@first, minute * 60)

  defp pseudo(seed, i), do: rem(seed * 7919 + i * 104_729, 1000) / 1000

  defp common(service, name, unit, description, attrs, minute) do
    %{
      "ResourceAttributes" => %{"service.name" => service, "host.name" => "#{service}-1"},
      "ResourceSchemaUrl" => "",
      "ScopeName" => "otel-seed",
      "ScopeVersion" => "1.0.0",
      "ScopeAttributes" => %{},
      "ScopeDroppedAttrCount" => 0,
      "ScopeSchemaUrl" => "",
      "ServiceName" => service,
      "MetricName" => name,
      "MetricDescription" => description,
      "MetricUnit" => unit,
      "Attributes" => attrs,
      "StartTimeUnix" => @first,
      "TimeUnix" => at(minute),
      "Flags" => 0
    }
  end

  defp rows("otel_metrics_gauge") do
    for {service, s} <- Enum.with_index(@services), cpu <- [0, 1], i <- 0..59 do
      value = Float.round(0.2 + 0.6 * pseudo(s + cpu, i), 4)
      attrs = %{"cpu" => "#{cpu}", "state" => "user"}

      Map.put(
        common(service, "system.cpu.utilization", "1", "CPU busy fraction", attrs, i),
        "Value",
        value
      )
    end ++
      for {service, s} <- Enum.with_index(@services), i <- 0..59 do
        memory = 100_000_000 + s * 10_000_000 + i * 60_000.0

        Map.put(
          common(
            service,
            "process.runtime.memory",
            "By",
            "Resident memory",
            %{"type" => "rss"},
            i
          ),
          "Value",
          memory
        )
      end
  end

  defp rows("otel_metrics_sum") do
    counters =
      for {service, s} <- Enum.with_index(@services),
          status <- ["200", "500"],
          do: counter_rows(service, s, status)

    queue =
      for {service, s} <- Enum.with_index(@services), i <- 0..59, do: queue_row(service, s, i)

    Enum.concat(counters) ++ queue
  end

  defp rows("otel_metrics_histogram") do
    cumulative = for {service, s} <- Enum.with_index(@services), do: histogram_rows(service, s)

    delta =
      for {service, s} <- Enum.with_index(@services),
          i <- 0..59,
          do: delta_histogram_row(service, s, i)

    Enum.concat(cumulative) ++ delta
  end

  defp counter_rows(service, s, status) do
    0..59
    |> Enum.map_reduce(0, fn i, total ->
      total = counter_total(service, i, total, counter_increment(s, status, i))

      {counter_row(service, status, i, total), total}
    end)
    |> elem(0)
  end

  defp counter_increment(s, "500", i), do: trunc(pseudo(s, i) * 3)
  defp counter_increment(s, _status, i), do: 5 + trunc(pseudo(s + 3, i) * 20)

  defp counter_total("worker", 30, _total, increment), do: increment
  defp counter_total(_service, _minute, total, increment), do: total + increment

  defp counter_row(service, status, i, total) do
    attrs = %{"status" => status, "route" => "/v1/query"}

    service
    |> common("http.server.request.count", "{request}", "Requests served", attrs, i)
    |> Map.merge(%{"Value" => total * 1.0, "AggregationTemporality" => 2, "IsMonotonic" => true})
  end

  defp queue_row(service, s, i) do
    attrs = %{"queue" => "default"}

    service
    |> common("queue.enqueued", "{job}", "Jobs enqueued per interval", attrs, i)
    |> Map.merge(%{
      "Value" => 1.0 + trunc(pseudo(s + 5, i) * 10),
      "AggregationTemporality" => 1,
      "IsMonotonic" => false
    })
  end

  defp histogram_rows(service, s) do
    0..59
    |> Enum.map_reduce({List.duplicate(0, 9), 0, 0.0}, fn i, {counts, count, total} ->
      increments = Enum.map(0..8, &bucket_increment(s, &1, i))
      counts = Enum.zip_with(counts, increments, &(&1 + &2))
      count = count + Enum.sum(increments)
      total = total + Enum.sum(increments) * (20.0 + s * 5)

      {histogram_row(service, i, counts, count, total), {counts, count, total}}
    end)
    |> elem(0)
  end

  defp bucket_increment(s, b, i) when b in 2..4, do: trunc(pseudo(s + b, i) * 12)
  defp bucket_increment(s, b, i), do: trunc(pseudo(s + b, i) * 3)

  defp histogram_row(service, i, counts, count, total) do
    service
    |> common("http.server.duration", "ms", "Request duration", %{"route" => "/v1/query"}, i)
    |> Map.merge(%{
      "Count" => count,
      "Sum" => Float.round(total, 2),
      "BucketCounts" => counts,
      "ExplicitBounds" => @bounds,
      "Min" => 1.5,
      "Max" => 980.0,
      "AggregationTemporality" => 2
    })
  end

  defp delta_histogram_row(service, s, i) do
    counts = Enum.map(0..8, fn b -> trunc(pseudo(s + b + 1, i) * 5) end)

    service
    |> common("db.client.duration", "ms", "Query duration, delta", %{"db" => "lake"}, i)
    |> Map.merge(%{
      "Count" => Enum.sum(counts),
      "Sum" => Enum.sum(counts) * 30.0,
      "BucketCounts" => counts,
      "ExplicitBounds" => @bounds,
      "Min" => 2.0,
      "Max" => 700.0,
      "AggregationTemporality" => 1
    })
  end

  defp run(%{name: name, fixture: fixture}, step) do
    %{"sql" => sql, "format" => format, "params" => params} =
      Enum.find(fixture["steps"], &(&1["step"] == step))

    query =
      fixture["settings"]
      |> Map.merge(Map.new(params, fn {key, value} -> {"param_" <> key, value} end))
      |> Map.put("query_id", "hyperdx-metrics-#{step}")

    response =
      conn(:post, "/?" <> URI.encode_query(query), sql <> " \nFORMAT " <> format)
      |> put_req_header("x-clickhouse-user", "default")
      |> put_req_header("x-clickhouse-key", @password)
      |> Router.call(name)

    assert response.status == 200, "#{step}: #{response.resp_body}"

    JSON.decode!(response.resp_body)
  end

  defp types(%{"meta" => meta}), do: Enum.map(meta, & &1["type"])
  defp names(%{"meta" => meta}), do: Enum.map(meta, & &1["name"])

  defp values(%{"data" => data}, column), do: Enum.map(data, &to_number(&1[column]))

  defp to_number(value) when is_binary(value) do
    case Integer.parse(value) do
      {integer, ""} -> integer
      _not_an_integer -> String.to_float(value)
    end
  end

  defp to_number(value), do: value

  test "the metric catalog, name search, and a metric's unit, attributes and values", context do
    assert run(context, "catalog_gauge")["data"] == [
             %{
               "MetricName" => "process.runtime.memory",
               "MetricUnit" => "By",
               "MetricDescription" => "Resident memory"
             },
             %{
               "MetricName" => "system.cpu.utilization",
               "MetricUnit" => "1",
               "MetricDescription" => "CPU busy fraction"
             }
           ]

    assert Enum.map(run(context, "catalog_sum")["data"], & &1["MetricName"]) ==
             ["http.server.request.count", "queue.enqueued"]

    assert Enum.map(run(context, "catalog_histogram")["data"], & &1["MetricName"]) ==
             ["db.client.duration", "http.server.duration"]

    assert run(context, "metric_metadata")["data"] ==
             [%{"MetricUnit" => "1", "MetricDescription" => "CPU busy fraction"}]

    attributes = run(context, "metric_attributes")
    assert attributes["rows"] == 6
    assert Enum.map(attributes["meta"], & &1["type"]) == List.duplicate("Map(String, String)", 3)

    assert run(context, "metric_attribute_values")["data"] == [
             %{"value" => "0"},
             %{"value" => "1"}
           ]
  end

  test "gauge charts: the last value per series and bucket, averaged, maxed, filtered, grouped, as a delta",
       context do
    averaged = run(context, "gauge_avg_by_service")

    assert types(averaged) == ["Float64", "Nullable(String)", "DateTime64(6)"]
    assert names(averaged) |> Enum.drop(1) == ["ServiceName", "__hdx_time_bucket"]
    assert averaged["rows"] == 36
    assert Enum.all?(values(averaged, hd(names(averaged))), &(&1 >= 0.2 and &1 <= 0.8))

    assert hd(averaged["data"])["__hdx_time_bucket"] == "2026-10-15T11:00:00.000000Z"

    maxed = run(context, "gauge_max_number")
    assert types(maxed) == ["Nullable(Float64)"]
    assert [max] = values(maxed, hd(names(maxed)))
    assert max > 0.7 and max <= 0.8

    filtered = run(context, "gauge_avg_filtered")
    assert types(filtered) == ["Float64", "DateTime64(6)"]
    assert filtered["rows"] == 12

    by_attribute = run(context, "gauge_by_attribute")
    assert types(by_attribute) == ["Float64", "Nullable(String)", "DateTime64(6)"]

    assert by_attribute["data"]
           |> Enum.map(& &1["Attributes['cpu']"])
           |> Enum.uniq()
           |> Enum.sort() == ["0", "1"]

    delta = run(context, "gauge_delta")
    assert types(delta) == ["Float64", "Nullable(String)", "DateTime64(6)"]
    assert Enum.all?(values(delta, hd(names(delta))), &(&1 > 0))
  end

  test "sum charts: a counter's rate per bucket, its increase with top groups, and its last cumulative value",
       context do
    rate = run(context, "sum_rate_by_service")

    assert names(rate) == ["Value", "ServiceName", "__hdx_time_bucket"]
    assert types(rate) == ["Float64", "Nullable(String)", "DateTime64(6)"]
    assert rate["rows"] == 36
    assert Enum.all?(values(rate, "Value"), &(&1 >= 0))

    increase = run(context, "sum_increase_by_service")
    assert types(increase) == ["Float64", "Nullable(String)", "DateTime64(6)"]
    assert increase["rows"] == 36
    assert Enum.sum(values(increase, "Value")) == Enum.sum(values(rate, "Value"))

    number = run(context, "sum_increase_number")
    assert types(number) == ["Nullable(Float64)"]
    assert [total] = values(number, "Value")
    assert total == Enum.sum(values(rate, "Value"))

    last = run(context, "sum_last_value")
    assert types(last) == ["Nullable(Float64)", "Nullable(String)", "DateTime64(6)"]
    assert Enum.all?(values(last, "Value"), &(&1 > 0))

    delta = run(context, "sum_delta_temporality")
    assert types(delta) == ["Float64", "DateTime64(6)"]
    assert delta["rows"] == 12
    assert Enum.all?(values(delta, "Value"), &(&1 >= 15 and &1 <= 150))
  end

  test "histogram charts: quantiles from bucket deltas, and counts", context do
    p95 = run(context, "histogram_p95_by_service")

    assert names(p95) == ["__hdx_time_bucket", "group", "Value"]
    assert types(p95) == ["DateTime64(6)", "Array(Nullable(String))", "Nullable(Float64)"]

    assert p95["data"] |> Enum.map(& &1["group"]) |> Enum.uniq() |> Enum.sort() ==
             Enum.map(Enum.sort(@services), &[&1])

    assert Enum.all?(values(p95, "Value"), &(&1 > 25 and &1 <= 1000)),
           "a p95 in the overflow bucket answers the last explicit bound, as HyperDX's SQL says"

    p50 = run(context, "histogram_p50_number")
    assert types(p50) == ["Nullable(Float64)"]
    assert [median] = values(p50, "Value")
    assert median > 10 and median < 100

    count = run(context, "histogram_count")
    assert names(count) == ["__hdx_time_bucket", "Value"]
    assert types(count) == ["DateTime64(6)", "Nullable(Int128)"]
    assert count["rows"] == 12
    assert Enum.sum(values(count, "Value")) == expected_histogram_count()

    by_service = run(context, "histogram_count_by_service")
    assert types(by_service) == ["DateTime64(6)", "Array(Nullable(String))", "Nullable(Int128)"]
    assert by_service["rows"] == 36
    assert Enum.sum(values(by_service, "Value")) == expected_histogram_count()
  end

  defp expected_histogram_count do
    rows = rows("otel_metrics_histogram")

    for service <- @services do
      counts =
        for row <- rows,
            row["ServiceName"] == service and row["MetricName"] == "http.server.duration",
            do: row["Count"]

      List.last(counts) - hd(counts)
    end
    |> Enum.sum()
  end

  test "multi-series charts: two metrics pivoted per bucket, a ratio, a formula, and a histogram beside a gauge",
       context do
    pivot = run(context, "multi_gauge_sum")

    assert names(pivot) == ["cpu", "reqs", "ServiceName", "__hdx_time_bucket"]

    assert types(pivot) == [
             "Nullable(Float64)",
             "Nullable(Float64)",
             "Nullable(String)",
             "DateTime64(6)"
           ]

    assert pivot["rows"] == 36
    assert Enum.all?(values(pivot, "cpu"), &(&1 >= 0.2 and &1 <= 0.8))

    ratio = run(context, "multi_ratio")
    assert types(ratio) == ["Nullable(Float64)", "DateTime64(6)"]
    assert ratio["rows"] == 12
    assert Enum.all?(values(ratio, hd(names(ratio))), &(&1 >= 0 and &1 <= 1))

    formula = run(context, "multi_formula")
    assert Enum.at(names(formula), 2) == "error_pct"

    assert types(formula) == [
             "Nullable(Float64)",
             "Nullable(Float64)",
             "Nullable(Float64)",
             "DateTime64(6)"
           ]

    assert Enum.all?(values(formula, "error_pct"), &(&1 >= 0 and &1 <= 100))

    packed = run(context, "multi_gauge_histogram")

    assert types(packed) == [
             "Nullable(Float64)",
             "Nullable(Float64)",
             "Nullable(String)",
             "DateTime64(6)",
             "Array(Nullable(String))"
           ]

    assert packed["rows"] == 72
  end
end
