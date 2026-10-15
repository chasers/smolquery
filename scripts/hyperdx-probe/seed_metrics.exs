out = System.get_env("OUT") || "."
now = DateTime.utc_now() |> DateTime.truncate(:second)
services = ["api", "worker", "web"]

ts = fn seconds_ago ->
  now |> DateTime.add(-seconds_ago, :second) |> DateTime.to_naive() |> NaiveDateTime.to_iso8601()
end

common = fn service, name, unit, description, attrs, seconds_ago ->
  %{
    "ResourceAttributes" => %{"service.name" => service, "host.name" => "#{service}-1", "deployment.environment" => "dev"},
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
    "StartTimeUnix" => ts.(3600),
    "TimeUnix" => ts.(seconds_ago),
    "Flags" => 0
  }
end

points = Enum.map(0..359, &(3590 - &1 * 10))
pseudo = fn seed, i -> rem(seed * 7919 + i * 104_729, 1000) / 1000 end

gauge =
  for {service, s} <- Enum.with_index(services), cpu <- [0, 1], t <- points do
    value = 0.2 + 0.6 * pseudo.(s + cpu, t)
    Map.put(common.(service, "system.cpu.utilization", "1", "CPU busy fraction", %{"cpu" => "#{cpu}", "state" => "user"}, t), "Value", Float.round(value, 4))
  end ++
    for {service, s} <- Enum.with_index(services), t <- points do
      Map.put(common.(service, "process.runtime.memory", "By", "Resident memory", %{"type" => "rss"}, t), "Value", 100_000_000 + s * 10_000_000 + (3600 - t) * 1000.0)
    end

sum =
  Enum.flat_map(Enum.with_index(services), fn {service, s} ->
    Enum.flat_map(["200", "500"], fn status ->
      {rows, _total} =
        Enum.map_reduce(points, 0, fn t, total ->
          increment = if status == "500", do: trunc(pseudo.(s, t) * 3), else: 5 + trunc(pseudo.(s + 3, t) * 20)
          total = if service == "worker" and t == 1800, do: increment, else: total + increment
          row = common.(service, "http.server.request.count", "{request}", "Requests served", %{"status" => status, "route" => "/v1/query"}, t)
          {Map.merge(row, %{"Value" => total * 1.0, "AggregationTemporality" => 2, "IsMonotonic" => true}), total}
        end)
      rows
    end)
  end) ++
    for {service, s} <- Enum.with_index(services), t <- points do
      row = common.(service, "queue.enqueued", "{job}", "Jobs enqueued per interval", %{"queue" => "default"}, t)
      Map.merge(row, %{"Value" => 1.0 + trunc(pseudo.(s + 5, t) * 10), "AggregationTemporality" => 1, "IsMonotonic" => false})
    end

bounds = [5.0, 10.0, 25.0, 50.0, 100.0, 250.0, 500.0, 1000.0]

histogram =
  Enum.flat_map(Enum.with_index(services), fn {service, s} ->
    {rows, _acc} =
      Enum.map_reduce(points, {List.duplicate(0, 9), 0, 0.0}, fn t, {counts, count, total} ->
        increments = Enum.map(0..8, fn b -> trunc(pseudo.(s + b, t) * (if b in 2..4, do: 12, else: 3)) end)
        counts = Enum.zip_with(counts, increments, &(&1 + &2))
        added = Enum.sum(increments)
        count = count + added
        total = total + added * (20.0 + s * 5)
        row = common.(service, "http.server.duration", "ms", "Request duration", %{"route" => "/v1/query"}, t)
        {Map.merge(row, %{
           "Count" => count, "Sum" => Float.round(total, 2), "BucketCounts" => counts, "ExplicitBounds" => bounds,
           "Min" => 1.5, "Max" => 980.0, "AggregationTemporality" => 2
         }), {counts, count, total}}
      end)
    rows
  end) ++
    for {service, s} <- Enum.with_index(services), t <- points do
      counts = Enum.map(0..8, fn b -> trunc(pseudo.(s + b + 1, t) * 5) end)
      row = common.(service, "db.client.duration", "ms", "Query duration, delta", %{"db" => "lake"}, t)
      Map.merge(row, %{
        "Count" => Enum.sum(counts), "Sum" => Enum.sum(counts) * 30.0, "BucketCounts" => counts, "ExplicitBounds" => bounds,
        "Min" => 2.0, "Max" => 700.0, "AggregationTemporality" => 1
      })
    end

exponential =
  for {service, s} <- Enum.with_index(services), t <- Enum.take_every(points, 6) do
    row = common.(service, "rpc.client.duration", "ms", "RPC duration", %{"rpc" => "gen_rpc"}, t)
    Map.merge(row, %{
      "Count" => 100 + s, "Sum" => 2500.0, "Scale" => 3, "ZeroCount" => 2, "PositiveOffset" => 10,
      "PositiveBucketCounts" => [5, 10, 20, 30, 20, 10, 3], "NegativeOffset" => 0, "NegativeBucketCounts" => [],
      "Min" => 1.0, "Max" => 300.0, "AggregationTemporality" => 2
    })
  end

summary =
  for {service, _s} <- Enum.with_index(services), t <- Enum.take_every(points, 6) do
    row = common.(service, "gc.pause", "ms", "GC pause summary", %{"gen" => "old"}, t)
    Map.merge(row, %{"Count" => 12, "Sum" => 48.0})
  end

write = fn name, rows ->
  path = Path.join(out, name <> ".ndjson")
  File.write!(path, Enum.map_join(rows, "\n", &JSON.encode!/1) <> "\n")
  IO.puts("#{path}: #{length(rows)} rows")
end

write.("otel_metrics_gauge", gauge)
write.("otel_metrics_sum", sum)
write.("otel_metrics_histogram", histogram)
write.("otel_metrics_exponential_histogram", exponential)
write.("otel_metrics_summary", summary)
