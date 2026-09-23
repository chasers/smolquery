defmodule SmolqueryVictoriaMetrics.QueryTest do
  @moduledoc """
  `/api/v1/query` and `/api/v1/query_range` end to end: samples written
  through the edge's remote write, read back through the router, a real
  query service and a real buffer.
  """

  use ExUnit.Case, async: false

  import Plug.Conn, only: [get_resp_header: 2]

  alias Smolquery.Test.VictoriaMetricsStack
  alias SmolqueryVictoriaMetrics.Query
  alias SmolqueryVictoriaMetrics.Runtime

  @moduletag :tmp_dir
  @moduletag :capture_log

  @t0 1_789_812_000
  @t0_ms @t0 * 1000
  @inf 1.797_693_134_862_315_7e308

  setup context do
    stack = VictoriaMetricsStack.start(context)

    counter =
      for i <- 0..20 do
        value = if i < 10, do: 6 * i, else: 3 + 6 * (i - 10)
        {@t0_ms + i * 15_000, value}
      end

    :ok =
      VictoriaMetricsStack.write(stack, [
        {%{"__name__" => "up", "job" => "a"}, for(i <- 0..20, do: {@t0_ms + i * 15_000, 1})},
        {%{"__name__" => "up", "job" => "b"}, for(i <- 0..20, do: {@t0_ms + i * 15_000, 0})},
        {%{"__name__" => "http_requests_total", "job" => "api"}, counter},
        {%{"__name__" => "gauge"},
         [
           {@t0_ms, 0.1},
           {@t0_ms + 15_000, 1.0e21},
           {@t0_ms + 30_000, @inf},
           {@t0_ms + 60_000, -@inf},
           {@t0_ms + 75_000, 3.0}
         ]}
      ])

    handler = "vm-query-test-#{stack.name}"
    test = self()

    :telemetry.attach(
      handler,
      [:smolquery, :victoriametrics, :query],
      fn _event, measurements, _meta, _config -> send(test, {:query, measurements}) end,
      nil
    )

    on_exit(fn -> :telemetry.detach(handler) end)

    %{stack: stack}
  end

  defp get(stack, path, params) do
    VictoriaMetricsStack.request(stack, :get, path <> "?" <> URI.encode_query(params))
  end

  defp body(response), do: JSON.decode!(response.resp_body)

  defp limited(stack, limits) do
    name = :"#{stack.name}_limited_#{:erlang.unique_integer([:positive])}"
    Runtime.put(struct!(%{stack.runtime | name: name}, limits))
    on_exit(fn -> Runtime.delete(name) end)
    %{stack | name: name}
  end

  describe "/api/v1/query" do
    test "a selector answers a vector, __name__ kept", %{stack: stack} do
      response = get(stack, "/api/v1/query", %{"query" => "up", "time" => "#{@t0 + 100}"})

      assert response.status == 200
      assert get_resp_header(response, "content-type") == ["application/json; charset=utf-8"]

      assert body(response) == %{
               "status" => "success",
               "isPartial" => false,
               "data" => %{
                 "resultType" => "vector",
                 "result" => [
                   %{
                     "metric" => %{"__name__" => "up", "job" => "a"},
                     "value" => [@t0 + 100, "1"]
                   },
                   %{"metric" => %{"__name__" => "up", "job" => "b"}, "value" => [@t0 + 100, "0"]}
                 ]
               },
               "stats" => %{
                 "seriesFetched" => "2",
                 "executionTimeMsec" => body(response)["stats"]["executionTimeMsec"]
               }
             }

      assert_receive {:query, %{series: 2, samples: samples, duration_us: us, fetch_us: fetch}}
      assert samples > 0 and fetch > 0 and us >= fetch
    end

    test "offset, back and forward", %{stack: stack} do
      back =
        get(stack, "/api/v1/query", %{
          "query" => "http_requests_total offset 1m",
          "time" => "#{@t0 + 120}"
        })

      at = @t0 + 120
      assert %{"data" => %{"result" => [%{"value" => [^at, "24"]}]}} = body(back)

      forward =
        get(stack, "/api/v1/query", %{
          "query" => "http_requests_total offset -1m",
          "time" => "#{@t0 + 60}"
        })

      assert %{"data" => %{"result" => [%{"value" => [_t, "48"]}]}} = body(forward)
    end

    test "a bare range vector answers its raw samples as a matrix", %{stack: stack} do
      response =
        get(stack, "/api/v1/query", %{
          "query" => ~s|up{job="a"}[30s]|,
          "time" => "#{@t0 + 30}"
        })

      assert %{"data" => %{"resultType" => "matrix", "result" => [series]}} = body(response)
      assert series["metric"] == %{"__name__" => "up", "job" => "a"}
      assert series["values"] == [[@t0 + 15, "1"], [@t0 + 30, "1"]]
    end

    test "a number answers a scalar", %{stack: stack} do
      response = get(stack, "/api/v1/query", %{"query" => "42", "time" => "#{@t0}.5"})

      assert %{"data" => %{"resultType" => "scalar", "result" => [1_789_812_000.5, "42"]}} =
               body(response)
    end

    test "an unknown metric answers an empty success", %{stack: stack} do
      response = get(stack, "/api/v1/query", %{"query" => "nothing_here", "time" => "#{@t0}"})

      assert response.status == 200
      assert %{"status" => "success", "data" => %{"result" => []}} = body(response)
    end

    test "a POST reads a form-encoded body, as Grafana sends it", %{stack: stack} do
      response =
        VictoriaMetricsStack.request(
          stack,
          :post,
          "/api/v1/query",
          URI.encode_query(%{"query" => ~s|up{job="a"}|, "time" => "#{@t0 + 30}"}),
          [{"content-type", "application/x-www-form-urlencoded"}]
        )

      assert %{"data" => %{"result" => [%{"value" => [_t, "1"]}]}} = body(response)
    end

    test "answers under the /prometheus and /select/<n>/prometheus prefixes", %{stack: stack} do
      for prefix <- ["/prometheus", "/select/0/prometheus"] do
        response = get(stack, prefix <> "/api/v1/query", %{"query" => "up", "time" => "#{@t0}"})
        assert response.status == 200, prefix
      end
    end
  end

  describe "/api/v1/query_range" do
    test "rate() over a counter: no extrapolation, the sample before the window counts, a reset added back",
         %{stack: stack} do
      response =
        get(stack, "/api/v1/query_range", %{
          "query" => "rate(http_requests_total[1m])",
          "start" => "#{@t0 + 60}",
          "end" => "#{@t0 + 180}",
          "step" => "60"
        })

      assert %{
               "data" => %{
                 "resultType" => "matrix",
                 "result" => [%{"metric" => %{"job" => "api"}, "values" => values}]
               }
             } = body(response)

      assert values == [[@t0 + 60, "0.4"], [@t0 + 120, "0.4"], [@t0 + 180, "0.35"]]
    end

    test "floats as Prometheus writes them, gaps left out, infinities answered", %{stack: stack} do
      response =
        get(stack, "/api/v1/query_range", %{
          "query" => "last_over_time(gauge[10s])",
          "start" => "#{@t0}",
          "end" => "#{@t0 + 90}",
          "step" => "15s"
        })

      assert %{
               "data" => %{
                 "result" => [%{"metric" => %{"__name__" => "gauge"}, "values" => values}]
               }
             } =
               body(response)

      assert values == [
               [@t0, "0.1"],
               [@t0 + 15, "1000000000000000000000"],
               [@t0 + 30, "+Inf"],
               [@t0 + 60, "-Inf"],
               [@t0 + 75, "3"]
             ]
    end

    test "a scalar answers one series with no labels", %{stack: stack} do
      response =
        get(stack, "/api/v1/query_range", %{
          "query" => "1.5",
          "start" => "#{@t0}",
          "end" => "#{@t0 + 30}",
          "step" => "15"
        })

      assert %{"data" => %{"result" => [%{"metric" => %{}, "values" => values}]}} = body(response)
      assert values == [[@t0, "1.5"], [@t0 + 15, "1.5"], [@t0 + 30, "1.5"]]
    end
  end

  describe "refusals" do
    test "a query past max_query_bytes, or not UTF-8, is 400 bad_data before it is parsed", %{
      stack: stack
    } do
      long = limited(stack, max_query_bytes: 16)

      response = get(long, "/api/v1/query", %{"query" => String.duplicate("a+", 20) <> "a"})

      assert response.status == 400
      assert %{"errorType" => "bad_data", "error" => error} = body(response)
      assert error =~ "past the 16-byte limit"
      assert error =~ "SMOLQUERY_VICTORIAMETRICS_MAX_QUERY_BYTES"

      response = VictoriaMetricsStack.request(stack, :get, "/api/v1/query?query=%FF")

      assert response.status == 400

      assert %{"errorType" => "bad_data", "error" => "the query is not valid UTF-8"} =
               body(response)
    end

    test "a regex the engine refuses is 422 execution with the engine's message", %{
      stack: stack
    } do
      response = get(stack, "/api/v1/query", %{"query" => ~s|up{job=~"(?=x)"}|})

      assert response.status == 422
      assert %{"errorType" => "execution", "error" => error} = body(response)
      assert error =~ "invalid perl operator: (?="
    end

    test "a window below zero is 422", %{stack: stack} do
      for query <- ["up[5m-10m]", "rate(up[5m-10m])", "max_over_time(up[5m-10m:1m])"] do
        response = get(stack, "/api/v1/query", %{"query" => query})

        assert response.status == 422, query
        assert body(response)["error"] =~ "duration cannot be negative", query
      end
    end

    test "a parse error is 422 execution", %{stack: stack} do
      response = get(stack, "/api/v1/query", %{"query" => "rate(up[5m]"})

      assert response.status == 422
      assert %{"status" => "error", "errorType" => "execution", "error" => error} = body(response)
      assert error =~ "cannot parse the query"
    end

    test "a function not ported is 422, naming it", %{stack: stack} do
      response = get(stack, "/api/v1/query", %{"query" => "holt_winters(up[5m], 0.5, 0.5)"})

      assert response.status == 422

      assert body(response)["error"] ==
               "rollup function holt_winters() is not supported by this edge yet"
    end

    test "an argument of the wrong kind is 422", %{stack: stack} do
      response = get(stack, "/api/v1/query", %{"query" => ~s|label_set(up, "env", 3)|})

      assert response.status == 422
      assert body(response)["error"] == "arg #2 must be a string"
    end

    test "series that collide after an operator are 422", %{stack: stack} do
      response = get(stack, "/api/v1/query", %{"query" => "up + on() up", "time" => "#{@t0}"})

      assert response.status == 422
      assert body(response)["error"] =~ "duplicate time series on the left side of + on()"
    end

    test "a selector with no non-empty matcher is 422", %{stack: stack} do
      response = get(stack, "/api/v1/query", %{"query" => ~s|{job=~".*"}|})

      assert response.status == 422
      assert body(response)["error"] =~ "at least one non-empty matcher"
    end

    test "past max_points_per_series is 422", %{stack: stack} do
      stack = limited(stack, max_points_per_series: 10)

      response =
        get(stack, "/api/v1/query_range", %{
          "query" => "up",
          "start" => "#{@t0}",
          "end" => "#{@t0 + 150}",
          "step" => "15s"
        })

      assert response.status == 422
      assert body(response)["error"] =~ "the maximum number of points is 10"
    end

    test "past max_series is 422", %{stack: stack} do
      stack = limited(stack, max_series: 1)

      response = get(stack, "/api/v1/query", %{"query" => "up", "time" => "#{@t0}"})

      assert response.status == 422
      assert body(response)["error"] =~ "more than 1 series"
    end

    test "a missing query, or a start, end or step that does not read, is 400 bad_data", %{
      stack: stack
    } do
      for {path, params} <- [
            {"/api/v1/query", %{}},
            {"/api/v1/query", %{"query" => "up", "time" => "yesterday"}},
            {"/api/v1/query_range", %{"query" => "up", "start" => "soon"}},
            {"/api/v1/query_range", %{"query" => "up", "end" => "2022-x7"}},
            {"/api/v1/query_range", %{"query" => "up", "step" => "0"}},
            {"/api/v1/query_range", %{"query" => "up", "step" => "-15s"}},
            {"/api/v1/query_range", %{"query" => "up", "step" => "often"}},
            {"/api/v1/query", %{"query" => "up", "timeout" => "never"}}
          ] do
        response = get(stack, path, params)

        assert response.status == 400, inspect(params)
        assert %{"status" => "error", "errorType" => "bad_data"} = body(response)
      end
    end

    test "a query service that is not running is 503 with retry-after", %{stack: stack} do
      stack = limited(stack, query_name: :vm_query_test_no_such_service)

      response = get(stack, "/api/v1/query", %{"query" => "up", "time" => "#{@t0}"})

      assert response.status == 503
      assert get_resp_header(response, "retry-after") == ["5"]
      assert %{"errorType" => "unavailable"} = body(response)
    end
  end

  describe "the evaluator over real samples" do
    test "sum by (job) (rate(...)) aggregates the rollups", %{stack: stack} do
      :ok =
        VictoriaMetricsStack.write(stack, [
          {%{"__name__" => "hits_total", "job" => "web", "instance" => "1"},
           for(i <- 0..20, do: {@t0_ms + i * 15_000, 15 * i})},
          {%{"__name__" => "hits_total", "job" => "web", "instance" => "2"},
           for(i <- 0..20, do: {@t0_ms + i * 15_000, 30 * i})},
          {%{"__name__" => "hits_total", "job" => "db", "instance" => "1"},
           for(i <- 0..20, do: {@t0_ms + i * 15_000, 60 * i})}
        ])

      response =
        get(stack, "/api/v1/query", %{
          "query" => "sum by (job) (rate(hits_total[1m]))",
          "time" => "#{@t0 + 120}"
        })

      assert %{"data" => %{"resultType" => "vector", "result" => result}} = body(response)

      assert result == [
               %{"metric" => %{"job" => "db"}, "value" => [@t0 + 120, "4"]},
               %{"metric" => %{"job" => "web"}, "value" => [@t0 + 120, "3"]}
             ]
    end

    test "histogram_quantile over real buckets, summed by le", %{stack: stack} do
      buckets =
        for {le, per_scrape} <- [{"0.1", 3}, {"0.5", 6}, {"1", 9}, {"+Inf", 10}],
            instance <- ["1", "2"] do
          labels = %{
            "__name__" => "http_request_duration_seconds_bucket",
            "le" => le,
            "instance" => instance
          }

          {labels, for(i <- 0..20, do: {@t0_ms + i * 15_000, per_scrape * i})}
        end

      :ok = VictoriaMetricsStack.write(stack, buckets)

      query =
        "histogram_quantile(0.5, sum by (le) (rate(http_request_duration_seconds_bucket[5m])))"

      response = get(stack, "/api/v1/query", %{"query" => query, "time" => "#{@t0 + 300}"})

      assert %{"data" => %{"result" => [%{"metric" => metric, "value" => [_t, value]}]}} =
               body(response)

      assert metric == %{}
      assert_in_delta String.to_float(value), 0.1 + 0.4 * (1 / 3 - 0.2) / 0.2, 1.0e-9

      bounded =
        get(stack, "/api/v1/query", %{
          "query" =>
            ~s|histogram_quantile(0.99, sum by (le) (rate(http_request_duration_seconds_bucket[5m])), "b")|,
          "time" => "#{@t0 + 300}"
        })

      assert %{"data" => %{"result" => series}} = body(bounded)

      assert Enum.map(series, &{&1["metric"], List.last(&1["value"])}) == [
               {%{}, "1"},
               {%{"b" => "lower"}, "1"},
               {%{"b" => "upper"}, "+Inf"}
             ]
    end

    test "up == 0 filters to the matching series; up == bool 0 answers 0 or 1", %{stack: stack} do
      filtered = get(stack, "/api/v1/query", %{"query" => "up == 0", "time" => "#{@t0 + 60}"})

      assert %{"data" => %{"result" => [%{"metric" => metric, "value" => [_t, "0"]}]}} =
               body(filtered)

      assert metric == %{"__name__" => "up", "job" => "b"}

      bools = get(stack, "/api/v1/query", %{"query" => "up == bool 0", "time" => "#{@t0 + 60}"})

      assert %{"data" => %{"result" => result}} = body(bools)

      assert Enum.map(result, &{&1["metric"], List.last(&1["value"])}) == [
               {%{"job" => "a"}, "0"},
               {%{"job" => "b"}, "1"}
             ]
    end

    test "a / on (job) group_left (team) b copies the label from the one side", %{stack: stack} do
      :ok =
        VictoriaMetricsStack.write(stack, [
          {%{"__name__" => "job_info", "job" => "api", "team" => "core"},
           for(i <- 0..20, do: {@t0_ms + i * 15_000, 2})}
        ])

      response =
        get(stack, "/api/v1/query", %{
          "query" => "http_requests_total / on (job) group_left (team) job_info",
          "time" => "#{@t0 + 60}"
        })

      assert %{"data" => %{"result" => [%{"metric" => metric, "value" => [_t, "12"]}]}} =
               body(response)

      assert metric == %{"job" => "api", "team" => "core"}
    end

    test "label_replace rewrites a label from a capture group", %{stack: stack} do
      response =
        get(stack, "/api/v1/query", %{
          "query" => ~s|label_replace(up, "env", "prod-$1", "job", "(.*)")|,
          "time" => "#{@t0}"
        })

      assert %{"data" => %{"result" => result}} = body(response)

      assert Enum.map(result, & &1["metric"]) == [
               %{"__name__" => "up", "job" => "a", "env" => "prod-a"},
               %{"__name__" => "up", "job" => "b", "env" => "prod-b"}
             ]
    end

    test "max_over_time(rate(m[1m])[10m:1m]) rolls up a subquery", %{stack: stack} do
      response =
        get(stack, "/api/v1/query_range", %{
          "query" => "max_over_time(rate(http_requests_total[1m])[10m:1m])",
          "start" => "#{@t0 + 120}",
          "end" => "#{@t0 + 240}",
          "step" => "60"
        })

      assert %{"data" => %{"resultType" => "matrix", "result" => [series]}} = body(response)
      assert series["metric"] == %{"job" => "api"}
      assert series["values"] == [[@t0 + 120, "0.4"], [@t0 + 180, "0.4"], [@t0 + 240, "0.4"]]
    end

    test "1+1 answers the scalar Grafana's connection test expects", %{stack: stack} do
      response = get(stack, "/api/v1/query", %{"query" => "1+1", "time" => "#{@t0}"})

      assert response.status == 200
      assert %{"data" => %{"resultType" => "scalar", "result" => [@t0, "2"]}} = body(response)

      time = get(stack, "/api/v1/query", %{"query" => "time()", "time" => "#{@t0}"})
      assert %{"data" => %{"resultType" => "scalar", "result" => [@t0, now]}} = body(time)
      assert now == Integer.to_string(@t0)
    end

    test "absent(nonexistent) answers 1, labelled by its = matchers", %{stack: stack} do
      bare = get(stack, "/api/v1/query", %{"query" => "absent(nonexistent)", "time" => "#{@t0}"})

      assert %{
               "data" => %{
                 "resultType" => "vector",
                 "result" => [%{"metric" => %{}, "value" => [_t, "1"]}]
               }
             } =
               body(bare)

      labelled =
        get(stack, "/api/v1/query", %{
          "query" => ~s|absent(nonexistent{job="x", i=~"."})|,
          "time" => "#{@t0}"
        })

      assert %{"data" => %{"result" => [%{"metric" => %{"job" => "x"}}]}} = body(labelled)

      present = get(stack, "/api/v1/query", %{"query" => "absent(up)", "time" => "#{@t0}"})
      assert %{"data" => %{"result" => []}} = body(present)
    end

    test "topk(1, ...) keeps the highest series, name included", %{stack: stack} do
      response = get(stack, "/api/v1/query", %{"query" => "topk(1, up)", "time" => "#{@t0}"})

      assert %{"data" => %{"result" => [%{"metric" => metric, "value" => [_t, "1"]}]}} =
               body(response)

      assert metric == %{"__name__" => "up", "job" => "a"}
    end

    test "an instant subquery answers the inner query's range as a matrix", %{stack: stack} do
      response =
        get(stack, "/api/v1/query", %{"query" => "up[1m:30s]", "time" => "#{@t0 + 120}"})

      assert %{"data" => %{"resultType" => "matrix", "result" => [a, _b]}} = body(response)
      assert a["values"] == [[@t0 + 60, "1"], [@t0 + 90, "1"], [@t0 + 120, "1"]]
    end
  end

  describe "align/4, VictoriaMetrics' AdjustStartEnd" do
    test "a grid of 50 points or more aligns to the step, keeping its count" do
      assert Query.align(1_000_007, 1_000_007 + 49 * 10_000, 10_000, false) ==
               {1_000_000, 1_000_000 + 49 * 10_000}

      assert Query.align(7, 7 + 48 * 10, 10, false) == {7, 7 + 48 * 10}

      assert Query.align(1_000_007, 1_000_007 + 49 * 10_000, 10_000, true) ==
               {1_000_007, 1_000_007 + 49 * 10_000}
    end
  end

  test "failure/1 maps a timeout and a busy query service" do
    assert {503, "timeout", _message, nil} = Query.failure(:timeout)
    assert {503, "unavailable", _message, 1} = Query.failure(:too_many_jobs)

    assert {503, "unavailable", _message, 1} =
             Query.failure({:job, {:hot_tier_unavailable, :econnrefused}})

    assert {422, "execution", "Invalid regex", nil} =
             Query.failure({:job, {:invalid_query, "Invalid regex"}})
  end
end
