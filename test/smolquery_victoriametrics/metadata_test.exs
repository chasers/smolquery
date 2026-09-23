defmodule SmolqueryVictoriaMetrics.MetadataTest do
  use ExUnit.Case, async: false

  alias Smolquery.Test.VictoriaMetricsStack
  alias SmolqueryVictoriaMetrics.Metadata
  alias SmolqueryVictoriaMetrics.MetricsQL.Ast.LabelFilter
  alias SmolqueryVictoriaMetrics.MetricsQL.Ast.MetricExpr
  alias SmolqueryVictoriaMetrics.Runtime

  doctest Metadata

  @t0 1_789_812_000
  @t0_ms @t0 * 1000

  describe "range/2, as getCommonParamsForLabelsAPI has it" do
    test "no start or end is the 5 minutes before now, now rounded to the second" do
      assert Metadata.range(%{}, 1_000_000_123) == {:ok, {999_700_000, 1_000_000_000}}
    end

    test "an end without a start is the 5 minutes before it" do
      assert Metadata.range(%{"end" => "1000"}, 5_000_000) == {:ok, {700_000, 1_000_000}}
    end

    test "a start of 0 is taken as missing" do
      assert Metadata.range(%{"start" => "0", "end" => "1000"}, 0) == {:ok, {700_000, 1_000_000}}
    end

    test "both given are kept, and an end before the start is the start" do
      assert Metadata.range(%{"start" => "10", "end" => "20"}, 0) == {:ok, {10_000, 20_000}}
      assert Metadata.range(%{"start" => "30", "end" => "20"}, 0) == {:ok, {30_000, 30_000}}
    end

    test "an unreadable time is bad data" do
      assert {:error, {:bad_data, message}} = Metadata.range(%{"start" => "soon"}, 0)
      assert message =~ "cannot parse start=soon"
    end
  end

  describe "selector/2" do
    test "no match is no selector, or a refusal where one is required" do
      assert Metadata.selector([], false) == {:ok, nil}
      assert Metadata.selector([], true) == {:error, {:bad_data, "missing `match[]` arg"}}
    end

    test "every match's filter sets are pooled, so any may match" do
      assert {:ok, %MetricExpr{filter_sets: [[up], [zone], [other]]}} =
               Metadata.selector([~s(up), ~s({zone="z" or job="x"})], false)

      assert up == %LabelFilter{name: "__name__", op: :eq, value: "up"}
      assert zone == %LabelFilter{name: "zone", op: :eq, value: "z"}
      assert other == %LabelFilter{name: "job", op: :eq, value: "x"}
    end

    test "anything but a non-empty selector is bad data" do
      for {match, reason} <- [
            {"{}", "labelFilterss cannot be empty"},
            {"rate(up[5m])", "expecting metricSelector"},
            {"up[5m]", "expecting metricSelector"},
            {"up{", "cannot parse match[]=up{"}
          ] do
        assert {:error, {:bad_data, message}} = Metadata.selector([match], false), match
        assert message =~ reason, match
      end
    end
  end

  describe "request/4" do
    test "defaults, and match[] repeated with match singular" do
      pairs = [{"match[]", "up"}, {"match", "load"}, {"match[]", "node"}]

      assert {:ok, request} = Metadata.request(:labels, pairs, @t0_ms, 30_000)
      assert request.range == {@t0_ms - 300_000, @t0_ms}
      assert request.limit == 0
      assert request.opts == [timeout_ms: 30_000]
      assert [["up"], ["node"], ["load"]] = names(request.selector)
    end

    test "limit and timeout are read, the body's pairs first" do
      pairs = [{"limit", "5"}, {"timeout", "2s"}, {"limit", "9"}]

      assert {:ok, %{limit: 5, opts: [timeout_ms: 2_000], selector: nil}} =
               Metadata.request(:labels, pairs, @t0_ms, 30_000)
    end

    test "a timeout past the ceiling is the ceiling" do
      for timeout <- ["1000h", "100y"] do
        assert {:ok, %{opts: [timeout_ms: 30_000]}} =
                 Metadata.request(:labels, [{"timeout", timeout}], @t0_ms, 30_000)
      end
    end

    test "an unreadable limit or timeout is bad data" do
      assert {:error, {:bad_data, ~s(cannot parse integer "limit"="ten")}} =
               Metadata.request(:labels, [{"limit", "ten"}], @t0_ms, 30_000)

      assert {:error, {:bad_data, _message}} =
               Metadata.request(:labels, [{"timeout", "-1"}], @t0_ms, 30_000)
    end

    test "series requires a match" do
      assert {:error, {:bad_data, "missing `match[]` arg"}} =
               Metadata.request(:series, [], @t0_ms, 30_000)
    end
  end

  describe "label_name/1" do
    test "a legacy name is kept, and other routes pass" do
      assert Metadata.label_name({:label_values, "__name__"}) ==
               {:ok, {:label_values, "__name__"}}

      assert Metadata.label_name({:label_values, "job_2"}) == {:ok, {:label_values, "job_2"}}
      assert Metadata.label_name(:labels) == {:ok, :labels}
    end

    test "U__ names are unescaped as Prometheus escapes them" do
      for {escaped, name} <- [
            {"U__http_2e_method", "http.method"},
            {"U__a__b", "a_b"},
            {"U___1f600_", "😀"},
            {"U__bad_zz_", "U__bad_zz_"},
            {"U__trailing_", "U__trailing_"},
            {"U___d800_", "U___d800_"}
          ] do
        assert Metadata.label_name({:label_values, escaped}) == {:ok, {:label_values, name}},
               escaped
      end
    end

    test "anything else is bad data" do
      for name <- ["1job", "job-name", "", "a.b", "%C3%A9"] do
        assert {:error, {:bad_data, "invalid label name " <> _quoted}} =
                 Metadata.label_name({:label_values, name}),
               name
      end
    end
  end

  defp names(%MetricExpr{filter_sets: sets}),
    do: Enum.map(sets, fn filters -> Enum.map(filters, & &1.value) end)

  describe "end to end" do
    @describetag :tmp_dir
    @describetag :capture_log

    setup context do
      stack = VictoriaMetricsStack.start(context)

      :ok =
        VictoriaMetricsStack.write(stack, [
          {%{"__name__" => "up", "job" => "node", "instance" => "i2"}, [{@t0_ms, 1}]},
          {%{"__name__" => "up", "job" => "api", "instance" => "i1"}, [{@t0_ms + 1_000, 1}]},
          {%{"__name__" => "load", "zone" => "z"}, [{@t0_ms + 2_000, 0.5}]},
          {%{"__name__" => "http_requests_total", "job" => "api", "code" => "200"},
           [{@t0_ms + 3_000, 7}]}
        ])

      %{stack: stack}
    end

    defp window(params),
      do: Map.merge(%{"start" => "#{@t0}", "end" => "#{@t0 + 60}"}, Map.new(params))

    defp get(stack, path, params) do
      query = params |> window() |> URI.encode_query()
      VictoriaMetricsStack.request(stack, :get, path <> "?" <> query)
    end

    defp get_raw(stack, path, query),
      do: VictoriaMetricsStack.request(stack, :get, path <> "?" <> query)

    defp data(response) do
      assert response.status == 200, response.resp_body
      assert %{"status" => "success", "data" => data} = JSON.decode!(response.resp_body)
      data
    end

    defp error(response, status) do
      assert response.status == status, response.resp_body
      JSON.decode!(response.resp_body)
    end

    test "/api/v1/labels: sorted, narrowed by match[]", %{stack: stack} do
      assert data(get(stack, "/api/v1/labels", [])) ==
               ["__name__", "code", "instance", "job", "zone"]

      assert data(get(stack, "/api/v1/labels", [{"match[]", "up"}])) ==
               ["__name__", "instance", "job"]

      assert data(get(stack, "/api/v1/labels", [{"limit", "2"}])) == ["__name__", "code"]
    end

    test "/api/v1/labels: two match[] are ORed", %{stack: stack} do
      query = "match[]=load&match[]=http_requests_total&start=#{@t0}&end=#{@t0 + 60}"

      assert data(get_raw(stack, "/api/v1/labels", query)) ==
               ["__name__", "code", "job", "zone"]
    end

    test "/api/v1/label/<name>/values with and without match[]", %{stack: stack} do
      assert data(get(stack, "/api/v1/label/job/values", [])) == ["api", "node"]

      assert data(get(stack, "/api/v1/label/job/values", [{"match[]", ~s(up{instance="i2"})}])) ==
               ["node"]

      assert data(get(stack, "/api/v1/label/instance/values", [{"limit", "1"}])) == ["i1"]
    end

    test "/api/v1/label/__name__/values", %{stack: stack} do
      assert data(get(stack, "/api/v1/label/__name__/values", [])) ==
               ["http_requests_total", "load", "up"]

      assert data(get(stack, "/api/v1/label/__name__/values", [{"match[]", ~s({job="api"})}])) ==
               ["http_requests_total", "up"]
    end

    test "an invalid label name is 400", %{stack: stack} do
      assert %{"errorType" => "bad_data", "error" => "invalid label name \"job-name\""} =
               error(get(stack, "/api/v1/label/job-name/values", []), 400)
    end

    test "/api/v1/series for one matcher and for two, sorted, __name__ first", %{stack: stack} do
      one = get(stack, "/api/v1/series", [{"match[]", "up"}])

      assert one.resp_body =~ ~s({"__name__":"up","instance":"i1","job":"api"})

      assert data(one) == [
               %{"__name__" => "up", "instance" => "i1", "job" => "api"},
               %{"__name__" => "up", "instance" => "i2", "job" => "node"}
             ]

      query = "match[]=up&match[]=load&start=#{@t0}&end=#{@t0 + 60}"

      assert data(get_raw(stack, "/api/v1/series", query)) == [
               %{"__name__" => "load", "zone" => "z"},
               %{"__name__" => "up", "instance" => "i1", "job" => "api"},
               %{"__name__" => "up", "instance" => "i2", "job" => "node"}
             ]

      assert data(get(stack, "/api/v1/series", [{"match[]", "up"}, {"limit", "1"}])) == [
               %{"__name__" => "up", "instance" => "i1", "job" => "api"}
             ]
    end

    test "/api/v1/series takes a form-encoded POST", %{stack: stack} do
      response =
        VictoriaMetricsStack.request(
          stack,
          :post,
          "/api/v1/series",
          "match%5B%5D=load&start=#{@t0}&end=#{@t0 + 60}",
          [{"content-type", "application/x-www-form-urlencoded"}]
        )

      assert data(response) == [%{"__name__" => "load", "zone" => "z"}]
    end

    test "/api/v1/series without match[] is 400, past max_series 422", %{stack: stack} do
      assert %{"errorType" => "bad_data", "error" => "missing `match[]` arg"} =
               error(get(stack, "/api/v1/series", []), 400)

      name = :"#{stack.name}_limited"
      Runtime.put(%{stack.runtime | name: name, max_series: 1})
      on_exit(fn -> Runtime.delete(name) end)

      assert %{"errorType" => "execution", "error" => message} =
               error(get(%{stack | name: name}, "/api/v1/series", [{"match[]", "up"}]), 422)

      assert message =~ "more than 1 series"
    end

    test "an empty range answers empty lists", %{stack: stack} do
      empty = [{"start", "#{@t0 - 600}"}, {"end", "#{@t0 - 60}"}]

      assert data(get(stack, "/api/v1/labels", empty)) == []
      assert data(get(stack, "/api/v1/label/job/values", empty)) == []
      assert data(get(stack, "/api/v1/label/__name__/values", empty)) == []
      assert data(get(stack, "/api/v1/series", [{"match[]", "up"} | empty])) == []
    end

    test "a match[] past max_query_bytes, or not UTF-8, is 400 bad_data", %{stack: stack} do
      name = :"#{stack.name}_short_#{:erlang.unique_integer([:positive])}"
      Runtime.put(%{stack.runtime | name: name, max_query_bytes: 8})
      on_exit(fn -> Runtime.delete(name) end)
      short = %{stack | name: name}

      assert %{"errorType" => "bad_data", "error" => error} =
               error(get(short, "/api/v1/series", [{"match[]", "up{job=\"node\"}"}]), 400)

      assert error =~ "match[]: the query is 14 bytes, past the 8-byte limit"

      for path <- ["/api/v1/series", "/api/v1/labels"] do
        assert %{"error" => "match[]: the query is not valid UTF-8"} =
                 error(get_raw(stack, path, "match[]=%FF"), 400)
      end
    end

    test "a bad match[] is 400, one matching everything 422", %{stack: stack} do
      assert %{"errorType" => "bad_data"} =
               error(get(stack, "/api/v1/labels", [{"match[]", "sum(up)"}]), 400)

      assert %{"errorType" => "execution"} =
               error(get(stack, "/api/v1/labels", [{"match[]", ~s({job=~".*"})}]), 422)
    end
  end
end
