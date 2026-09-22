defmodule SmolqueryVictoriaMetrics.ResponseTest do
  use ExUnit.Case, async: true

  alias SmolqueryVictoriaMetrics.Eval.Series
  alias SmolqueryVictoriaMetrics.Response

  @inf 1.797_693_134_862_315_7e308
  @stats %{series: 2, duration_ms: 7}

  defp decode(iodata), do: iodata |> IO.iodata_to_binary() |> JSON.decode!()

  describe "value/1 writes floats as Go's FormatFloat(v, 'f', -1, 64)" do
    test "the fewest digits, no exponent, no trailing .0" do
      for {value, text} <- [
            {1.0, "1"},
            {0.1, "0.1"},
            {-2.5, "-2.5"},
            {0.0, "0"},
            {-0.0, "-0"},
            {100.0, "100"},
            {1.0e21, "1000000000000000000000"},
            {1.5e22, "15000000000000000000000"},
            {1.0e-7, "0.0000001"},
            {1.2345e-5, "0.000012345"},
            {123_456.789, "123456.789"},
            {0.30000000000000004, "0.30000000000000004"},
            {9.007_199_254_740_993e15, "9007199254740992"}
          ] do
        assert Response.value(value) == text, inspect(value)
      end
    end

    test "NaN, and the largest double as the infinity it stands for" do
      assert Response.value(nil) == "NaN"
      assert Response.value(@inf) == "+Inf"
      assert Response.value(-@inf) == "-Inf"
      assert Response.value(1.797_693_134_862_315_5e308) =~ ~r/\A17976931348623155\d+\z/
    end
  end

  test "timestamp/1 writes seconds, with a fraction only when there is one" do
    assert Response.timestamp(1_695_000_000_000) == "1695000000"
    assert Response.timestamp(1_695_000_000_500) == "1695000000.5"
    assert Response.timestamp(1_695_000_000_050) == "1695000000.05"
    assert Response.timestamp(1_695_000_000_001) == "1695000000.001"
    assert Response.timestamp(0) == "0"
  end

  test "matrix/2 leaves out the points with no value and the series with none" do
    series = [
      %Series{
        labels: %{"job" => "b", "__name__" => "up", "a" => "1"},
        values: [{1_000, 1.0}, {2_000, nil}, {3_000, 0.5}]
      },
      %Series{labels: %{"job" => "c"}, values: [{1_000, nil}]}
    ]

    body = series |> Response.matrix(@stats) |> IO.iodata_to_binary()

    assert body =~ ~s|{"metric":{"__name__":"up","a":"1","job":"b"},"values":[[1,"1"],[3,"0.5"]]}|

    assert decode(body) == %{
             "status" => "success",
             "isPartial" => false,
             "data" => %{
               "resultType" => "matrix",
               "result" => [
                 %{
                   "metric" => %{"__name__" => "up", "a" => "1", "job" => "b"},
                   "values" => [[1, "1"], [3, "0.5"]]
                 }
               ]
             },
             "stats" => %{"seriesFetched" => "2", "executionTimeMsec" => 7}
           }
  end

  test "vector/2 answers each series' point" do
    series = [
      %Series{labels: %{"job" => "a"}, values: [{1_500, 2.0}]},
      %Series{labels: %{"job" => "b"}, values: [{1_500, nil}]}
    ]

    assert %{"data" => %{"resultType" => "vector", "result" => result}} =
             decode(Response.vector(series, @stats))

    assert result == [%{"metric" => %{"job" => "a"}, "value" => [1.5, "2"]}]
  end

  test "scalar/2 answers one point" do
    assert %{"data" => %{"resultType" => "scalar", "result" => [1, "42"]}} =
             decode(Response.scalar({1_000, 42.0}, @stats))

    assert %{"data" => %{"result" => [1, "NaN"]}} = decode(Response.scalar({1_000, nil}, @stats))
  end
end
