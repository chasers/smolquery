defmodule Smolquery.QueryService.RunnerTest do
  use ExUnit.Case, async: true

  alias Smolquery.QueryService.Plan
  alias Smolquery.QueryService.Runner

  describe "classify/3" do
    @url "http://buffer-2.invalid:4001/v1/datasets/analytics/tables/events/segments/01ABC.parquet"

    defp plan(hot) do
      %Plan{sql: "SELECT 1", snapshot: 1, hot: hot}
    end

    test "a failed hot attach is the hot tier being unreachable" do
      hot = %{{"analytics", "events"} => [%{"url" => @url}]}

      statement =
        "CREATE TEMP TABLE hot AS SELECT * FROM read_parquet(['#{@url}'], union_by_name := true)"

      reason = %RuntimeError{message: "IO Error: connection refused"}

      assert Runner.classify(plan(hot), statement, reason) == {:hot_tier_unavailable, reason}
    end

    test "a failed statement that merely mentions the url passes through" do
      hot = %{{"analytics", "events"} => [%{"url" => @url}]}
      statement = "SET allowed_paths = ['#{@url}']"

      assert Runner.classify(plan(hot), statement, :boom) == :boom
    end

    test "a failed statement outside the hot tier passes through" do
      assert Runner.classify(plan(%{}), "SELECT nope", :boom) == :boom
    end
  end
end
