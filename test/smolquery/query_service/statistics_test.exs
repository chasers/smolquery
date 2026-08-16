defmodule Smolquery.QueryService.StatisticsTest do
  use ExUnit.Case, async: true

  alias Smolquery.QueryService.Statistics

  describe "totals" do
    test "sum the two tiers" do
      statistics =
        Statistics.new(
          Statistics.tier(4, 2, 100, 1_000),
          Statistics.tier(3, 3, 900, 2_048)
        )

      assert Statistics.files_total(statistics) == 7
      assert Statistics.files_scanned(statistics) == 5
      assert Statistics.rows_scanned(statistics) == 1_000
      assert Statistics.bytes_scanned(statistics) == 3_048
    end

    test "an empty plan weighs nothing" do
      statistics = Statistics.new(Statistics.tier(0, 0, 0, 0), Statistics.tier(0, 0, 0, 0))

      assert Statistics.files_scanned(statistics) == 0
      assert Statistics.bytes_scanned(statistics) == 0
      assert Statistics.mib_scanned(statistics) == 0.0
    end
  end

  describe "mib_scanned/1" do
    test "converts bytes to MiB, rounded to two decimals" do
      statistics =
        Statistics.new(
          Statistics.tier(1, 1, 1, 1_048_576),
          Statistics.tier(1, 1, 1, 524_288)
        )

      assert Statistics.mib_scanned(statistics) == 1.5
    end
  end
end
