defmodule SmolqueryVictoriaMetrics.Rollup.WindowTest do
  use ExUnit.Case, async: true

  alias SmolqueryVictoriaMetrics.Rollup.Window

  test "new/3 holds exactly the samples given, with the other fields from opts" do
    window = Window.new([1.0, 2.0], [10, 20], prev_value: 0.5, curr_timestamp: 20, window: 15)

    assert %Window{first: 0, last: 2, prev_value: 0.5, curr_timestamp: 20, window: 15} = window
    assert Window.values(window) == [1.0, 2.0]
    assert Window.timestamps(window) == [10, 20]
  end

  test "values/1 and timestamps/1 copy out only the window's range of the series" do
    window = %{Window.new([1.0, 2.0, 3.0, 4.0], [10, 20, 30, 40]) | first: 1, last: 3}

    assert Window.values(window) == [2.0, 3.0]
    assert Window.timestamps(window) == [20, 30]
    assert Window.values(%{window | last: 1}) == []
  end
end
