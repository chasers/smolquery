defmodule Smolquery.AllocatorTest do
  use ExUnit.Case, async: false

  alias Smolquery.Allocator
  alias Smolquery.DuckDB
  alias Smolquery.Engine.Connection
  alias Smolquery.Engine.Result

  # What a connection sees is its own instance's configured value — the
  # process-wide jemalloc flag is not readable from Elixir — so these tests
  # pin the two places the node applies it: every instance's open, and the
  # keeper's own re-assertion.
  defp configured(database) do
    {:ok, connection} = Connection.start_link(database: database, extensions: [])

    {:ok, result} =
      Connection.query(
        connection,
        "SELECT current_setting('allocator_background_threads')",
        [],
        5_000
      )

    Result.one!(result)
  end

  test "every DuckDB instance opens with the node's allocator flag (T-452)" do
    {:ok, database} = DuckDB.start_link()

    assert configured(database) == true
    assert DuckDB.allocator_background_threads?() == true
  end

  test "a caller's own database option still wins for its instance" do
    {:ok, database} = DuckDB.start_link(allocator_background_threads: "false")

    assert configured(database) == false
  end

  test "the keeper runs, holds an armed instance, and re-asserts on its interval" do
    pid = start_supervised!({Allocator, name: :t452_allocator, interval_ms: 10})

    Process.sleep(50)
    assert Process.alive?(pid)

    %{connection: connection} = :sys.get_state(pid)

    {:ok, result} =
      Connection.query(
        connection,
        "SELECT current_setting('allocator_background_threads')",
        [],
        5_000
      )

    assert Result.one!(result) == true
  end

  test "is :ignore when the node runs with the flag off" do
    assert Allocator.start_link(enabled: false, name: :t452_off) == :ignore
  end
end
