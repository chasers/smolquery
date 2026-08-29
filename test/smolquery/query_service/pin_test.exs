defmodule Smolquery.QueryService.PinTest do
  use ExUnit.Case, async: false

  alias Smolquery.QueryService
  alias Smolquery.QueryService.Client
  alias Smolquery.QueryService.Runtime
  alias Smolquery.Test.FixedCatalog

  setup do
    name = :"query_pin_#{:erlang.unique_integer([:positive])}"

    start_supervised!(
      {QueryService.Supervisor,
       name: name, catalog: FixedCatalog.new(%{snapshot: 7, schemas: %{}, segments: %{}})},
      id: name
    )

    on_exit(fn -> Runtime.delete(name) end)

    %{name: name}
  end

  test "snapshot: pins the plan instead of the catalog's current version (PL-58)", %{name: name} do
    assert {:ok, %{state: :done, snapshot: 7}, _frame} = Client.query(name, "SELECT 1")

    assert {:ok, %{state: :done, snapshot: 42}, _frame} =
             Client.query(name, "SELECT 1", snapshot: 42)
  end

  test "hot_before_ms: rides through submission without changing a hot-less query", %{name: name} do
    assert {:ok, %{state: :done} = job, _frame} =
             Client.query(name, "SELECT 1", hot_before_ms: System.system_time(:millisecond))

    assert job.snapshot == 7
  end
end
