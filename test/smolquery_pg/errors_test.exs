defmodule SmolqueryPg.ErrorsTest do
  use ExUnit.Case, async: true

  alias SmolqueryPg.Errors

  test "maps the planner's refusals to SQLSTATEs" do
    assert {"42601", "bad"} = Errors.from_reason({:invalid_query, "bad"})
    assert {"42601", _message} = Errors.from_reason(:multiple_statements)
    assert {"42P01", message} = Errors.from_reason({:unknown_table, {"a", "b"}})
    assert message =~ ~s|"a.b"|
    assert {"54000", message} = Errors.from_reason({:result_too_large, 10})
    assert message =~ "10"
    assert {"57014", _message} = Errors.from_reason(:timeout)
    assert {"57P03", _message} = Errors.from_reason(:query_service_unavailable)

    assert {"72000", message} =
             Errors.from_reason({:pinned_hot_retired, {"analytics", "events"}, ["01A"]})

    assert message =~ "analytics.events"
  end

  test "a DuckDB error keeps its message and takes the code its class implies" do
    assert {"42601", "Parser Error: x"} = Errors.from_reason("Parser Error: x")
    assert {"42P01", _message} = Errors.from_reason("Catalog Error: y")
    assert {"XX000", "other"} = Errors.from_reason("other")
  end

  test "an engine failure unwraps to the statement's error, never the statement" do
    reason =
      {:engine_failed, {:statement_failed, "ATTACH 'secret'", %{message: "Binder Error: z"}}}

    assert {"42000", "Binder Error: z"} = Errors.from_reason(reason)
  end

  test "an unknown reason answers XX000 with its inspected form" do
    assert {"XX000", "{:weird, 1}"} = Errors.from_reason({:weird, 1})
  end
end
