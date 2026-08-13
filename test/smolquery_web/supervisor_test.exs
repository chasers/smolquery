defmodule SmolqueryWeb.SupervisorTest do
  use ExUnit.Case, async: true

  alias Smolquery.Test.MapCatalog

  test "refuses to start without a session secret" do
    assert_raise ArgumentError, ~r/at least 64 bytes \(got 0\)/, fn ->
      SmolqueryWeb.Supervisor.start_link(catalog: MapCatalog.new(), secret_key_base: nil)
    end
  end
end
