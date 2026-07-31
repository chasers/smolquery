defmodule SmolqueryTest do
  use ExUnit.Case, async: true

  doctest Smolquery

  test "version/0 reports the application version" do
    assert Smolquery.version() == to_string(Application.spec(:smolquery, :vsn))
  end
end
