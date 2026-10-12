defmodule Smolquery.Engine.UntypedResultTest do
  use ExUnit.Case, async: true

  alias Smolquery.Engine.UntypedResult

  test "names what to cast, for a batch with no row count and for a nested NULL" do
    assert Exception.message(%UntypedResult{reason: :unsized}) =~ "no row count; cast one column"

    assert Exception.message(%UntypedResult{reason: {:nested, "l"}}) ==
             ~s(column "l" holds an untyped NULL inside a list, struct or map; cast it)
  end
end
