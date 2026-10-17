defmodule Smolquery.Burst.LifetimeTest do
  use ExUnit.Case, async: true

  alias Smolquery.Burst.Lifetime

  test "stops the VM once the lifetime has passed, through the given stop" do
    parent = self()
    name = :"lifetime_#{System.unique_integer([:positive])}"

    start_supervised!(
      {Lifetime, max_lifetime_ms: 20, stop: fn -> send(parent, :stopped) end, name: name}
    )

    refute_received :stopped
    assert_receive :stopped, 1_000
  end

  test "an integer alone is the lifetime, with System.stop/0 as the stop" do
    name = :"lifetime_default_#{System.unique_integer([:positive])}"
    pid = start_supervised!({Lifetime, max_lifetime_ms: 600_000, name: name})

    assert %{max_lifetime_ms: 600_000, stop: stop} = :sys.get_state(pid)
    assert stop == (&System.stop/0)
  end
end
