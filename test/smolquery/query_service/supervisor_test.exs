defmodule Smolquery.QueryService.SupervisorTest do
  use ExUnit.Case, async: false

  alias Smolquery.Engine
  alias Smolquery.QueryService

  setup do
    start_supervised!(QueryService.Supervisor)
    :ok
  end

  test "starts an engine ready to serve queries" do
    assert {:ok, _result} = Engine.query(Engine, "SELECT 1")
  end

  test "supervises the engine subtree" do
    children = Supervisor.which_children(QueryService.Supervisor)

    assert [{Engine, pid, :supervisor, _}] = children
    assert is_pid(pid)
  end
end
