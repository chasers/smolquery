defmodule Smolquery.Cluster.PgGroup.MemberTest do
  use ExUnit.Case, async: false

  alias Smolquery.Cluster
  alias Smolquery.Cluster.PgGroup

  defmodule FakeService do
  end

  setup do
    previous = Application.fetch_env(:smolquery, Cluster)

    Application.put_env(:smolquery, Cluster,
      enabled: true,
      postgres: [hostname: "localhost", port: 5432, username: "postgres", database: "postgres"]
    )

    on_exit(fn -> restore(previous) end)

    scope = start_scope()
    name = :"member_#{:erlang.unique_integer([:positive])}"

    %{scope: scope, name: name}
  end

  test "joins on start and leaves when it dies", %{name: name} do
    {:ok, member} = PgGroup.Member.start_link({FakeService, name})

    assert PgGroup.nodes(FakeService, name, []) == [node()]

    Process.unlink(member)
    Process.exit(member, :kill)

    assert wait_until(fn -> PgGroup.nodes(FakeService, name, []) == [] end)
  end

  test "re-joins after the :pg scope process restarts", %{scope: scope, name: name} do
    {:ok, _member} = PgGroup.Member.start_link({FakeService, name})

    assert PgGroup.nodes(FakeService, name, []) == [node()]

    Process.exit(scope, :kill)
    start_scope()

    assert wait_until(fn -> PgGroup.nodes(FakeService, name, []) == [node()] end)
  end

  test "leave/1 is permanent — a scope restart does not resurrect membership", %{
    scope: scope,
    name: name
  } do
    {:ok, member} = PgGroup.Member.start_link({FakeService, name})

    assert :ok = PgGroup.Member.leave(member)
    assert PgGroup.nodes(FakeService, name, []) == []

    Process.exit(scope, :kill)
    start_scope()

    refute wait_until(fn -> PgGroup.nodes(FakeService, name, []) == [node()] end, 500)
  end

  test "leave/1 on a member that is not running is :ok" do
    assert PgGroup.Member.leave(:nonexistent_member) == :ok
  end

  defp start_scope do
    {:ok, pid} =
      case :pg.start(Cluster.pg_scope()) do
        {:ok, pid} -> {:ok, pid}
        {:error, {:already_started, pid}} -> {:ok, pid}
      end

    pid
  end

  defp wait_until(check, budget_ms \\ 2_000) do
    cond do
      check.() ->
        true

      budget_ms <= 0 ->
        false

      true ->
        Process.sleep(10)
        wait_until(check, budget_ms - 10)
    end
  end

  defp restore({:ok, value}), do: Application.put_env(:smolquery, Cluster, value)
  defp restore(:error), do: Application.delete_env(:smolquery, Cluster)
end
