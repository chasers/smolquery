defmodule Smolquery.BufferService.MembershipTest do
  use ExUnit.Case, async: false

  alias Smolquery.BufferService.Membership
  alias Smolquery.Cluster

  setup do
    ensure_pg_scope!()

    previous = Application.fetch_env(:smolquery, Cluster)
    on_exit(fn -> restore(previous) end)
  end

  describe "clustering off" do
    test "join/leave are no-ops, and nodes/2 always answers the static fallback" do
      name = unique_name()

      assert :ok = Membership.join(name, self())
      assert Membership.nodes(name, [:static@host]) == [:static@host]
      assert :ok = Membership.leave(name, self())
    end
  end

  describe "clustering on" do
    setup do
      Application.put_env(:smolquery, Cluster,
        enabled: true,
        postgres: [hostname: "localhost", port: 5432, username: "postgres", database: "postgres"]
      )
    end

    test "nodes/2 falls back to the static list until someone joins" do
      name = unique_name()

      assert Membership.nodes(name, [:fallback@host]) == [:fallback@host]
    end

    test "join/2 makes this node answer nodes/2, leave/2 removes it" do
      name = unique_name()

      assert :ok = Membership.join(name, self())
      assert Membership.nodes(name, [:fallback@host]) == [node()]

      assert :ok = Membership.leave(name, self())
      assert Membership.nodes(name, [:fallback@host]) == [:fallback@host]
    end

    test "a joined pid's exit removes this node without an explicit leave" do
      name = unique_name()
      test = self()

      {:ok, pid} =
        Task.start(fn ->
          Membership.join(name, self())
          send(test, :joined)
          Process.sleep(:infinity)
        end)

      assert_receive :joined
      assert Membership.nodes(name, [:fallback@host]) == [node()]

      Process.exit(pid, :kill)
      wait_until(fn -> Membership.nodes(name, [:fallback@host]) == [:fallback@host] end)
    end

    test "leave/2 on a pid that was never joined is a no-op" do
      name = unique_name()

      assert :ok = Membership.leave(name, self())
    end

    test "leave/2 with a nil pid is a no-op" do
      assert :ok = Membership.leave(unique_name(), nil)
    end
  end

  defp unique_name, do: :"membership_test_#{:erlang.unique_integer([:positive])}"

  defp ensure_pg_scope! do
    case :pg.start_link(Cluster.pg_scope()) do
      {:ok, _pid} -> :ok
      {:error, {:already_started, _pid}} -> :ok
    end
  end

  defp wait_until(fun, attempts \\ 50)
  defp wait_until(_fun, 0), do: flunk("condition never became true")

  defp wait_until(fun, attempts) do
    if fun.() do
      :ok
    else
      Process.sleep(20)
      wait_until(fun, attempts - 1)
    end
  end

  defp restore({:ok, value}), do: Application.put_env(:smolquery, Cluster, value)
  defp restore(:error), do: Application.delete_env(:smolquery, Cluster)
end
