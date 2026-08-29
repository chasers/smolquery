defmodule Smolquery.Cluster.PgGroupTest do
  use ExUnit.Case, async: false

  alias Smolquery.Cluster
  alias Smolquery.Cluster.PgGroup

  setup do
    ensure_pg_scope!()

    previous = Application.fetch_env(:smolquery, Cluster)
    on_exit(fn -> restore(previous) end)
  end

  describe "clustering off" do
    test "join/leave are no-ops, and nodes/3 always answers the static fallback" do
      name = unique_name()

      assert :ok = PgGroup.join(__MODULE__, name, self())
      assert PgGroup.nodes(__MODULE__, name, [:static@host]) == [:static@host]
      assert :ok = PgGroup.leave(__MODULE__, name, self())
    end
  end

  describe "clustering on" do
    setup do
      Application.put_env(:smolquery, Cluster,
        enabled: true,
        postgres: [hostname: "localhost", port: 5432, username: "postgres", database: "postgres"]
      )
    end

    test "nodes/3 falls back to the static list until someone joins" do
      name = unique_name()

      assert PgGroup.nodes(__MODULE__, name, [:fallback@host]) == [:fallback@host]
    end

    test "join/3 makes this node answer nodes/3, leave/3 removes it" do
      name = unique_name()

      assert :ok = PgGroup.join(__MODULE__, name, self())
      assert PgGroup.nodes(__MODULE__, name, [:fallback@host]) == [node()]

      assert :ok = PgGroup.leave(__MODULE__, name, self())
      assert PgGroup.nodes(__MODULE__, name, [:fallback@host]) == [:fallback@host]
    end

    test "a joined pid's exit removes this node without an explicit leave" do
      name = unique_name()
      test = self()

      {:ok, pid} =
        Task.start(fn ->
          PgGroup.join(__MODULE__, name, self())
          send(test, :joined)
          Process.sleep(:infinity)
        end)

      assert_receive :joined
      assert PgGroup.nodes(__MODULE__, name, [:fallback@host]) == [node()]

      Process.exit(pid, :kill)
      wait_until(fn -> PgGroup.nodes(__MODULE__, name, [:fallback@host]) == [:fallback@host] end)
    end

    test "leave/3 on a pid that was never joined is a no-op" do
      name = unique_name()

      assert :ok = PgGroup.leave(__MODULE__, name, self())
    end

    test "leave/3 with a nil pid is a no-op" do
      assert :ok = PgGroup.leave(__MODULE__, unique_name(), nil)
    end

    test "different scopes never see each other's members for the same name" do
      name = unique_name()

      assert :ok = PgGroup.join(Smolquery.BufferService, name, self())
      assert PgGroup.nodes(Smolquery.BufferService, name, [:fallback@host]) == [node()]
      assert PgGroup.nodes(Smolquery.StorageService, name, [:fallback@host]) == [:fallback@host]
    end
  end

  defp unique_name, do: :"pg_group_test_#{:erlang.unique_integer([:positive])}"

  defp ensure_pg_scope! do
    case :pg.start(Cluster.pg_scope()) do
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
