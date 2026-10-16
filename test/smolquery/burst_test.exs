defmodule Smolquery.BurstTest do
  use ExUnit.Case, async: false

  alias Smolquery.Burst

  setup do
    previous = Application.get_env(:smolquery, Burst)

    on_exit(fn ->
      if previous,
        do: Application.put_env(:smolquery, Burst, previous),
        else: Application.delete_env(:smolquery, Burst)
    end)

    :ok
  end

  describe "runner?/0" do
    test "is false without FLAME_PARENT in the environment" do
      refute Burst.runner?()
    end
  end

  describe "children/0" do
    test "starts no pool when burst is off" do
      Application.delete_env(:smolquery, Burst)
      assert Burst.children() == []
    end

    test "starts one pool, one shard per runner, when burst is on" do
      Application.put_env(:smolquery, Burst, enabled: true, backend: :local, max: 3)

      assert [{FLAME.Pool, opts}] = Burst.children()
      assert opts[:name] == Burst.pool()
      assert opts[:backend] == FLAME.LocalBackend
      assert opts[:max] == 3
      assert opts[:max_concurrency] == 1
      assert opts[:min] == 0
    end

    test "names the Lambda backend" do
      Application.put_env(:smolquery, Burst, enabled: true, backend: :lambda)

      assert [{FLAME.Pool, opts}] = Burst.children()
      assert opts[:backend] == Smolquery.Burst.LambdaBackend
    end
  end

  describe "a local pool" do
    test "runs a call on a runner and answers" do
      name = Module.concat(__MODULE__, Pool)

      start_supervised!(
        {FLAME.Pool, name: name, backend: FLAME.LocalBackend, min: 0, max: 1, max_concurrency: 1}
      )

      assert FLAME.call(name, fn -> {:ran_on, node()} end) == {:ran_on, node()}
    end
  end
end
