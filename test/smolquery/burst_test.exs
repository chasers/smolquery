defmodule Smolquery.BurstTest do
  use ExUnit.Case, async: false

  alias Smolquery.Burst
  alias Smolquery.Test.FlameParent

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

    test "is true once a backend has set FLAME_PARENT" do
      FlameParent.put_env()
      assert Burst.runner?()
    end
  end

  describe "children/0" do
    test "starts no pool when burst is off" do
      Application.delete_env(:smolquery, Burst)
      assert Burst.children() == []
    end

    test "starts one pool, one shard per runner, with the module's defaults" do
      Application.put_env(:smolquery, Burst, enabled: true, backend: :local)

      assert [{FLAME.Pool, opts}] = Burst.children()
      assert opts[:name] == Burst.pool()
      assert opts[:backend] == FLAME.LocalBackend
      assert opts[:min] == 0
      assert opts[:max] == 64
      assert opts[:max_concurrency] == 1
      assert opts[:boot_timeout] == 120_000
      assert opts[:idle_shutdown_after] == 15_000
      assert opts[:timeout] == 600_000
    end

    test "a configured key overrides its default" do
      Application.put_env(:smolquery, Burst,
        enabled: true,
        max: 3,
        idle_shutdown_after_ms: 1_000,
        call_timeout_ms: 5_000
      )

      assert [{FLAME.Pool, opts}] = Burst.children()
      assert opts[:max] == 3
      assert opts[:idle_shutdown_after] == 1_000
      assert opts[:timeout] == 5_000
    end

    test "takes a FLAME.Backend module, with or without its options" do
      Application.put_env(:smolquery, Burst, enabled: true, backend: FLAME.FlyBackend)
      assert [{FLAME.Pool, opts}] = Burst.children()
      assert opts[:backend] == FLAME.FlyBackend

      Application.put_env(:smolquery, Burst, enabled: true, backend: {FLAME.FlyBackend, cpus: 2})
      assert [{FLAME.Pool, opts}] = Burst.children()
      assert opts[:backend] == {FLAME.FlyBackend, cpus: 2}
    end

    test "names the key when the backend is neither" do
      Application.put_env(:smolquery, Burst, enabled: true, backend: "lambda")

      assert_raise ArgumentError, ~r/Smolquery\.Burst, backend:.*"lambda"/, fn ->
        Burst.children()
      end
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
