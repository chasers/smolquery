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

  describe "max/0" do
    test "is the default until configured" do
      Application.delete_env(:smolquery, Burst)
      assert Burst.max() == 64

      Application.put_env(:smolquery, Burst, enabled: true, max: 3)
      assert Burst.max() == 3
    end
  end

  describe "backend_name/0" do
    test "local by default, else the backend module's short name" do
      Application.delete_env(:smolquery, Burst)
      assert Burst.backend_name() == "local"

      Application.put_env(:smolquery, Burst, backend: FLAME.FlyBackend)
      assert Burst.backend_name() == "fly"

      Application.put_env(:smolquery, Burst,
        backend: {Smolquery.Burst.LambdaBackend, region: "x"}
      )

      assert Burst.backend_name() == "lambda"
    end
  end

  describe "the boot hooks" do
    test "emit one boot event per runner with its duration and outcome, across the two hook processes" do
      start_supervised!(Smolquery.Burst.Boots)
      parent = self()
      handler = "burst-boot-#{System.unique_integer([:positive])}"

      :telemetry.attach(
        handler,
        [:smolquery, :burst, :boot],
        fn _event, measurements, meta, _config -> send(parent, {:boot, measurements, meta}) end,
        nil
      )

      on_exit(fn -> :telemetry.detach(handler) end)

      booter = spawn(fn -> :ok end)

      spawn(fn ->
        :ok = Burst.grow_started(%{name: Burst.pool(), count: 1, pid: booter})
        send(parent, :started)
      end)

      assert_receive :started
      Process.sleep(20)
      :ok = Burst.grow_ended(:ok, %{name: Burst.pool(), count: 1, pid: booter})

      assert_receive {:boot, %{duration_ms: duration},
                      %{outcome: :ok, backend: "local", runners: 1}}

      assert is_integer(duration) and duration >= 20

      :ok = Burst.grow_ended({:exit, :boot_timeout}, %{name: Burst.pool(), count: 0, pid: booter})
      assert_receive {:boot, %{duration_ms: 0}, %{outcome: :error, runners: 0}}
    end

    test "a local pool built from children/0 reports its first runner's boot" do
      Application.put_env(:smolquery, Burst, enabled: true, backend: :local, max: 1)
      for spec <- Burst.children(), do: start_supervised!(spec)

      parent = self()
      handler = "burst-pool-boot-#{System.unique_integer([:positive])}"

      :telemetry.attach(
        handler,
        [:smolquery, :burst, :boot],
        fn _event, measurements, meta, _config -> send(parent, {:boot, measurements, meta}) end,
        nil
      )

      on_exit(fn -> :telemetry.detach(handler) end)

      assert FLAME.call(Burst.pool(), fn -> :ran end) == :ran

      assert_receive {:boot, %{duration_ms: duration},
                      %{outcome: :ok, backend: "local", runners: 1}},
                     5_000

      assert is_integer(duration) and duration >= 0
    end
  end

  describe "runner_children/0" do
    test "is the lifetime clock when the assignment set one, else nothing" do
      Application.delete_env(:smolquery, Burst)
      assert Burst.runner_children() == []

      Application.put_env(:smolquery, Burst, max_lifetime_ms: 840_000)
      assert Burst.runner_children() == [{Smolquery.Burst.Lifetime, 840_000}]
    end
  end

  describe "runner_limits/0" do
    setup do
      previous = System.get_env("AWS_LAMBDA_FUNCTION_MEMORY_SIZE")
      System.delete_env("AWS_LAMBDA_FUNCTION_MEMORY_SIZE")

      on_exit(fn ->
        if previous,
          do: System.put_env("AWS_LAMBDA_FUNCTION_MEMORY_SIZE", previous),
          else: System.delete_env("AWS_LAMBDA_FUNCTION_MEMORY_SIZE")
      end)
    end

    test "is empty on a node that is not a runner, whatever is configured" do
      Application.put_env(:smolquery, Burst, runner_memory_limit: "7GB", runner_threads: 6)
      System.put_env("AWS_LAMBDA_FUNCTION_MEMORY_SIZE", "10240")
      assert Burst.runner_limits() == []
    end

    test "a runner with no Lambda and no overrides sizes threads from the VM" do
      FlameParent.put_env()
      Application.delete_env(:smolquery, Burst)
      assert Burst.runner_limits() == [threads: System.schedulers_online()]
    end

    test "a Lambda runner takes three quarters of the function's memory" do
      FlameParent.put_env()
      Application.delete_env(:smolquery, Burst)
      System.put_env("AWS_LAMBDA_FUNCTION_MEMORY_SIZE", "10240")

      assert Burst.runner_limits() == [
               memory_limit: "7680MB",
               threads: System.schedulers_online()
             ]
    end

    test "the configured overrides win over the host" do
      FlameParent.put_env()
      Application.put_env(:smolquery, Burst, runner_memory_limit: "7GB", runner_threads: 6)
      System.put_env("AWS_LAMBDA_FUNCTION_MEMORY_SIZE", "10240")

      assert Burst.runner_limits() == [memory_limit: "7GB", threads: 6]
    end

    test "a memory size that is not a positive integer is ignored" do
      FlameParent.put_env()
      Application.delete_env(:smolquery, Burst)

      for junk <- ["", "lots", "0", "-1", "10GB"] do
        System.put_env("AWS_LAMBDA_FUNCTION_MEMORY_SIZE", junk)
        assert Burst.runner_limits() == [threads: System.schedulers_online()]
      end
    end
  end

  describe "children/0" do
    test "starts no pool when burst is off" do
      Application.delete_env(:smolquery, Burst)
      assert Burst.children() == []
    end

    test "starts one pool, one shard per runner, with the module's defaults" do
      Application.put_env(:smolquery, Burst, enabled: true, backend: :local)

      assert [Smolquery.Burst.Boots, {FLAME.Pool, opts}] = Burst.children()
      assert opts[:name] == Burst.pool()
      assert opts[:backend] == FLAME.LocalBackend
      assert opts[:min] == 0
      assert opts[:max] == 64
      assert opts[:max_concurrency] == 1
      assert opts[:boot_timeout] == 120_000
      assert opts[:idle_shutdown_after] == 15_000
      assert opts[:timeout] == 600_000
      assert is_function(opts[:on_grow_start], 1)
      assert is_function(opts[:on_grow_end], 2)
    end

    test "a configured key overrides its default" do
      Application.put_env(:smolquery, Burst,
        enabled: true,
        max: 3,
        idle_shutdown_after_ms: 1_000,
        call_timeout_ms: 5_000
      )

      assert [_boots, {FLAME.Pool, opts}] = Burst.children()
      assert opts[:max] == 3
      assert opts[:idle_shutdown_after] == 1_000
      assert opts[:timeout] == 5_000
    end

    test "takes a FLAME.Backend module, with or without its options" do
      Application.put_env(:smolquery, Burst, enabled: true, backend: FLAME.FlyBackend)
      assert [_boots, {FLAME.Pool, opts}] = Burst.children()
      assert opts[:backend] == FLAME.FlyBackend

      Application.put_env(:smolquery, Burst, enabled: true, backend: {FLAME.FlyBackend, cpus: 2})
      assert [_boots, {FLAME.Pool, opts}] = Burst.children()
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
