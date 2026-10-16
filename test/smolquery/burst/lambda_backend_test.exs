defmodule Smolquery.Burst.LambdaBackendTest do
  use ExUnit.Case, async: true

  alias Smolquery.Burst.LambdaBackend

  describe "init/1" do
    test "takes the function and region from options and generates a runner name" do
      {:ok, state} = LambdaBackend.init(function: "fn", region: "eu-central-1")

      assert state.function == "fn"
      assert state.region == "eu-central-1"
      assert state.runner_node_base =~ ~r/^smolquery-burst-[a-z2-7]{12}$/
      assert is_reference(state.parent_ref)
    end

    test "tolerates missing function and region, which a runner's own pool never uses" do
      assert {:ok, %LambdaBackend{function: nil}} = LambdaBackend.init([])
    end

    test "rejects unknown options" do
      assert_raise ArgumentError, fn -> LambdaBackend.init(image: "x") end
    end
  end

  describe "payload/1" do
    test "carries the assignment as base64 KEY=VALUE lines" do
      {:ok, state} =
        LambdaBackend.init(function: "fn", region: "eu-central-1", env: %{"EXTRA" => "1"})

      %{"env" => encoded} = LambdaBackend.payload(state)

      lines =
        encoded
        |> Base.decode64!()
        |> String.split("\n", trim: true)
        |> Map.new(fn line ->
          [key, value] = String.split(line, "=", parts: 2)
          {key, value}
        end)

      assert lines["RELEASE_DISTRIBUTION"] == "name"
      assert lines["RELEASE_NODE"] == state.runner_node_base <> "@127.0.0.1"
      assert lines["RELEASE_COOKIE"] == Atom.to_string(Node.get_cookie())
      assert lines["ELIXIR_ERL_OPTIONS"] == "-dist_listen false"
      assert lines["EXTRA"] == "1"

      parent = lines["FLAME_PARENT"] |> Base.decode64!() |> :erlang.binary_to_term()
      assert parent.ref == state.parent_ref
      assert parent.pid == self()
      assert parent.backend == LambdaBackend
      assert parent.node_base == state.runner_node_base
    end
  end

  describe "remote_boot/1" do
    test "refuses to boot without a function" do
      {:ok, state} = LambdaBackend.init(region: "eu-central-1")

      assert_raise ArgumentError, ~r/missing :function/, fn ->
        LambdaBackend.remote_boot(state)
      end
    end
  end
end
