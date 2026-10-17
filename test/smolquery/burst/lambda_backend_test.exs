defmodule Smolquery.Burst.LambdaBackendTest do
  use ExUnit.Case, async: false

  alias Smolquery.Burst.LambdaBackend

  setup do
    previous = System.get_env("POD_IP")

    on_exit(fn ->
      if previous, do: System.put_env("POD_IP", previous), else: System.delete_env("POD_IP")
    end)

    :ok
  end

  describe "init/1" do
    test "takes the function and region from options and generates a runner name" do
      {:ok, state} = LambdaBackend.init(function: "fn", region: "eu-central-1")

      assert state.function == "fn"
      assert state.region == "eu-central-1"
      assert state.max_lifetime_ms == 840_000
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
    test "carries the assignment as base64 KEY=VALUE lines, with the parent's address" do
      System.put_env("POD_IP", "10.1.2.3")

      {:ok, state} =
        LambdaBackend.init(
          function: "fn",
          region: "eu-central-1",
          max_lifetime_ms: 60_000,
          env: %{"EXTRA" => "1", "SPACED" => "a b", "QUOTED" => "it's"}
        )

      assert {:ok, %{"env" => encoded}} = LambdaBackend.payload(state)
      lines = decode(encoded)

      assert lines["RELEASE_DISTRIBUTION"] == "name"
      assert lines["RELEASE_NODE"] == state.runner_node_base <> "@127.0.0.1"
      assert lines["RELEASE_COOKIE"] == Atom.to_string(Node.get_cookie())
      assert lines["ELIXIR_ERL_OPTIONS"] == "-hidden -dist_listen false"
      assert lines["SMOLQUERY_BURST_MAX_LIFETIME_MS"] == "60000"
      assert lines["EXTRA"] == "1"
      assert lines["SPACED"] == "a b"
      assert lines["QUOTED"] == "it's"

      host = node() |> Atom.to_string() |> String.split("@") |> List.last()
      assert lines["SMOLQUERY_BURST_INETRC"] =~ "{host, {10, 1, 2, 3}, [#{inspect(host)}]}."
      assert lines["SMOLQUERY_BURST_INETRC"] =~ "{lookup, [file, native]}."

      parent = lines["FLAME_PARENT"] |> Base.decode64!() |> :erlang.binary_to_term()
      assert parent.ref == state.parent_ref
      assert parent.pid == self()
      assert parent.backend == LambdaBackend
      assert parent.node_base == state.runner_node_base
    end

    test "refuses to assign a runner when this node's name resolves nowhere" do
      System.delete_env("POD_IP")
      {:ok, state} = LambdaBackend.init(function: "fn", region: "eu-central-1")

      host = node() |> Atom.to_string() |> String.split("@") |> List.last()
      assert LambdaBackend.payload(state) == {:error, {:parent_unresolved, host}}
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

  defp decode(encoded) do
    encoded
    |> Base.decode64!()
    |> String.split("\n", trim: true)
    |> Enum.reduce({%{}, nil}, &collect/2)
    |> elem(0)
  end

  defp collect(line, {lines, nil}) do
    [key, quoted] = String.split(line, "=", parts: 2)

    if String.ends_with?(quoted, "'") and String.length(quoted) > 1,
      do: {Map.put(lines, key, unquote_value(quoted)), nil},
      else: {lines, {key, quoted}}
  end

  defp collect(line, {lines, {key, partial}}) do
    joined = partial <> "\n" <> line

    if String.ends_with?(joined, "'"),
      do: {Map.put(lines, key, unquote_value(joined)), nil},
      else: {lines, {key, joined}}
  end

  defp unquote_value(quoted),
    do:
      quoted
      |> String.trim_leading("'")
      |> String.trim_trailing("'")
      |> String.replace("'\\''", "'")
end
