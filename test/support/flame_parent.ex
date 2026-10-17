defmodule Smolquery.Test.FlameParent do
  @moduledoc """
  Makes the test VM look like a burst runner: `FLAME_PARENT` set the way a
  `FLAME.Backend` sets it on a child it boots, pointing at the test process.

  `put_env/0` sets the variable and removes it when the test exits, so a
  test module that uses it must be `async: false`.
  """

  @doc "Sets `FLAME_PARENT` for the rest of the test and clears it on exit."
  @spec put_env() :: :ok
  def put_env do
    System.put_env("FLAME_PARENT", encoded())
    ExUnit.Callbacks.on_exit(fn -> System.delete_env("FLAME_PARENT") end)
  end

  @doc "The encoded parent a backend would hand a runner."
  @spec encoded() :: String.t()
  def encoded do
    make_ref()
    |> FLAME.Parent.new(self(), FLAME.LocalBackend, "runner", "SMOLQUERY_BURST_HOST")
    |> FLAME.Parent.encode()
  end
end
