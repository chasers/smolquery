defmodule Smolquery.Test.ExitingCatalog do
  @moduledoc """
  A `Smolquery.Catalog` that answers like the catalog it wraps, except that
  chosen calls answer `{:error, %Smolquery.Engine.CallExited{}}` — what
  `Smolquery.Catalog.DuckLake` answers when a call times out behind a busy
  connection or finds the connection gone (T-464).

  A real wedge cannot be staged deterministically: the abandoned statement
  that causes one runs on the connection's own time. This double lets a test
  say which call, or which call on which table, is the one that exited, and
  everything else still reaches the real catalog underneath.

      ExitingCatalog.new(catalog, [:tables])
      ExitingCatalog.new(catalog, retention: fn [table] -> table == other end)

  A bare function name exits on every call; a predicate over the call's
  arguments (the config excluded) exits when it answers true.
  """

  @behaviour Smolquery.Catalog

  alias Smolquery.Catalog
  alias Smolquery.Engine.CallExited

  @type exiting :: [atom()] | [{atom(), (list() -> boolean())}]

  @spec new(Catalog.t(), exiting()) :: Catalog.t()
  def new(%Catalog{} = inner, exiting) when is_list(exiting) do
    %Catalog{impl: __MODULE__, config: %{inner: inner, exiting: exiting}}
  end

  @impl Catalog
  def on_connection(%{inner: inner} = config, slot) do
    %{config | inner: %{inner | config: inner.impl.on_connection(inner.config, slot)}}
  end

  for {name, arity} <- Catalog.behaviour_info(:callbacks), name != :on_connection do
    args = Macro.generate_arguments(arity - 1, __MODULE__)

    @impl Catalog
    def unquote(name)(%{inner: inner, exiting: exiting}, unquote_splicing(args)) do
      if exits?(exiting, unquote(name), [unquote_splicing(args)]) do
        {:error, %CallExited{reason: :timeout}}
      else
        inner.impl.unquote(name)(inner.config, unquote_splicing(args))
      end
    end
  end

  defp exits?(exiting, name, args) do
    Enum.any?(exiting, fn
      ^name -> true
      {^name, predicate} when is_function(predicate, 1) -> predicate.(args)
      _other -> false
    end)
  end
end
