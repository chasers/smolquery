defmodule SmolqueryWeb.ConnCase do
  @moduledoc """
  Test case for anything that talks to the web UI.

  The endpoint is a module-global singleton, so web tests are `async: false`
  and each one boots the `:web` subtree itself — `start_web!/1` starts
  `SmolqueryWeb.Supervisor` (catalog defaulting to a fresh
  `Smolquery.Test.MapCatalog`) and tears the published runtime down with the
  test.
  """

  use ExUnit.CaseTemplate

  alias Smolquery.Test.MapCatalog

  using do
    quote do
      @endpoint SmolqueryWeb.Endpoint

      use SmolqueryWeb, :verified_routes

      import Phoenix.ConnTest
      import Phoenix.LiveViewTest
      import Plug.Conn
      import SmolqueryWeb.ConnCase
    end
  end

  setup _tags do
    {:ok, conn: Phoenix.ConnTest.build_conn()}
  end

  @doc """
  Boots the web subtree for one test and returns its runtime.
  """
  @spec start_web!(keyword()) :: SmolqueryWeb.Runtime.t()
  def start_web!(opts \\ []) do
    opts = Keyword.put_new_lazy(opts, :catalog, fn -> MapCatalog.new() end)

    ExUnit.Callbacks.start_supervised!({SmolqueryWeb.Supervisor, opts})
    ExUnit.Callbacks.on_exit(fn -> SmolqueryWeb.Runtime.delete(SmolqueryWeb) end)

    {:ok, runtime} = SmolqueryWeb.Runtime.fetch(SmolqueryWeb)
    runtime
  end
end
