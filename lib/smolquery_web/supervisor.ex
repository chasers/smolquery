defmodule SmolqueryWeb.Supervisor do
  @moduledoc """
  Top-level subtree for the `:web` role.

  Started only on nodes whose roles include `:web` (see `Smolquery.Roles`).
  The strategy is `rest_for_one`, catalog engine first, because every LiveView
  resolves tables through it; a catalog engine that dies takes the endpoint
  down with it rather than leaving pages rendering through a dead handle.

  This module checks two requirements before the endpoint listens:

    * a basic-auth credential (`SmolqueryWeb.Runtime`)
    * a session secret of at least 64 bytes, which signs the marker that
      guards the LiveView socket

  The checks live here, not in `config/runtime.exs`. The image build also
  evaluates that file, and no secret exists at build time.
  """

  use Supervisor

  alias Smolquery.Catalog.DuckLake
  alias SmolqueryWeb.Runtime

  @min_secret_bytes 64

  @doc """
  Starts the web UI.

  Takes any `SmolqueryWeb.Runtime` option; application config supplies
  whatever is not passed.
  """
  @spec start_link(keyword()) :: Supervisor.on_start()
  def start_link(opts \\ []) do
    runtime = Runtime.new(opts)
    validate_session_secret!()

    Supervisor.start_link(__MODULE__, runtime, name: Runtime.supervisor(runtime.name))
  end

  defp validate_session_secret! do
    secret =
      :smolquery
      |> Application.get_env(SmolqueryWeb.Endpoint, [])
      |> Keyword.get(:secret_key_base)

    if byte_size(secret || "") < @min_secret_bytes do
      raise ArgumentError,
            "the web UI refuses to boot without a session secret of at least " <>
              "#{@min_secret_bytes} bytes (got #{byte_size(secret || "")}): set " <>
              "SMOLQUERY_SECRET_KEY_BASE on every node running the :web role. " <>
              "Every :web node needs the same value, or the LiveView socket " <>
              "cannot connect. Generate one with `mix phx.gen.secret`."
    end
  end

  @impl Supervisor
  def init(%Runtime{} = runtime) do
    Runtime.put(runtime)

    children =
      DuckLake.children(runtime.catalog_opts, Runtime.catalog_engine(runtime.name)) ++
        [
          {Phoenix.PubSub, name: Smolquery.PubSub},
          SmolqueryWeb.Endpoint
        ]

    Supervisor.init(children, strategy: :rest_for_one)
  end
end
