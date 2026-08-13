defmodule SmolqueryWeb.Supervisor do
  @moduledoc """
  Top-level subtree for the `:web` role.

  Started only on nodes whose roles include `:web` (see `Smolquery.Roles`).
  The strategy is `rest_for_one`, catalog engine first, because every LiveView
  resolves tables through it; a catalog engine that dies takes the endpoint
  down with it rather than leaving pages rendering through a dead handle.

  Two things must hold before the endpoint listens, and both are checked here
  rather than at config-evaluation time — `config/runtime.exs` is also
  evaluated during an image build, where no secret exists yet:

    * a basic-auth credential (`SmolqueryWeb.Runtime`)
    * a session secret long enough for the cookie store, which signs the
      marker that fences the LiveView socket
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
              "SMOLQUERY_SECRET_KEY_BASE to the same value on every node running " <>
              "the :web role. It signs the session that fences the LiveView " <>
              "socket, so a per-node or per-boot value leaves the UI unable to " <>
              "connect. Generate one with `mix phx.gen.secret`."
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
