defmodule SmolqueryWeb.Runtime do
  @moduledoc """
  A running web UI's resolved configuration.

  The same shape as `SmolqueryApi.Runtime`: configuration becomes a struct
  once at boot and lands in `:persistent_term`, so every LiveView mount reads
  its handles for free.

  ## Configuration

      config :smolquery, SmolqueryWeb,
        auth_mode: :static,
        username: "...",
        password: "...",
        catalog: [metadata: "sqlite:...", data_path: "..."]

  `auth_mode: :static` explicitly selects the static Basic-auth adapter.
  There is no default or fallback: a node holding the `:web` role with a
  missing or unsupported mode, or without credentials, refuses to boot. The
  credential is not the API key, so a UI rotation does not break an ingest
  client. Multiple credentials and rotation come later, as for the API.

  `catalog` is where the UI resolves datasets, tables, and schemas — the same
  seam `SmolqueryApi` uses. Given options (or nothing), the UI starts its own
  `Smolquery.Catalog.DuckLake` engine; given a `%Smolquery.Catalog{}` outright,
  it reads through that and starts nothing.

  The endpoint's listener (ip, port) is Phoenix's own concern and lives under
  `config :smolquery, SmolqueryWeb.Endpoint`. The session secret lives there
  too, but `new/1` reads and validates it: the cookie store needs at least 64
  bytes. `new/1` also derives `session_marker` from the secret and the
  credential. `SmolqueryWeb.Auth` writes that marker into the session and
  requires it on the LiveView socket, so a credential rotation revokes every
  old session. A `:secret_key_base` option overrides the endpoint config in a
  test.
  """

  alias Smolquery.Auth.Context
  alias Smolquery.Auth.Static
  alias Smolquery.Catalog

  @enforce_keys [:name, :auth_mode, :username, :password, :session_marker, :context, :catalog]
  @derive {Inspect, except: [:username, :password, :session_marker]}
  defstruct [
    :name,
    :auth_mode,
    :username,
    :password,
    :session_marker,
    :context,
    :catalog,
    :catalog_opts,
    ingest_name: Smolquery.IngestService,
    query_name: Smolquery.QueryService,
    buffer_name: Smolquery.BufferService
  ]

  @type t :: %__MODULE__{
          name: atom(),
          auth_mode: :static,
          username: String.t(),
          password: String.t(),
          session_marker: String.t(),
          context: Context.t(),
          catalog: Catalog.t(),
          catalog_opts: keyword() | nil,
          ingest_name: atom(),
          query_name: atom(),
          buffer_name: atom()
        }

  @session_secret_min_bytes 64

  @doc """
  Resolves configuration into a runtime.

  Application config for `SmolqueryWeb` supplies the defaults; `opts`
  overrides them. Raises if the authentication mode is missing or unsupported,
  or unless the merged options hold a non-empty `username` and a non-empty
  `password`. Also raises unless a session secret of at least 64 bytes is
  present — the `:secret_key_base` option, or the endpoint's own
  `secret_key_base` config.
  """
  @spec new(keyword()) :: t()
  def new(opts \\ []) do
    config = Keyword.merge(Application.get_env(:smolquery, SmolqueryWeb, []), opts)
    name = Keyword.get(config, :name, SmolqueryWeb)

    {catalog, catalog_opts} =
      Catalog.DuckLake.resolve(Keyword.get(config, :catalog), catalog_engine(name))

    auth_mode = Static.mode!(config, "the web UI", :web)
    username = fetch_credential!(config, :username)
    password = fetch_credential!(config, :password)
    secret_key_base = validate_session_secret!(resolve_session_secret(config))

    %__MODULE__{
      name: name,
      auth_mode: auth_mode,
      username: username,
      password: password,
      session_marker: session_marker(secret_key_base, username, password),
      context: Static.web_context(),
      catalog: catalog,
      catalog_opts: catalog_opts
    }
    |> struct!(Keyword.take(config, [:ingest_name, :query_name, :buffer_name]))
  end

  defp fetch_credential!(config, key) do
    Smolquery.Runtime.fetch_required!(config, key,
      service: "the web UI",
      missing: "a credential",
      env_var: env_var(key),
      scope: SmolqueryWeb,
      role: :web
    )
  end

  defp env_var(:username), do: "SMOLQUERY_WEB_USERNAME"
  defp env_var(:password), do: "SMOLQUERY_WEB_PASSWORD"

  defp resolve_session_secret(config) do
    case Keyword.fetch(config, :secret_key_base) do
      {:ok, secret} ->
        secret

      :error ->
        :smolquery
        |> Application.get_env(SmolqueryWeb.Endpoint, [])
        |> Keyword.get(:secret_key_base)
    end
  end

  defp validate_session_secret!(secret)
       when is_binary(secret) and byte_size(secret) >= @session_secret_min_bytes,
       do: secret

  defp validate_session_secret!(secret) do
    raise ArgumentError,
          "the web UI refuses to boot without a session secret of at least " <>
            "#{@session_secret_min_bytes} bytes (got #{byte_size(secret || "")}): set " <>
            "SMOLQUERY_SECRET_KEY_BASE on every node running the :web role. " <>
            "Every :web node needs the same value, or the LiveView socket " <>
            "cannot connect. Generate one with `mix phx.gen.secret`."
  end

  defp session_marker(secret_key_base, username, password) do
    Base.encode64(:crypto.mac(:hmac, :sha256, secret_key_base, [username, 0, password]))
  end

  use Smolquery.Runtime

  @doc """
  The top-level supervisor for an instance, as `Supervisor.start_link/1` names it.
  """
  @spec supervisor(atom()) :: atom()
  def supervisor(name), do: Module.concat(name, "Supervisor")

  @doc """
  The engine instance the UI resolves the catalog through.
  """
  @spec catalog_engine(atom()) :: atom()
  def catalog_engine(name), do: Module.concat(name, "Catalog")
end
