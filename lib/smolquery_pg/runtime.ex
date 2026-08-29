defmodule SmolqueryPg.Runtime do
  @moduledoc """
  A running Postgres wire edge's resolved configuration (PL-58).

  The same shape as `SmolqueryApi.Runtime`: configuration becomes a struct
  once at boot and lands in `:persistent_term`, so every connection reads the
  password and the query-service name for free. Naming derives from one
  instance name, so a test can run an isolated edge beside the application's
  own.

  ## Configuration

      config :smolquery, SmolqueryPg,
        password: "...",
        ip: {127, 0, 0, 1},
        port: 5432

  `password` is what every client must present at startup. It defaults to
  the API key (`config :smolquery, SmolqueryApi, api_key: ...`), so one
  credential opens both front doors. `SMOLQUERY_PG_PASSWORD` sets a separate
  one. There is no fallback past that: a node holding the `:pg` role with
  neither configured refuses to boot rather than serve an open listener.

  The listener binds loopback by default. `auth` selects how the password
  crosses: `:scram_sha_256` (the default — the password never crosses at
  all) or `:cleartext` for a legacy client. `tls_cert`/`tls_key`
  (`SMOLQUERY_PG_TLS_CERT`/`_KEY`) make the edge accept `SSLRequest` and
  upgrade the connection; without them it declines and the session stays
  plaintext, which is what binding beyond loopback should not do.

  `query_name` is the `Smolquery.QueryService` instance every `SELECT` runs
  through.
  """

  alias Smolquery.Catalog
  alias SmolqueryPg.Scram

  @enforce_keys [:name, :password]
  @derive {Inspect, except: [:password]}
  defstruct [
    :name,
    :password,
    :catalog,
    :catalog_opts,
    :tls_cert,
    :tls_key,
    :scram,
    query_name: Smolquery.QueryService,
    auth: :scram_sha_256,
    auth_timeout_ms: 30_000,
    idle_in_transaction_timeout_ms: 300_000,
    ip: {127, 0, 0, 1},
    port: 5432
  ]

  @type t :: %__MODULE__{
          name: atom(),
          password: String.t(),
          catalog: Catalog.t(),
          catalog_opts: keyword() | nil,
          query_name: atom(),
          auth: :scram_sha_256 | :cleartext,
          auth_timeout_ms: pos_integer(),
          idle_in_transaction_timeout_ms: non_neg_integer(),
          tls_cert: Path.t() | nil,
          tls_key: Path.t() | nil,
          scram: Scram.verifier(),
          ip: :inet.ip_address(),
          port: :inet.port_number()
        }

  @doc """
  Resolves configuration into a runtime.

  Application config for `SmolqueryPg` supplies the defaults; `opts`
  overrides them. Raises if no non-empty password is present in either, or
  as the API key.
  """
  @spec new(keyword()) :: t()
  def new(opts \\ []) do
    config =
      Application.get_env(:smolquery, SmolqueryPg, [])
      |> Keyword.merge(opts)
      |> Keyword.put_new_lazy(:password, &api_key/0)

    name = Keyword.get(config, :name, SmolqueryPg)

    {catalog, catalog_opts} =
      Catalog.DuckLake.resolve(Keyword.get(config, :catalog), lake_engine(name))

    %__MODULE__{
      name: name,
      catalog: catalog,
      catalog_opts: catalog_opts,
      password:
        Smolquery.Runtime.fetch_required!(config, :password,
          service: "the Postgres wire edge",
          missing: "a password",
          env_var: "SMOLQUERY_PG_PASSWORD (or SMOLQUERY_API_KEY)",
          scope: SmolqueryPg,
          role: :pg
        )
    }
    |> struct!(
      Keyword.take(config, [
        :query_name,
        :auth,
        :auth_timeout_ms,
        :idle_in_transaction_timeout_ms,
        :tls_cert,
        :tls_key,
        :ip,
        :port
      ])
    )
    |> derive_scram()
    |> validate_tls()
  end

  defp derive_scram(%__MODULE__{password: password} = runtime),
    do: %{runtime | scram: Scram.verifier(password)}

  defp validate_tls(%__MODULE__{tls_cert: nil, tls_key: nil} = runtime), do: runtime

  defp validate_tls(%__MODULE__{tls_cert: cert, tls_key: key} = runtime)
       when is_binary(cert) and is_binary(key) do
    validate_pem!("tls_cert", cert)
    validate_pem!("tls_key", key)

    runtime
  end

  defp validate_tls(_runtime) do
    raise ArgumentError,
          "the Postgres wire edge takes tls_cert and tls_key together " <>
            "(SMOLQUERY_PG_TLS_CERT and SMOLQUERY_PG_TLS_KEY), or neither"
  end

  defp validate_pem!(label, path) do
    case File.read(path) do
      {:ok, pem} ->
        if :public_key.pem_decode(pem) == [] do
          raise ArgumentError, "the Postgres wire edge's #{label} holds no PEM entries: #{path}"
        end

      {:error, reason} ->
        raise ArgumentError,
              "the Postgres wire edge cannot read its #{label} (#{inspect(reason)}): #{path}"
    end
  end

  @doc """
  Whether the edge answers `SSLRequest` with an upgrade.
  """
  @spec tls?(t()) :: boolean()
  def tls?(%__MODULE__{tls_cert: cert}), do: cert != nil

  defp api_key, do: Keyword.get(Application.get_env(:smolquery, SmolqueryApi, []), :api_key)

  use Smolquery.Runtime

  @doc """
  The top-level supervisor for an instance, as `Supervisor.start_link/1` names it.
  """
  @spec supervisor(atom()) :: atom()
  def supervisor(name), do: Module.concat(name, "Supervisor")

  @doc """
  The `ThousandIsland` server for an instance, as its supervisor is named.
  """
  @spec listener(atom()) :: atom()
  def listener(name), do: Module.concat(name, "Listener")

  @doc """
  The registry that maps a session's backend key to the job it is running,
  for `CancelRequest`.
  """
  @spec cancels(atom()) :: atom()
  def cancels(name), do: Module.concat(name, "Cancels")

  @doc """
  The `SmolqueryPg.PgCatalog` server for an instance.
  """
  @spec pg_catalog(atom()) :: atom()
  def pg_catalog(name), do: Module.concat(name, "PgCatalog")

  @doc """
  The DuckDB engine the catalog emulation runs in.
  """
  @spec catalog_engine(atom()) :: atom()
  def catalog_engine(name), do: Module.concat(name, "CatalogEngine")

  @doc """
  The engine a runtime-owned `Smolquery.Catalog.DuckLake` reads through.
  """
  @spec lake_engine(atom()) :: atom()
  def lake_engine(name), do: Module.concat(name, "Lake")
end
