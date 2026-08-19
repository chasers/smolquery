defmodule Smolquery.Catalog.Connection do
  @moduledoc """
  A registered Postgres database a query may join against (T-322).

  The catalog is where these live, for the reason `Smolquery.Catalog.DuckLake`
  gives its partition-count side table: every node must read one answer rather
  than each trusting its own configuration. A connection registered through the
  API on one node has to be visible to whichever node plans the next query.

  ## The password is never a field a caller can read

  `:secret` holds what `Smolquery.Secrets` sealed, not a password. `new/1`
  takes the plaintext, seals it, and drops it; nothing here returns it, and
  `connection_string/1` is the only path back to the cleartext. The struct
  derives `Inspect` with `:secret` excluded, so a connection in a log line, a
  crash report, or an error envelope shows `#Connection<...>` rather than the
  ciphertext an offline attack would want.

  ## `name` is an identifier because DuckDB will resolve it

  A connection's name becomes the catalog alias a federated query qualifies
  with (`mypg.public.users`), and it is interpolated into an `ATTACH`. So it
  passes `Smolquery.Identifier.validate/1` here, at registration, rather than
  being escaped at every later use.

  ## `sslmode` defaults to `require`

  libpq defaults to `prefer`, which silently accepts plaintext when the server
  declines TLS — a default that turns a misconfigured server into an
  unencrypted credential on the wire, with nothing in the result to say so. A
  federated connection crosses a network by definition, so the default here is
  `require`. An operator who means to reach a local database without TLS says
  `disable` and has said it on purpose.
  """

  alias Smolquery.Identifier
  alias Smolquery.Secrets

  @derive {Inspect, except: [:secret]}
  @enforce_keys [:name, :host, :port, :database, :username, :secret, :sslmode]
  defstruct [
    :name,
    :host,
    :port,
    :database,
    :username,
    :secret,
    :sslmode,
    :created_at,
    :updated_at
  ]

  @type t :: %__MODULE__{
          name: String.t(),
          host: String.t(),
          port: :inet.port_number(),
          database: String.t(),
          username: String.t(),
          secret: String.t(),
          sslmode: String.t(),
          created_at: integer() | nil,
          updated_at: integer() | nil
        }

  @sslmodes ~w(disable allow prefer require verify-ca verify-full)
  @default_sslmode "require"
  @default_port 5432

  @doc """
  The `sslmode` values a connection may carry, libpq's own list.
  """
  @spec sslmodes() :: [String.t()]
  def sslmodes, do: @sslmodes

  @doc """
  The port a connection uses when none is given.
  """
  @spec default_port() :: :inet.port_number()
  def default_port, do: @default_port

  @doc """
  Builds a connection, sealing the plaintext `:password` into `:secret`.

  Takes a map keyed by strings — the shape the API and the UI both already
  hold — so neither has to convert before validating. `:port` and `:sslmode`
  have defaults; everything else is required.
  """
  @spec new(map()) :: {:ok, t()} | {:error, term()}
  def new(params) when is_map(params) do
    with {:ok, name} <- name(params),
         {:ok, host} <- required(params, "host"),
         {:ok, database} <- required(params, "database"),
         {:ok, username} <- required(params, "username"),
         {:ok, password} <- required(params, "password"),
         {:ok, port} <- port(params),
         {:ok, sslmode} <- sslmode(params),
         {:ok, secret} <- Secrets.seal(password) do
      {:ok,
       %__MODULE__{
         name: name,
         host: host,
         port: port,
         database: database,
         username: username,
         secret: secret,
         sslmode: sslmode
       }}
    end
  end

  @doc """
  Applies `params` to an existing connection, sealing a new password only when
  one is present.

  An absent `"password"` leaves the stored secret alone, which is what lets a
  caller edit a host or a port without re-entering a credential it can never
  read back.
  """
  @spec update(t(), map()) :: {:ok, t()} | {:error, term()}
  def update(%__MODULE__{} = connection, params) when is_map(params) do
    with {:ok, host} <- optional(params, "host", connection.host),
         {:ok, database} <- optional(params, "database", connection.database),
         {:ok, username} <- optional(params, "username", connection.username),
         {:ok, port} <- port(params, connection.port),
         {:ok, sslmode} <- sslmode(params, connection.sslmode),
         {:ok, secret} <- secret(params, connection.secret) do
      {:ok,
       %{
         connection
         | host: host,
           database: database,
           username: username,
           port: port,
           sslmode: sslmode,
           secret: secret
       }}
    end
  end

  @doc """
  The libpq connection string a DuckDB `ATTACH` takes, with the password
  opened.

  This is the only place the cleartext exists after registration. Callers pass
  the result straight into an `ATTACH` and keep no copy — see
  `Smolquery.QueryService.Runner` for how the statement's own failures are
  scrubbed before they reach an error envelope.
  """
  @spec connection_string(t()) :: {:ok, String.t()} | {:error, term()}
  def connection_string(%__MODULE__{} = connection) do
    with {:ok, password} <- Secrets.open(connection.secret) do
      {:ok,
       Enum.map_join(
         [
           {"dbname", connection.database},
           {"host", connection.host},
           {"port", Integer.to_string(connection.port)},
           {"user", connection.username},
           {"password", password},
           {"sslmode", connection.sslmode}
         ],
         " ",
         fn {key, value} -> "#{key}=#{quote_value(value)}" end
       )}
    end
  end

  @doc """
  The connection as JSON-ready data, without the secret.

  Every read surface answers through here, so a field can only reach a client
  by being named in this map.
  """
  @spec to_json(t()) :: map()
  def to_json(%__MODULE__{} = connection) do
    %{
      "name" => connection.name,
      "host" => connection.host,
      "port" => connection.port,
      "database" => connection.database,
      "username" => connection.username,
      "sslmode" => connection.sslmode,
      "createdAt" => connection.created_at,
      "updatedAt" => connection.updated_at
    }
  end

  defp quote_value(value) do
    if String.contains?(value, [" ", "'", "\\"]) do
      "'" <> String.replace(value, ["\\", "'"], &("\\" <> &1)) <> "'"
    else
      value
    end
  end

  defp name(params) do
    with {:ok, name} <- required(params, "name"), do: Identifier.validate(name)
  end

  defp required(params, key) do
    case Map.get(params, key) do
      value when is_binary(value) and value != "" -> {:ok, value}
      _missing -> {:error, {:missing_field, key}}
    end
  end

  defp optional(params, key, current) do
    if Map.has_key?(params, key), do: required(params, key), else: {:ok, current}
  end

  defp secret(params, current) do
    if Map.has_key?(params, "password") do
      with {:ok, password} <- required(params, "password"), do: Secrets.seal(password)
    else
      {:ok, current}
    end
  end

  defp port(params, default \\ @default_port) do
    case Map.get(params, "port", default) do
      port when is_integer(port) and port in 1..65_535 -> {:ok, port}
      _invalid -> {:error, {:invalid_param, "port"}}
    end
  end

  defp sslmode(params, default \\ @default_sslmode) do
    case Map.get(params, "sslmode", default) do
      mode when mode in @sslmodes -> {:ok, mode}
      _invalid -> {:error, {:invalid_param, "sslmode"}}
    end
  end
end
