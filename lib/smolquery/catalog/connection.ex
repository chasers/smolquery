defmodule Smolquery.Catalog.Connection do
  @moduledoc """
  A registered external database a query may join against: a Postgres
  database (T-322), or another DuckLake, its catalog and its files (T-610).

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

  ## Two kinds

  `kind` is `"postgres"`, the default and every connection made before
  T-610, or `"ducklake"`. A DuckLake connection is the lake's metadata
  database, which is Postgres, so it carries the same host, port, database,
  username, sealed password and `sslmode` a Postgres connection does, plus:

    * `data_path`, where the lake's files live, required: an `s3://` URL, or a
      local path under one of `Smolquery.Federation`'s `:local_roots`, which
      are none by default, so a connection cannot hand a query the node's own
      filesystem. The query engine is allowed to read exactly this path.
    * optional S3 credentials for it, `s3_key_id` and `s3_secret`, both or
      neither, with `s3_region`, `s3_endpoint` and `s3_url_style`. The secret
      is sealed like the password, into `:storage_secret`, and like it is
      never returned.

  The kind is fixed at registration: `update/2` does not change it.

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

  @derive {Inspect, except: [:secret, :storage_secret]}
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
    :updated_at,
    kind: "postgres",
    data_path: nil,
    storage: %{},
    storage_secret: nil
  ]

  @type storage :: %{
          optional(:key_id) => String.t(),
          optional(:region) => String.t(),
          optional(:endpoint) => String.t(),
          optional(:url_style) => String.t()
        }

  @type t :: %__MODULE__{
          name: String.t(),
          host: String.t(),
          port: :inet.port_number(),
          database: String.t(),
          username: String.t(),
          secret: String.t(),
          sslmode: String.t(),
          created_at: integer() | nil,
          updated_at: integer() | nil,
          kind: String.t(),
          data_path: String.t() | nil,
          storage: storage(),
          storage_secret: String.t() | nil
        }

  @kinds ~w(postgres ducklake)
  @url_styles ~w(path vhost)
  @storage_fields [
    {"s3_key_id", :key_id},
    {"s3_region", :region},
    {"s3_endpoint", :endpoint},
    {"s3_url_style", :url_style}
  ]

  @sslmodes ~w(disable allow prefer require verify-ca verify-full)
  @default_sslmode "require"
  @default_port 5432

  @doc """
  The `sslmode` values a connection may carry, libpq's own list.
  """
  @spec sslmodes() :: [String.t()]
  def sslmodes, do: @sslmodes

  @doc """
  The kinds a connection may be.
  """
  @spec kinds() :: [String.t()]
  def kinds, do: @kinds

  @doc """
  The port a connection uses when none is given.
  """
  @spec default_port() :: :inet.port_number()
  def default_port, do: @default_port

  @doc """
  Builds a connection, sealing the plaintext `:password` into `:secret`.

  Takes a map keyed by strings — the shape the API and the UI both already
  hold — so neither has to convert before validating. `:kind`, `:port` and
  `:sslmode` have defaults; a DuckLake connection also requires
  `"data_path"`; everything else is required.
  """
  @spec new(map()) :: {:ok, t()} | {:error, term()}
  def new(params) when is_map(params) do
    with {:ok, name} <- name(params),
         {:ok, kind} <- kind(params),
         {:ok, host} <- required(params, "host"),
         {:ok, database} <- required(params, "database"),
         {:ok, username} <- required(params, "username"),
         {:ok, password} <- required(params, "password"),
         {:ok, port} <- port(params),
         {:ok, sslmode} <- sslmode(params),
         {:ok, secret} <- Secrets.seal(password) do
      %__MODULE__{
        name: name,
        kind: kind,
        host: host,
        port: port,
        database: database,
        username: username,
        secret: secret,
        sslmode: sslmode
      }
      |> with_lake(params)
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
      %{
        connection
        | host: host,
          database: database,
          username: username,
          port: port,
          sslmode: sslmode,
          secret: secret
      }
      |> with_lake(params)
    end
  end

  @doc """
  The S3 secret of a DuckLake connection's storage, opened, or `nil` when it
  has none. Like `connection_string/1`, the only path back to a cleartext:
  `Smolquery.Federation` passes it straight into a `CREATE SECRET`.
  """
  @spec storage_secret(t()) :: {:ok, String.t() | nil} | {:error, term()}
  def storage_secret(%__MODULE__{storage_secret: nil}), do: {:ok, nil}
  def storage_secret(%__MODULE__{storage_secret: sealed}), do: Secrets.open(sealed)

  @doc """
  What `Smolquery.Catalog` stores beside the columns every kind shares: the
  data path and the storage settings that are not secret, as JSON-ready data.
  """
  @spec options(t()) :: map()
  def options(%__MODULE__{data_path: nil, storage: storage}) when map_size(storage) == 0,
    do: %{}

  def options(%__MODULE__{} = connection) do
    connection.storage
    |> Map.new(fn {key, value} -> {Atom.to_string(key), value} end)
    |> Map.put("data_path", connection.data_path)
  end

  @doc """
  The kind-specific fields `options/1` stored, and the sealed storage secret,
  put back on a connection read from the catalog.
  """
  @spec with_options(t(), map(), String.t() | nil) :: t()
  def with_options(%__MODULE__{} = connection, options, storage_secret) do
    storage =
      for {_param, key} <- @storage_fields,
          value = Map.get(options, Atom.to_string(key)),
          is_binary(value),
          into: %{},
          do: {key, value}

    %{
      connection
      | data_path: Map.get(options, "data_path"),
        storage: storage,
        storage_secret: storage_secret
    }
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
      "kind" => connection.kind,
      "createdAt" => connection.created_at,
      "updatedAt" => connection.updated_at
    }
    |> Map.merge(lake_json(connection))
  end

  defp lake_json(%__MODULE__{kind: "ducklake"} = connection) do
    %{
      "dataPath" => connection.data_path,
      "s3" =>
        connection.storage
        |> Map.new(fn {key, value} -> {json_key(key), value} end)
        |> Map.put("hasSecret", connection.storage_secret != nil)
    }
  end

  defp lake_json(_connection), do: %{}

  defp json_key(:key_id), do: "keyId"
  defp json_key(:url_style), do: "urlStyle"
  defp json_key(key), do: Atom.to_string(key)

  defp with_lake(%__MODULE__{kind: "postgres"} = connection, _params), do: {:ok, connection}

  defp with_lake(%__MODULE__{kind: "ducklake"} = connection, params) do
    with {:ok, data_path} <- data_path(params, connection.data_path),
         {:ok, storage} <- storage(params, connection.storage),
         {:ok, storage_secret} <- storage_secret_param(params, connection.storage_secret),
         :ok <- paired(storage, storage_secret) do
      {:ok,
       %{connection | data_path: data_path, storage: storage, storage_secret: storage_secret}}
    end
  end

  defp kind(params) do
    case Map.get(params, "kind", "postgres") do
      kind when kind in @kinds -> {:ok, kind}
      _invalid -> {:error, {:invalid_param, "kind"}}
    end
  end

  defp data_path(params, current) do
    case Map.get(params, "data_path", current) do
      "s3://" <> rest = path when rest != "" -> {:ok, path}
      path when is_binary(path) and path != "" -> local_path(path)
      _missing -> {:error, {:missing_field, "data_path"}}
    end
  end

  defp local_path(path) do
    expanded = Path.expand(path)

    if Enum.any?(local_roots(), &within?(expanded, Path.expand(&1))),
      do: {:ok, path},
      else: {:error, {:invalid_param, "data_path"}}
  end

  defp within?(path, root), do: path == root or String.starts_with?(path, root <> "/")

  defp local_roots do
    :smolquery |> Application.get_env(Smolquery.Federation, []) |> Keyword.get(:local_roots, [])
  end

  defp storage(params, current) do
    Enum.reduce_while(@storage_fields, {:ok, current}, fn {param, key}, {:ok, storage} ->
      case Map.fetch(params, param) do
        :error -> {:cont, {:ok, storage}}
        {:ok, value} when value in [nil, ""] -> {:cont, {:ok, Map.delete(storage, key)}}
        {:ok, value} -> storage_value(key, value, storage)
      end
    end)
  end

  defp storage_value(:url_style, value, _storage) when value not in @url_styles,
    do: {:halt, {:error, {:invalid_param, "s3_url_style"}}}

  defp storage_value(key, value, storage) when is_binary(value),
    do: {:cont, {:ok, Map.put(storage, key, value)}}

  defp storage_value(key, _value, _storage),
    do: {:halt, {:error, {:invalid_param, "s3_" <> Atom.to_string(key)}}}

  defp storage_secret_param(params, current) do
    case Map.fetch(params, "s3_secret") do
      :error -> {:ok, current}
      {:ok, value} when value in [nil, ""] -> {:ok, nil}
      {:ok, value} when is_binary(value) -> Secrets.seal(value)
      {:ok, _invalid} -> {:error, {:invalid_param, "s3_secret"}}
    end
  end

  defp paired(storage, secret) do
    if Map.has_key?(storage, :key_id) == (secret != nil),
      do: :ok,
      else: {:error, {:invalid_param, "s3_secret"}}
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
