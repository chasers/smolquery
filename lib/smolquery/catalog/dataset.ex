defmodule Smolquery.Catalog.Dataset do
  @moduledoc """
  A dataset and where its metadata and its segments live (PL-51).

  A dataset used to be a schema name and nothing more. This struct makes it a
  record, so a dataset can name its own DuckLake metadata database
  (`:catalog`) and its own object store (`:storage`). Either axis left `nil`
  means the deployment default — `CATALOG_DATABASE_URL` and
  `SMOLQUERY_S3_BUCKET` — so a dataset created with only a name behaves
  exactly as before. The two axes are independent.

  The target is a Supabase project: its Postgres is the metadata database and
  its Storage S3 endpoint holds the segments. Both are plain protocols, so the
  same fields serve any Postgres and any S3-compatible store.

  ## Secrets are sealed, never read back

  `Smolquery.Catalog.Connection` set the rule this follows. `Catalog.secret`
  holds the sealed Postgres password and `Storage.secret` the sealed S3 secret
  key; `new/1` takes the plaintext, seals it, and drops it. `metadata/1` and
  `store_options/1` are the only paths back to the cleartext, and both exist
  for an engine or a store to consume, never for a client. Every struct here
  derives `Inspect` with its secret excluded, and `to_json/1` names the fields
  a client may see.

  ## What may change after creation

  A dataset's name, its metadata database, and its bucket are its identity:
  segments are registered under them, so a change would orphan every segment
  the dataset holds. `update/2` therefore accepts only credentials — the
  Postgres username and password, and the S3 access key pair — and refuses
  any other field with `{:error, {:immutable_field, name}}`.

  ## Storage credentials: both keys or neither

  `Smolquery.Segments.Store.S3` takes static keys or the AWS credential
  chain, and half a pair is a configuration error rather than a chain. The
  same rule applies here, at registration, so a store built from a dataset
  never raises for a shape the API could have refused.
  """

  alias Smolquery.DatabaseUrl
  alias Smolquery.Identifier
  alias Smolquery.Secrets

  defmodule Catalog do
    @moduledoc """
    A dataset's own DuckLake metadata database — a Postgres the dataset's
    lake attaches to.

    `sslmode` defaults to `require` for the reason
    `Smolquery.Catalog.Connection` gives: a metadata database crosses a
    network by definition.

    `version` is the DuckLake format the lake is pinned to (PL-51 D7). Every
    attach names it, so DuckLake serves the lake at exactly that format,
    creates a fresh database at it, and never migrates on first touch. It
    is part of the dataset's identity, not a credential: it changes only
    through an explicit per-dataset upgrade, never through `update/2`.
    """

    @derive {Inspect, except: [:secret]}
    @enforce_keys [:host, :port, :database, :username, :secret, :sslmode, :version]
    defstruct [:host, :port, :database, :username, :secret, :sslmode, :version]

    @type t :: %__MODULE__{
            host: String.t(),
            port: :inet.port_number(),
            database: String.t(),
            username: String.t(),
            secret: String.t(),
            sslmode: String.t(),
            version: String.t()
          }
  end

  defmodule Storage do
    @moduledoc """
    A dataset's own object store: an S3 bucket, optionally under a key
    prefix, with either static keys or the AWS credential chain.

    `prefix` is what makes a shared bucket usable — a Supabase Storage
    bucket usually holds more than one thing — and it is part of the
    dataset's identity for the same reason the bucket is.
    """

    @derive {Inspect, except: [:secret]}
    @enforce_keys [:bucket, :prefix, :region]
    defstruct [:bucket, :prefix, :endpoint, :region, :url_style, :access_key_id, :secret]

    @type t :: %__MODULE__{
            bucket: String.t(),
            prefix: String.t(),
            endpoint: String.t() | nil,
            region: String.t(),
            url_style: String.t() | nil,
            access_key_id: String.t() | nil,
            secret: String.t() | nil
          }
  end

  @enforce_keys [:name]
  defstruct [:name, :catalog, :storage, :created_at, :updated_at]

  @type t :: %__MODULE__{
          name: String.t(),
          catalog: Catalog.t() | nil,
          storage: Storage.t() | nil,
          created_at: integer() | nil,
          updated_at: integer() | nil
        }

  @sslmodes ~w(disable allow prefer require verify-ca verify-full)
  @default_sslmode "require"
  @default_version "1.0"
  @version_pattern ~r/^\d+\.\d+$/
  @default_port 5432
  @default_region "us-east-1"
  @url_styles ~w(path vhost)
  @bucket_pattern ~r/^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$/
  @prefix_pattern ~r/^[A-Za-z0-9_.\-\/]*$/
  @catalog_credentials ~w(username password)
  @storage_credentials ~w(access_key_id secret_access_key)
  @lake_prefix "ds_"

  @doc """
  The `sslmode` values a dataset's catalog may carry, libpq's own list.
  """
  @spec sslmodes() :: [String.t()]
  def sslmodes, do: @sslmodes

  @doc """
  The `url_style` values a dataset's storage may carry.
  """
  @spec url_styles() :: [String.t()]
  def url_styles, do: @url_styles

  @doc """
  The DuckLake format a new dataset's own catalog is pinned to when the
  request names none: `SMOLQUERY_DUCKLAKE_VERSION`, or `#{@default_version}`.
  """
  @spec default_version() :: String.t()
  def default_version, do: Application.get_env(:smolquery, :ducklake_version, @default_version)

  @doc """
  A dataset on the deployment defaults: a name and nothing else.

  This is the shape every dataset created before PL-51 has, and what the
  string form of `Smolquery.Catalog.create_dataset/2` builds.
  """
  @spec default(String.t()) :: {:ok, t()} | {:error, term()}
  def default(name) do
    with {:ok, name} <- Identifier.validate(name) do
      {:ok, %__MODULE__{name: name}}
    end
  end

  @doc """
  Builds a dataset, sealing the plaintext secrets.

  Takes a map keyed by strings — the shape the API and the UI both hold.
  `"catalog"` and `"storage"` are optional maps; an absent or `nil` axis
  means the deployment default.

      Dataset.new(%{
        "name" => "analytics",
        "catalog" => %{"host" => "db.abc.supabase.co", "database" => "postgres",
                       "username" => "postgres", "password" => "...",
                       "version" => "1.0"},
        "storage" => %{"bucket" => "lake", "prefix" => "analytics",
                       "endpoint" => "https://abc.storage.supabase.co/storage/v1/s3",
                       "region" => "us-east-1", "url_style" => "path",
                       "access_key_id" => "...", "secret_access_key" => "..."}
      })
  """
  @spec new(map()) :: {:ok, t()} | {:error, term()}
  def new(params) when is_map(params) do
    with {:ok, name} <- name(params),
         {:ok, catalog} <- axis(params, "catalog", &new_catalog/1),
         {:ok, storage} <- axis(params, "storage", &new_storage/1) do
      {:ok, %__MODULE__{name: name, catalog: catalog, storage: storage}}
    end
  end

  @doc """
  Applies a credential change to an existing dataset.

  Only `catalog.username`, `catalog.password`, `storage.access_key_id`, and
  `storage.secret_access_key` may appear; any other field, and either axis on
  a dataset that uses the default for it, is `{:error, {:immutable_field, name}}`.
  A storage key pair must still arrive whole.
  """
  @spec update(t(), map()) :: {:ok, t()} | {:error, term()}
  def update(%__MODULE__{} = dataset, params) when is_map(params) do
    with :ok <- only_keys(params, ["catalog", "storage"], ""),
         {:ok, catalog} <- update_catalog(dataset.catalog, Map.get(params, "catalog")),
         {:ok, storage} <- update_storage(dataset.storage, Map.get(params, "storage")) do
      {:ok, %{dataset | catalog: catalog, storage: storage}}
    end
  end

  @doc """
  The DuckDB catalog alias a dataset with its own metadata database attaches
  under — `ds_<name>` — and `nil` for a dataset on the default lake.

  The alias is derived rather than stored so it can never disagree with the
  name, and the `ds_` prefix keeps it clear of the default lake's own alias
  and of any name a dataset could take.
  """
  @spec lake(t() | String.t()) :: String.t() | nil
  def lake(%__MODULE__{catalog: nil}), do: nil
  def lake(%__MODULE__{name: name}), do: lake(name)
  def lake(name) when is_binary(name), do: @lake_prefix <> name

  @doc """
  The DuckLake metadata string for a dataset's own catalog, with the
  password opened — what an `ATTACH 'ducklake:<metadata>'` takes.

  `nil` for a dataset on the default lake. The cleartext exists only in the
  returned string, which the caller passes straight into the statement.
  """
  @spec metadata(t()) :: {:ok, String.t() | nil} | {:error, term()}
  def metadata(%__MODULE__{catalog: nil}), do: {:ok, nil}

  def metadata(%__MODULE__{catalog: %Catalog{} = catalog}) do
    with {:ok, password} <- Secrets.open(catalog.secret) do
      {:ok,
       DatabaseUrl.libpq_metadata(%{
         database: catalog.database,
         hostname: catalog.host,
         port: catalog.port,
         username: catalog.username,
         password: password,
         sslmode: catalog.sslmode
       })}
    end
  end

  @doc """
  The options `Smolquery.Segments.Store.S3.new/1` takes for a dataset's own
  store, with the secret key opened — everything but `:staging_dir`, which
  is the node's to supply. The DuckDB secret is named after the dataset
  (`secret_name/1`), so it never collides with the default store's.

  `nil` for a dataset on the default store.
  """
  @spec store_options(t()) :: {:ok, keyword() | nil} | {:error, term()}
  def store_options(%__MODULE__{storage: nil}), do: {:ok, nil}

  def store_options(%__MODULE__{storage: %Storage{} = storage} = dataset) do
    with {:ok, credentials} <- store_credentials(storage) do
      {:ok,
       Enum.reject(
         [
           bucket: storage.bucket,
           prefix: storage.prefix,
           endpoint: storage.endpoint,
           region: storage.region,
           url_style: storage.url_style,
           secret_name: secret_name(dataset)
         ] ++ credentials,
         fn {_option, value} -> is_nil(value) end
       )}
    end
  end

  @doc """
  The name of the DuckDB secret a dataset's own store creates:
  `smolquery_ds_<name>`. Derived, like `lake/1`, so it never disagrees with
  the name.
  """
  @spec secret_name(t() | String.t()) :: String.t()
  def secret_name(%__MODULE__{name: name}), do: secret_name(name)
  def secret_name(name) when is_binary(name), do: "smolquery_" <> lake(name)

  @doc """
  Whether two datasets carry the same identity: the same name, catalog, and
  storage, credentials aside.

  This is what makes `Smolquery.Catalog.create_dataset/2` idempotent — a
  repeat with the same settings is a no-op, one with different settings a
  conflict. Credentials are left out because a secret is sealed with a fresh
  IV each time and can never compare equal, and because they are the one
  thing a dataset may change.
  """
  @spec same_settings?(t(), t()) :: boolean()
  def same_settings?(%__MODULE__{} = left, %__MODULE__{} = right) do
    identity(left) == identity(right)
  end

  defp identity(%__MODULE__{} = dataset) do
    dataset
    |> to_json()
    |> Map.take(["name", "catalog", "storage"])
    |> Map.update!("catalog", &drop_credentials(&1, ["username"]))
    |> Map.update!("storage", &drop_credentials(&1, ["access_key_id"]))
  end

  defp drop_credentials(nil, _keys), do: nil
  defp drop_credentials(axis, keys), do: Map.drop(axis, keys)

  @doc """
  The dataset as JSON-ready data, without its secrets.

  Every read surface answers through here, so a field reaches a client only
  by being named in this map.
  """
  @spec to_json(t()) :: map()
  def to_json(%__MODULE__{} = dataset) do
    %{
      "name" => dataset.name,
      "catalog" => catalog_json(dataset.catalog),
      "storage" => storage_json(dataset.storage),
      "createdAt" => dataset.created_at,
      "updatedAt" => dataset.updated_at
    }
  end

  defp catalog_json(nil), do: nil

  defp catalog_json(%Catalog{} = catalog) do
    %{
      "host" => catalog.host,
      "port" => catalog.port,
      "database" => catalog.database,
      "username" => catalog.username,
      "sslmode" => catalog.sslmode,
      "version" => catalog.version
    }
  end

  defp storage_json(nil), do: nil

  defp storage_json(%Storage{} = storage) do
    %{
      "bucket" => storage.bucket,
      "prefix" => storage.prefix,
      "endpoint" => storage.endpoint,
      "region" => storage.region,
      "url_style" => storage.url_style,
      "access_key_id" => storage.access_key_id
    }
  end

  defp name(params) do
    with {:ok, name} <- required(params, "name", ""), do: Identifier.validate(name)
  end

  defp axis(params, key, build) do
    case Map.get(params, key) do
      nil -> {:ok, nil}
      value when is_map(value) -> build.(value)
      _invalid -> {:error, {:invalid_param, key}}
    end
  end

  defp new_catalog(params) do
    with {:ok, host} <- required(params, "host", "catalog."),
         {:ok, database} <- required(params, "database", "catalog."),
         {:ok, username} <- required(params, "username", "catalog."),
         {:ok, password} <- required(params, "password", "catalog."),
         {:ok, port} <- port(params, @default_port),
         {:ok, sslmode} <- sslmode(params, @default_sslmode),
         {:ok, version} <- version(params),
         {:ok, secret} <- Secrets.seal(password) do
      {:ok,
       %Catalog{
         host: host,
         port: port,
         database: database,
         username: username,
         secret: secret,
         sslmode: sslmode,
         version: version
       }}
    end
  end

  defp new_storage(params) do
    with {:ok, bucket} <- bucket(params),
         {:ok, prefix} <- prefix(params),
         {:ok, endpoint} <- optional_string(params, "endpoint", "storage."),
         {:ok, region} <- string(params, "region", @default_region, "storage."),
         {:ok, url_style} <- url_style(params),
         {:ok, {access_key_id, secret}} <- storage_credentials(params, {nil, nil}) do
      {:ok,
       %Storage{
         bucket: bucket,
         prefix: prefix,
         endpoint: endpoint,
         region: region,
         url_style: url_style,
         access_key_id: access_key_id,
         secret: secret
       }}
    end
  end

  defp update_catalog(catalog, nil), do: {:ok, catalog}
  defp update_catalog(nil, _params), do: {:error, {:immutable_field, "catalog"}}

  defp update_catalog(%Catalog{} = catalog, params) when is_map(params) do
    with :ok <- only_keys(params, @catalog_credentials, "catalog."),
         {:ok, username} <- optional(params, "username", catalog.username, "catalog."),
         {:ok, secret} <- sealed(params, "password", catalog.secret, "catalog.") do
      {:ok, %{catalog | username: username, secret: secret}}
    end
  end

  defp update_catalog(_catalog, _invalid), do: {:error, {:invalid_param, "catalog"}}

  defp update_storage(storage, nil), do: {:ok, storage}
  defp update_storage(nil, _params), do: {:error, {:immutable_field, "storage"}}

  defp update_storage(%Storage{} = storage, params) when is_map(params) do
    current = {storage.access_key_id, storage.secret}

    with :ok <- only_keys(params, @storage_credentials, "storage."),
         {:ok, {access_key_id, secret}} <- storage_credentials(params, current) do
      {:ok, %{storage | access_key_id: access_key_id, secret: secret}}
    end
  end

  defp update_storage(_storage, _invalid), do: {:error, {:invalid_param, "storage"}}

  defp only_keys(params, allowed, scope) do
    case Enum.find(Map.keys(params), &(&1 not in allowed)) do
      nil -> :ok
      key -> {:error, {:immutable_field, scope <> to_string(key)}}
    end
  end

  defp storage_credentials(params, {current_id, current_secret}) do
    given = {Map.get(params, "access_key_id"), Map.get(params, "secret_access_key")}

    case given do
      {nil, nil} ->
        {:ok, {current_id, current_secret}}

      {id, key} when is_binary(id) and id != "" and is_binary(key) and key != "" ->
        with {:ok, secret} <- Secrets.seal(key), do: {:ok, {id, secret}}

      {_id, nil} ->
        {:error, {:missing_field, "storage.secret_access_key"}}

      {nil, _key} ->
        {:error, {:missing_field, "storage.access_key_id"}}

      _blank ->
        {:error, {:invalid_param, "storage.access_key_id"}}
    end
  end

  defp store_credentials(%Storage{access_key_id: nil}), do: {:ok, []}

  defp store_credentials(%Storage{} = storage) do
    with {:ok, key} <- Secrets.open(storage.secret) do
      {:ok, [access_key_id: storage.access_key_id, secret_access_key: key]}
    end
  end

  defp required(params, key, scope) do
    case Map.get(params, key) do
      value when is_binary(value) and value != "" -> {:ok, value}
      _missing -> {:error, {:missing_field, scope <> key}}
    end
  end

  defp optional(params, key, current, scope) do
    if Map.has_key?(params, key), do: required(params, key, scope), else: {:ok, current}
  end

  defp optional_string(params, key, scope) do
    case Map.get(params, key) do
      nil -> {:ok, nil}
      "" -> {:ok, nil}
      value when is_binary(value) -> {:ok, value}
      _invalid -> {:error, {:invalid_param, scope <> key}}
    end
  end

  defp string(params, key, default, scope) do
    case Map.get(params, key, default) do
      value when is_binary(value) and value != "" -> {:ok, value}
      _invalid -> {:error, {:invalid_param, scope <> key}}
    end
  end

  defp sealed(params, key, current, scope) do
    if Map.has_key?(params, key) do
      with {:ok, plaintext} <- required(params, key, scope), do: Secrets.seal(plaintext)
    else
      {:ok, current}
    end
  end

  defp port(params, default) do
    case Map.get(params, "port", default) do
      port when is_integer(port) and port in 1..65_535 -> {:ok, port}
      _invalid -> {:error, {:invalid_param, "catalog.port"}}
    end
  end

  defp version(params) do
    case Map.get(params, "version", default_version()) do
      version when is_binary(version) ->
        if Regex.match?(@version_pattern, version),
          do: {:ok, version},
          else: {:error, {:invalid_param, "catalog.version"}}

      _invalid ->
        {:error, {:invalid_param, "catalog.version"}}
    end
  end

  defp sslmode(params, default) do
    case Map.get(params, "sslmode", default) do
      mode when mode in @sslmodes -> {:ok, mode}
      _invalid -> {:error, {:invalid_param, "catalog.sslmode"}}
    end
  end

  defp bucket(params) do
    with {:ok, bucket} <- required(params, "bucket", "storage.") do
      if Regex.match?(@bucket_pattern, bucket),
        do: {:ok, bucket},
        else: {:error, {:invalid_param, "storage.bucket"}}
    end
  end

  defp prefix(params) do
    case Map.get(params, "prefix", "") do
      prefix when is_binary(prefix) ->
        trimmed = String.trim(prefix, "/")

        if Regex.match?(@prefix_pattern, trimmed) and not String.contains?(trimmed, "..") and
             not String.contains?(trimmed, "//"),
           do: {:ok, trimmed},
           else: {:error, {:invalid_param, "storage.prefix"}}

      _invalid ->
        {:error, {:invalid_param, "storage.prefix"}}
    end
  end

  defp url_style(params) do
    case Map.get(params, "url_style") do
      nil -> {:ok, nil}
      "" -> {:ok, nil}
      style when style in @url_styles -> {:ok, style}
      _invalid -> {:error, {:invalid_param, "storage.url_style"}}
    end
  end
end
