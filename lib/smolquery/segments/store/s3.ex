defmodule Smolquery.Segments.Store.S3 do
  @moduledoc """
  `Smolquery.Segments.Store` over an S3-compatible object store, via `req_s3`
  (AWS S3, MinIO, R2, ...). Milestone 8 L3: the sealed tier's answer to
  "buffer-node disks hold only the hot tier" (PL-1 §8) — the hot tier stays
  on `Store.Local`, this is for `StorageService`'s configured store.

  `shared?/1` is `true`: a location this store returns is an `s3://` URL any
  node can read directly through DuckDB's `httpfs`, which is exactly the flag
  `QueryService.Planner` (and `StorageService.Compactor`, re-reading sealed
  segments to re-merge them) uses to skip the hop through
  `BufferService.HotServer` a non-shared store needs.

  ## Encoding still goes through a local file

  `put/3`'s encoder writes Parquet from Rust (`Smolquery.Segments.Writer`),
  so the staging path this store hands it is still a real local path under
  `:staging_dir` — S3 durability is a single `PUT`, atomic on S3's side, but
  there is no such thing as writing Parquet bytes to an object store as they
  are generated. `sweep_staging/2` cleans up a killed encoder's leftovers the
  same way `Store.Local` does.

  ## A committed path can never be blind-overwritten

  Every `put/3` PUT carries `If-None-Match: *`, so it can only ever create a
  key, never replace one. `StorageService.Handoff.Seal` re-encodes and
  re-uploads to the same deterministic key when it retries a claim whose
  merge already ran but whose catalog registration did not land before a
  crash (`register_segments/3` is idempotent by path for exactly that retry).
  Without the conditional write, a retry that itself dies mid-upload — a
  truncated re-encode racing an OOM kill, say — silently overwrites the
  fully-committed original with garbage bytes no reader can then parse
  (T-308). With it, that retry gets a `412 Precondition Failed` back; `put/3`
  treats that as success, since the object already there is the correct one
  and there is nothing left to do.

  ## Reading sealed segments back needs credentials DuckDB has, not just Elixir

  This module reads and writes bytes over HTTP; it says nothing about how the
  DuckDB engines that later `read_parquet('s3://...')` these locations
  authenticate. `create_secret_statement/1` returns the bootstrap `CREATE
  SECRET` for that — `StorageService.Supervisor` (compaction re-reads sealed
  segments) and `QueryService.Runner` (every query touching the sealed tier)
  both run it when their configured store is this module, the same way
  `Smolquery.InternalSecret.create_secret_statement/1` bootstraps the hot
  tier's header.

  ## Two credential modes, one rule

  Configure `:access_key_id` and `:secret_access_key` and both paths use those
  static keys. Configure neither and both paths use the AWS default credential
  chain — Elixir through `Smolquery.AwsCredentials`, DuckDB through `PROVIDER
  credential_chain`. That mode exists because an organization SCP denying
  `s3:*` to IAM users cannot be overridden by any IAM policy, and static keys
  are only ever an IAM user; EKS Pod Identity is the way in (T-240).

  The two resolvers are independent, but they read the same environment and
  refresh themselves, so neither side goes stale while the other rotates.

  ## Usage

      store = Smolquery.Segments.Store.S3.new(
        bucket: "smolquery-sealed",
        access_key_id: "...",
        secret_access_key: "...",
        endpoint: "http://minio:9000",
        staging_dir: "/var/lib/smolquery/sealed-staging"
      )

      store = Smolquery.Segments.Store.S3.new(
        bucket: "smolquery-sealed",
        region: "us-east-2",
        staging_dir: "/var/lib/smolquery/sealed-staging"
      )
  """

  @behaviour Smolquery.Segments.Store

  alias Smolquery.AwsCredentials
  alias Smolquery.Identifier
  alias Smolquery.Segments.Store

  @enforce_keys [:bucket, :staging_dir]
  defstruct [
    :bucket,
    :access_key_id,
    :secret_access_key,
    :staging_dir,
    :endpoint,
    region: "us-east-1",
    url_style: nil,
    list_max_keys: nil
  ]

  @type t :: %__MODULE__{
          bucket: String.t(),
          access_key_id: String.t() | nil,
          secret_access_key: String.t() | nil,
          staging_dir: String.t(),
          endpoint: String.t() | nil,
          region: String.t(),
          url_style: String.t() | nil,
          list_max_keys: pos_integer() | nil
        }

  @type option ::
          {:bucket, String.t()}
          | {:access_key_id, String.t()}
          | {:secret_access_key, String.t()}
          | {:staging_dir, String.t()}
          | {:endpoint, String.t()}
          | {:region, String.t()}
          | {:url_style, String.t()}
          | {:list_max_keys, pos_integer()}

  @staging ".tmp"
  @chunk_bytes 1_048_576
  @secret_name "smolquery_sealed_tier"

  @doc """
  A store over `:bucket`.

  ## Options

    * `:bucket` (required)
    * `:access_key_id`, `:secret_access_key` — static keys. Set both, or
      neither: leaving both out puts the store in credential-chain mode
      (`Smolquery.AwsCredentials`), which is how a deployment runs with no
      static S3 secrets at all. Only one of the pair is a configuration
      error, not a half-specified chain.
    * `:staging_dir` (required) — local scratch directory the encoder writes
      to before the upload; must be on a filesystem with room for the
      largest sealed segment
    * `:endpoint` — S3-compatible endpoint (e.g. `"http://minio:9000"`);
      unset targets AWS S3
    * `:region` — defaults to `"us-east-1"`
    * `:url_style` — `"path"` or `"vhost"`; defaults to `"path"` when
      `:endpoint` is set (MinIO and most self-hosted stores require it),
      otherwise DuckDB's own default
    * `:list_max_keys` — ListObjectsV2 page size; unset takes S3's default
      (1000). `list/2` follows continuation tokens either way, so this only
      shapes how many round trips a listing costs — and gives tests a page
      size small enough to prove the pagination loop against a real store.

  """
  @spec new([option()]) :: Store.t()
  def new(opts) do
    config = %__MODULE__{
      bucket: Keyword.fetch!(opts, :bucket),
      access_key_id: Keyword.get(opts, :access_key_id),
      secret_access_key: Keyword.get(opts, :secret_access_key),
      staging_dir: Keyword.fetch!(opts, :staging_dir),
      endpoint: Keyword.get(opts, :endpoint),
      region: Keyword.get(opts, :region, "us-east-1"),
      url_style: Keyword.get(opts, :url_style),
      list_max_keys: Keyword.get(opts, :list_max_keys)
    }

    validate_credentials!(config)

    if credential_chain?(config), do: AwsCredentials.ensure_started()

    %Store{impl: __MODULE__, config: config}
  end

  defp validate_credentials!(%__MODULE__{access_key_id: nil, secret_access_key: nil}), do: :ok

  defp validate_credentials!(%__MODULE__{access_key_id: id, secret_access_key: secret})
       when is_binary(id) and is_binary(secret),
       do: :ok

  defp validate_credentials!(%__MODULE__{access_key_id: nil}) do
    raise ArgumentError,
          "the sealed tier has SMOLQUERY_S3_SECRET_ACCESS_KEY without " <>
            "SMOLQUERY_S3_ACCESS_KEY_ID: set both to use static keys, or neither " <>
            "to use the AWS credential chain"
  end

  defp validate_credentials!(%__MODULE__{}) do
    raise ArgumentError,
          "the sealed tier has SMOLQUERY_S3_ACCESS_KEY_ID without " <>
            "SMOLQUERY_S3_SECRET_ACCESS_KEY: set both to use static keys, or neither " <>
            "to use the AWS credential chain"
  end

  @doc """
  Whether this store authenticates through the AWS credential chain rather
  than static keys — true when no `:access_key_id` is configured.
  """
  @spec credential_chain?(t()) :: boolean()
  def credential_chain?(%__MODULE__{access_key_id: nil}), do: true
  def credential_chain?(%__MODULE__{}), do: false

  @impl Store
  def put(%__MODULE__{} = config, key, encoder) do
    staged = staging_path(config, key)

    with :ok <- File.mkdir_p(Path.dirname(staged)),
         :ok <- encode(staged, encoder),
         {:ok, %File.Stat{size: size}} <- File.stat(staged),
         {:ok, ^size} <- upload(config, key, staged, size) do
      File.rm(staged)
      {:ok, %{location: location(config, key), byte_size: size}}
    else
      {:error, reason} ->
        File.rm(staged)
        {:error, {:put_failed, key, reason}}
    end
  end

  @impl Store
  def location(%__MODULE__{bucket: bucket}, key), do: "s3://#{bucket}/#{key}"

  @impl Store
  def list(%__MODULE__{} = config, prefix) do
    list_pages(config, directory_prefix(prefix), nil, [])
  end

  defp list_pages(%__MODULE__{bucket: bucket} = config, prefix, continuation, acc) do
    case Req.get(request(config),
           url: "s3://#{bucket}?#{list_query(config, prefix, continuation)}"
         ) do
      {:ok, %{status: 200, body: body}} ->
        pages = [keys_from_listing(body) | acc]

        case next_continuation(body) do
          nil -> {:ok, pages |> List.flatten() |> Enum.sort()}
          token -> list_pages(config, prefix, token, pages)
        end

      {:ok, %{status: status, body: body}} ->
        {:error, {:s3_status, status, body}}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp directory_prefix(""), do: ""
  defp directory_prefix(prefix), do: String.trim_trailing(prefix, "/") <> "/"

  defp next_continuation(%{"ListBucketResult" => %{"IsTruncated" => "true"} = listing}),
    do: listing["NextContinuationToken"]

  defp next_continuation(_final_page), do: nil

  @impl Store
  def delete(%__MODULE__{bucket: bucket} = config, key) do
    case Req.delete(request(config), url: "s3://#{bucket}/#{key}") do
      {:ok, %{status: status}} when status in [200, 204, 404] -> :ok
      {:ok, %{status: status, body: body}} -> {:error, {:delete_failed, key, {status, body}}}
      {:error, reason} -> {:error, {:delete_failed, key, reason}}
    end
  end

  @impl Store
  def shared?(%__MODULE__{}), do: true

  @impl Store
  def sweep_staging(%__MODULE__{staging_dir: dir}, age_ms) do
    cutoff = System.os_time(:second) - div(age_ms, 1000)

    swept =
      [dir, @staging, "*"]
      |> Path.join()
      |> Path.wildcard()
      |> Enum.filter(&(Store.staged_at(&1) <= cutoff and File.rm(&1) == :ok))

    {:ok, Enum.map(swept, &Path.basename/1)}
  end

  @doc """
  The `s3://` prefix every location of this store lives under — what a
  locked-down job engine appends to `allowed_directories`, so the sealed
  segments a plan references stay readable after external access is
  disabled (`Smolquery.QueryService.Runner`).
  """
  @spec location_prefix(t()) :: String.t()
  def location_prefix(%__MODULE__{bucket: bucket}), do: "s3://" <> bucket <> "/"

  @doc """
  The bootstrap `CREATE SECRET` that lets a DuckDB engine read `s3://`
  locations under `:bucket` — engines only ever read through it; every write
  goes through this module's own `put/3`.

  Without static keys the secret delegates to DuckDB's own credential chain,
  which resolves EKS Pod Identity from the container credentials endpoint.
  `REFRESH auto` matters for the long-lived storage-service engine, whose
  temporary credentials would otherwise expire under it; per-job query
  engines are short-lived enough that they only ever use the first
  resolution. That branch needs the `aws` extension — see
  `required_extensions/1`.
  """
  @spec create_secret_statement(t()) :: String.t()
  def create_secret_statement(%__MODULE__{} = config) do
    options =
      ["TYPE S3"] ++
        credential_options(config) ++
        [
          "REGION #{Identifier.sql_string(config.region)}",
          "SCOPE #{Identifier.sql_string("s3://" <> config.bucket)}"
        ] ++ endpoint_options(config)

    "CREATE SECRET IF NOT EXISTS #{@secret_name} (#{Enum.join(options, ", ")})"
  end

  defp credential_options(%__MODULE__{access_key_id: nil}),
    do: ["PROVIDER credential_chain", "REFRESH auto"]

  defp credential_options(%__MODULE__{} = config) do
    [
      "KEY_ID #{Identifier.sql_string(config.access_key_id)}",
      "SECRET #{Identifier.sql_string(config.secret_access_key)}"
    ]
  end

  @doc """
  DuckDB extensions an engine must load before `create_secret_statement/1`
  will run — `aws` supplies the `credential_chain` provider, and only the
  chain branch needs it. A static-key secret is plain `httpfs`.
  """
  @spec required_extensions(t()) :: [atom()]
  def required_extensions(%__MODULE__{access_key_id: nil}), do: [:aws]
  def required_extensions(%__MODULE__{}), do: []

  defp endpoint_options(%__MODULE__{endpoint: nil}), do: []

  defp endpoint_options(%__MODULE__{endpoint: endpoint} = config) do
    uri = URI.parse(endpoint)
    host = if uri.port, do: "#{uri.host}:#{uri.port}", else: uri.host

    [
      "ENDPOINT #{Identifier.sql_string(host)}",
      "URL_STYLE #{Identifier.sql_string(url_style(config))}",
      "USE_SSL #{uri.scheme == "https"}"
    ]
  end

  defp url_style(%__MODULE__{url_style: nil}), do: "path"
  defp url_style(%__MODULE__{url_style: style}), do: style

  defp upload(config, key, staged, size) do
    case Req.put(request(config),
           url: "s3://#{config.bucket}/#{key}",
           headers: [
             {"content-length", Integer.to_string(size)},
             {"if-none-match", "*"}
           ],
           body: File.stream!(staged, @chunk_bytes)
         ) do
      {:ok, %{status: status}} when status in 200..299 -> {:ok, size}
      {:ok, %{status: 412}} -> {:ok, size}
      {:ok, %{status: status, body: body}} -> {:error, {:s3_status, status, body}}
      {:error, reason} -> {:error, reason}
    end
  end

  defp list_query(config, prefix, continuation) do
    [
      {"list-type", "2"},
      {"prefix", prefix},
      {"continuation-token", continuation},
      {"max-keys", config.list_max_keys}
    ]
    |> Enum.reject(fn {_key, value} -> value in [nil, ""] end)
    |> URI.encode_query()
  end

  defp keys_from_listing(%{"ListBucketResult" => %{"Contents" => contents}}) do
    contents
    |> List.wrap()
    |> Enum.map(& &1["Key"])
    |> Enum.filter(&String.ends_with?(&1, ".parquet"))
  end

  defp keys_from_listing(_empty), do: []

  defp encode(staged, encoder) do
    encoder.(staged)
  catch
    kind, reason ->
      File.rm(staged)
      :erlang.raise(kind, reason, __STACKTRACE__)
  end

  defp staging_path(%__MODULE__{staging_dir: dir}, key) do
    name = Path.basename(key) <> "." <> Integer.to_string(:erlang.unique_integer([:positive]))

    Path.join([dir, @staging, name])
  end

  defp request(%__MODULE__{} = config) do
    Req.new()
    |> ReqS3.attach(
      aws_sigv4: sigv4(config),
      aws_endpoint_url_s3: config.endpoint
    )
  end

  defp sigv4(%__MODULE__{access_key_id: nil, region: region}),
    do: AwsCredentials.sigv4_options(region)

  defp sigv4(%__MODULE__{} = config) do
    [
      access_key_id: config.access_key_id,
      secret_access_key: config.secret_access_key,
      region: config.region
    ]
  end
end
