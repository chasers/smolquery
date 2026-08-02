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

  ## Reading sealed segments back needs credentials DuckDB has, not just Elixir

  This module reads and writes bytes over HTTP; it says nothing about how the
  DuckDB engines that later `read_parquet('s3://...')` these locations
  authenticate. `create_secret_statement/1` returns the bootstrap `CREATE
  SECRET` for that — `StorageService.Supervisor` (compaction re-reads sealed
  segments) and `QueryService.Runner` (every query touching the sealed tier)
  both run it when their configured store is this module, the same way
  `Smolquery.InternalSecret.create_secret_statement/1` bootstraps the hot
  tier's header.

  ## Usage

      store = Smolquery.Segments.Store.S3.new(
        bucket: "smolquery-sealed",
        access_key_id: "...",
        secret_access_key: "...",
        endpoint: "http://minio:9000",
        staging_dir: "/var/lib/smolquery/sealed-staging"
      )
  """

  @behaviour Smolquery.Segments.Store

  alias Smolquery.Identifier
  alias Smolquery.Segments.Store

  @enforce_keys [:bucket, :access_key_id, :secret_access_key, :staging_dir]
  defstruct [
    :bucket,
    :access_key_id,
    :secret_access_key,
    :staging_dir,
    :endpoint,
    region: "us-east-1",
    url_style: nil
  ]

  @type t :: %__MODULE__{
          bucket: String.t(),
          access_key_id: String.t(),
          secret_access_key: String.t(),
          staging_dir: String.t(),
          endpoint: String.t() | nil,
          region: String.t(),
          url_style: String.t() | nil
        }

  @type option ::
          {:bucket, String.t()}
          | {:access_key_id, String.t()}
          | {:secret_access_key, String.t()}
          | {:staging_dir, String.t()}
          | {:endpoint, String.t()}
          | {:region, String.t()}
          | {:url_style, String.t()}

  @staging ".tmp"
  @chunk_bytes 1_048_576
  @secret_name "smolquery_sealed_tier"

  @doc """
  A store over `:bucket`.

  ## Options

    * `:bucket` (required)
    * `:access_key_id`, `:secret_access_key` (required)
    * `:staging_dir` (required) — local scratch directory the encoder writes
      to before the upload; must be on a filesystem with room for the
      largest sealed segment
    * `:endpoint` — S3-compatible endpoint (e.g. `"http://minio:9000"`);
      unset targets AWS S3
    * `:region` — defaults to `"us-east-1"`
    * `:url_style` — `"path"` or `"vhost"`; defaults to `"path"` when
      `:endpoint` is set (MinIO and most self-hosted stores require it),
      otherwise DuckDB's own default

  """
  @spec new([option()]) :: Store.t()
  def new(opts) do
    config = %__MODULE__{
      bucket: Keyword.fetch!(opts, :bucket),
      access_key_id: Keyword.fetch!(opts, :access_key_id),
      secret_access_key: Keyword.fetch!(opts, :secret_access_key),
      staging_dir: Keyword.fetch!(opts, :staging_dir),
      endpoint: Keyword.get(opts, :endpoint),
      region: Keyword.get(opts, :region, "us-east-1"),
      url_style: Keyword.get(opts, :url_style)
    }

    %Store{impl: __MODULE__, config: config}
  end

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
  def list(%__MODULE__{bucket: bucket} = config, prefix) do
    case Req.get(request(config), url: "s3://#{bucket}?#{list_query(prefix)}") do
      {:ok, %{status: 200, body: body}} -> {:ok, keys_from_listing(body)}
      {:ok, %{status: status, body: body}} -> {:error, {:s3_status, status, body}}
      {:error, reason} -> {:error, reason}
    end
  end

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
  The bootstrap `CREATE SECRET` that lets a DuckDB engine read (and, for the
  sealer's own uploads via `httpfs` rather than this module, write) `s3://`
  locations under `:bucket`.
  """
  @spec create_secret_statement(t()) :: String.t()
  def create_secret_statement(%__MODULE__{} = config) do
    options =
      [
        "TYPE S3",
        "KEY_ID #{Identifier.sql_string(config.access_key_id)}",
        "SECRET #{Identifier.sql_string(config.secret_access_key)}",
        "REGION #{Identifier.sql_string(config.region)}",
        "SCOPE #{Identifier.sql_string("s3://" <> config.bucket)}"
      ] ++ endpoint_options(config)

    "CREATE SECRET IF NOT EXISTS #{@secret_name} (#{Enum.join(options, ", ")})"
  end

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
           headers: [{"content-length", Integer.to_string(size)}],
           body: File.stream!(staged, @chunk_bytes)
         ) do
      {:ok, %{status: status}} when status in 200..299 -> {:ok, size}
      {:ok, %{status: status, body: body}} -> {:error, {:s3_status, status, body}}
      {:error, reason} -> {:error, reason}
    end
  end

  defp list_query(""), do: "list-type=2"
  defp list_query(prefix), do: "list-type=2&prefix=#{URI.encode_www_form(prefix)}"

  defp keys_from_listing(%{"ListBucketResult" => %{"Contents" => contents}}) do
    contents
    |> List.wrap()
    |> Enum.map(& &1["Key"])
    |> Enum.sort()
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
      aws_sigv4: [
        access_key_id: config.access_key_id,
        secret_access_key: config.secret_access_key
      ],
      aws_endpoint_url_s3: config.endpoint
    )
  end
end
