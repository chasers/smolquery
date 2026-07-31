defmodule Smolquery.Segments.Store do
  @moduledoc """
  Where written segments live — the seam between the write path and its storage.

  Both tiers write through this: buffer nodes put micro-segments, the sealer puts
  merged sealed segments. One abstraction serves both because a segment is the
  same kind of thing either way — write-once bytes named by a key — and because
  the hot tier's storage is a live architectural question. Micro-segments on
  local disk make ack latency a local fsync; micro-segments in an object store
  close the single-copy loss window instead. Which one a deployment wants is a
  trade-off, not a truth, so it is configuration.

  A store is a value, not a process: `%Store{}` pairs an implementation module
  with its configuration, the same shape as `Smolquery.Catalog`.

  ## The durability contract

  `put/3` returns only once the object is durable *by that store's definition* —
  `Store.Local` fsyncs and renames, an object store would return on a committed
  upload. Everything above this module writes its ack contract against `put/3`
  and needs to know nothing else, which is what lets the BufferService promise
  "acked means durable" without naming a storage medium. What differs between
  stores is the size of the loss window, and that belongs in each
  implementation's `@moduledoc`.

  ## Encoding happens inside `put/3`

  `put/3` takes a function rather than bytes or a path the caller chose. The
  store creates the staging location, hands it over to be written, and commits
  it. Two reasons, both structural: Polars encodes Parquet to a file from Rust,
  so no segment ever becomes an Elixir binary on the way to storage — which
  matters most for the sealer's 128–512 MB segments — and a caller cannot stage a
  file on the wrong filesystem, which would silently turn `Store.Local`'s atomic
  rename into a cross-device copy.

  ## Reading a segment back

  `location/2` is what a reader opens: an absolute path for `Store.Local`, an
  `s3://` URL for an object store. `shared?/1` says whether that location means
  anything from another node. It is `false` for local disk, which is precisely
  why `BufferService.HotServer` exists to serve those bytes over HTTP; a shared
  store makes that hop unnecessary and the query engine reads the location
  directly through `httpfs`.

  ## Usage

      store = Smolquery.Segments.Store.Local.new(dir: "/var/lib/smolquery/buffer")

      {:ok, prefix} = Smolquery.Segments.Store.prefix({"analytics", "events"})
      {:ok, segment} = Smolquery.Segments.Writer.write(rows, schema, store: store, prefix: prefix)

      {:ok, keys} = Smolquery.Segments.Store.list(store, prefix)

  """

  alias Smolquery.Identifier
  alias Smolquery.Segments.Id

  @enforce_keys [:impl, :config]
  defstruct [:impl, :config]

  @type t :: %__MODULE__{impl: module(), config: term()}

  @typedoc """
  A segment's name within a store — `"<dataset>/<table>/<ulid>.parquet"`.

  Keys are store-agnostic on purpose: the same key names a path under
  `Store.Local` and an object under an object store, so `list/2` over a table
  prefix means the same thing in both.
  """
  @type key :: String.t()

  @typedoc """
  A table, qualified by its dataset: `{dataset, table}`.

  Defined here rather than borrowed from `Smolquery.Catalog` so the buffer service
  can name a table without depending on a catalog it is forbidden to touch.
  """
  @type table_ref :: {String.t(), String.t()}

  @typedoc """
  Writes the segment's bytes to the staging path it is given.
  """
  @type encoder :: (Path.t() -> :ok | {:error, term()})

  @type put_result :: %{location: String.t(), byte_size: non_neg_integer()}

  @extension ".parquet"

  @callback put(config :: term(), key(), encoder()) :: {:ok, put_result()} | {:error, term()}
  @callback location(config :: term(), key()) :: String.t()
  @callback list(config :: term(), prefix :: String.t()) :: {:ok, [key()]} | {:error, term()}
  @callback delete(config :: term(), key()) :: :ok | {:error, term()}
  @callback shared?(config :: term()) :: boolean()

  @doc """
  Stores a segment at `key`, calling `encoder` with the staging path to write.

  Returns the object's location and its size once the write is durable. A failed
  encode leaves nothing behind at `key`, so a reader never sees a partial
  segment.

  The key is validated here, not only in `key/2` — a key recovered from a log,
  a catalog row, or a wire message gets the same traversal check as one this
  module built.
  """
  @spec put(t(), key(), encoder()) :: {:ok, put_result()} | {:error, term()}
  def put(%__MODULE__{} = store, key, encoder) do
    with {:ok, key} <- validate_key(key) do
      store.impl.put(store.config, key, encoder)
    end
  end

  @doc """
  Where a reader opens the segment at `key`.

  Raises on a key that fails validation: every key reaching this function came
  from this module or from a manifest this module wrote, so an invalid one is a
  bug or tampering, never a routine outcome.
  """
  @spec location(t(), key()) :: String.t()
  def location(%__MODULE__{} = store, key) do
    case validate_key(key) do
      {:ok, key} -> store.impl.location(store.config, key)
      {:error, reason} -> raise ArgumentError, "invalid store key: #{inspect(reason)}"
    end
  end

  @doc """
  Every segment key under `prefix`.

  This is how recovery finds what storage actually holds, which the manifest is
  then reconciled against.
  """
  @spec list(t(), String.t()) :: {:ok, [key()]} | {:error, term()}
  def list(%__MODULE__{} = store, prefix) do
    with {:ok, prefix} <- validate_prefix(prefix) do
      store.impl.list(store.config, prefix)
    end
  end

  @doc """
  Removes the segment at `key`.

  Deleting a key the store does not hold is `:ok` — retirement and GC both retry,
  and neither should need to know whether a previous attempt got that far.
  """
  @spec delete(t(), key()) :: :ok | {:error, term()}
  def delete(%__MODULE__{} = store, key) do
    with {:ok, key} <- validate_key(key) do
      store.impl.delete(store.config, key)
    end
  end

  @doc """
  Whether a `location/2` is readable from another node.

  `false` for a store on one node's disk, `true` for an object store. Callers use
  it to decide whether a segment needs serving over HTTP or can be handed to a
  remote reader as-is.
  """
  @spec shared?(t()) :: boolean()
  def shared?(%__MODULE__{} = store), do: store.impl.shared?(store.config)

  @doc """
  The key prefix a table's segments live under.

  Both components are validated as identifiers before becoming key components,
  so a key can never carry a path traversal or a quote into a store.
  """
  @spec prefix(table_ref()) :: {:ok, String.t()} | {:error, term()}
  def prefix({dataset, table}) do
    with {:ok, dataset} <- Identifier.validate(dataset),
         {:ok, table} <- Identifier.validate(table) do
      {:ok, dataset <> "/" <> table}
    end
  end

  @doc """
  The key naming segment `id` under `prefix`.
  """
  @spec key(String.t(), String.t()) :: {:ok, key()} | {:error, term()}
  def key(prefix, id) do
    with {:ok, prefix} <- validate_prefix(prefix),
         {:ok, id} <- validate_id(id) do
      {:ok, join(prefix, id <> @extension)}
    end
  end

  @doc """
  The segment id a key names, if it names one.
  """
  @spec id(key()) :: {:ok, String.t()} | :error
  def id(key) do
    id = Path.basename(key, @extension)

    if Id.valid?(id), do: {:ok, id}, else: :error
  end

  defp validate_prefix(""), do: {:ok, ""}

  defp validate_prefix(prefix) when is_binary(prefix) do
    if valid_segments?(String.split(prefix, "/")) do
      {:ok, prefix}
    else
      {:error, {:invalid_prefix, prefix}}
    end
  end

  defp validate_prefix(prefix), do: {:error, {:invalid_prefix, prefix}}

  defp validate_key(key) when is_binary(key) and key != "" do
    if valid_segments?(String.split(key, "/")) do
      {:ok, key}
    else
      {:error, {:invalid_key, key}}
    end
  end

  defp validate_key(key), do: {:error, {:invalid_key, key}}

  defp valid_segments?([first | _rest] = segments) do
    not String.starts_with?(first, ".") and
      Enum.all?(segments, &(&1 != "" and &1 != "." and &1 != ".."))
  end

  defp validate_id(id) do
    if Id.valid?(id), do: {:ok, id}, else: {:error, {:invalid_segment_id, id}}
  end

  defp join("", name), do: name
  defp join(prefix, name), do: prefix <> "/" <> name
end
