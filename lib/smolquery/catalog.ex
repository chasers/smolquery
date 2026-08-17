defmodule Smolquery.Catalog do
  @moduledoc """
  The metadata seam: datasets, tables, and which segments a table is made of.

  Everything durable about a table other than its bytes lives behind this
  behaviour — the sealer commits through it, the planner resolves through it,
  and maintenance rewrites through it. DuckLake is the implementation
  (`Smolquery.Catalog.DuckLake`); the behaviour exists because the Milestone 2
  spike found real DuckLake limits, and being able to replace it without
  touching callers is what keeps those limits from becoming architecture.

  A catalog is a value, not a process: `%Catalog{}` pairs an implementation
  module with its configuration, so a caller holds one handle and passes it
  around.

  ## Snapshots

  Every mutation returns the snapshot it committed at, and `segments/3` reads
  as of a snapshot. That pairing is what makes the Milestone 4 seal handoff
  exactly-once: the sealer stamps retirement with the snapshot its commit
  produced, and a query at snapshot `S` includes a micro-segment only if it is
  unsealed or was sealed after `S`.

  ## Usage

      catalog = Smolquery.Catalog.DuckLake.new(engine: MyLake)

      :ok = Smolquery.Catalog.create_dataset(catalog, "analytics")
      :ok = Smolquery.Catalog.create_table(catalog, {"analytics", "events"}, schema)
      {:ok, snapshot} = Smolquery.Catalog.register_segments(catalog, {"analytics", "events"}, [segment])

  """

  alias Smolquery.Schema
  alias Smolquery.Segments.Segment

  @enforce_keys [:impl, :config]
  defstruct [:impl, :config]

  @type t :: %__MODULE__{impl: module(), config: term()}

  @typedoc """
  A table, qualified by its dataset: `{dataset, table}`.
  """
  @type table_ref :: {String.t(), String.t()}

  @type snapshot :: non_neg_integer()

  @typedoc """
  A table's TTL policy: rows age out of `column` after `ttl_ms`.

  Retention is segment-grained — a segment is dropped once the *maximum* of
  `column` across its rows has passed the horizon — so `column` names which
  timestamp the age is measured against, and a policy is only as good as that
  column's Parquet stats.
  """
  @type retention :: %{column: String.t(), ttl_ms: pos_integer()}

  @typedoc """
  Column names a table is physically sorted by at write time, in key order.

  Empty means unsorted (the default). Like retention, this is table metadata —
  mutating it affects future writes only; existing segments are not rewritten.
  """
  @type clustering :: [String.t()]

  @typedoc """
  Aggregate size of a table's sealed tier at a snapshot: how many segment
  files a read at that snapshot lists, and their total rows and bytes.
  """
  @type segment_stats :: %{
          files: non_neg_integer(),
          rows: non_neg_integer(),
          bytes: non_neg_integer()
        }

  @typedoc """
  One sealed segment's path and weight, for `segment_files/3`.
  """
  @type segment_file :: %{
          path: String.t(),
          rows: non_neg_integer(),
          bytes: non_neg_integer()
        }

  @typedoc """
  The mutable table settings a `PATCH` can carry, keyed by which were present.

  A key that is absent is left untouched; `retention: nil` and `clustering: []`
  are the explicit clears.
  """
  @type table_options :: %{
          optional(:retention) => retention() | nil,
          optional(:clustering) => clustering()
        }

  @callback create_dataset(config :: term(), dataset :: String.t()) :: :ok | {:error, term()}
  @callback list_datasets(config :: term()) :: {:ok, [String.t()]} | {:error, term()}
  @callback create_table(config :: term(), table_ref(), Schema.t()) :: :ok | {:error, term()}
  @callback list_tables(config :: term(), dataset :: String.t()) ::
              {:ok, [String.t()]} | {:error, term()}
  @callback table_schema(config :: term(), table_ref()) :: {:ok, Schema.t()} | {:error, term()}
  @callback register_segments(config :: term(), table_ref(), [Segment.t()]) ::
              {:ok, snapshot()} | {:error, term()}
  @callback segments(config :: term(), table_ref(), snapshot() | :current) ::
              {:ok, [String.t()]} | {:error, term()}
  @callback registered_through(config :: term(), table_ref(), snapshot()) ::
              {:ok, [String.t()]} | {:error, term()}
  @callback segment_stats(config :: term(), table_ref(), snapshot()) ::
              {:ok, segment_stats()} | {:error, term()}
  @callback segment_files(config :: term(), table_ref(), snapshot()) ::
              {:ok, [segment_file()]} | {:error, term()}
  @callback drop_segments(config :: term(), table_ref(), [String.t()]) ::
              {:ok, snapshot()} | {:error, term()}
  @callback replace_segments(config :: term(), table_ref(), [Segment.t()], [String.t()]) ::
              {:ok, snapshot()} | {:error, term()}
  @callback current_snapshot(config :: term()) :: {:ok, snapshot()} | {:error, term()}
  @callback known_segments(config :: term()) :: {:ok, [String.t()]} | {:error, term()}
  @callback put_retention(config :: term(), table_ref(), retention() | nil) ::
              :ok | {:error, term()}
  @callback retention(config :: term(), table_ref()) ::
              {:ok, retention() | nil} | {:error, term()}
  @callback put_clustering(config :: term(), table_ref(), clustering()) ::
              :ok | {:error, term()}
  @callback clustering(config :: term(), table_ref()) ::
              {:ok, clustering()} | {:error, term()}
  @callback put_table_options(config :: term(), table_ref(), table_options()) ::
              :ok | {:error, term()}
  @callback expire_snapshots(config :: term(), older_than_ms :: pos_integer()) ::
              {:ok, non_neg_integer()} | {:error, term()}

  @optional_callbacks put_table_options: 3

  @doc """
  Creates a dataset, if it does not already exist.
  """
  @spec create_dataset(t(), String.t()) :: :ok | {:error, term()}
  def create_dataset(%__MODULE__{} = catalog, dataset),
    do: catalog.impl.create_dataset(catalog.config, dataset)

  @doc """
  Every dataset in the catalog.
  """
  @spec list_datasets(t()) :: {:ok, [String.t()]} | {:error, term()}
  def list_datasets(%__MODULE__{} = catalog), do: catalog.impl.list_datasets(catalog.config)

  @doc """
  Creates a table from a `Smolquery.Schema`, if it does not already exist.
  """
  @spec create_table(t(), table_ref(), Schema.t()) :: :ok | {:error, term()}
  def create_table(%__MODULE__{} = catalog, table, schema),
    do: catalog.impl.create_table(catalog.config, table, schema)

  @doc """
  Every table in `dataset`.
  """
  @spec list_tables(t(), String.t()) :: {:ok, [String.t()]} | {:error, term()}
  def list_tables(%__MODULE__{} = catalog, dataset),
    do: catalog.impl.list_tables(catalog.config, dataset)

  @doc """
  Every table in the catalog, qualified — the flattened form of
  `list_datasets/1` × `list_tables/2`.

  A convenience over the callbacks rather than one itself: maintenance
  sweeps (compaction, retention) want "all tables" and no implementation
  could answer it better than the composition does.
  """
  @spec tables(t()) :: {:ok, [table_ref()]} | {:error, term()}
  def tables(%__MODULE__{} = catalog) do
    with {:ok, datasets} <- list_datasets(catalog) do
      Enum.reduce_while(datasets, {:ok, []}, &collect_tables(catalog, &1, &2))
    end
  end

  defp collect_tables(catalog, dataset, {:ok, acc}) do
    case list_tables(catalog, dataset) do
      {:ok, tables} -> {:cont, {:ok, acc ++ Enum.map(tables, &{dataset, &1})}}
      {:error, reason} -> {:halt, {:error, reason}}
    end
  end

  @doc """
  A table's schema as the catalog holds it.
  """
  @spec table_schema(t(), table_ref()) :: {:ok, Schema.t()} | {:error, term()}
  def table_schema(%__MODULE__{} = catalog, table),
    do: catalog.impl.table_schema(catalog.config, table)

  @doc """
  Registers written segments against a table, returning the committed snapshot.

  Registration is idempotent by segment path: re-registering a segment the
  catalog already holds is a no-op that still reports a snapshot, so a sealer
  that crashed between committing and retiring can safely retry.
  """
  @spec register_segments(t(), table_ref(), [Segment.t()]) :: {:ok, snapshot()} | {:error, term()}
  def register_segments(%__MODULE__{} = catalog, table, segments),
    do: catalog.impl.register_segments(catalog.config, table, segments)

  @doc """
  The paths of a table's segments, as of `snapshot` (or `:current`).
  """
  @spec segments(t(), table_ref(), snapshot() | :current) ::
          {:ok, [String.t()]} | {:error, term()}
  def segments(%__MODULE__{} = catalog, table, snapshot \\ :current),
    do: catalog.impl.segments(catalog.config, table, snapshot)

  @doc """
  Every path ever registered against the table at a snapshot at or before
  `snapshot` — including paths a later snapshot dropped.

  This is the seal-membership question, and it is deliberately not
  `segments/3`: a planner deciding whether a hot micro-segment's seal has
  committed must not change its answer because compaction or retention later
  *replaced* the sealed file. A drop never un-commits a seal — the rows live
  on in whatever replaced it — so membership tests "was it ever registered by
  `snapshot`", while `segments/3` answers the different question "what would
  I read at `snapshot`".
  """
  @spec registered_through(t(), table_ref(), snapshot()) ::
          {:ok, [String.t()]} | {:error, term()}
  def registered_through(%__MODULE__{} = catalog, table, snapshot),
    do: catalog.impl.registered_through(catalog.config, table, snapshot)

  @doc """
  The count, total rows, and total bytes of a table's segments at `snapshot`.

  This is `segments/3`'s question — "what would I read at `snapshot`" — as
  sizes rather than paths. The planner asks it once per referenced table so a
  query's response can report what its sealed tier weighs
  (`Smolquery.QueryService.Statistics`), without fetching a path list it has
  no other use for.
  """
  @spec segment_stats(t(), table_ref(), snapshot()) ::
          {:ok, segment_stats()} | {:error, term()}
  def segment_stats(%__MODULE__{} = catalog, table, snapshot),
    do: catalog.impl.segment_stats(catalog.config, table, snapshot)

  @doc """
  Each segment's path, rows, and bytes at `snapshot` — `segment_stats/3` per
  file rather than summed.

  The lifecycle UI's question (T-295): compaction is visible only in the size
  distribution — how many segments sit under `compact_below_bytes`, and how
  they merge away over time — which no aggregate answers.
  """
  @spec segment_files(t(), table_ref(), snapshot()) ::
          {:ok, [segment_file()]} | {:error, term()}
  def segment_files(%__MODULE__{} = catalog, table, snapshot),
    do: catalog.impl.segment_files(catalog.config, table, snapshot)

  @doc """
  Removes segments from a table's current snapshot, returning the new snapshot.

  The files themselves are left alone — deleting them is GC's job, after any
  in-flight reader's snapshot has expired.
  """
  @spec drop_segments(t(), table_ref(), [String.t()]) :: {:ok, snapshot()} | {:error, term()}
  def drop_segments(%__MODULE__{} = catalog, table, paths),
    do: catalog.impl.drop_segments(catalog.config, table, paths)

  @doc """
  Registers `segments` and drops `paths` in one commit, returning its snapshot.

  This is the atomic swap compaction is built on: a single snapshot both adds
  the merged segment and retires the inputs it replaced, so no snapshot ever
  holds both (its rows counted twice) or neither (a hole where they used to
  be). A swap that registers nothing is refused with `{:error, :no_segments}`
  rather than quietly becoming a drop — `drop_segments/3` is how a caller
  says that on purpose.

  Idempotent the way `register_segments/3` is: a retry whose additions the
  catalog already holds committed its drops in the same snapshot, so it
  reports the current snapshot without re-writing. And like `drop_segments/3`,
  the dropped files are left on disk — older snapshots still read them, and
  deleting them is GC's job once no snapshot does.
  """
  @spec replace_segments(t(), table_ref(), [Segment.t()], [String.t()]) ::
          {:ok, snapshot()} | {:error, term()}
  def replace_segments(%__MODULE__{} = catalog, table, segments, paths),
    do: catalog.impl.replace_segments(catalog.config, table, segments, paths)

  @doc """
  The catalog's current snapshot.
  """
  @spec current_snapshot(t()) :: {:ok, snapshot()} | {:error, term()}
  def current_snapshot(%__MODULE__{} = catalog),
    do: catalog.impl.current_snapshot(catalog.config)

  @doc """
  Every segment path the catalog has ever referenced, at any snapshot.

  Not the same question as `segments/3`, and the difference is what makes garbage
  collection safe. A path missing from the *current* snapshot may still be the only
  copy of rows an older snapshot can read, so deleting on that basis would break
  time travel. A path missing from *every* snapshot was never successfully
  committed — an upload whose commit did not follow — and is the only garbage a
  sealer can produce.
  """
  @spec known_segments(t()) :: {:ok, [String.t()]} | {:error, term()}
  def known_segments(%__MODULE__{} = catalog),
    do: catalog.impl.known_segments(catalog.config)

  @doc """
  Sets (or with `nil` clears) a table's retention policy.

  Policy is metadata about a table's segments, so it lives behind this seam
  with the rest of the table's metadata — not in service configuration, where
  it could disagree between nodes, and not in job-history-style side storage,
  because unlike a job row it is exactly what the catalog is *for*.

  Whether `column` exists and carries a time type is the caller's check
  (`Smolquery.Schema`), made where a validation error can still reach the
  user who typed it.
  """
  @spec put_retention(t(), table_ref(), retention() | nil) :: :ok | {:error, term()}
  def put_retention(%__MODULE__{} = catalog, table, policy),
    do: catalog.impl.put_retention(catalog.config, table, policy)

  @doc """
  A table's retention policy, or `nil` when it keeps rows forever.
  """
  @spec retention(t(), table_ref()) :: {:ok, retention() | nil} | {:error, term()}
  def retention(%__MODULE__{} = catalog, table),
    do: catalog.impl.retention(catalog.config, table)

  @doc """
  Sets a table's clustering key — the columns future writes sort by.

  `[]` clears it. Whether each name exists on the table is the caller's check
  (`Smolquery.Schema`), made where a validation error can still reach the user
  who typed it. Changing the key does not rewrite existing segments.
  """
  @spec put_clustering(t(), table_ref(), clustering()) :: :ok | {:error, term()}
  def put_clustering(%__MODULE__{} = catalog, table, columns) when is_list(columns),
    do: catalog.impl.put_clustering(catalog.config, table, columns)

  @doc """
  A table's clustering key, or `[]` when writes are unsorted.
  """
  @spec clustering(t(), table_ref()) :: {:ok, clustering()} | {:error, term()}
  def clustering(%__MODULE__{} = catalog, table),
    do: catalog.impl.clustering(catalog.config, table)

  @doc """
  Applies a table's mutable settings — retention and/or clustering — as one
  change: on an error, none of them stick.

  This is the write behind a `PATCH`, and atomicity is why it exists as its
  own operation rather than a loop over `put_retention/3` and
  `put_clustering/3`: those are two writes, and an infrastructure failure
  between them would persist the first while reporting an error for the whole
  request. An implementation that can bind both into one transaction exports
  the optional callback; for one that cannot, this falls back to sequential
  puts — retention first — and keeps only the sequential guarantee.
  """
  @spec put_table_options(t(), table_ref(), table_options()) :: :ok | {:error, term()}
  def put_table_options(%__MODULE__{} = catalog, table, options) when is_map(options) do
    if function_exported?(catalog.impl, :put_table_options, 3) do
      catalog.impl.put_table_options(catalog.config, table, options)
    else
      with :ok <- sequential_put(catalog, table, options, :retention) do
        sequential_put(catalog, table, options, :clustering)
      end
    end
  end

  defp sequential_put(catalog, table, %{retention: policy}, :retention),
    do: put_retention(catalog, table, policy)

  defp sequential_put(catalog, table, %{clustering: columns}, :clustering),
    do: put_clustering(catalog, table, columns)

  defp sequential_put(_catalog, _table, _options, _key), do: :ok

  @doc """
  Expires snapshots older than `older_than_ms`, returning how many expired.

  Expiry is what turns a logical drop into a reclaimable file: GC spares any
  path some snapshot still references, and dropped or compacted-away segments
  stay referenced by the snapshots that predate the drop. Expiring those
  snapshots is therefore the step that lets GC's membership test finally say
  no — and it is also what bounds time travel, so the window must exceed the
  longest query any reader may still have pinned.
  """
  @spec expire_snapshots(t(), pos_integer()) :: {:ok, non_neg_integer()} | {:error, term()}
  def expire_snapshots(%__MODULE__{} = catalog, older_than_ms),
    do: catalog.impl.expire_snapshots(catalog.config, older_than_ms)
end
