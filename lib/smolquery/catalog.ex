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
  @callback drop_segments(config :: term(), table_ref(), [String.t()]) ::
              {:ok, snapshot()} | {:error, term()}
  @callback current_snapshot(config :: term()) :: {:ok, snapshot()} | {:error, term()}

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
  Removes segments from a table's current snapshot, returning the new snapshot.

  The files themselves are left alone — deleting them is GC's job, after any
  in-flight reader's snapshot has expired.
  """
  @spec drop_segments(t(), table_ref(), [String.t()]) :: {:ok, snapshot()} | {:error, term()}
  def drop_segments(%__MODULE__{} = catalog, table, paths),
    do: catalog.impl.drop_segments(catalog.config, table, paths)

  @doc """
  The catalog's current snapshot.
  """
  @spec current_snapshot(t()) :: {:ok, snapshot()} | {:error, term()}
  def current_snapshot(%__MODULE__{} = catalog),
    do: catalog.impl.current_snapshot(catalog.config)
end
