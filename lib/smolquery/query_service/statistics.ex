defmodule Smolquery.QueryService.Statistics do
  @moduledoc """
  What serving a query costs, counted where the plan is made.

  The planner already decides exactly which files a query reads — the hot
  micro-segments that survive the membership rule and the pruner, and the
  sealed segments listed at the pinned snapshot. This struct carries those
  decisions out to the caller as numbers, so pruning can be *counted* rather
  than inferred from wall time (T-267): a regression that silently stops
  pruning shows up as `files_scanned` jumping, not as "a bit slower".

  The two tiers are reported separately because they prune by different
  mechanisms. The hot tier is pruned by the planner — the WHERE pruner
  (`Smolquery.QueryService.Pruner`) and the Top-N bound
  (`Smolquery.QueryService.TopN`) — so `files_total` (entries that passed
  the membership rule) versus `files_scanned` (entries the plan kept)
  measures the two together. The
  sealed tier prunes inside DuckDB from DuckLake's per-file stats, which the
  planner cannot observe — there `files_scanned` equals `files_total` and
  both mean "sealed segments listed at the snapshot": the upper bound the
  engine starts from, and the files needed to serve the query.

  Rows and bytes sum over the scanned files' manifest and catalog stats. They
  are plan-derived sizes, not engine-measured I/O — `EXPLAIN ANALYZE` is the
  tool for the latter.
  """

  @enforce_keys [:hot, :sealed]
  defstruct [:hot, :sealed]

  @typedoc """
  One tier's scan counts: files the planner considered, files the plan keeps,
  and the kept files' total rows and bytes.
  """
  @type tier :: %{
          files_total: non_neg_integer(),
          files_scanned: non_neg_integer(),
          rows_scanned: non_neg_integer(),
          bytes_scanned: non_neg_integer()
        }

  @type t :: %__MODULE__{hot: tier(), sealed: tier()}

  @doc """
  Statistics from the two tiers' counts.
  """
  @spec new(tier(), tier()) :: t()
  def new(hot, sealed), do: %__MODULE__{hot: hot, sealed: sealed}

  @doc """
  One tier's counts as a `t:tier/0`.
  """
  @spec tier(non_neg_integer(), non_neg_integer(), non_neg_integer(), non_neg_integer()) ::
          tier()
  def tier(files_total, files_scanned, rows_scanned, bytes_scanned) do
    %{
      files_total: files_total,
      files_scanned: files_scanned,
      rows_scanned: rows_scanned,
      bytes_scanned: bytes_scanned
    }
  end

  @doc """
  Files the planner considered, both tiers.
  """
  @spec files_total(t()) :: non_neg_integer()
  def files_total(%__MODULE__{hot: hot, sealed: sealed}),
    do: hot.files_total + sealed.files_total

  @doc """
  Files the plan reads, both tiers.
  """
  @spec files_scanned(t()) :: non_neg_integer()
  def files_scanned(%__MODULE__{hot: hot, sealed: sealed}),
    do: hot.files_scanned + sealed.files_scanned

  @doc """
  Rows in the files the plan reads, both tiers.
  """
  @spec rows_scanned(t()) :: non_neg_integer()
  def rows_scanned(%__MODULE__{hot: hot, sealed: sealed}),
    do: hot.rows_scanned + sealed.rows_scanned

  @doc """
  Bytes in the files the plan reads, both tiers.
  """
  @spec bytes_scanned(t()) :: non_neg_integer()
  def bytes_scanned(%__MODULE__{hot: hot, sealed: sealed}),
    do: hot.bytes_scanned + sealed.bytes_scanned

  @doc """
  `bytes_scanned/1` in MiB, rounded to two decimals.
  """
  @spec mib_scanned(t()) :: float()
  def mib_scanned(%__MODULE__{} = statistics),
    do: Float.round(bytes_scanned(statistics) / 1_048_576, 2)
end
