defmodule Smolquery.Segments.Segment do
  @moduledoc """
  A written, immutable Parquet segment.

  This is what the write path hands to everything downstream: the sealer
  registers it in the catalog, a hot manifest entry describes it, and the
  planner prunes on its stats. A segment is write-once — a `Segment` struct
  always refers to a file that is complete and will never change.

  A segment has two names, and the difference matters. `key` names it *within its
  store* — stable, store-agnostic, what `Smolquery.Segments.Store` takes to
  delete or list it. `path` is where a reader opens it, which the store derives
  from the key: an absolute filesystem path on local disk, an `s3://` URL in an
  object store. The catalog registers `path`; the hot manifest holds both.
  """

  @enforce_keys [:id, :key, :path, :row_count, :byte_size]
  defstruct [:id, :key, :path, :row_count, :byte_size, :field_ids, stats: %{}]

  @type column_stats :: %{
          min: term(),
          max: term(),
          null_count: non_neg_integer()
        }

  @typedoc """
  The column id each of the file's columns was written under, by the name it
  has in the file — `Smolquery.Schema.field_ids/1` of the schema at write
  time — or `nil` for a file written without ids (PL-62).
  """
  @type field_ids :: %{String.t() => pos_integer()} | nil

  @type t :: %__MODULE__{
          id: String.t(),
          key: String.t(),
          path: String.t(),
          row_count: non_neg_integer(),
          byte_size: non_neg_integer(),
          field_ids: field_ids(),
          stats: %{optional(String.t()) => column_stats()}
        }

  @doc """
  The min-max stats for one column, if the segment carries them.

  Columns whose type has no useful ordering for pruning (strings, booleans)
  carry a `null_count` with `min` and `max` set to `nil`.
  """
  @spec column_stats(t(), String.t()) :: {:ok, column_stats()} | :error
  def column_stats(%__MODULE__{stats: stats}, column), do: Map.fetch(stats, column)
end
