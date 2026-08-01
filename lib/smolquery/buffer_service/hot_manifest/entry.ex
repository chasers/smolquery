defmodule Smolquery.BufferService.HotManifest.Entry do
  @moduledoc """
  One micro-segment, as the hot manifest knows it.

  An entry is what a query planner prunes on and what the sealer asks for: the
  segment's key, its size, and the flush-time min-max stats the writer produced.
  It also carries the two facts the seal handoff turns on — `sealed_at`, the
  catalog snapshot a sealer committed this segment at, and `retired_at`, when that
  happened. A query at snapshot `S` includes an entry only if it is unsealed or
  `sealed_at > S`; the grace-period reaper deletes on `retired_at`. One is a
  correctness rule, the other a cleanup schedule, which is why they are separate
  fields rather than one timestamp.

  ## Records survive a restart, so stats are typed on the way out

  An entry is persisted as a line of NDJSON in the table's manifest log, and JSON
  has no date or timestamp. Stats bounds are therefore tagged when they are not
  plain scalars, so `~N[2026-07-31 12:00:00]` comes back a `NaiveDateTime` rather
  than a string that no longer compares against anything. Losing stats would not
  corrupt a query — it would silently stop pruning after every restart, which is
  worse than a crash for being invisible.
  """

  alias Smolquery.Segments.Segment

  @enforce_keys [:id, :key, :row_count, :byte_size, :added_at]
  defstruct [
    :id,
    :key,
    :row_count,
    :byte_size,
    :added_at,
    :sealed_at,
    :retired_at,
    stats: %{},
    claim_keys: []
  ]

  @type column_stats :: Segment.column_stats()

  @type t :: %__MODULE__{
          id: String.t(),
          key: String.t(),
          row_count: non_neg_integer(),
          byte_size: non_neg_integer(),
          added_at: integer(),
          sealed_at: non_neg_integer() | nil,
          retired_at: integer() | nil,
          stats: %{optional(String.t()) => column_stats()},
          claim_keys: [String.t()]
        }

  @doc """
  The entry describing a freshly written segment, added at `added_at`.
  """
  @spec from_segment(Segment.t(), integer()) :: t()
  def from_segment(%Segment{} = segment, added_at) do
    %__MODULE__{
      id: segment.id,
      key: segment.key,
      row_count: segment.row_count,
      byte_size: segment.byte_size,
      added_at: added_at,
      stats: segment.stats
    }
  end

  @doc """
  Whether a sealer has committed this segment to the catalog.
  """
  @spec sealed?(t()) :: boolean()
  def sealed?(%__MODULE__{sealed_at: sealed_at}), do: not is_nil(sealed_at)

  @doc """
  Stamps an entry as sealed at a catalog snapshot.
  """
  @spec seal(t(), non_neg_integer(), integer()) :: t()
  def seal(%__MODULE__{} = entry, snapshot, retired_at),
    do: %{entry | sealed_at: snapshot, retired_at: retired_at}

  @doc """
  Whether a sealer has been asked to seal this segment, and into what.

  `claim_keys` names the sealed segment(s) the claim covering this entry will
  produce. It is what makes the handoff exactly-once, and it is deliberately
  *not* `sealed_at`: a planner cannot tell from a snapshot stamp whether the
  commit it describes is visible to *its* snapshot, but it can ask whether these
  keys are in the catalog's segment list at the snapshot it is reading.
  """
  @spec claimed?(t()) :: boolean()
  def claimed?(%__MODULE__{claim_keys: keys}), do: keys != []

  @doc """
  Stamps an entry as claimed for the sealed segment(s) `keys` name.
  """
  @spec claim(t(), [String.t()]) :: t()
  def claim(%__MODULE__{} = entry, keys), do: %{entry | claim_keys: keys}

  @doc """
  The log record for an entry, in full — replaying it restores the entry exactly.
  """
  @spec to_record(t()) :: map()
  def to_record(%__MODULE__{} = entry) do
    %{
      "op" => "add",
      "id" => entry.id,
      "key" => entry.key,
      "row_count" => entry.row_count,
      "byte_size" => entry.byte_size,
      "added_at" => entry.added_at,
      "sealed_at" => entry.sealed_at,
      "retired_at" => entry.retired_at,
      "claim_keys" => entry.claim_keys,
      "stats" => encode_stats(entry.stats)
    }
  end

  @doc """
  The entry a log record describes.
  """
  @spec from_record(map()) :: {:ok, t()} | {:error, term()}
  def from_record(%{"id" => id, "key" => key, "row_count" => rows} = record)
      when is_binary(id) and is_binary(key) and is_integer(rows) do
    {:ok,
     %__MODULE__{
       id: id,
       key: key,
       row_count: rows,
       byte_size: Map.get(record, "byte_size", 0),
       added_at: Map.get(record, "added_at", 0),
       sealed_at: Map.get(record, "sealed_at"),
       retired_at: Map.get(record, "retired_at"),
       claim_keys: Map.get(record, "claim_keys") || [],
       stats: record |> Map.get("stats", %{}) |> decode_stats()
     }}
  end

  def from_record(record), do: {:error, {:invalid_record, record}}

  defp encode_stats(stats) do
    Map.new(stats, fn {column, column_stats} ->
      {column,
       %{
         "min" => encode_bound(column_stats.min),
         "max" => encode_bound(column_stats.max),
         "null_count" => column_stats.null_count
       }}
    end)
  end

  defp decode_stats(stats) do
    Map.new(stats, fn {column, column_stats} ->
      {column,
       %{
         min: decode_bound(column_stats["min"]),
         max: decode_bound(column_stats["max"]),
         null_count: column_stats["null_count"]
       }}
    end)
  end

  defp encode_bound(%NaiveDateTime{} = value),
    do: %{"type" => "naive_datetime", "value" => NaiveDateTime.to_iso8601(value)}

  defp encode_bound(%Date{} = value),
    do: %{"type" => "date", "value" => Date.to_iso8601(value)}

  defp encode_bound(value), do: value

  defp decode_bound(%{"type" => "naive_datetime", "value" => value}) do
    case NaiveDateTime.from_iso8601(value) do
      {:ok, datetime} -> datetime
      {:error, _reason} -> nil
    end
  end

  defp decode_bound(%{"type" => "date", "value" => value}) do
    case Date.from_iso8601(value) do
      {:ok, date} -> date
      {:error, _reason} -> nil
    end
  end

  defp decode_bound(value), do: value
end
