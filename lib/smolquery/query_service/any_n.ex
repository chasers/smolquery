defmodule Smolquery.QueryService.AnyN do
  @moduledoc """
  The any-N bound: an unordered `LIMIT n` with no WHERE reads only as many
  hot micro-segments as can hold n rows (T-449).

  `SELECT * FROM t LIMIT 50` is the row preview on the table page and the
  editor's sample query, and the first thing anyone types at a new table.
  Without an ORDER BY or a WHERE, *any* n rows of the table are a correct
  answer — DuckDB itself promises nothing about which ones — so the planner
  may hand it the fewest sources that hold n rows. It used to hand it every
  micro-segment the buffer nodes held, and the hot read unions those files by
  name, which makes DuckDB open every one of them before it can produce a
  row: the preview of a table with a deep seal backlog took seconds per
  thousand files, and the LIMIT could not stop it early.

  ## What qualifies

  One SELECT over exactly one table reference
  (`Smolquery.QueryService.SingleTable`), with a constant LIMIT (and OFFSET,
  whose rows are read before they are skipped) as its only modifier: no ORDER
  BY (that is `Smolquery.QueryService.TopN`'s case), no WHERE (a predicate
  decides how many rows survive, and the file count that covers it is
  unknowable from row counts), no DISTINCT, GROUP BY, HAVING, QUALIFY, or
  SAMPLE, no CTE, and no subquery, window, or function anywhere in the
  statement — `SELECT count(*) FROM t LIMIT 1` counts every row, and the
  only way to be sure no select item aggregates is to accept none that calls
  a function at all. Column references, `*`, casts, and constants are what a
  preview projects, and they pass.

  ## The bound

  `trim/3` covers `n` with the sealed tier first — its row count at the
  plan's snapshot is one catalog read the planner already makes, and DuckLake
  reads its own files lazily — and then with the newest micro-segments by
  id, until their row counts make up the rest. A sealed tier that already
  holds n rows needs no hot file at all. Unknown sealed statistics count as
  zero rows, which reads more hot files than needed, never fewer.

  Every miss is `nil` — no bound, never an error.
  """

  alias Smolquery.BufferService.HotClient
  alias Smolquery.Catalog
  alias Smolquery.QueryService.SingleTable

  @typedoc "The table the LIMIT belongs to, and n: the LIMIT plus any OFFSET."
  @type t :: %{ref: Catalog.table_ref(), limit: pos_integer()}

  @excluded_classes ["SUBQUERY", "WINDOW", "FUNCTION"]

  @doc """
  The any-N spec of a serialized statement, or `nil` when it does not qualify.

  `refs` are the statement's resolved table references; the FROM table must
  be one of them.
  """
  @spec spec(map(), [Catalog.table_ref()]) :: t() | nil
  def spec(%{"node" => %{"type" => "SELECT_NODE"} = node} = statement, refs) do
    with {:ok, %{ref: ref}} <- SingleTable.source(node["from_table"], refs),
         true <- is_nil(node["where_clause"]),
         true <- SingleTable.simple?(node),
         true <- SingleTable.single_reference?(statement, @excluded_classes),
         [%{"type" => "LIMIT_MODIFIER"} = modifier] <- node["modifiers"],
         {:ok, limit} <- SingleTable.limit(modifier) do
      %{ref: ref, limit: limit}
    else
      _ineligible -> nil
    end
  end

  def spec(_statement, _refs), do: nil

  @doc """
  The fewest of `entries` that, with `sealed_rows` rows already in the sealed
  tier, hold `limit` rows: the newest by id, until their row counts cover the
  rest. `[]` when the sealed tier covers it alone; every entry when even all
  of them fall short.
  """
  @spec trim([HotClient.entry()], non_neg_integer() | :unavailable, pos_integer()) ::
          [HotClient.entry()]
  def trim(entries, sealed_rows, limit) do
    covered = if is_integer(sealed_rows), do: sealed_rows, else: 0

    entries
    |> Enum.sort_by(& &1["id"], :desc)
    |> take_rows(max(limit - covered, 0), [])
  end

  defp take_rows(_entries, 0, taken), do: Enum.reverse(taken)
  defp take_rows([], _needed, taken), do: Enum.reverse(taken)

  defp take_rows([entry | rest], needed, taken) do
    rows = if is_integer(entry["row_count"]), do: entry["row_count"], else: 0

    take_rows(rest, max(needed - rows, 0), [entry | taken])
  end
end
