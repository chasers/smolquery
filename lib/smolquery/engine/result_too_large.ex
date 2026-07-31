defmodule Smolquery.Engine.ResultTooLarge do
  @moduledoc """
  Raised when a query returns more rows than `Smolquery.Engine.Result` will
  convert.

  `Result` builds a row list of plain Elixir terms, which costs roughly a
  kilobyte and two microseconds per row — fine for the catalog and control-plane
  queries it exists for, and ruinous for a user query that happens to match five
  million rows. Without a ceiling the failure mode is a node that quietly spends
  gigabytes of heap; with one it is this error.

  What a refusal saves is the conversion, not the query. ADBC has already run the
  statement and materialized its Arrow result by the time the ceiling is checked,
  and that part grows with the result no matter what: refusing a five-million-row
  result takes 593 ms and refusing twenty million takes 1.2 s, nearly all of it
  the Arrow fetch. Converting those five million rows instead takes 11.5 s and
  4.8 GiB. So the ceiling bounds the heap and the transposition, which is where
  the damage was, and leaves the fetch alone.

  It carries the limit rather than the row count because the conversion stops at
  the limit and never counts the rest.
  """

  defexception [:max]

  @type t :: %__MODULE__{max: pos_integer()}

  @impl true
  def message(%__MODULE__{max: max}) do
    "query returned more than #{max} rows, the limit for " <>
      "Smolquery.Engine.Result. Read a result this size with " <>
      "Smolquery.Engine.frame/3, which keeps it in Arrow, or raise " <>
      ":max_result_rows if the caller really wants Elixir terms."
  end
end
