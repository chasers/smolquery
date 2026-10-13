defmodule Smolquery.Engine.UntypedResult do
  @moduledoc """
  Raised, or returned by `Smolquery.Engine.Connection.query/4`, for a result
  `Smolquery.Engine.Result` cannot read because DuckDB sent a column with no
  type (T-654).

  From DuckDB 2.0, an untyped `NULL` reaches Arrow as the null type, and
  adbc's `Adbc.Column.to_list/1` has no clause for it. `Result` reads a
  top-level such column as nils, as many as its neighbours hold, which leaves
  two shapes it cannot read:

    * `:unsized` — every column of the batch is untyped, so nothing in it
      says how many rows it holds.
    * `{:nested, column}` — `column` holds an untyped NULL inside a list,
      struct or map (`[NULL]`, `{'k': NULL}`), whose values adbc cannot decode
      at all.

  Either way casting the NULL (`NULL::INTEGER`, `[NULL::INTEGER]`) gives it a
  type, and the result reads.
  """

  defexception [:reason]

  @type reason :: :unsized | {:nested, String.t()}
  @type t :: %__MODULE__{reason: reason()}

  @impl true
  def message(%__MODULE__{reason: :unsized}),
    do: "a result of only untyped NULL columns carries no row count; cast one column"

  def message(%__MODULE__{reason: {:nested, column}}),
    do: "column #{inspect(column)} holds an untyped NULL inside a list, struct or map; cast it"
end
