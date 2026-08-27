defmodule Smolquery.QueryService.VariantResults do
  @moduledoc """
  Casts a plan's `VARIANT` result columns to `JSON` before the result crosses
  Arrow.

  Arrow has no VARIANT type, so DuckDB cannot export a VARIANT column: the
  ADBC stream fails on the first record and Polars refuses the batch. Inside
  DuckDB the type is fine — `attrs['n']::BIGINT` in a `WHERE`, `variant_typeof`
  in a projection — so the cast belongs at the one place a VARIANT would
  leave: the outermost `SELECT`. A top-level `attrs::JSON` crosses as text,
  and `Smolquery.Engine.Frame.to_rows/2` decodes that text back into the
  nested JSON value the client inserted.

  ## Only when a variant is reachable

  Finding the result's column types costs a `DESCRIBE` — a bind of the whole
  query, which for a hot tier means fetching each micro-segment's footer once
  more. So the describe runs only when a referenced table declares a variant
  column (`Plan.schemas`); every other plan passes through untouched. A
  variant conjured without a table (`SELECT '1'::VARIANT`) is not rewritten
  and fails the export the way DuckDB reports it. The described columns ride
  out in `outputs`, so `Smolquery.QueryService.Scatter` does not bind the same
  statement a second time.

  ## Not a nested variant

  `DESCRIBE` names a variant inside a struct or list too (`STRUCT(v VARIANT)`,
  `VARIANT[]`). This module cannot cast those column-wide without changing the
  value's shape, so it refuses the query with a message that says how to
  select them.

  ## The wrapper is a subquery

  A rewritten plan reads `SELECT ... FROM (<canonical>)`. The decomposer
  refuses a subquery in `FROM`, so the runner does not offer a rewritten plan
  to the scatter path at all.
  """

  alias Smolquery.Engine.Connection
  alias Smolquery.Identifier
  alias Smolquery.QueryService.Plan
  alias Smolquery.Schema
  alias Smolquery.Schema.Field

  @typedoc "A result column as `DESCRIBE` reports it: its name and DuckDB type."
  @type column :: {String.t(), String.t()}

  @typedoc """
  The plan to run, the names of the result columns that now carry JSON text,
  and the described result columns — `nil` when no describe was needed.
  """
  @type t :: %__MODULE__{
          plan: Plan.t(),
          json_columns: [String.t()],
          outputs: [column()] | nil
        }

  @enforce_keys [:plan]
  defstruct [:plan, json_columns: [], outputs: nil]

  @doc """
  The plan, with `VARIANT` result columns cast to `JSON`, beside the names of
  those columns and the described outputs.
  """
  @spec prepare(GenServer.server(), Plan.t()) :: {:ok, t()} | {:error, term()}
  def prepare(connection, %Plan{} = plan) do
    if variant_reachable?(plan),
      do: describe_and_cast(connection, plan),
      else: {:ok, %__MODULE__{plan: plan}}
  end

  defp describe_and_cast(connection, plan) do
    with {:ok, columns} <- Connection.describe(connection, plan.canonical_sql, :infinity),
         :ok <- refuse_nested(columns) do
      {:ok, cast(plan, columns, variant_columns(columns))}
    end
  end

  defp cast(plan, columns, []), do: %__MODULE__{plan: plan, outputs: columns}

  defp cast(plan, columns, json_columns),
    do: %__MODULE__{plan: rewrite(plan, columns), json_columns: json_columns, outputs: columns}

  @doc """
  Whether any table the plan references declares a variant column.
  """
  @spec variant_reachable?(Plan.t()) :: boolean()
  def variant_reachable?(%Plan{schemas: schemas}) do
    Enum.any?(schemas, fn {_ref, %Schema{fields: fields}} ->
      Enum.any?(fields, &match?(%Field{type: :variant}, &1))
    end)
  end

  @doc """
  What an export failure means, when the result's types can say.

  A `VARIANT` that reaches the result without a table behind it
  (`SELECT '1'::VARIANT`) is not rewritten, and Arrow refuses it with an
  internal error. When a frame read fails that way, one describe names the
  column, and the caller gets the same `{:invalid_query, _}` a nested variant
  gets, instead of the engine's opaque text. Any other error passes through.
  """
  @spec explain_export_failure(GenServer.server(), Plan.t(), Exception.t()) :: term()
  def explain_export_failure(connection, %Plan{} = plan, error) do
    with true <- arrow_failure?(error),
         {:ok, columns} <- Connection.describe(connection, plan.canonical_sql, :infinity),
         [name | _rest] <- variant_columns(columns) do
      {:invalid_query,
       "column #{inspect(name)} is a VARIANT with no table column behind it, which cannot " <>
         "be returned; cast it with ::JSON"}
    else
      _not_a_variant_export -> error
    end
  end

  defp arrow_failure?(error), do: String.contains?(Exception.message(error), "Arrow")

  defp refuse_nested(columns) do
    case Enum.find(columns, fn {_name, type} -> nested_variant?(type) end) do
      nil -> :ok
      {name, type} -> {:error, {:invalid_query, nested_message(name, type)}}
    end
  end

  @quoted_literal ~r/'(?:[^']|'')*'/
  @variant_word ~r/\bVARIANT\b/

  defp nested_variant?("VARIANT"), do: false

  defp nested_variant?(type),
    do: type |> String.replace(@quoted_literal, "''") |> then(&Regex.match?(@variant_word, &1))

  defp variant_columns(columns), do: for({name, "VARIANT"} <- columns, do: name)

  defp rewrite(%Plan{canonical_sql: canonical} = plan, columns) do
    sql = "SELECT #{Enum.map_join(columns, ", ", &column_expression/1)} FROM (#{canonical})"

    %{plan | sql: sql, canonical_sql: sql}
  end

  defp column_expression({name, "VARIANT"}) do
    quoted = Identifier.quote_label(name)

    "#{quoted}::JSON AS #{quoted}"
  end

  defp column_expression({name, _type}), do: Identifier.quote_label(name)

  defp nested_message(name, type) do
    "column #{inspect(name)} has type #{type}: a VARIANT nested in a struct or list " <>
      "cannot be returned; select the variant on its own, or cast it with ::JSON"
  end
end
