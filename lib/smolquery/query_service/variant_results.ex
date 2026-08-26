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
  and fails the export the way DuckDB reports it.

  ## Not a nested variant

  `DESCRIBE` names a variant inside a struct or list too (`STRUCT(v VARIANT)`,
  `VARIANT[]`). Those cannot be cast column-wide without changing the value's
  shape, so they are refused with a message that says how to select them.
  """

  alias Smolquery.Engine.Connection
  alias Smolquery.Identifier
  alias Smolquery.QueryService.Plan
  alias Smolquery.Schema
  alias Smolquery.Schema.Field

  @doc """
  The plan, with `VARIANT` result columns cast to `JSON`, and the names of
  those columns — `[]` when nothing needed casting.
  """
  @spec prepare(GenServer.server(), Plan.t()) ::
          {:ok, Plan.t(), [String.t()]} | {:error, term()}
  def prepare(connection, %Plan{} = plan) do
    if variant_reachable?(plan) do
      with {:ok, columns} <- Connection.describe(connection, plan.canonical_sql, :infinity),
           {:ok, projection, json_columns} <- projection(columns) do
        {:ok, rewrite(plan, projection, json_columns), json_columns}
      end
    else
      {:ok, plan, []}
    end
  end

  @doc """
  Whether any table the plan references declares a variant column.
  """
  @spec variant_reachable?(Plan.t()) :: boolean()
  def variant_reachable?(%Plan{schemas: schemas}) do
    Enum.any?(schemas, fn {_ref, %Schema{fields: fields}} ->
      Enum.any?(fields, &match?(%Field{type: :variant}, &1))
    end)
  end

  defp projection(columns) do
    Enum.reduce_while(columns, {:ok, [], []}, fn {name, type}, {:ok, exprs, json} ->
      quoted = Identifier.quote_name!(name)

      cond do
        type == "VARIANT" ->
          {:cont, {:ok, ["#{quoted}::JSON AS #{quoted}" | exprs], [name | json]}}

        String.contains?(type, "VARIANT") ->
          {:halt, {:error, {:invalid_query, nested_message(name, type)}}}

        true ->
          {:cont, {:ok, [quoted | exprs], json}}
      end
    end)
    |> case do
      {:ok, _exprs, []} -> {:ok, nil, []}
      {:ok, exprs, json} -> {:ok, Enum.join(Enum.reverse(exprs), ", "), Enum.reverse(json)}
      {:error, reason} -> {:error, reason}
    end
  end

  defp rewrite(plan, nil, []), do: plan

  defp rewrite(%Plan{canonical_sql: canonical} = plan, projection, _json_columns) do
    sql = "SELECT #{projection} FROM (#{canonical})"

    %{plan | sql: sql, canonical_sql: sql}
  end

  defp nested_message(name, type) do
    "column #{inspect(name)} has type #{type}: a VARIANT nested in a struct or list " <>
      "cannot be returned; select the variant on its own, or cast it with ::JSON"
  end
end
