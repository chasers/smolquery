defmodule Smolquery.IngestService.Validator do
  @moduledoc """
  Rows against a schema: what may proceed to the buffer, and what each
  rejected row was rejected for.

  BigQuery `insertErrors` semantics (PL-8 D2): validation is per row, valid
  rows proceed even when neighbors fail, and every rejected row reports its
  original index with every problem found — a client fixing a batch fixes it
  once, not one error at a time.

  A valid row comes out with only the schema's columns, each value coerced by
  `Smolquery.Schema.value_from_json/2`; a column the row does not carry is
  simply absent, which the segment writer stores as `NULL`.
  """

  alias Smolquery.Schema
  alias Smolquery.Schema.Field
  alias Smolquery.Segments.Writer

  @type row_errors :: %{index: non_neg_integer(), errors: [%{message: String.t()}]}

  @doc """
  Splits `rows` into coerced valid rows and per-index rejections.
  """
  @spec validate(Schema.t(), [term()]) :: {[Writer.row()], [row_errors()]}
  def validate(%Schema{} = schema, rows) when is_list(rows) do
    names = Schema.names(schema)

    {valid, errors} =
      rows
      |> Stream.with_index()
      |> Enum.reduce({[], []}, fn {row, index}, {valid, errors} ->
        case validate_row(schema, names, row) do
          {:ok, row} -> {[row | valid], errors}
          {:error, messages} -> {valid, [%{index: index, errors: messages} | errors]}
        end
      end)

    {Enum.reverse(valid), Enum.reverse(errors)}
  end

  defp validate_row(schema, names, row) when is_map(row) do
    problems = unknown_columns(names, row) ++ field_problems(schema, row)

    case problems do
      [] -> {:ok, coerce(schema, row)}
      problems -> {:error, Enum.map(problems, &%{message: &1})}
    end
  end

  defp validate_row(_schema, _names, row) do
    {:error, [%{message: "row must be a JSON object, got: #{inspect(row)}"}]}
  end

  defp unknown_columns(names, row) do
    row
    |> Map.keys()
    |> Enum.reject(&(&1 in names))
    |> Enum.sort()
    |> Enum.map(&"unknown column: #{inspect(&1)}")
  end

  defp field_problems(schema, row) do
    Enum.flat_map(schema.fields, &value_problems(&1, Map.get(row, &1.name)))
  end

  defp value_problems(%Field{nullable: true}, nil), do: []

  defp value_problems(%Field{} = field, nil), do: ["column #{field.name} must not be null"]

  defp value_problems(%Field{} = field, value) do
    case Schema.value_from_json(field.type, value) do
      {:ok, _value} -> []
      {:error, {:invalid_value, type, value}} -> [invalid_message(field, type, value)]
    end
  end

  defp invalid_message(field, type, value) do
    {:ok, name} = Schema.api_type(type)

    "column #{field.name} (#{name}) cannot accept #{inspect(value)}"
  end

  defp coerce(schema, row) do
    schema.fields
    |> Enum.flat_map(fn %Field{} = field ->
      case Map.get(row, field.name) do
        nil ->
          []

        value ->
          {:ok, coerced} = Schema.value_from_json(field.type, value)
          [{field.name, coerced}]
      end
    end)
    |> Map.new()
  end
end
