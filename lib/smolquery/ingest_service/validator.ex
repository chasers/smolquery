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

  One walk per row: each value is coerced as it is checked, so a valid value
  pays `Smolquery.Schema.value_from_json/2` exactly once.
  """
  @spec validate(Schema.t(), [term()]) :: {[Writer.row()], [row_errors()]}
  def validate(%Schema{} = schema, rows) when is_list(rows) do
    names = MapSet.new(Schema.names(schema))

    {valid, errors, _count} =
      Enum.reduce(rows, {[], [], 0}, fn row, {valid, errors, index} ->
        case validate_row(schema, names, row) do
          {:ok, row} -> {[row | valid], errors, index + 1}
          {:error, messages} -> {valid, [%{index: index, errors: messages} | errors], index + 1}
        end
      end)

    {Enum.reverse(valid), Enum.reverse(errors)}
  end

  defp validate_row(schema, names, row) when is_map(row) do
    {pairs, problems} = coerce_fields(schema, row)

    case unknown_columns(names, row) ++ problems do
      [] -> {:ok, Map.new(pairs)}
      problems -> {:error, Enum.map(problems, &%{message: &1})}
    end
  end

  defp validate_row(_schema, _names, row) do
    {:error, [%{message: "row must be a JSON object, got: #{inspect(row)}"}]}
  end

  defp unknown_columns(names, row) do
    row
    |> Map.keys()
    |> Enum.reject(&MapSet.member?(names, &1))
    |> Enum.sort()
    |> Enum.map(&"unknown column: #{inspect(&1)}")
  end

  defp coerce_fields(schema, row) do
    {pairs, problems} =
      Enum.reduce(schema.fields, {[], []}, fn %Field{} = field, acc ->
        coerce_field(field, Map.get(row, field.name), acc)
      end)

    {pairs, Enum.reverse(problems)}
  end

  defp coerce_field(%Field{nullable: true}, nil, acc), do: acc

  defp coerce_field(%Field{} = field, nil, {pairs, problems}),
    do: {pairs, ["column #{field.name} must not be null" | problems]}

  defp coerce_field(%Field{} = field, value, {pairs, problems}) do
    case Schema.value_from_json(field.type, value) do
      {:ok, coerced} ->
        {[{field.name, coerced} | pairs], problems}

      {:error, {:invalid_value, type, value}} ->
        {pairs, [invalid_message(field, type, value) | problems]}
    end
  end

  defp invalid_message(field, type, value) do
    {:ok, name} = Schema.api_type(type)

    "column #{field.name} (#{name}) cannot accept #{inspect(value)}"
  end
end
