defmodule Smolquery.IngestService.ColumnarValidator do
  @moduledoc """
  The optimistic columnar half of insert validation: NDJSON bytes become a
  schema-shaped `Explorer.DataFrame` in one native pass, or the batch falls
  back to `Smolquery.IngestService.Validator`'s per-row walk.

  The per-row validator costs rows × columns of Elixir term work — measured
  at 39% of a wide batch's insert-path CPU (`bench/results/otel_logs.md`),
  with the JSON decode it follows costing another 26%. Here Polars parses
  the NDJSON and casts every column in Rust; the whole decode-and-validate
  is ~20% of what the term path pays, and what comes out is already the
  frame the segment writer wants (T-139's shape, fronted by JSON).

  ## Fallback is the error path, not an error

  This module never reports *which* rows were bad — the per-index
  `insertErrors` contract needs the row walk. Any batch this module cannot
  prove entirely valid answers `:fallback`, and the caller re-runs the
  per-row path to get exact per-index errors (or a clean accept, for shapes
  this module does not attempt — extra JSON depth, exotic coercions).
  Falling back costs one wasted native parse; batches that need it were
  about to pay the term walk anyway.

  ## Coercion parity, enforced by construction

  A cast is accepted only where `Smolquery.Schema.value_from_json/2` would
  accept the same value, and every cast is checked for silent nulling:
  Polars casts are lenient (`"xyz"` as int64 becomes null, not an error),
  so a cast that *increases* a column's null count means some value did not
  convert — exactly the rows the per-row path must reject, so: fallback.
  Two lenient-cast hazards are refused outright rather than checked:

    * float → int truncates (`1.5` becomes `1`) where `value_from_json`
      rejects, so a JSON float in an int64 column always falls back
    * anything → string stringifies where `value_from_json` requires a
      binary, so only a string column satisfies a string field

  Timestamps accept the ISO 8601 UTC shape (`...Z`, any fractional
  precision, none included); offsets and space-separated forms fall back
  to the per-row path, which handles them.
  """

  alias Explorer.DataFrame
  alias Explorer.Series
  alias Smolquery.Schema
  alias Smolquery.Schema.Field

  @utc_iso8601 "%Y-%m-%dT%H:%M:%S%.fZ"
  @date_iso8601 "%Y-%m-%d"

  @doc """
  Parses and validates `ndjson` against `schema` in one columnar pass.

  `{:ok, frame}` carries every row, coerced, with exactly the schema's
  columns in the schema's order — ready for `Smolquery.Segments.Writer`.
  `:fallback` means the batch could not be proven entirely valid (or used a
  shape this pass does not attempt); the caller must re-run the per-row
  validator for exact per-index errors.
  """
  @spec validate(Schema.t(), binary()) :: {:ok, DataFrame.t()} | :fallback
  def validate(%Schema{} = schema, ndjson) when is_binary(ndjson) do
    with {:ok, frame} <- load(ndjson),
         :ok <- known_columns(frame, schema),
         {:ok, columns} <- cast_columns(frame, schema) do
      {:ok, DataFrame.new(columns)}
    else
      _cannot_prove_valid -> :fallback
    end
  end

  defp load(ndjson) do
    case DataFrame.load_ndjson(ndjson, infer_schema_length: nil) do
      {:ok, frame} -> if DataFrame.n_rows(frame) > 0, do: {:ok, frame}, else: :fallback
      {:error, _reason} -> :fallback
    end
  rescue
    _error in [ArgumentError, RuntimeError] -> :fallback
  end

  defp known_columns(frame, schema) do
    names = MapSet.new(Schema.names(schema))

    if Enum.all?(DataFrame.names(frame), &MapSet.member?(names, &1)), do: :ok, else: :fallback
  end

  defp cast_columns(frame, schema) do
    rows = DataFrame.n_rows(frame)
    present = MapSet.new(DataFrame.names(frame))

    Enum.reduce_while(schema.fields, {:ok, []}, fn %Field{} = field, {:ok, columns} ->
      series = if MapSet.member?(present, field.name), do: frame[field.name], else: nil

      case cast_column(series, field, rows) do
        {:ok, cast} -> {:cont, {:ok, [{field.name, cast} | columns]}}
        :fallback -> {:halt, :fallback}
      end
    end)
    |> case do
      {:ok, columns} -> {:ok, Enum.reverse(columns)}
      :fallback -> :fallback
    end
  end

  defp cast_column(nil, %Field{nullable: false}, _rows), do: :fallback

  defp cast_column(nil, %Field{} = field, rows) do
    with {:ok, dtype} <- Schema.explorer_dtype(field.type) do
      {:ok, Series.from_list(List.duplicate(nil, rows), dtype: dtype)}
    end
  end

  defp cast_column(series, %Field{} = field, _rows) do
    with {:ok, dtype} <- Schema.explorer_dtype(field.type),
         {:ok, cast} <- cast_series(series, Series.dtype(series), field.type, dtype) do
      require_nullability(cast, field)
    else
      _unsupported -> :fallback
    end
  end

  defp require_nullability(series, %Field{nullable: false}) do
    if Series.nil_count(series) == 0, do: {:ok, series}, else: :fallback
  end

  defp require_nullability(series, %Field{}), do: {:ok, series}

  defp cast_series(series, dtype, _logical, dtype), do: {:ok, series}

  defp cast_series(series, :null, _logical, dtype) do
    {:ok, Series.cast(series, dtype)}
  end

  defp cast_series(series, :string, :timestamp, _dtype),
    do: checked(series, &Series.strptime(&1, @utc_iso8601))

  defp cast_series(series, :string, :date, dtype),
    do: checked(series, &(&1 |> Series.strptime(@date_iso8601) |> Series.cast(dtype)))

  defp cast_series(series, :string, :int64, dtype), do: checked(series, &Series.cast(&1, dtype))

  defp cast_series(series, :string, :float64, dtype),
    do: checked(series, &Series.cast(&1, dtype))

  defp cast_series(series, {:s, 64}, :float64, dtype), do: {:ok, Series.cast(series, dtype)}

  defp cast_series(_series, _dtype, _logical, _target), do: :fallback

  defp checked(series, cast) do
    cast_series = cast.(series)

    if Series.nil_count(cast_series) == Series.nil_count(series),
      do: {:ok, cast_series},
      else: :fallback
  rescue
    _error in [ArgumentError, RuntimeError] -> :fallback
  end
end
