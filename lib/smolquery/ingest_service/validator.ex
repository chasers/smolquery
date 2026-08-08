defmodule Smolquery.IngestService.Validator do
  @moduledoc """
  Rows against a schema: what may proceed to the buffer, and what each
  rejected row was rejected for.

  BigQuery `insertErrors` semantics (PL-8 D2): validation is per row, valid
  rows proceed even when neighbors fail, and every rejected row reports its
  original index with every problem found — a client fixing a batch fixes it
  once, not one error at a time.

  ## Rows go in, columns come out

  What comes out is not rows. Every accepted value is appended straight to its
  column's accumulator, so the batch leaves here in the shape
  `Smolquery.Segments.Writer` needs — and the two row-shaped terms that used to
  stand between them are both gone: the map this module built per row, and the
  transpose the writer did to take it apart again, one full pass over the batch
  per column. Measured at 3 062 rows and 62 columns, deleting them takes the
  stage from 98.7 ms to 67.7 ms and the write path's two deep copies from
  13.5 ms to 6.2 ms, because the same values now travel as 62 containers rather
  than 3 062 (`bench/columnar.exs`, `bench/results/pathprof.md`).

  One rule comes with the shape and is not optional: **every field gets a value
  for every accepted row, `nil` included**. Row-major got that for free — a
  column a row did not carry was simply an absent key, and `Map.get/2` answered
  `nil` at read time. Column-major has to write the `nil` down, because a column
  that skips a row shifts every value after it against the other columns, and
  nothing downstream can detect that.

  Each value is coerced by `Smolquery.Schema.value_from_json/2`, and a column
  the row does not carry becomes `NULL` in the segment as before.

  ## The batch measures itself on the way through

  `Smolquery.BufferService.TableBuffer`'s admission bound is a byte budget over
  the accumulated *term*, and it used to be fed by an `:erlang.external_size/1`
  call on the coerced rows — a second full traversal of a term set this module
  had just walked, to produce one integer. The number comes out of this walk
  instead: every coerced value's external-term size is known from its type the
  moment it is produced, and adding it up costs arithmetic on a value already
  in hand rather than a pass over megabytes of heap.

  The rule the estimate has to hold to is one-directional. It may exceed
  `:erlang.external_size/1` on the same rows — the buffer then admits and
  flushes slightly sooner than the exact number would — but it must never come
  out below it, because a memory bound fed an under-count admits more than it
  promised. Every clause of the estimate is therefore an upper bound on its
  value's external term format encoding: a binary is `BINARY_EXT`'s five bytes
  of tag and length plus its own, an integer is priced by which of the three
  integer encodings can hold it, a `NaiveDateTime` or `Date` by the largest its
  fixed shape can reach, and anything else — another calendar, a `Decimal`
  whose coefficient is a bignum — falls back to measuring itself exactly. The
  list framing is counted the same way, so the total is the sum of parts that
  external term format does not share.

  Column-major changed what that total is measuring, and by a lot. A row-shaped
  term repeated all 62 column *names* on every row; columns carry each name
  zero times, so the same rows now price out far smaller — roughly half, on
  OTel-shaped data whose names average about 15 bytes. Nothing is being
  under-counted: the term really is that much smaller, and so is the heap the
  bound exists to protect. The visible consequence is that `:flush_max_bytes`
  now buys about twice the rows it used to, which is a change in flush sizing
  and not only in accounting.
  """

  alias Smolquery.Schema
  alias Smolquery.Schema.Field

  @type row_errors :: %{index: non_neg_integer(), errors: [%{message: String.t()}]}

  @version_bytes 1
  @list_bytes 6
  @binary_bytes 5
  @small_integer_bytes 2
  @integer_bytes 5
  @small_big_bytes 11
  @float_bytes 9
  @atom_bytes 8
  @naive_datetime_bytes 148
  @date_bytes 92

  @integer_max 2_147_483_647
  @integer_min -2_147_483_648
  @small_big_max 9_223_372_036_854_775_807

  @typedoc """
  The accepted rows, transposed, plus what the buffer needs to bound them.

  `:columns` holds one list of values per field of the schema, in its `:fields`
  order and each the length of `:row_count` — `t:Smolquery.Segments.Writer.columns/0`
  without its tag.
  """
  @type batch :: %{
          columns: [[term()]],
          row_count: non_neg_integer(),
          byte_size: non_neg_integer()
        }

  @doc """
  Splits `rows` into an accepted `t:batch/0` and per-index rejections.

  One walk per row: each value is coerced as it is checked and priced as it is
  coerced, so a valid value pays `Smolquery.Schema.value_from_json/2` exactly
  once and its size costs no traversal of its own.

  `:byte_size` is what the buffer's byte budget measures the batch as. It is an
  upper bound on `:erlang.external_size/1` over `:columns`, never a lower one,
  for the reason the moduledoc gives.
  """
  @spec validate(Schema.t(), [term()]) :: {batch(), [row_errors()]}
  def validate(%Schema{} = schema, rows) when is_list(rows) do
    names = MapSet.new(Schema.names(schema))

    # One accumulator per field, held in reverse field order because
    # `coerce_fields/2` produces a row's values in that order and the two are
    # walked in lockstep. Both are put right at the end, once per batch rather
    # than once per row.
    empty = Enum.map(schema.fields, fn _field -> [] end)

    {stacks, errors, _index, accepted, bytes} =
      Enum.reduce(rows, {empty, [], 0, 0, 0}, fn row, {stacks, errors, index, accepted, bytes} ->
        case validate_row(schema, names, row) do
          {:ok, values, row_bytes} ->
            {push(stacks, values), errors, index + 1, accepted + 1, bytes + row_bytes}

          {:error, messages} ->
            {stacks, [%{index: index, errors: messages} | errors], index + 1, accepted, bytes}
        end
      end)

    batch = %{
      columns: stacks |> Enum.reverse() |> Enum.map(&:lists.reverse/1),
      row_count: accepted,
      byte_size: bytes + framing(length(stacks))
    }

    {batch, Enum.reverse(errors)}
  end

  @doc """
  The same, for a batch that arrived already in column order.

  `columns` is the request's own map of column name to list of values — the
  shape a client sends to avoid repeating 62 column names on every one of its
  rows, which is roughly half the bytes of an OTel-shaped body.

  The rejection contract is unchanged and that is the whole difficulty: errors
  are still per row, still carry the original index, and still report every
  problem the row has. So a value's problems are attributed to its *position*,
  and a position with any problem is dropped from every column — otherwise one
  bad value in one column would shift that column against the other 61.

  A column the schema does not declare is a problem of every row, because that
  is what it is: the client sent data for a column that does not exist, and
  there is no row it belongs to more than another.
  """
  @spec validate_columns(Schema.t(), %{optional(String.t()) => [term()]}, non_neg_integer()) ::
          {batch(), [row_errors()]}
  def validate_columns(%Schema{} = schema, columns, row_count) when is_map(columns) do
    unknown = Enum.sort(Map.keys(columns) -- Schema.names(schema))

    {coerced, problems, bytes} = coerce_columns(schema, columns, row_count)

    rejected = rejected_indices(problems, unknown, row_count)

    batch = %{
      columns: keep(coerced, rejected, row_count),
      row_count: row_count - MapSet.size(rejected),
      byte_size: bytes + framing(length(coerced))
    }

    {batch, errors_for(problems, unknown, rejected)}
  end

  # One pass per column rather than per row: the field's type is known once for
  # the whole column, so the coercion never looks a type up per value.
  defp coerce_columns(schema, columns, row_count) do
    {acc, problems, bytes} =
      Enum.reduce(schema.fields, {[], %{}, 0}, fn %Field{} = field, {acc, problems, bytes} ->
        values = pad(Map.get(columns, field.name), row_count)

        {column, problems, bytes} = coerce_column(field, values, problems, bytes)

        {[column | acc], problems, bytes}
      end)

    {:lists.reverse(acc), problems, bytes}
  end

  # A column the body omits is all nulls, which is the same thing row-major
  # meant by an absent key. A column shorter than the batch is padded rather
  # than rejected outright, and the nulls it gains are checked like any other.
  defp pad(nil, row_count), do: List.duplicate(nil, row_count)

  defp pad(values, row_count) when is_list(values) do
    case row_count - length(values) do
      short when short > 0 -> values ++ List.duplicate(nil, short)
      _long_or_exact -> Enum.take(values, row_count)
    end
  end

  defp coerce_column(field, values, problems, bytes) do
    {column, problems, bytes, _index} =
      Enum.reduce(values, {[], problems, bytes, 0}, fn value, {column, problems, bytes, index} ->
        case coerce_field(field, value, {[], [], 0}) do
          {[coerced], [], value_bytes} ->
            {[coerced | column], problems, bytes + value_bytes, index + 1}

          {_values, messages, _bytes} ->
            {[nil | column], add_problems(problems, index, messages), bytes, index + 1}
        end
      end)

    {:lists.reverse(column), problems, bytes}
  end

  defp add_problems(problems, index, messages),
    do: Map.update(problems, index, messages, &(messages ++ &1))

  defp rejected_indices(_problems, unknown, row_count) when unknown != [],
    do: MapSet.new(0..(row_count - 1)//1)

  defp rejected_indices(problems, _unknown, _row_count), do: MapSet.new(Map.keys(problems))

  defp keep(columns, rejected, row_count) do
    if MapSet.size(rejected) == 0 do
      columns
    else
      kept = Enum.reject(0..(row_count - 1)//1, &MapSet.member?(rejected, &1))

      Enum.map(columns, fn column ->
        values = List.to_tuple(column)

        Enum.map(kept, &elem(values, &1))
      end)
    end
  end

  defp errors_for(problems, unknown, rejected) do
    unknown_messages = Enum.map(unknown, &"unknown column: #{inspect(&1)}")

    rejected
    |> Enum.sort()
    |> Enum.map(fn index ->
      messages = unknown_messages ++ Enum.reverse(Map.get(problems, index, []))

      %{index: index, errors: Enum.map(messages, &%{message: &1})}
    end)
  end

  # A rejected row must leave no trace in any column, so a row's values are
  # gathered into a flat list first and pushed only once the row is known to be
  # valid. That list dies with the row; the map it replaced outlived the batch.
  defp validate_row(schema, names, row) when is_map(row) do
    {values, problems, bytes} = coerce_fields(schema, row)

    case unknown_columns(names, row) ++ problems do
      [] -> {:ok, values, bytes}
      problems -> {:error, Enum.map(problems, &%{message: &1})}
    end
  end

  defp validate_row(_schema, _names, row) do
    {:error, [%{message: "row must be a JSON object, got: #{inspect(row)}"}]}
  end

  # Both lists are in reverse field order and the same length, which
  # `coerce_fields/2` guarantees by pushing exactly one value per field. Built
  # body-recursively rather than with an accumulator: an accumulator would
  # reverse the column order on every row, and alternating it is worse than
  # either order.
  defp push([], []), do: []

  defp push([stack | stacks], [value | values]), do: [[value | stack] | push(stacks, values)]

  # The outer list of columns and each column's own list framing. Everything
  # inside is priced per value as it is coerced.
  defp framing(columns), do: @version_bytes + @list_bytes + columns * @list_bytes

  defp unknown_columns(names, row) do
    row
    |> Map.keys()
    |> Enum.reject(&MapSet.member?(names, &1))
    |> Enum.sort()
    |> Enum.map(&"unknown column: #{inspect(&1)}")
  end

  # Returns the row's values in reverse field order, one per field without
  # exception. A rejected row's values are thrown away, so their contents past
  # the first problem do not matter — but their *count* still has to match, or
  # `push/2` would walk two lists of different lengths.
  defp coerce_fields(schema, row) do
    {values, problems, bytes} =
      Enum.reduce(schema.fields, {[], [], 0}, fn %Field{} = field, acc ->
        coerce_field(field, Map.get(row, field.name), acc)
      end)

    {values, Enum.reverse(problems), bytes}
  end

  # A column the row does not carry still has to advance by one, or every column
  # after it in the batch shifts up a row against the others. Row-major got this
  # for free from `Map.get/2` returning nil at read time; column-major has to
  # write the nil down.
  defp coerce_field(%Field{nullable: true}, nil, {values, problems, bytes}),
    do: {[nil | values], problems, bytes + @atom_bytes}

  defp coerce_field(%Field{} = field, nil, {values, problems, bytes}),
    do: {[nil | values], ["column #{field.name} must not be null" | problems], bytes}

  defp coerce_field(%Field{} = field, value, {values, problems, bytes}) do
    case Schema.value_from_json(field.type, value) do
      {:ok, coerced} ->
        {[coerced | values], problems, bytes + value_bytes(coerced)}

      {:error, {:invalid_value, type, value}} ->
        {[nil | values], [invalid_message(field, type, value) | problems], bytes}
    end
  end

  defp value_bytes(value) when is_binary(value), do: @binary_bytes + byte_size(value)

  defp value_bytes(value) when is_integer(value) and value >= 0 and value <= 255,
    do: @small_integer_bytes

  defp value_bytes(value) when is_integer(value) and value >= @integer_min,
    do: integer_bytes(value)

  defp value_bytes(value) when is_float(value), do: @float_bytes
  defp value_bytes(value) when is_boolean(value), do: @atom_bytes
  defp value_bytes(%NaiveDateTime{calendar: Calendar.ISO}), do: @naive_datetime_bytes
  defp value_bytes(%Date{calendar: Calendar.ISO}), do: @date_bytes
  defp value_bytes(value), do: :erlang.external_size(value)

  defp integer_bytes(value) when value <= @integer_max, do: @integer_bytes
  defp integer_bytes(value) when value <= @small_big_max, do: @small_big_bytes
  defp integer_bytes(value), do: :erlang.external_size(value)

  defp invalid_message(field, type, value) do
    {:ok, name} = Schema.api_type(type)

    "column #{field.name} (#{name}) cannot accept #{inspect(value)}"
  end
end
