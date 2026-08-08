Code.require_file("otel_support.exs", __DIR__)

defmodule Bench.Columnar do
  @moduledoc """
  What the row-major detour costs, measured before anyone builds a way around it.

  `bench/results/pathprof.md` shows a batch becoming maps twice and then being
  transposed, all before Rust sees anything:

    1. `JSON.decode!` builds a map per row — 62 binary keys
    2. `Validator.validate/2` builds a *second* map per row, `Map.new(pairs)`
    3. `Writer.build_frame/2` walks the row list once per column, 62 times, to
       turn those maps back into 62 lists

  Steps 2 and 3 undo each other: the validator assembles a map out of pairs it
  produced in column order, and the writer takes it apart again in that same
  order. This measures deleting both — the validator appending each coerced
  value straight to its column's accumulator, and the writer taking columns.

  It is a ceiling, not a plan. The columnar arm here does exactly the work a
  real implementation would have to do — same coercion, same unknown-column
  check, same per-index rejection, nulls appended so columns stay aligned — but
  it is a flat function rather than a change threaded through the buffer, the
  committer and the writer's contract. What it answers is whether that change is
  worth designing.

  Both arms are checked against `Smolquery.Segments.Writer` before either is
  timed: same columns, same values, same order.

      mix run --no-start bench/columnar.exs

  `ROWS` (default 3062, the flush unit) and `REPS` (default 15) tune it.
  """

  alias Explorer.DataFrame
  alias Explorer.Series
  alias Smolquery.IngestService.Validator
  alias Smolquery.Schema
  alias Smolquery.Schema.Field

  def main do
    rows = env_int("ROWS", 3_062)
    reps = env_int("REPS", 15)
    schema = %{Bench.Otel.table_schema() | clustering: ["project_id", "timestamp"]}
    batch = Bench.Otel.rows(Bench.Otel.pool(), rows, 0)

    agree!(schema, batch)

    row_major = time(reps, fn -> row_major(schema, batch) end)
    columnar = time(reps, fn -> columnar(schema, batch) end)

    # The same values in either shape, to price the two deep copies the write
    # path makes of them: 3062 maps of 62 keys, or 62 lists of 3062 values. Same
    # leaves, two orders of magnitude fewer containers.
    {maps, _errors, _bytes} = Validator.validate(schema, batch)
    lists = stacks(schema, batch)

    copy_rows = time(reps, fn -> round_trip(maps) end)
    copy_columns = time(reps, fn -> round_trip(lists) end)

    report(rows, length(schema.fields), row_major, columnar, copy_rows, copy_columns)
  end

  # The write path deep-copies the batch twice — ingest to buffer, buffer to
  # committer — so both legs are walked.
  defp round_trip(term) do
    caller = self()

    second = spawn(fn -> receive do: (received -> send(caller, {:done, length(received)})) end)
    first = spawn(fn -> receive do: (received -> send(second, received)) end)

    send(first, term)

    receive do: ({:done, _count} -> :ok)
  end

  defp stacks(schema, batch) do
    names = MapSet.new(Schema.names(schema))
    empty = Enum.map(schema.fields, fn _field -> [] end)

    batch
    |> Enum.reduce(empty, fn row, stacks ->
      case coerce_row(schema, names, row) do
        {:ok, values} -> push(stacks, values)
        {:error, _messages} -> stacks
      end
    end)
    |> Enum.map(&:lists.reverse/1)
  end

  # ── the two arms ──────────────────────────────────────────────────────────

  # What runs today: validate into maps, then transpose the maps into columns.
  defp row_major(schema, batch) do
    {valid, errors, _bytes} = Validator.validate(schema, batch)

    {:ok, dtypes} = Schema.explorer_dtypes(schema)

    columns =
      Enum.map(dtypes, fn {name, dtype} ->
        {name, Series.from_list(Enum.map(valid, &Map.get(&1, name)), dtype: dtype)}
      end)

    {sort(DataFrame.new(columns), schema), errors}
  end

  # The same batch, same checks, one walk: a row's coerced values go straight
  # into the accumulators, one per column, and no row-shaped term is ever built.
  defp columnar(schema, batch) do
    {:ok, dtypes} = Schema.explorer_dtypes(schema)
    names = MapSet.new(Schema.names(schema))
    empty = Enum.map(schema.fields, fn _field -> [] end)

    {stacks, errors, _index} =
      Enum.reduce(batch, {empty, [], 0}, fn row, {stacks, errors, index} ->
        case coerce_row(schema, names, row) do
          {:ok, values} -> {push(stacks, values), errors, index + 1}
          {:error, messages} -> {stacks, [%{index: index, errors: messages} | errors], index + 1}
        end
      end)

    columns =
      stacks
      |> Enum.zip(dtypes)
      |> Enum.map(fn {stack, {name, dtype}} ->
        {name, Series.from_list(:lists.reverse(stack), dtype: dtype)}
      end)

    {sort(DataFrame.new(columns), schema), Enum.reverse(errors)}
  end

  # A rejected row must leave no trace in any column, so the whole row is coerced
  # into a flat list first and only pushed once it is known to be valid. The list
  # is per row and dies immediately; the map it replaces would have outlived the
  # batch.
  defp coerce_row(schema, names, row) when is_map(row) do
    {values, problems} =
      Enum.reduce(schema.fields, {[], []}, fn %Field{} = field, {values, problems} ->
        coerce_field(field, Map.get(row, field.name), values, problems)
      end)

    case unknown_columns(names, row) ++ Enum.reverse(problems) do
      [] -> {:ok, :lists.reverse(values)}
      problems -> {:error, Enum.map(problems, &%{message: &1})}
    end
  end

  defp coerce_row(_schema, _names, row),
    do: {:error, [%{message: "row must be a JSON object, got: #{inspect(row)}"}]}

  # A column a row does not carry still has to advance, or every column below it
  # in the batch shifts by one row. This is the one rule the row-major form gets
  # for free, from `Map.get/2` returning nil.
  defp coerce_field(%Field{nullable: true}, nil, values, problems), do: {[nil | values], problems}

  defp coerce_field(%Field{} = field, nil, values, problems),
    do: {values, ["column #{field.name} must not be null" | problems]}

  defp coerce_field(%Field{} = field, value, values, problems) do
    case Schema.value_from_json(field.type, value) do
      {:ok, coerced} -> {[coerced | values], problems}
      {:error, _reason} -> {values, ["column #{field.name} is invalid" | problems]}
    end
  end

  defp unknown_columns(names, row) do
    row
    |> Map.keys()
    |> Enum.reject(&MapSet.member?(names, &1))
    |> Enum.sort()
    |> Enum.map(&"unknown column: #{inspect(&1)}")
  end

  defp push(stacks, values), do: push(stacks, values, [])

  defp push([], [], acc), do: :lists.reverse(acc)

  defp push([stack | stacks], [value | values], acc),
    do: push(stacks, values, [[value | stack] | acc])

  defp sort(frame, schema) do
    case Schema.clustering_columns(schema) do
      [] ->
        frame

      key ->
        DataFrame.sort_with(frame, fn lf -> Enum.map(key, &lf[&1]) end, stable: true, nils: :last)
    end
  end

  # ── the arms have to agree, or the timing means nothing ───────────────────

  defp agree!(schema, batch) do
    sample = Enum.take(batch, 128)

    {row_frame, row_errors} = row_major(schema, sample)
    {column_frame, column_errors} = columnar(schema, sample)

    checks = [
      {"errors", row_errors, column_errors},
      {"columns", DataFrame.names(row_frame), DataFrame.names(column_frame)},
      {"row count", DataFrame.n_rows(row_frame), DataFrame.n_rows(column_frame)},
      {"dtypes", DataFrame.dtypes(row_frame), DataFrame.dtypes(column_frame)},
      {"values", dump(row_frame), dump(column_frame)}
    ]

    for {what, row, column} <- checks, row != column do
      raise """
      the columnar arm does not reproduce the row-major one — #{what}
        row-major #{inspect(row, limit: 6, printable_limit: 200)}
        columnar  #{inspect(column, limit: 6, printable_limit: 200)}
      """
    end

    :ok
  end

  defp dump(frame) do
    Map.new(DataFrame.names(frame), &{&1, Series.to_list(frame[&1])})
  end

  # ── measurement and output ────────────────────────────────────────────────

  defp time(reps, fun) do
    fun.()

    1..reps
    |> Enum.map(fn _rep -> elem(:timer.tc(fun), 0) / 1_000 end)
    |> Enum.sort()
    |> Enum.at(div(reps, 2))
  end

  @e2e 194.6

  defp report(rows, columns, row_major, columnar, copy_rows, copy_columns) do
    stage = row_major - columnar
    copies = copy_rows - copy_columns
    total = stage + copies

    IO.puts("""

    #{IO.ANSI.bright()}#{rows} rows × #{columns} columns, one flush#{IO.ANSI.reset()}

      decoded rows → sorted DataFrame
        row-major (today) #{fmt(row_major)} ms   validate into maps, then #{columns} passes to transpose
        columnar          #{fmt(columnar)} ms   one pass, values pushed to their column

      two deep copies (ingest → buffer → committer)
        row-major (today) #{fmt(copy_rows)} ms   #{rows} maps of #{columns} keys
        columnar          #{fmt(copy_columns)} ms   #{columns} lists of #{rows} values

      saved, stage        #{fmt(stage)} ms   #{pct(stage, row_major)} of it
      saved, copies       #{fmt(copies)} ms   #{pct(copies, copy_rows)} of them
      #{IO.ANSI.bright()}saved, together     #{fmt(total)} ms   #{pct(total, @e2e)} of a #{fmt(@e2e)} ms end-to-end POST#{IO.ANSI.reset()}

    A ceiling for one structural change, not a plan: the arms here are flat
    functions, while the real one threads a column-major batch through the
    validator's contract, the buffer's accumulator and byte accounting, the
    committer, and `Writer.write/3`.
    """)
  end

  defp fmt(ms), do: String.pad_leading(:erlang.float_to_binary(ms, decimals: 1), 6)

  defp pct(part, whole), do: "#{:erlang.float_to_binary(part / whole * 100, decimals: 1)}%"

  defp env_int(name, default) do
    case System.get_env(name) do
      nil -> default
      value -> String.to_integer(value)
    end
  end
end

Bench.Columnar.main()
