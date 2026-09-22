defmodule SmolqueryVictoriaMetrics.Samples do
  @moduledoc """
  The SQL that reads a MetricsQL selector's series and raw samples from the
  edge's table, through the query service (PL-70, T-564).

  Rollups run in Elixir over raw samples (PL-70 D3), so a selector is read
  twice, over the same time range and under the same predicate:

      SELECT series, name, labels FROM metrics.samples
      WHERE <predicate> GROUP BY series, name, labels LIMIT max_series + 1

      SELECT series, epoch_ms(ts) AS ts, value FROM metrics.samples
      WHERE <predicate> ORDER BY series, ts LIMIT max_samples + 1

  The first names each series once; the second carries only the `series`
  fingerprint beside each sample, and its rows arrive in series and time
  order, so they are grouped in one pass and never sorted again here. One
  row past a ceiling refuses the query with `{:too_many_series, max}` or
  `{:too_many_samples, max}`. The samples query raises its job's result
  budget to its own ceiling (`result_max_rows:`), since the query service's
  default is sized for a page of API results.

  ## The predicate, and why it prunes

  A selector `m{job="api", code=~"5.."}` over `[from, to]` is

      name = $1 AND ts BETWEEN $2 AND $3
        AND labels[$4] = $5 AND regexp_full_match(coalesce(labels[$6], ''), $7)

  `name = $1` and `ts BETWEEN $2 AND $3` are top-level conjuncts of the
  `WHERE`, in that order, which is the shape `Smolquery.QueryService.Pruner`
  reads: a hot micro-segment whose `name` range or `ts` range cannot hold a
  match is never opened, so a query for one metric over one hour reads that
  metric's hour. The sealed tier prunes on the same predicate from DuckLake's
  file statistics. The table is clustered by `name, ts` for the same reason.

  Each matcher compiles as PromQL defines it, with a label missing from a
  series read as the empty string:

  | matcher | SQL |
  |---|---|
  | `__name__="m"` | `name = $n` |
  | `__name__!="m"` | `name <> $n` |
  | `__name__=~"re"` | `regexp_full_match(name, $n)` |
  | `__name__!~"re"` | `NOT regexp_full_match(name, $n)` |
  | `k="v"` | `labels[$k] = $v` |
  | `k=""` | `coalesce(labels[$k], '') = $v` |
  | `k!="v"` | `coalesce(labels[$k], '') <> $v` |
  | `k=~"re"` | `regexp_full_match(coalesce(labels[$k], ''), $v)` |
  | `k!~"re"` | `NOT regexp_full_match(coalesce(labels[$k], ''), $v)` |

  `labels[$k]` on a `MAP(VARCHAR, VARCHAR)` is the value, or `NULL` for a
  key the map lacks, in DuckDB 1.5.3. PromQL's regular expressions are RE2
  and anchored at both ends, and so is DuckDB's `regexp_full_match`, so an
  expression passes through unchanged. Every value, label names included, is
  a bound parameter; nothing a client sends is written into the SQL. The
  time bounds are bound as `NaiveDateTime`s at microsecond precision.

  MetricsQL's `{a="1" or b="2"}` is several filter sets, any of which a
  series may match. When every set names the same metric with `=`, that
  `name = $1` stays a top-level conjunct and only the rest is an `OR`, so
  the query still prunes by name. When the sets name different metrics, or
  any of them names none, the name sits inside the `OR`, which the pruner
  does not read: such a query prunes by time only.

  A selector every matcher of which matches the empty string, `{}`
  included, would select every series in the table; it is refused before
  any SQL runs, as Prometheus refuses it.

  ## A table that does not exist yet

  The edge creates its table on the first write, so a query can arrive
  before there is one. VictoriaMetrics answers an unknown metric with an
  empty result, and a missing table is answered the same way: no series.
  """

  alias Explorer.DataFrame
  alias Smolquery.Engine.Frame
  alias Smolquery.Identifier
  alias Smolquery.QueryService.Client
  alias Smolquery.QueryService.Job
  alias SmolqueryVictoriaMetrics.MetricsQL.Ast.LabelFilter
  alias SmolqueryVictoriaMetrics.MetricsQL.Ast.MetricExpr
  alias SmolqueryVictoriaMetrics.Runtime
  alias SmolqueryVictoriaMetrics.Write

  @empty_selector "vector selector must contain at least one non-empty matcher"

  @typedoc "A series' metric name and its other labels."
  @type series_info :: %{name: String.t(), labels: %{String.t() => String.t()}}

  @typedoc "A series as a rollup reads it: every label, `__name__` included, and its samples."
  @type series :: %{
          labels: %{String.t() => String.t()},
          timestamps: [integer()],
          values: [float()]
        }

  @type reason ::
          {:empty_selector, String.t()}
          | {:too_many_series, pos_integer()}
          | {:too_many_samples, pos_integer()}
          | {:invalid_time, integer()}
          | {:job, term()}
          | :cancelled
          | term()

  @doc """
  The `WHERE` predicate and its parameters for `expr` over `[from_ms, to_ms]`,
  numbered from `$1`.
  """
  @spec where(MetricExpr.t(), integer(), integer()) ::
          {:ok, String.t(), [term()]} | {:error, reason()}
  def where(%MetricExpr{filter_sets: sets}, from_ms, to_ms) do
    with :ok <- selective(sets),
         {:ok, from} <- time(max(from_ms, 0)),
         {:ok, to} <- time(max(to_ms, 0)) do
      {name, sets} = shared_name(sets)
      {conjuncts, params} = name_conjunct(name)

      {between, params} =
        add(params, "ts BETWEEN #{param(params, 1)} AND #{param(params, 2)}", [from, to])

      {rest, params} = alternatives(sets, params)
      {:ok, Enum.join(conjuncts ++ [between | rest], " AND "), params}
    end
  end

  @doc """
  The series query for `expr` over `range`, and its parameters.
  """
  @spec series_query(Runtime.t(), MetricExpr.t(), {integer(), integer()}) ::
          {:ok, String.t(), [term()]} | {:error, reason()}
  def series_query(%Runtime{} = runtime, %MetricExpr{} = expr, {from_ms, to_ms}) do
    with {:ok, predicate, params} <- where(expr, from_ms, to_ms) do
      {:ok,
       "SELECT series, name, labels FROM #{table(runtime)} WHERE #{predicate} " <>
         "GROUP BY series, name, labels LIMIT #{runtime.max_series + 1}", params}
    end
  end

  @doc """
  The samples query for `expr` over `range`, and its parameters.
  """
  @spec samples_query(Runtime.t(), MetricExpr.t(), {integer(), integer()}) ::
          {:ok, String.t(), [term()]} | {:error, reason()}
  def samples_query(%Runtime{} = runtime, %MetricExpr{} = expr, {from_ms, to_ms}) do
    with {:ok, predicate, params} <- where(expr, from_ms, to_ms) do
      {:ok,
       "SELECT series, epoch_ms(ts) AS ts, value FROM #{table(runtime)} " <>
         "WHERE #{predicate} ORDER BY series, ts LIMIT #{runtime.max_samples + 1}", params}
    end
  end

  @doc """
  The series `expr` matches with a sample in `[from_ms, to_ms]`, keyed by
  their `series` fingerprint; `{:error, {:too_many_series, max}}` past the
  runtime's `max_series`.
  """
  @spec series(Runtime.t(), MetricExpr.t(), {integer(), integer()}, keyword()) ::
          {:ok, %{integer() => series_info()}} | {:error, reason()}
  def series(%Runtime{} = runtime, %MetricExpr{} = expr, range, opts \\ []) do
    max = runtime.max_series

    with {:ok, sql, params} <- series_query(runtime, expr, range),
         {:ok, frame} <- run(runtime, sql, params, opts) do
      rows = frame_rows(frame)

      if length(rows) > max,
        do: {:error, {:too_many_series, max}},
        else:
          {:ok,
           Map.new(rows, fn row ->
             {row["series"], %{name: row["name"], labels: row["labels"] || %{}}}
           end)}
    end
  end

  @doc """
  The samples of every series `expr` matches in `[from_ms, to_ms]`, keyed by
  their `series` fingerprint, each as its timestamps and values in time
  order; `{:error, {:too_many_samples, max}}` past the runtime's
  `max_samples`.
  """
  @spec fetch(Runtime.t(), MetricExpr.t(), {integer(), integer()}, keyword()) ::
          {:ok, %{integer() => {[integer()], [float()]}}} | {:error, reason()}
  def fetch(%Runtime{} = runtime, %MetricExpr{} = expr, range, opts \\ []) do
    max = runtime.max_samples

    with {:ok, sql, params} <- samples_query(runtime, expr, range),
         {:ok, frame} <- run(runtime, sql, params, Keyword.put(opts, :result_max_rows, max + 1)) do
      cond do
        frame == nil -> {:ok, %{}}
        DataFrame.n_rows(frame) > max -> {:error, {:too_many_samples, max}}
        true -> {:ok, group(DataFrame.to_columns(frame))}
      end
    end
  end

  @doc """
  The series `expr` matches in `[from_ms, to_ms]` with their samples:
  `series/4` then `fetch/4`, joined on the fingerprint, in fingerprint
  order. A series whose samples arrived between the two queries is left out.
  """
  @spec select(Runtime.t(), MetricExpr.t(), {integer(), integer()}, keyword()) ::
          {:ok, [series()]} | {:error, reason()}
  def select(%Runtime{} = runtime, %MetricExpr{} = expr, range, opts \\ []) do
    with {:ok, infos} <- series(runtime, expr, range, opts),
         {:ok, samples} <- fetch_known(runtime, expr, range, opts, infos) do
      {:ok, joined(infos, samples)}
    end
  end

  defp joined(infos, samples) do
    for {id, {timestamps, values}} <- Enum.sort(samples),
        %{name: name, labels: labels} <- List.wrap(Map.get(infos, id)) do
      %{labels: Map.put(labels, "__name__", name), timestamps: timestamps, values: values}
    end
  end

  defp fetch_known(_runtime, _expr, _range, _opts, infos) when map_size(infos) == 0,
    do: {:ok, %{}}

  defp fetch_known(runtime, expr, range, opts, _infos), do: fetch(runtime, expr, range, opts)

  defp group(%{"series" => ids, "ts" => timestamps, "value" => values}),
    do: group(ids, timestamps, values, nil, [], [], %{})

  defp group([], [], [], nil, _ts, _vs, acc), do: acc

  defp group([], [], [], current, ts, vs, acc),
    do: Map.put(acc, current, {Enum.reverse(ts), Enum.reverse(vs)})

  defp group([id | ids], [t | timestamps], [v | values], id, ts, vs, acc),
    do: group(ids, timestamps, values, id, [t | ts], [v | vs], acc)

  defp group([id | ids], [t | timestamps], [v | values], nil, _ts, _vs, acc),
    do: group(ids, timestamps, values, id, [t], [v], acc)

  defp group([id | ids], [t | timestamps], [v | values], current, ts, vs, acc) do
    acc = Map.put(acc, current, {Enum.reverse(ts), Enum.reverse(vs)})
    group(ids, timestamps, values, id, [t], [v], acc)
  end

  defp frame_rows(nil), do: []
  defp frame_rows(frame), do: Frame.to_rows(frame)

  @doc """
  Runs `sql` with `params` through the runtime's query service: the result
  frame, `nil` when the table does not exist yet, or why the job failed.
  `opts` are `Smolquery.QueryService.Client.query/3`'s.
  """
  @spec run(Runtime.t(), String.t(), [term()], keyword()) ::
          {:ok, DataFrame.t() | nil} | {:error, reason()}
  def run(%Runtime{} = runtime, sql, params, opts) do
    case Client.query(runtime.query_name, sql, [params: params] ++ opts) do
      {:ok, %Job{state: :done}, frame} ->
        {:ok, frame}

      {:ok, %Job{state: :cancelled, error: :timeout}, _frame} ->
        {:error, :timeout}

      {:ok, %Job{state: :cancelled}, _frame} ->
        {:error, :cancelled}

      {:ok, %Job{error: {missing, _name}}, _frame}
      when missing in [:unknown_table, :unknown_dataset] ->
        {:ok, nil}

      {:ok, %Job{error: error}, _frame} ->
        {:error, {:job, error}}

      {:error, reason} ->
        {:error, reason}
    end
  end

  @doc """
  The edge's table as the SQL names it, each part quoted.
  """
  @spec table(Runtime.t()) :: String.t()
  def table(%Runtime{table: {dataset, table}}),
    do: Identifier.quote_name!(dataset) <> "." <> Identifier.quote_name!(table)

  @doc """
  A millisecond time as the `ts` bound the SQL binds, or
  `{:error, {:invalid_time, ms}}` past what a timestamp holds.
  """
  @spec time(integer()) :: {:ok, NaiveDateTime.t()} | {:error, {:invalid_time, integer()}}
  def time(ms) do
    case Write.timestamp(ms) do
      {:ok, timestamp} -> {:ok, timestamp}
      :error -> {:error, {:invalid_time, ms}}
    end
  end

  defp selective([]), do: {:error, {:empty_selector, @empty_selector}}

  defp selective(sets) do
    if Enum.all?(sets, fn filters -> Enum.any?(filters, &(not matches_empty?(&1))) end),
      do: :ok,
      else: {:error, {:empty_selector, @empty_selector}}
  end

  defp matches_empty?(%LabelFilter{op: :eq, value: value}), do: value == ""
  defp matches_empty?(%LabelFilter{op: :neq, value: value}), do: value != ""
  defp matches_empty?(%LabelFilter{op: :re, value: value}), do: regex_matches_empty?(value)
  defp matches_empty?(%LabelFilter{op: :nre, value: value}), do: not regex_matches_empty?(value)

  defp regex_matches_empty?(value) do
    case Regex.compile("\\A(?:" <> value <> ")\\z") do
      {:ok, regex} -> Regex.match?(regex, "")
      {:error, _reason} -> false
    end
  end

  defp shared_name(
         [[%LabelFilter{name: "__name__", op: :eq, value: name} | _rest] | _more] = sets
       ) do
    if Enum.all?(sets, &(name_of(&1) == name)),
      do: {name, Enum.map(sets, &tl/1)},
      else: {nil, sets}
  end

  defp shared_name(sets), do: {nil, sets}

  defp name_of([%LabelFilter{name: "__name__", op: :eq, value: name} | _rest]), do: name
  defp name_of(_filters), do: nil

  defp name_conjunct(nil), do: {[], []}
  defp name_conjunct(name), do: {["name = $1"], [name]}

  defp alternatives([filters], params), do: conjuncts(filters, params)

  defp alternatives(sets, params) do
    if Enum.any?(sets, &(&1 == [])) do
      {[], params}
    else
      {groups, params} =
        Enum.map_reduce(sets, params, fn filters, params ->
          {conjuncts, params} = conjuncts(filters, params)
          {["(", Enum.intersperse(conjuncts, " AND "), ")"], params}
        end)

      {[IO.iodata_to_binary(["(", Enum.intersperse(groups, " OR "), ")"])], params}
    end
  end

  defp conjuncts(filters, params), do: Enum.map_reduce(filters, params, &matcher/2)

  defp matcher(%LabelFilter{name: "__name__", op: op, value: value}, params),
    do: add(params, compare(op, "name", param(params, 1)), [value])

  defp matcher(%LabelFilter{name: label, op: :eq, value: value}, params) when value != "",
    do: add(params, "labels[#{param(params, 1)}] = #{param(params, 2)}", [label, value])

  defp matcher(%LabelFilter{name: label, op: op, value: value}, params) do
    extract = "coalesce(labels[#{param(params, 1)}], '')"
    add(params, compare(op, extract, param(params, 2)), [label, value])
  end

  defp compare(:eq, left, right), do: "#{left} = #{right}"
  defp compare(:neq, left, right), do: "#{left} <> #{right}"
  defp compare(:re, left, right), do: "regexp_full_match(#{left}, #{right})"
  defp compare(:nre, left, right), do: "NOT regexp_full_match(#{left}, #{right})"

  defp param(params, offset), do: "$#{length(params) + offset}"

  defp add(params, sql, values), do: {sql, params ++ values}
end
