defmodule SmolqueryVictoriaMetrics.Eval.Aggregate do
  @moduledoc """
  MetricsQL's aggregate functions over evaluated series, a port of
  VictoriaMetrics v1.152.0's `app/vmselect/promql/aggr.go` (PL-70, T-565).

  Series with no value at any point are left out first. The rest are
  grouped by their labels: `by (a, b)` keeps only `a` and `b`, `without
  (a, b)` drops them and `__name__`, and no modifier groups everything into
  one series with no labels; `__name__` stays only when `by` lists it.
  VictoriaMetrics' `limit N` keeps the first `N` groups and drops the rest.
  Each group is then reduced point by point, skipping points with no value.

    * one series per group, with the group's labels: `sum`, `sum2`, `avg`,
      `min`, `max`, `count`, `group`, `stddev`, `stdvar`, `geomean`,
      `distinct`, `mode`, `median`, `mad`, `quantile(phi, q)` and
      `quantiles("label", phi, ..., q)`, and `count_values("label", q)`, one
      series per distinct value;
    * the group's own series, labels untouched: `any` (its first series),
      `share`, `zscore`, `limitk(k, q)` (the `k` with the lowest XXH64 of
      their labels), `outliers_iqr`, `outliers_mad(tolerance, q)` and
      `outliersk(k, q)`;
    * `topk(k, q)` and `bottomk(k, q)` keep, at each point, the `k` series
      with the highest or lowest value there, VictoriaMetrics' per-point
      rule: a series can be in the answer at some points and not others.
      `topk_max`, `topk_min`, `topk_avg`, `topk_median`, `topk_last` and
      their `bottomk_` mirrors rank whole series by that statistic of their
      values instead, and take a third argument, `"label"` or
      `"label=value"`, naming a series that sums the rest.

  `histogram` is not ported: it answers `{:unsupported, _}`.
  """

  alias SmolqueryVictoriaMetrics.Eval.Args
  alias SmolqueryVictoriaMetrics.Eval.Series
  alias SmolqueryVictoriaMetrics.Eval.Value
  alias SmolqueryVictoriaMetrics.Eval.XXHash
  alias SmolqueryVictoriaMetrics.MetricsQL.Ast.AggrFuncExpr
  alias SmolqueryVictoriaMetrics.MetricsQL.Ast.Modifier

  @reducers ~w(sum sum2 geomean min max avg stddev stdvar count distinct mode group mad)
  @range_topk %{
    "topk_max" => {:max, false},
    "topk_min" => {:min, false},
    "topk_avg" => {:avg, false},
    "topk_median" => {:median, false},
    "topk_last" => {:last, false},
    "bottomk_max" => {:max, true},
    "bottomk_min" => {:min, true},
    "bottomk_avg" => {:avg, true},
    "bottomk_median" => {:median, true},
    "bottomk_last" => {:last, true}
  }

  @type reason :: Args.reason() | {:unsupported, String.t()}

  @doc "The aggregates this module computes."
  @spec functions() :: [String.t()]
  def functions,
    do:
      @reducers ++
        ~w(any share zscore count_values topk bottomk limitk quantile quantiles median
          outliers_iqr outliers_mad outliersk) ++ Map.keys(@range_topk)

  @doc """
  Aggregates `args`, each argument evaluated to its series, as `node` says;
  `grid` is the query's step grid.
  """
  @spec apply(AggrFuncExpr.t(), [[Series.t()]], [integer()]) ::
          {:ok, [Series.t()]} | {:error, reason()}
  def apply(%AggrFuncExpr{name: name} = node, args, grid) do
    cond do
      name in @reducers -> reduce(node, List.flatten(args))
      Map.has_key?(@range_topk, name) -> range_topk(node, args, Map.fetch!(@range_topk, name))
      name in functions() -> special(name, node, args, grid)
      true -> {:error, {:unsupported, "aggregate function #{name}()"}}
    end
  end

  defp special("any", node, args, _grid) do
    limit = min(node.limit || 0, 1)
    {:ok, node |> groups(List.flatten(args), true, limit) |> Enum.flat_map(&Enum.take(&1, 1))}
  end

  defp special("share", node, args, _grid),
    do: {:ok, each_group(node, List.flatten(args), &share/1)}

  defp special("zscore", node, args, _grid),
    do: {:ok, each_group(node, List.flatten(args), &zscore/1)}

  defp special("median", node, args, grid) do
    phis = Enum.map(grid, fn _t -> 0.5 end)
    {:ok, quantile_groups(node, List.flatten(args), phis)}
  end

  defp special("quantile", node, args, _grid) do
    with :ok <- Args.count(args, 2),
         {:ok, phis} <- Args.scalar(Enum.at(args, 0), 0),
         do: {:ok, quantile_groups(node, Enum.at(args, 1), phis)}
  end

  defp special("quantiles", node, args, _grid) do
    with :ok <- Args.at_least(args, 3),
         {:ok, label} <- Args.string(hd(args), 0),
         {:ok, phis} <- phis(Enum.slice(args, 1..-2//1)) do
      {:ok, quantiles(node, List.last(args), label, phis)}
    end
  end

  defp special("count_values", node, args, _grid) do
    with :ok <- Args.count(args, 2),
         {:ok, label} <- Args.string(Enum.at(args, 0), 0),
         do: {:ok, count_values(node, Enum.at(args, 1), label)}
  end

  defp special(name, node, args, _grid) when name in ["topk", "bottomk"] do
    with :ok <- Args.count(args, 2),
         {:ok, ks} <- Args.scalar(Enum.at(args, 0), 0) do
      {:ok, each_group(node, Enum.at(args, 1), &topk(&1, ks, name == "bottomk"))}
    end
  end

  defp special("limitk", node, args, _grid) do
    with :ok <- Args.count(args, 2),
         {:ok, limit} <- Args.integer(Enum.at(args, 0), 0) do
      {:ok, each_group(node, Enum.at(args, 1), &limitk(&1, max(limit, 0)))}
    end
  end

  defp special("outliers_iqr", node, args, _grid) do
    with :ok <- Args.count(args, 1), do: {:ok, each_group(node, hd(args), &outliers_iqr/1)}
  end

  defp special("outliers_mad", node, args, _grid) do
    with :ok <- Args.count(args, 2),
         {:ok, tolerances} <- Args.scalar(Enum.at(args, 0), 0) do
      {:ok, each_group(node, Enum.at(args, 1), &outliers_mad(&1, tolerances))}
    end
  end

  defp special("outliersk", node, args, _grid) do
    with :ok <- Args.count(args, 2),
         {:ok, ks} <- Args.scalar(Enum.at(args, 0), 0) do
      {:ok, each_group(node, Enum.at(args, 1), &outliersk(&1, ks, node.modifier))}
    end
  end

  defp phis(args) do
    args
    |> Enum.with_index(1)
    |> Args.collect(fn {arg, index} ->
      with {:ok, values} <- Args.scalar(arg, index), do: {:ok, List.first(values)}
    end)
  end

  @doc """
  The labels a series is grouped by under `modifier` (`removeGroupTags`).
  """
  @spec group_labels(Series.labels(), Modifier.t() | nil) :: Series.labels()
  def group_labels(_labels, nil), do: %{}
  def group_labels(labels, %Modifier{op: :by, labels: names}), do: Series.on(labels, names)

  def group_labels(labels, %Modifier{op: :without, labels: names}),
    do: labels |> Series.ignoring(names) |> Series.drop_name()

  defp groups(node, series, keep_original, limit \\ nil) do
    limit = if limit == nil, do: node.limit || 0, else: limit

    grouped =
      series
      |> Series.drop_empty()
      |> Series.group(&group_labels(&1.labels, node.modifier))

    grouped = if limit > 0, do: Enum.take(grouped, limit), else: grouped

    Enum.map(grouped, fn {key, members} ->
      if keep_original, do: members, else: Enum.map(members, &%{&1 | labels: key})
    end)
  end

  defp each_group(node, series, fun), do: node |> groups(series, true) |> Enum.flat_map(fun)

  defp reduce(%AggrFuncExpr{name: name} = node, series) do
    {:ok, node |> groups(series, false) |> Enum.map(&reduce_group(name, &1))}
  end

  defp reduce_group(name, [only]) when name in ~w(sum geomean min max avg), do: only

  defp reduce_group(name, [only]) when name in ~w(stddev stdvar),
    do: Series.map_values(only, &if(&1, do: 0.0))

  defp reduce_group("mad", [first | _rest] = group),
    do: Series.put_values(first, group |> columns() |> Enum.map(&mad/1))

  defp reduce_group(name, [first | _rest] = group) do
    fun = reducer(name)
    Series.put_values(first, group |> columns() |> Enum.map(fun))
  end

  defp columns(group), do: group |> Enum.map(&Series.values/1) |> Enum.zip_with(& &1)

  defp reducer("sum"), do: &Value.sum/1
  defp reducer("sum2"), do: fn column -> column |> Enum.map(&Value.mul(&1, &1)) |> Value.sum() end
  defp reducer("avg"), do: &average/1
  defp reducer("min"), do: &extreme(&1, fn v, best -> v < best end)
  defp reducer("max"), do: &extreme(&1, fn v, best -> v > best end)
  defp reducer("count"), do: &count/1
  defp reducer("group"), do: &if(Enum.any?(&1, fn v -> v != nil end), do: 1.0)
  defp reducer("stdvar"), do: &Value.stdvar/1
  defp reducer("stddev"), do: &Value.sqrt(Value.stdvar(&1))
  defp reducer("geomean"), do: &geomean/1
  defp reducer("distinct"), do: &distinct/1
  defp reducer("mode"), do: &present_mode/1

  defp present_mode(column), do: column |> Enum.reject(&is_nil/1) |> mode()

  defp average(column) do
    case Enum.reject(column, &is_nil/1) do
      [] -> nil
      present -> Value.divide(Value.sum(present), :erlang.float(length(present)))
    end
  end

  defp count(column) do
    case Enum.count(column, &(&1 != nil)) do
      0 -> nil
      n -> n * 1.0
    end
  end

  defp extreme([first | rest], better?) do
    Enum.reduce(rest, first, fn
      v, nil -> v
      nil, best -> best
      v, best -> if better?.(v, best), do: v, else: best
    end)
  end

  defp geomean(column) do
    case Enum.reject(column, &is_nil/1) do
      [] ->
        nil

      present ->
        present
        |> Enum.reduce(1.0, &Value.mul(&2, &1))
        |> Value.pow(1 / length(present))
    end
  end

  defp distinct(column) do
    case column |> Enum.reject(&is_nil/1) |> Enum.uniq_by(&if(&1 == 0.0, do: 0.0, else: &1)) do
      [] -> nil
      present -> :erlang.float(length(present))
    end
  end

  @doc """
  The most frequent value of `values`, which hold no `nil`; the smallest
  among ties, `nil` for none (`modeNoNaNs`).
  """
  @spec mode([float()]) :: Value.t()
  def mode([]), do: nil

  def mode(values) do
    values
    |> Enum.sort()
    |> Enum.chunk_by(& &1)
    |> Enum.reduce({nil, 0}, fn [v | _rest] = run, {best, best_count} ->
      if length(run) > best_count, do: {v, length(run)}, else: {best, best_count}
    end)
    |> elem(0)
  end

  defp mad(column) do
    median = Value.quantile(0.5, column)

    column
    |> Enum.reject(&is_nil/1)
    |> Enum.map(&abs(Value.sub(&1, median) || 0.0))
    |> then(&Value.quantile(0.5, &1))
  end

  defp quantile_groups(node, series, phis) do
    node
    |> groups(series, false)
    |> Enum.map(fn [first | _rest] = group ->
      values = Enum.zip_with(phis, columns(group), &Value.quantile(&1, &2))
      Series.put_values(first, values)
    end)
  end

  defp quantiles(node, series, label, phis) do
    node
    |> groups(series, false)
    |> Enum.flat_map(fn [first | _rest] = group ->
      columns = columns(group)

      Enum.map(phis, fn phi ->
        labels = Series.put_label(first.labels, label, Value.format_general(phi))
        %{Series.put_values(first, Enum.map(columns, &Value.quantile(phi, &1))) | labels: labels}
      end)
    end)
  end

  defp count_values(node, series, label) do
    modifier =
      case node.modifier do
        %Modifier{op: :without, labels: names} = m -> %{m | labels: names ++ [label]}
        %Modifier{op: :by, labels: names} = m -> %{m | labels: List.delete(names, label)}
        nil -> nil
      end

    %{node | modifier: modifier}
    |> groups(series, false)
    |> Enum.flat_map(fn [first | _rest] = group ->
      counts =
        group
        |> Enum.flat_map(fn one -> one |> Series.values() |> Enum.with_index() end)
        |> Enum.reject(fn {v, _index} -> v == nil end)
        |> Enum.group_by(fn {v, _index} -> v + 0.0 end, fn {_v, index} -> index end)

      counts
      |> Enum.sort_by(fn {v, _indexes} -> v end)
      |> Enum.map(fn {v, indexes} ->
        frequencies = Enum.frequencies(indexes)
        values = first.values |> Enum.with_index() |> Enum.map(&count_at(&1, frequencies))
        labels = Series.put_label(first.labels, label, Value.format(v))
        %Series{labels: labels, values: values}
      end)
    end)
  end

  defp count_at({{t, _v}, index}, frequencies) do
    case Map.fetch(frequencies, index) do
      {:ok, n} -> {t, n * 1.0}
      :error -> {t, nil}
    end
  end

  defp share(group) do
    sums =
      group
      |> columns()
      |> Enum.map(fn column ->
        column |> Enum.filter(&(&1 != nil and &1 >= 0)) |> Enum.reduce(0.0, &Value.add(&2, &1))
      end)

    Enum.map(group, fn one ->
      values =
        Enum.zip_with(Series.values(one), sums, fn
          v, sum when is_float(v) and v >= 0 -> Value.divide(v, sum)
          _v, _sum -> nil
        end)

      Series.put_values(one, values)
    end)
  end

  defp zscore(group) do
    stats =
      group
      |> columns()
      |> Enum.map(fn column ->
        case Value.stdvar(column) do
          nil -> nil
          variance -> {average(column), Value.sqrt(variance)}
        end
      end)

    Enum.map(group, fn one ->
      values =
        Enum.zip_with(Series.values(one), stats, fn
          nil, _stats -> nil
          v, nil -> v
          v, {avg, stddev} -> Value.divide(Value.sub(v, avg), stddev)
        end)

      Series.put_values(one, values)
    end)
  end

  defp topk(group, ks, bottom) do
    points = group |> hd() |> Map.fetch!(:values) |> length()
    less = if bottom, do: &greater_with_nans?/2, else: &less_with_nans?/2
    rows = Enum.map(group, fn one -> {one, one |> Series.values() |> List.to_tuple()} end)

    ks
    |> Enum.take(points)
    |> Stream.with_index()
    |> Enum.reduce(rows, fn {k, n}, rows ->
      rows = Enum.sort(rows, fn {_a, va}, {_b, vb} -> not less.(elem(vb, n), elem(va, n)) end)
      nan_first(rows, n, length(rows) - int_k(k, length(rows)))
    end)
    |> Enum.map(fn {one, values} -> Series.put_values(one, Tuple.to_list(values)) end)
    |> Series.drop_empty()
    |> Enum.reverse()
  end

  defp nan_first(rows, n, count) do
    {cleared, kept} = Enum.split(rows, max(count, 0))
    Enum.map(cleared, fn {one, values} -> {one, put_elem(values, n, nil)} end) ++ kept
  end

  defp int_k(nil, _max), do: 0

  defp int_k(k, max) do
    case Args.to_integer(k) do
      kn when kn < 0 -> 0
      kn -> min(kn, max)
    end
  end

  defp less_with_nans?(nil, b), do: b != nil
  defp less_with_nans?(_a, nil), do: false
  defp less_with_nans?(a, b), do: a < b

  defp greater_with_nans?(nil, b), do: b != nil
  defp greater_with_nans?(_a, nil), do: false
  defp greater_with_nans?(a, b), do: a > b

  defp range_topk(node, args, {stat, bottom}) do
    with :ok <- range_topk_arity(args),
         {:ok, ks} <- Args.scalar(Enum.at(args, 0), 0),
         {:ok, remaining} <- remaining_label(args) do
      fun = &statistic(stat, &1)

      {:ok,
       each_group(node, Enum.at(args, 1), &rank(&1, node.modifier, ks, remaining, fun, bottom))}
    end
  end

  defp range_topk_arity([_k, _series]), do: :ok
  defp range_topk_arity([_k, _series, _label]), do: :ok

  defp range_topk_arity([_k, _series, _label | _more] = args),
    do: Args.invalid("unexpected number of args; got #{length(args)}; want no more than 3")

  defp range_topk_arity(args),
    do: Args.invalid("unexpected number of args; got #{length(args)}; want at least 2")

  defp remaining_label([_k, _series, label]), do: Args.string(label, 2)
  defp remaining_label(_args), do: {:ok, ""}

  defp statistic(:max, values), do: first_extreme(values, &Kernel.>/2)
  defp statistic(:min, values), do: first_extreme(values, &Kernel.</2)
  defp statistic(:avg, values), do: average(values)
  defp statistic(:median, values), do: Value.quantile(0.5, values)
  defp statistic(:last, values), do: values |> Enum.reject(&is_nil/1) |> List.last()

  defp first_extreme(values, better?) do
    case Enum.reject(values, &is_nil/1) do
      [] -> nil
      [first | rest] -> Enum.reduce(rest, first, &if(better?.(&1, &2), do: &1, else: &2))
    end
  end

  defp rank(group, modifier, ks, remaining, fun, bottom) do
    less = if bottom, do: &greater_with_nans?/2, else: &less_with_nans?/2

    sorted =
      group
      |> Enum.map(&{&1, fun.(Series.values(&1))})
      |> Enum.sort(fn {_a, va}, {_b, vb} -> not less.(vb, va) end)
      |> Enum.map(&elem(&1, 0))

    count = length(sorted)
    rest = remaining_sum(sorted, modifier, ks, remaining)
    per_point = Enum.map(ks, &(count - int_k(&1, count)))

    kept =
      sorted
      |> Enum.with_index()
      |> Enum.map(fn {one, position} -> clear_below(one, position, per_point) end)

    (kept ++ rest)
    |> Series.drop_empty()
    |> Enum.reverse()
  end

  defp clear_below(one, position, per_point) do
    values = Enum.zip_with(Series.values(one), per_point, &if(position < &2, do: nil, else: &1))
    Series.put_values(one, values)
  end

  defp remaining_sum(_sorted, _modifier, _ks, ""), do: []
  defp remaining_sum([], _modifier, _ks, _label), do: []

  defp remaining_sum([first | _rest] = sorted, modifier, ks, label) do
    {name, value} =
      case String.split(label, "=", parts: 2) do
        [name, value] -> {name, value}
        [name] -> {name, name}
      end

    labels = first.labels |> group_labels(modifier) |> Series.put_label(name, value)
    count = length(sorted)

    values =
      sorted
      |> columns()
      |> Enum.zip_with(ks, fn column, k ->
        column |> Enum.take(count - int_k(k, count)) |> Value.sum()
      end)

    [%{Series.put_values(first, values) | labels: labels}]
  end

  defp limitk(group, limit) do
    group
    |> Enum.sort_by(&XXHash.hash(hash_input(&1.labels)))
    |> Enum.take(limit)
  end

  defp hash_input(labels) do
    tags = labels |> Series.drop_name() |> Enum.sort() |> Enum.map(fn {k, v} -> [k, v] end)
    IO.iodata_to_binary([Series.label(labels, "__name__"), tags])
  end

  defp outliers_iqr(group) do
    bounds =
      group
      |> columns()
      |> Enum.map(fn column ->
        q25 = Value.quantile(0.25, column)
        q75 = Value.quantile(0.75, column)
        iqr = Value.mul(1.5, Value.sub(q75, q25))
        {Value.sub(q25, iqr), Value.add(q75, iqr)}
      end)

    Enum.filter(group, fn one ->
      Enum.zip_with(Series.values(one), bounds, fn v, {lower, upper} ->
        Value.gt?(v, upper) or Value.lt?(v, lower)
      end)
      |> Enum.any?()
    end)
  end

  defp outliers_mad(group, tolerances) do
    medians = group |> columns() |> Enum.map(&Value.quantile(0.5, &1))
    mads = group |> columns() |> Enum.map(&mad/1) |> Enum.zip_with(tolerances, &Value.mul/2)

    Enum.filter(group, fn one ->
      [Series.values(one), medians, mads]
      |> Enum.zip_with(fn [v, median, mad] -> Value.gt?(abs_value(Value.sub(v, median)), mad) end)
      |> Enum.any?()
    end)
  end

  defp outliersk(group, ks, modifier) do
    medians = group |> columns() |> Enum.map(&Value.quantile(0.5, &1))

    fun = fn values ->
      values
      |> Enum.zip_with(medians, fn v, median ->
        d = Value.sub(v, median)
        Value.mul(d, d)
      end)
      |> Enum.reduce(0.0, &Value.add(&2, &1))
    end

    rank(group, modifier, ks, "", fun, false)
  end

  defp abs_value(nil), do: nil
  defp abs_value(v), do: abs(v)
end
