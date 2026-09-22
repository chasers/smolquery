defmodule SmolqueryVictoriaMetrics.MetricsQL.Functions do
  @moduledoc """
  Every function MetricsQL knows, its kind, and how many arguments it takes
  (PL-70, T-563).

  The names are those of `rollup.go`, `transform.go` and `aggr.go` in
  VictoriaMetrics' `metricsql` v0.87.4, which is what the parser there
  validates against. The argument counts are what VictoriaMetrics v1.152.0's
  evaluator enforces (`app/vmselect/promql`: `expectRollupArgsNum`,
  `expectTransformArgsNum` and the checks in each function), counted as
  written in the call, parameters included: `quantile_over_time(0.9, m[5m])`
  takes two. VictoriaMetrics finds a wrong count when it evaluates a query;
  here it is refused when the query is parsed, which is earlier and says the
  same thing. `label_set`, `label_copy`, `label_move` and `label_map` take
  their string arguments in pairs, which VictoriaMetrics also checks only
  when evaluating; the count here is only a minimum.

  Names are case-insensitive, as in VictoriaMetrics.
  """

  alias SmolqueryVictoriaMetrics.MetricsQL.Ast.AggrFuncExpr
  alias SmolqueryVictoriaMetrics.MetricsQL.Ast.BinaryOpExpr
  alias SmolqueryVictoriaMetrics.MetricsQL.Ast.FuncExpr
  alias SmolqueryVictoriaMetrics.MetricsQL.Ast.ParensExpr
  alias SmolqueryVictoriaMetrics.MetricsQL.Ast.RollupExpr

  @type kind :: :rollup | :transform | :aggregate
  @type range :: {non_neg_integer(), non_neg_integer() | :infinity}

  @table [
    {"absent_over_time", :rollup, 1, 1},
    {"aggr_over_time", :rollup, 2, 2},
    {"ascent_over_time", :rollup, 1, 1},
    {"avg_over_time", :rollup, 1, 1},
    {"changes", :rollup, 1, 1},
    {"changes_prometheus", :rollup, 1, 1},
    {"count_eq_over_time", :rollup, 2, 2},
    {"count_gt_over_time", :rollup, 2, 2},
    {"count_le_over_time", :rollup, 2, 2},
    {"count_ne_over_time", :rollup, 2, 2},
    {"count_over_time", :rollup, 1, 1},
    {"count_values_over_time", :rollup, 2, 2},
    {"decreases_over_time", :rollup, 1, 1},
    {"default_rollup", :rollup, 1, 1},
    {"delta", :rollup, 1, 1},
    {"delta_prometheus", :rollup, 1, 1},
    {"deriv", :rollup, 1, 1},
    {"deriv_fast", :rollup, 1, 1},
    {"descent_over_time", :rollup, 1, 1},
    {"distinct_over_time", :rollup, 1, 1},
    {"duration_over_time", :rollup, 2, 2},
    {"first_over_time", :rollup, 1, 1},
    {"geomean_over_time", :rollup, 1, 1},
    {"histogram_over_time", :rollup, 1, 1},
    {"hoeffding_bound_lower", :rollup, 2, 2},
    {"hoeffding_bound_upper", :rollup, 2, 2},
    {"holt_winters", :rollup, 3, 3},
    {"idelta", :rollup, 1, 1},
    {"ideriv", :rollup, 1, 1},
    {"increase", :rollup, 1, 1},
    {"increase_prometheus", :rollup, 1, 1},
    {"increase_pure", :rollup, 1, 1},
    {"increases_over_time", :rollup, 1, 1},
    {"integrate", :rollup, 1, 1},
    {"irate", :rollup, 1, 1},
    {"lag", :rollup, 1, 1},
    {"last_over_time", :rollup, 1, 1},
    {"lifetime", :rollup, 1, 1},
    {"mad_over_time", :rollup, 1, 1},
    {"max_over_time", :rollup, 1, 1},
    {"median_over_time", :rollup, 1, 1},
    {"min_over_time", :rollup, 1, 1},
    {"mode_over_time", :rollup, 1, 1},
    {"outlier_iqr_over_time", :rollup, 1, 1},
    {"predict_linear", :rollup, 2, 2},
    {"present_over_time", :rollup, 1, 1},
    {"quantile_over_time", :rollup, 2, 2},
    {"quantiles_over_time", :rollup, 3, :infinity},
    {"range_over_time", :rollup, 1, 1},
    {"rate", :rollup, 1, 1},
    {"rate_over_sum", :rollup, 1, 1},
    {"rate_prometheus", :rollup, 1, 1},
    {"resets", :rollup, 1, 1},
    {"rollup", :rollup, 1, 2},
    {"rollup_candlestick", :rollup, 1, 2},
    {"rollup_delta", :rollup, 1, 2},
    {"rollup_deriv", :rollup, 1, 2},
    {"rollup_increase", :rollup, 1, 2},
    {"rollup_rate", :rollup, 1, 2},
    {"rollup_scrape_interval", :rollup, 1, 2},
    {"scrape_interval", :rollup, 1, 1},
    {"share_eq_over_time", :rollup, 2, 2},
    {"share_gt_over_time", :rollup, 2, 2},
    {"share_le_over_time", :rollup, 2, 2},
    {"stale_samples_over_time", :rollup, 1, 1},
    {"stddev_over_time", :rollup, 1, 1},
    {"stdvar_over_time", :rollup, 1, 1},
    {"sum2_over_time", :rollup, 1, 1},
    {"sum_eq_over_time", :rollup, 2, 2},
    {"sum_gt_over_time", :rollup, 2, 2},
    {"sum_le_over_time", :rollup, 2, 2},
    {"sum_over_time", :rollup, 1, 1},
    {"tfirst_over_time", :rollup, 1, 1},
    {"timestamp", :rollup, 1, 1},
    {"timestamp_with_name", :rollup, 1, 1},
    {"tlast_change_over_time", :rollup, 1, 1},
    {"tlast_over_time", :rollup, 1, 1},
    {"tmax_over_time", :rollup, 1, 1},
    {"tmin_over_time", :rollup, 1, 1},
    {"zscore_over_time", :rollup, 1, 1},
    {"abs", :transform, 1, 1},
    {"absent", :transform, 1, 1},
    {"acos", :transform, 1, 1},
    {"acosh", :transform, 1, 1},
    {"asin", :transform, 1, 1},
    {"asinh", :transform, 1, 1},
    {"atan", :transform, 1, 1},
    {"atanh", :transform, 1, 1},
    {"bitmap_and", :transform, 2, 2},
    {"bitmap_or", :transform, 2, 2},
    {"bitmap_xor", :transform, 2, 2},
    {"buckets_limit", :transform, 2, 2},
    {"ceil", :transform, 1, 1},
    {"clamp", :transform, 3, 3},
    {"clamp_max", :transform, 2, 2},
    {"clamp_min", :transform, 2, 2},
    {"cos", :transform, 1, 1},
    {"cosh", :transform, 1, 1},
    {"day_of_month", :transform, 0, 1},
    {"day_of_week", :transform, 0, 1},
    {"day_of_year", :transform, 0, 1},
    {"days_in_month", :transform, 0, 1},
    {"deg", :transform, 1, 1},
    {"drop_common_labels", :transform, 1, :infinity},
    {"drop_empty_series", :transform, 1, 1},
    {"end", :transform, 0, 0},
    {"exp", :transform, 1, 1},
    {"floor", :transform, 1, 1},
    {"histogram_avg", :transform, 1, 1},
    {"histogram_fraction", :transform, 3, 3},
    {"histogram_quantile", :transform, 2, 3},
    {"histogram_quantiles", :transform, 3, :infinity},
    {"histogram_share", :transform, 2, 3},
    {"histogram_stddev", :transform, 1, 1},
    {"histogram_stdvar", :transform, 1, 1},
    {"hour", :transform, 0, 1},
    {"interpolate", :transform, 1, 1},
    {"keep_last_value", :transform, 1, 1},
    {"keep_next_value", :transform, 1, 1},
    {"label_copy", :transform, 1, :infinity},
    {"label_del", :transform, 1, :infinity},
    {"label_graphite_group", :transform, 2, :infinity},
    {"label_join", :transform, 3, :infinity},
    {"label_keep", :transform, 1, :infinity},
    {"label_lowercase", :transform, 2, :infinity},
    {"label_map", :transform, 2, :infinity},
    {"label_match", :transform, 3, 3},
    {"label_mismatch", :transform, 3, 3},
    {"label_move", :transform, 1, :infinity},
    {"label_replace", :transform, 5, 5},
    {"label_set", :transform, 1, :infinity},
    {"label_transform", :transform, 4, 4},
    {"label_uppercase", :transform, 2, :infinity},
    {"label_value", :transform, 2, 2},
    {"labels_equal", :transform, 3, :infinity},
    {"limit_offset", :transform, 3, 3},
    {"ln", :transform, 1, 1},
    {"log10", :transform, 1, 1},
    {"log2", :transform, 1, 1},
    {"minute", :transform, 0, 1},
    {"month", :transform, 0, 1},
    {"now", :transform, 0, 0},
    {"pi", :transform, 0, 0},
    {"prometheus_buckets", :transform, 1, 1},
    {"rad", :transform, 1, 1},
    {"rand", :transform, 0, 1},
    {"rand_exponential", :transform, 0, 1},
    {"rand_normal", :transform, 0, 1},
    {"range_avg", :transform, 1, 1},
    {"range_first", :transform, 1, 1},
    {"range_last", :transform, 1, 1},
    {"range_linear_regression", :transform, 1, 1},
    {"range_mad", :transform, 1, 1},
    {"range_max", :transform, 1, 1},
    {"range_min", :transform, 1, 1},
    {"range_normalize", :transform, 0, :infinity},
    {"range_quantile", :transform, 2, 2},
    {"range_stddev", :transform, 1, 1},
    {"range_stdvar", :transform, 1, 1},
    {"range_sum", :transform, 1, 1},
    {"range_trim_outliers", :transform, 2, 2},
    {"range_trim_spikes", :transform, 2, 2},
    {"range_trim_zscore", :transform, 2, 2},
    {"range_zscore", :transform, 1, 1},
    {"remove_resets", :transform, 1, 1},
    {"round", :transform, 1, 2},
    {"running_avg", :transform, 1, 1},
    {"running_max", :transform, 1, 1},
    {"running_min", :transform, 1, 1},
    {"running_sum", :transform, 1, 1},
    {"scalar", :transform, 1, 1},
    {"sgn", :transform, 1, 1},
    {"sin", :transform, 1, 1},
    {"sinh", :transform, 1, 1},
    {"smooth_exponential", :transform, 2, 2},
    {"sort", :transform, 1, 1},
    {"sort_by_label", :transform, 2, :infinity},
    {"sort_by_label_desc", :transform, 2, :infinity},
    {"sort_by_label_numeric", :transform, 2, :infinity},
    {"sort_by_label_numeric_desc", :transform, 2, :infinity},
    {"sort_desc", :transform, 1, 1},
    {"sqrt", :transform, 1, 1},
    {"start", :transform, 0, 0},
    {"step", :transform, 0, 0},
    {"tan", :transform, 1, 1},
    {"tanh", :transform, 1, 1},
    {"time", :transform, 0, 0},
    {"timezone_offset", :transform, 1, 1},
    {"union", :transform, 0, :infinity},
    {"vector", :transform, 1, 1},
    {"year", :transform, 0, 1},
    {"any", :aggregate, 1, :infinity},
    {"avg", :aggregate, 1, :infinity},
    {"bottomk", :aggregate, 2, 2},
    {"bottomk_avg", :aggregate, 2, 3},
    {"bottomk_last", :aggregate, 2, 3},
    {"bottomk_max", :aggregate, 2, 3},
    {"bottomk_median", :aggregate, 2, 3},
    {"bottomk_min", :aggregate, 2, 3},
    {"count", :aggregate, 1, :infinity},
    {"count_values", :aggregate, 2, 2},
    {"distinct", :aggregate, 1, :infinity},
    {"geomean", :aggregate, 1, :infinity},
    {"group", :aggregate, 1, :infinity},
    {"histogram", :aggregate, 1, :infinity},
    {"limitk", :aggregate, 2, 2},
    {"mad", :aggregate, 1, :infinity},
    {"max", :aggregate, 1, :infinity},
    {"median", :aggregate, 1, :infinity},
    {"min", :aggregate, 1, :infinity},
    {"mode", :aggregate, 1, :infinity},
    {"outliers_iqr", :aggregate, 1, 1},
    {"outliers_mad", :aggregate, 2, 2},
    {"outliersk", :aggregate, 2, 2},
    {"quantile", :aggregate, 2, 2},
    {"quantiles", :aggregate, 3, :infinity},
    {"share", :aggregate, 1, :infinity},
    {"stddev", :aggregate, 1, :infinity},
    {"stdvar", :aggregate, 1, :infinity},
    {"sum", :aggregate, 1, :infinity},
    {"sum2", :aggregate, 1, :infinity},
    {"topk", :aggregate, 2, 2},
    {"topk_avg", :aggregate, 2, 3},
    {"topk_last", :aggregate, 2, 3},
    {"topk_max", :aggregate, 2, 3},
    {"topk_median", :aggregate, 2, 3},
    {"topk_min", :aggregate, 2, 3},
    {"zscore", :aggregate, 1, :infinity}
  ]

  @functions Map.new(@table, fn {name, kind, min, max} -> {name, {kind, {min, max}}} end)

  @doc """
  The kind of the function `name`, or `:unknown`.
  """
  @spec kind(String.t()) :: kind() | :unknown
  def kind(name) do
    case Map.fetch(@functions, String.downcase(name)) do
      {:ok, {kind, _arity}} -> kind
      :error -> :unknown
    end
  end

  @doc """
  The least and the most arguments `name` takes, or `:error` for a name
  MetricsQL does not know.
  """
  @spec arity(String.t()) :: {:ok, range()} | :error
  def arity(name) do
    with {:ok, {_kind, arity}} <- Map.fetch(@functions, String.downcase(name)), do: {:ok, arity}
  end

  @doc """
  Every function name, in the order of the table: rollups, transforms, then
  aggregates, each alphabetically.
  """
  @spec names() :: [String.t()]
  def names, do: Enum.map(@table, &elem(&1, 0))

  @doc """
  Checks every call in `expr`, innermost first: an unknown name is
  `{:error, {:unknown_function, name}}`, and a count outside the function's
  range is `{:error, {:arity, message}}`.
  """
  @spec check(SmolqueryVictoriaMetrics.MetricsQL.Ast.expr()) ::
          :ok | {:error, {:unknown_function, String.t()} | {:arity, String.t()}}
  def check(%FuncExpr{name: name, args: args}) do
    with :ok <- check_all(args), do: count(name, length(args))
  end

  def check(%AggrFuncExpr{name: name, args: args}) do
    with :ok <- check_all(args), do: count(name, length(args))
  end

  def check(%BinaryOpExpr{left: left, right: right}), do: check_all([left, right])
  def check(%RollupExpr{expr: expr, at: nil}), do: check(expr)
  def check(%RollupExpr{expr: expr, at: at}), do: check_all([expr, at])
  def check(%ParensExpr{exprs: exprs}), do: check_all(exprs)
  def check(_leaf), do: :ok

  defp check_all(exprs) do
    Enum.reduce_while(exprs, :ok, fn expr, :ok ->
      case check(expr) do
        :ok -> {:cont, :ok}
        error -> {:halt, error}
      end
    end)
  end

  defp count(name, given) do
    case arity(name) do
      {:ok, {min, :infinity}} when given >= min -> :ok
      {:ok, {min, max}} when given in min..max//1 -> :ok
      {:ok, range} -> {:error, {:arity, "#{name}() takes #{describe(range)}; got #{given}"}}
      :error -> {:error, {:unknown_function, name}}
    end
  end

  defp describe({count, count}), do: plural(count)
  defp describe({min, :infinity}), do: "at least #{plural(min)}"
  defp describe({min, max}), do: "#{min} to #{max} args"

  defp plural(1), do: "1 arg"
  defp plural(count), do: "#{count} args"
end
