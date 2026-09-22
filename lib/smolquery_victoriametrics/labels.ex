defmodule SmolqueryVictoriaMetrics.Labels do
  @moduledoc """
  The SQL behind `/api/v1/labels` and `/api/v1/label/<name>/values`: the
  label names, or one label's values, of the samples in a time range,
  optionally narrowed by a selector (PL-70, T-566).

      SELECT DISTINCT unnest(list_append(map_keys(labels), '__name__')) AS label
      FROM metrics.samples WHERE <predicate> ORDER BY label LIMIT $limit

      SELECT DISTINCT name AS value
      FROM metrics.samples WHERE <predicate> ORDER BY value LIMIT $limit

      SELECT DISTINCT labels[$k] AS value
      FROM metrics.samples WHERE <predicate> AND labels[$k] IS NOT NULL
      ORDER BY value LIMIT $limit

  Every row has a metric name, so `__name__` is a label name exactly when
  any sample matched, which is why it is appended to each row's keys
  rather than asked for separately. `__name__`'s values are the `name`
  column; any other label's are its map entry, and a series without the
  label contributes nothing, as in VictoriaMetrics.

  The predicate is `SmolqueryVictoriaMetrics.Samples.where/3`'s when there
  is a selector, so `name = $1` and `ts BETWEEN $2 AND $3` stay top-level
  conjuncts and prune, and just the `ts BETWEEN $1 AND $2` bound when
  there is none, which prunes by time only.

  ## Cost and bounds

  There is no series index yet (PL-70, T-569): an answer costs a scan of
  every sample in the range the predicate keeps, proportional to the range,
  not to the answer. VictoriaMetrics' own defaults keep that range short: a
  request without `start` reads the 5 minutes before `end`. What comes back
  is bounded by `limit`, which, as VictoriaMetrics'
  `-search.maxTagKeys` and `-search.maxTagValues` have it, is at most
  100,000 and is that ceiling when missing, zero or negative. The rows
  come back sorted, so a limit keeps the first names in order. Each job is
  bounded by the query service, and by the request's `timeout`.
  """

  alias Explorer.DataFrame
  alias SmolqueryVictoriaMetrics.MetricsQL.Ast.MetricExpr
  alias SmolqueryVictoriaMetrics.Runtime
  alias SmolqueryVictoriaMetrics.Samples

  @max_limit 100_000

  @typedoc "A time range in milliseconds, both ends included."
  @type range :: {integer(), integer()}

  @doc """
  The rows an answer may hold for the `limit` a client asked for: that
  number, or `#{@max_limit}` when it is not positive or is larger.

      iex> SmolqueryVictoriaMetrics.Labels.limit(0)
      100000
      iex> SmolqueryVictoriaMetrics.Labels.limit(10)
      10
  """
  @spec limit(integer()) :: pos_integer()
  def limit(requested) when requested <= 0 or requested > @max_limit, do: @max_limit
  def limit(requested), do: requested

  @doc """
  The `WHERE` predicate for `selector` over `range`, or for the range alone
  when `selector` is `nil`, with its parameters.
  """
  @spec where(MetricExpr.t() | nil, range()) ::
          {:ok, String.t(), [term()]} | {:error, Samples.reason()}
  def where(nil, {from_ms, to_ms}) do
    with {:ok, from} <- Samples.time(max(from_ms, 0)),
         {:ok, to} <- Samples.time(max(to_ms, 0)) do
      {:ok, "ts BETWEEN $1 AND $2", [from, to]}
    end
  end

  def where(%MetricExpr{} = selector, {from_ms, to_ms}),
    do: Samples.where(selector, from_ms, to_ms)

  @doc """
  The label names query and its parameters.
  """
  @spec names_query(Runtime.t(), MetricExpr.t() | nil, range(), pos_integer()) ::
          {:ok, String.t(), [term()]} | {:error, Samples.reason()}
  def names_query(%Runtime{} = runtime, selector, range, limit) do
    with {:ok, predicate, params} <- where(selector, range) do
      {:ok,
       "SELECT DISTINCT unnest(list_append(map_keys(labels), '__name__')) AS label " <>
         "FROM #{Samples.table(runtime)} WHERE #{predicate} ORDER BY label LIMIT #{limit}",
       params}
    end
  end

  @doc """
  The query for the values of the label `name`, and its parameters.
  """
  @spec values_query(Runtime.t(), String.t(), MetricExpr.t() | nil, range(), pos_integer()) ::
          {:ok, String.t(), [term()]} | {:error, Samples.reason()}
  def values_query(%Runtime{} = runtime, "__name__", selector, range, limit) do
    with {:ok, predicate, params} <- where(selector, range) do
      {:ok,
       "SELECT DISTINCT name AS value FROM #{Samples.table(runtime)} " <>
         "WHERE #{predicate} ORDER BY value LIMIT #{limit}", params}
    end
  end

  def values_query(%Runtime{} = runtime, name, selector, range, limit) do
    with {:ok, predicate, params} <- where(selector, range) do
      key = "$#{length(params) + 1}"

      {:ok,
       "SELECT DISTINCT labels[#{key}] AS value FROM #{Samples.table(runtime)} " <>
         "WHERE #{predicate} AND labels[#{key}] IS NOT NULL ORDER BY value LIMIT #{limit}",
       params ++ [name]}
    end
  end

  @doc """
  The sorted label names of the samples `selector` matches in `range`, at
  most `limit`.
  """
  @spec names(Runtime.t(), MetricExpr.t() | nil, range(), pos_integer(), keyword()) ::
          {:ok, [String.t()]} | {:error, Samples.reason()}
  def names(%Runtime{} = runtime, selector, range, limit, opts \\ []) do
    with {:ok, sql, params} <- names_query(runtime, selector, range, limit) do
      column(runtime, sql, params, limit, opts, "label")
    end
  end

  @doc """
  The sorted values of the label `name` among the samples `selector`
  matches in `range`, at most `limit`.
  """
  @spec values(Runtime.t(), String.t(), MetricExpr.t() | nil, range(), pos_integer(), keyword()) ::
          {:ok, [String.t()]} | {:error, Samples.reason()}
  def values(%Runtime{} = runtime, name, selector, range, limit, opts \\ []) do
    with {:ok, sql, params} <- values_query(runtime, name, selector, range, limit) do
      column(runtime, sql, params, limit, opts, "value")
    end
  end

  defp column(runtime, sql, params, limit, opts, name) do
    case Samples.run(runtime, sql, params, Keyword.put(opts, :result_max_rows, limit)) do
      {:ok, nil} -> {:ok, []}
      {:ok, frame} -> {:ok, frame |> DataFrame.pull(name) |> Explorer.Series.to_list()}
      {:error, reason} -> {:error, reason}
    end
  end
end
