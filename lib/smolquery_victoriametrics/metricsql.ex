defmodule SmolqueryVictoriaMetrics.MetricsQL do
  @moduledoc """
  MetricsQL, VictoriaMetrics' superset of PromQL, parsed to an expression
  tree and printed back (PL-70, T-563).

  This is the front of the edge's read path: Grafana sends a MetricsQL
  expression to `/api/v1/query` or `/api/v1/query_range`, it is parsed here,
  and later layers evaluate the tree (`SmolqueryVictoriaMetrics.MetricsQL.Ast`
  documents every node). The grammar is a port of `github.com/VictoriaMetrics/metricsql`
  v0.87.4, the version VictoriaMetrics v1.152.0 builds with, and its
  `parser_test.go` is this module's test corpus. Where MetricsQL and PromQL
  differ, MetricsQL is what is implemented: a window may be omitted
  (`rate(m)`), a window may follow any expression (`rate(m)[5m]`), `1h` is a
  number of seconds, `offset` and `@` come in either order, selectors take
  `or`, aggregations take `limit`, and there are `keep_metric_names`,
  `default`, `if`, `ifnot`, `fill` and `group_left(*) prefix "p"`.

  `WITH` templates are not supported yet: a query that uses one is refused
  with `{:error, {:unsupported, "WITH templates"}}`. Nothing is evaluated
  while parsing, not even `1 + 1`; VictoriaMetrics folds constants in its
  parser, and here that is the evaluator's.

  Pure: no process, no application environment, nothing but the text.
  """

  alias SmolqueryVictoriaMetrics.MetricsQL.Ast
  alias SmolqueryVictoriaMetrics.MetricsQL.Durations
  alias SmolqueryVictoriaMetrics.MetricsQL.Functions
  alias SmolqueryVictoriaMetrics.MetricsQL.Parser
  alias SmolqueryVictoriaMetrics.MetricsQL.Printer

  @typedoc """
  Why a query was refused: a `:syntax` error names the token, its
  `line:column` and what was expected there; `:unsupported` names the
  feature; `:unknown_function` the name; `:arity` the function, the count it
  takes and the count it was given.
  """
  @type reason ::
          {:syntax, String.t()}
          | {:unsupported, String.t()}
          | {:unknown_function, String.t()}
          | {:arity, String.t()}

  @doc """
  Parses `query`.

      iex> {:ok, expr} = SmolqueryVictoriaMetrics.MetricsQL.parse("sum(rate(m[5m])) by (job)")
      iex> expr.name
      "sum"
  """
  @spec parse(String.t()) :: {:ok, Ast.expr()} | {:error, reason()}
  def parse(query) when is_binary(query), do: Parser.parse(query)

  @doc """
  Prints `expr` as canonical MetricsQL, which parses back to `expr`.

      iex> {:ok, expr} = SmolqueryVictoriaMetrics.MetricsQL.parse("SUM BY (job) (rate(m[5m])) > 2")
      iex> SmolqueryVictoriaMetrics.MetricsQL.to_string(expr)
      "sum(rate(m[5m])) by(job) > 2"
  """
  @spec to_string(Ast.expr()) :: String.t()
  def to_string(expr), do: Printer.to_string(expr)

  @doc """
  Whether `name` is a rollup, a transform or an aggregate function, or none,
  case-insensitively.

      iex> SmolqueryVictoriaMetrics.MetricsQL.function_kind("RATE")
      :rollup
  """
  @spec function_kind(String.t()) :: Functions.kind() | :unknown
  def function_kind(name) when is_binary(name), do: Functions.kind(name)

  @doc """
  The milliseconds of the duration `text` for a query whose step is
  `step_ms`: `5m`, `1.5h`, `1h30m`, `2i` (two steps), `-5m`, or a bare
  number of seconds. `$__interval` is one step.

      iex> SmolqueryVictoriaMetrics.MetricsQL.duration_to_ms("1h2i", 15_000)
      {:ok, 3_630_000}
  """
  @spec duration_to_ms(String.t(), integer()) :: {:ok, integer()} | {:error, String.t()}
  def duration_to_ms("$__interval", step_ms), do: Durations.to_ms("1i", step_ms)
  def duration_to_ms(text, step_ms) when is_binary(text), do: Durations.to_ms(text, step_ms)
end
