defmodule SmolqueryVictoriaMetrics.MetricsQL.Ast do
  @moduledoc """
  The expression tree `SmolqueryVictoriaMetrics.MetricsQL.parse/1` answers
  (PL-70, T-563): plain structs, one per node kind, shaped after the exported
  types of VictoriaMetrics' `metricsql` v0.87.4 so that the evaluator ported
  from VictoriaMetrics reads the same fields its Go original reads.

  | node                    | MetricsQL                                        |
  |-------------------------|--------------------------------------------------|
  | `Ast.Number`            | `1`, `1.5e3`, `0x1f`, `12Ki`, `Inf`, `NaN`        |
  | `Ast.StringLiteral`     | `"a"`, `'a'`, `` `a` ``, `"a" + "b"`             |
  | `Ast.Duration`          | `5m`, `1h30m`, `2i`; also the window, step and offset of a rollup |
  | `Ast.MetricExpr`        | `m`, `m{a="b"}`, `{a="1" or b="2"}`, `{}`         |
  | `Ast.RollupExpr`        | `m[5m]`, `m[5m:1m]`, `q[5m:]`, `m offset 5m`, `m @ end()` |
  | `Ast.FuncExpr`          | `rate(m[5m]) keep_metric_names`, `label_replace(...)` |
  | `Ast.AggrFuncExpr`      | `sum(m) by (a) limit 10`, `topk(3, m)`            |
  | `Ast.BinaryOpExpr`      | `a + on(x) group_left(y) b`, `a > bool 1`, `a default 0` |
  | `Ast.ParensExpr`        | `(a, b)` and `()`: a union of series             |

  Where the tree differs from what was typed, it is VictoriaMetrics' own
  normalisation, done in the same places:

    * Parentheses around one expression leave no node; they only decide how
      operators group. Parentheses around none or several are a
      `ParensExpr`, VictoriaMetrics' `union()` without the name.
    * Unary minus is `0 - x` (`BinaryOpExpr` with a `Number` of `0` on the
      left), and it binds like the binary operator: `-a ^ 2` is `0 - (a ^ 2)`.
      Unary plus leaves no node.
    * A metric name is the first filter of its filter set, `__name__` with
      `op: :eq`, wherever it was written: `m{a="b"}`, `{"m", a="b"}` and
      `{a="b", __name__="m"}` are the same tree. A name one `or` branch gives
      is given to the branches that name none.
    * `m[$__interval]` has no window: VictoriaMetrics reads Grafana's interval
      there as "the step", which is what an omitted window already means.
      Anywhere else `$__interval` and `$__rate_interval` are the duration `1i`.
    * No constant folding: `1 + 2` stays a `BinaryOpExpr`. Evaluating
      constants is the evaluator's, with the IEEE rules it needs anyway.

  Spellings are kept where VictoriaMetrics prints them back: a number's and a
  duration's `text`, a function's name as typed (`RATE(m)` stays `RATE`; the
  function table is case-insensitive). An aggregate's name and every keyword
  are lower-cased. Identifiers are unescaped: `foo\\-bar` is the name
  `foo-bar`.
  """

  alias __MODULE__.AggrFuncExpr
  alias __MODULE__.BinaryOpExpr
  alias __MODULE__.Duration
  alias __MODULE__.FuncExpr
  alias __MODULE__.MetricExpr
  alias __MODULE__.Number
  alias __MODULE__.ParensExpr
  alias __MODULE__.RollupExpr
  alias __MODULE__.StringLiteral

  @typedoc "Any MetricsQL expression."
  @type expr ::
          Number.t()
          | StringLiteral.t()
          | Duration.t()
          | MetricExpr.t()
          | RollupExpr.t()
          | FuncExpr.t()
          | AggrFuncExpr.t()
          | BinaryOpExpr.t()
          | ParensExpr.t()

  defmodule Number do
    @moduledoc """
    A number literal, always non-negative: a minus sign is `0 - x`.

    `value` is the number's float, or `:inf` and `:nan`, which Elixir floats
    cannot hold; a literal too large for a double is `:inf`, as Go's
    `ParseFloat` makes it. `:neg_inf` only occurs as the fill value of
    `fill(-inf)`, the one place a sign is part of a number. Multiplier
    suffixes are applied (`12Ki` is `12288.0`, `3M` is `3.0e6`); `text` is the
    literal as typed, which is what `MetricsQL.to_string/1` prints.
    """
    @enforce_keys [:value, :text]
    defstruct [:value, :text]

    @type value :: float() | :inf | :neg_inf | :nan
    @type t :: %__MODULE__{value: value(), text: String.t()}
  end

  defmodule StringLiteral do
    @moduledoc """
    A string literal, unquoted and unescaped. Adjacent literals joined by `+`
    are one literal, as VictoriaMetrics joins them while parsing.
    """
    @enforce_keys [:value]
    defstruct [:value]

    @type t :: %__MODULE__{value: binary()}
  end

  defmodule Duration do
    @moduledoc """
    A duration: `text` as typed, and its value split into a fixed part, `ms`,
    and a part in steps, `steps` (`1h2i` is `ms: 3_600_000.0, steps: 2.0`), so
    that `SmolqueryVictoriaMetrics.MetricsQL.duration_to_ms/2` can resolve it
    against any step. A bare number in a duration's place is seconds. An
    offset may be negative; a window or a step never is.

    As an expression (`rate(m[1h]) / 1h`) a duration is its value in seconds.
    """
    @enforce_keys [:text, :ms, :steps]
    defstruct [:text, :ms, :steps]

    @type t :: %__MODULE__{text: String.t(), ms: number(), steps: number()}
  end

  defmodule LabelFilter do
    @moduledoc """
    One label matcher: `name="value"` (`:eq`), `!=` (`:neq`), `=~` (`:re`) or
    `!~` (`:nre`). A regular expression is anchored at both ends when it is
    evaluated, as in PromQL, and has already compiled once here.
    """
    @enforce_keys [:name, :op, :value]
    defstruct [:name, :op, :value]

    @type op :: :eq | :neq | :re | :nre
    @type t :: %__MODULE__{name: String.t(), op: op(), value: binary()}
  end

  defmodule MetricExpr do
    @moduledoc """
    A series selector. `filter_sets` is a list of alternatives — MetricsQL's
    `{a="1" or b="2"}` — and a series matches when it matches every filter of
    at least one set. A plain selector has one set. The metric name, when
    there is one, is the first filter of each set (`__name__`, `:eq`). `{}`
    has no sets.
    """
    @enforce_keys [:filter_sets]
    defstruct [:filter_sets]

    @type t :: %__MODULE__{
            filter_sets: [[SmolqueryVictoriaMetrics.MetricsQL.Ast.LabelFilter.t()]]
          }
  end

  defmodule RollupExpr do
    @moduledoc """
    An expression with a window, a step, an offset or an `@`, in any
    combination of them VictoriaMetrics accepts.

      * `window` — `m[5m]`; `nil` when omitted (`m[:1m]`, `m offset 1h`), which
        the evaluator reads as the step.
      * `step` — the subquery step, `q[5m:1m]`.
      * `inherit_step` — `q[5m:]`: a subquery at the query's own step.
      * `offset` — `m offset 5m`, possibly negative.
      * `at` — the expression after `@`: a number, `start()`, `end()`, or any
        expression without a rollup suffix of its own.

    `expr` is a selector for a plain range vector and anything else for a
    subquery: MetricsQL accepts a window on any expression, `rate(m)[5m]`,
    and evaluates it as a subquery at the step. A subquery of a range vector,
    `(m[5m])[10m:1m]`, is refused while parsing.
    """
    @enforce_keys [:expr]
    defstruct [:expr, :window, :step, :offset, :at, inherit_step: false]

    alias SmolqueryVictoriaMetrics.MetricsQL.Ast

    @type t :: %__MODULE__{
            expr: Ast.expr(),
            window: Ast.Duration.t() | nil,
            step: Ast.Duration.t() | nil,
            inherit_step: boolean(),
            offset: Ast.Duration.t() | nil,
            at: Ast.expr() | nil
          }
  end

  defmodule FuncExpr do
    @moduledoc """
    A rollup or transform function call. `name` is spelled as typed and
    matched case-insensitively (`MetricsQL.function_kind/1`);
    `keep_metric_names` is MetricsQL's modifier that keeps `__name__` on the
    result.
    """
    @enforce_keys [:name, :args]
    defstruct [:name, :args, keep_metric_names: false]

    @type t :: %__MODULE__{
            name: String.t(),
            args: [SmolqueryVictoriaMetrics.MetricsQL.Ast.expr()],
            keep_metric_names: boolean()
          }
  end

  defmodule Modifier do
    @moduledoc """
    A label list modifier: `by`/`without` on an aggregate, `on`/`ignoring` and
    `group_left`/`group_right` on a binary operator. `labels` is `:all` only
    for `group_left(*)` and `group_right(*)`, which copy every label of the
    one side.
    """
    @enforce_keys [:op, :labels]
    defstruct [:op, :labels]

    @type op :: :by | :without | :on | :ignoring | :group_left | :group_right
    @type t :: %__MODULE__{op: op(), labels: [String.t()] | :all}
  end

  defmodule AggrFuncExpr do
    @moduledoc """
    An aggregation. `args` holds the parameters before the series argument
    (`topk(3, m)`, `quantile(0.9, m)`, `count_values("le", m)`); `modifier` is
    `by`/`without` or `nil`; `limit` is MetricsQL's `limit N` on the number of
    output series, or `nil`.
    """
    @enforce_keys [:name, :args]
    defstruct [:name, :args, :modifier, :limit]

    alias SmolqueryVictoriaMetrics.MetricsQL.Ast

    @type t :: %__MODULE__{
            name: String.t(),
            args: [Ast.expr()],
            modifier: Ast.Modifier.t() | nil,
            limit: pos_integer() | nil
          }
  end

  defmodule BinaryOpExpr do
    @moduledoc """
    A binary operation.

      * `op` — `:^`, then `:*`, `:/`, `:%`, `:atan2`, then `:+`, `:-`, then
        `:==`, `:!=`, `:>`, `:<`, `:>=`, `:<=`, then `:and`, `:unless`, then
        `:or`, then MetricsQL's `:if`, `:ifnot`, and last `:default`, from the
        tightest binding to the loosest; `^` alone is right-associative.
      * `bool` — the comparison's `bool` modifier.
      * `group_modifier` — `on(...)` or `ignoring(...)`, or `nil`.
      * `join_modifier` — `group_left(...)` or `group_right(...)`, or `nil`;
        `join_prefix` is MetricsQL's `prefix "p"` after it, prepended to the
        names of the labels it copies.
      * `fill_left`, `fill_right` — MetricsQL's `fill(v)`, `fill_left(v)` and
        `fill_right(v)`: the value a missing series on that side stands in with.
      * `keep_metric_names` — `(a + b) keep_metric_names`.
    """
    @enforce_keys [:op, :left, :right]
    defstruct [
      :op,
      :left,
      :right,
      :group_modifier,
      :join_modifier,
      :join_prefix,
      :fill_left,
      :fill_right,
      bool: false,
      keep_metric_names: false
    ]

    alias SmolqueryVictoriaMetrics.MetricsQL.Ast

    @type op ::
            :^
            | :*
            | :/
            | :%
            | :atan2
            | :+
            | :-
            | :==
            | :!=
            | :>
            | :<
            | :>=
            | :<=
            | :and
            | :unless
            | :or
            | :if
            | :ifnot
            | :default
    @type t :: %__MODULE__{
            op: op(),
            left: Ast.expr(),
            right: Ast.expr(),
            bool: boolean(),
            group_modifier: Ast.Modifier.t() | nil,
            join_modifier: Ast.Modifier.t() | nil,
            join_prefix: binary() | nil,
            fill_left: Ast.Number.t() | nil,
            fill_right: Ast.Number.t() | nil,
            keep_metric_names: boolean()
          }
  end

  defmodule ParensExpr do
    @moduledoc """
    `(a, b, ...)` or `()`: the union of the series of every expression, which
    VictoriaMetrics evaluates as `union(a, b, ...)`. Parentheses around exactly
    one expression are never a `ParensExpr` in a parsed tree.
    """
    @enforce_keys [:exprs]
    defstruct [:exprs]

    @type t :: %__MODULE__{exprs: [SmolqueryVictoriaMetrics.MetricsQL.Ast.expr()]}
  end
end
