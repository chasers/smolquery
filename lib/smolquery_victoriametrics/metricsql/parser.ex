defmodule SmolqueryVictoriaMetrics.MetricsQL.Parser do
  @moduledoc """
  A recursive-descent parser from `SmolqueryVictoriaMetrics.MetricsQL.Lexer`
  tokens to a `SmolqueryVictoriaMetrics.MetricsQL.Ast` tree: a port of
  `parser.go` in VictoriaMetrics' `metricsql` v0.87.4, which is the PromQL
  grammar plus MetricsQL's extensions (PL-70, T-563).

      expr     = single { binop [bool] [on|ignoring (labels) [group_left|group_right [(labels)] [prefix "p"]]]
                          {fill(v) | fill_left(v) | fill_right(v)} single [keep_metric_names] }
      single   = primary [ "[" [window] [":" [step]] "]" ] [@ primary] [offset [-]duration] [@ primary]
      primary  = duration | string {+ string} | number | inf | nan | selector | call | aggregation
               | "(" [expr {, expr} [,]] ")" [keep_metric_names] | - single | + single

  ## Operators

  Operators group by precedence, into the trees VictoriaMetrics'
  `balanceBinaryOp` rebalances them to: `^` binds tightest and is
  right-associative; then `* / % atan2`; `+ -`; the comparisons;
  `and unless`; `or`; MetricsQL's `if ifnot`; and `default` loosest. Unary
  minus is `0 - x` at the precedence of `-`, and what binds tighter than it
  after its operand is its operand's, so `-a ^ 2` is `-(a ^ 2)` and
  `-a + b` is `(-a) + b`. The operators are read by precedence climbing, once
  each, so a chain of any length costs its length; rebalancing each new
  operator down the tree, as `balanceBinaryOp` does, costs the square.
  `bool` is only for a comparison, and `group_left`/`group_right` never for
  `and`, `or` or `unless`, nor a fill for any of those or `if`, `ifnot` or
  `default`. A fill takes one minus at most: `fill(-inf)`, not `fill(--1)`.

  ## Words

  Every keyword is also a metric name, and which one a word is depends on what
  follows it: `sum(x)` is an aggregation and `sum + 1` a metric; `offset` after
  an expression is a modifier and `offset` alone a metric; `a + (on)` is a
  metric where `a + on(x) b` is a modifier. An identifier followed by `(` is a
  call and must name a known function.

  ## Refused here

    * `WITH (...)` anywhere: `{:error, {:unsupported, "WITH templates"}}`.
    * A window or a subquery on a range vector, `(m[5m])[10m:1m]`.
    * `keep_metric_names` after an aggregation, a selector or a literal.
    * An aggregation `limit` past the int64 range, and a duration past what a
      double holds.
    * An unknown function, `{:unknown_function, name}`, and a wrong number of
      arguments, `{:arity, message}` (`SmolqueryVictoriaMetrics.MetricsQL.Functions`).
    * Brackets nested more than 1,000 deep: `{:syntax, "expression is nested
      too deeply at ..."}`, before anything is parsed.

  The built-in templates (`SmolqueryVictoriaMetrics.MetricsQL.Builtins`)
  are expanded once the whole expression is parsed, as VictoriaMetrics
  expands them, so a template is one operand.

  Every other error is `{:syntax, message}`, naming the token met, its
  `line:column`, and what was expected there.
  """

  alias SmolqueryVictoriaMetrics.MetricsQL.Ast.AggrFuncExpr
  alias SmolqueryVictoriaMetrics.MetricsQL.Ast.BinaryOpExpr
  alias SmolqueryVictoriaMetrics.MetricsQL.Ast.Duration
  alias SmolqueryVictoriaMetrics.MetricsQL.Ast.FuncExpr
  alias SmolqueryVictoriaMetrics.MetricsQL.Ast.Modifier
  alias SmolqueryVictoriaMetrics.MetricsQL.Ast.Number
  alias SmolqueryVictoriaMetrics.MetricsQL.Ast.ParensExpr
  alias SmolqueryVictoriaMetrics.MetricsQL.Ast.RollupExpr
  alias SmolqueryVictoriaMetrics.MetricsQL.Ast.StringLiteral
  alias SmolqueryVictoriaMetrics.MetricsQL.Builtins
  alias SmolqueryVictoriaMetrics.MetricsQL.Durations
  alias SmolqueryVictoriaMetrics.MetricsQL.Functions
  alias SmolqueryVictoriaMetrics.MetricsQL.Lexer
  alias SmolqueryVictoriaMetrics.MetricsQL.Literal
  alias SmolqueryVictoriaMetrics.MetricsQL.Selector

  @symbol_ops %{
    "+" => :+,
    "-" => :-,
    "*" => :*,
    "/" => :/,
    "%" => :%,
    "^" => :^,
    "==" => :==,
    "!=" => :!=,
    ">" => :>,
    "<" => :<,
    ">=" => :>=,
    "<=" => :<=
  }
  @word_ops %{
    "atan2" => :atan2,
    "and" => :and,
    "or" => :or,
    "unless" => :unless,
    "if" => :if,
    "ifnot" => :ifnot,
    "default" => :default
  }
  @priority %{
    default: -1,
    if: 0,
    ifnot: 0,
    or: 1,
    and: 2,
    unless: 2,
    ==: 3,
    !=: 3,
    <: 3,
    >: 3,
    <=: 3,
    >=: 3,
    +: 4,
    -: 4,
    *: 5,
    /: 5,
    %: 5,
    atan2: 5,
    ^: 6
  }
  @loosest -2
  @unary_minus 4
  @max_depth 1_000
  @max_int64 9_223_372_036_854_775_807
  @comparisons [:==, :!=, :>, :<, :>=, :<=]
  @logical_sets [:and, :or, :unless]
  @set_ops [:and, :or, :unless, :if, :ifnot, :default]
  @fills ["fill", "fill_left", "fill_right"]
  @metric_followers ["{", "[", ")", ",", "@"]
  @modifier_ops %{
    "on" => :on,
    "ignoring" => :ignoring,
    "group_left" => :group_left,
    "group_right" => :group_right,
    "by" => :by,
    "without" => :without
  }
  @keep_misplaced "keep_metric_names applies only to a function call or a binary operation"

  @type reason ::
          {:syntax, String.t()}
          | {:unsupported, String.t()}
          | {:unknown_function, String.t()}
          | {:arity, String.t()}

  @doc """
  Parses `query` to its expression tree.
  """
  @spec parse(String.t()) ::
          {:ok, SmolqueryVictoriaMetrics.MetricsQL.Ast.expr()} | {:error, reason()}
  def parse(query) when is_binary(query) do
    with {:ok, tokens} <- Lexer.tokenize(query),
         :ok <- shallow(tokens, 0),
         {:ok, expr, rest} <- expr(tokens),
         :ok <- finished(rest),
         {:ok, expr} <- expr |> unwrap() |> Builtins.expand_all(),
         :ok <- Functions.check(expr) do
      {:ok, expr}
    end
  end

  defp shallow([{:punct, open, _position} = token | rest], depth) when open in ["(", "[", "{"] do
    if depth == @max_depth,
      do: Lexer.error_at(token, "expression is nested too deeply"),
      else: shallow(rest, depth + 1)
  end

  defp shallow([{:punct, close, _position} | rest], depth) when close in [")", "]", "}"],
    do: shallow(rest, max(depth - 1, 0))

  defp shallow([{:eof, _text, _position}], _depth), do: :ok
  defp shallow([_token | rest], depth), do: shallow(rest, depth)

  defp finished([{:eof, _text, _position}]), do: :ok

  defp finished([token | _rest]) do
    if word(token) == "keep_metric_names",
      do: Lexer.error_at(token, @keep_misplaced),
      else: Lexer.unexpected(token, "an operator or the end of the query")
  end

  defp expr(tokens), do: operand(tokens, @loosest)

  defp operand(tokens, min) do
    with {:ok, single, rest} <- single(tokens), do: absorb(single, rest, min)
  end

  defp absorb(%BinaryOpExpr{} = unary, tokens, min) do
    with {:ok, right, rest} <- absorb(unary.right, tokens, max(min, @unary_minus)),
         do: climb(%{unary | right: right}, rest, min)
  end

  defp absorb(left, tokens, min), do: climb(left, tokens, min)

  defp climb(left, [token | rest] = tokens, min) do
    op = binary_op(token)

    if op != nil and Map.fetch!(@priority, op) > min do
      with {:ok, node, rest} <- operation(%BinaryOpExpr{op: op, left: left, right: nil}, rest),
           do: climb(node, rest, min)
    else
      {:ok, left, tokens}
    end
  end

  defp binary_op({:op, text, _position}), do: Map.get(@symbol_ops, text)
  defp binary_op({:ident, text, _position}), do: Map.get(@word_ops, String.downcase(text))
  defp binary_op(_token), do: nil

  defp operation(node, tokens) do
    with {:ok, node, rest} <- bool_modifier(node, tokens),
         {:ok, node, rest} <- group_modifier(node, rest),
         {:ok, node, rest} <- fills(node, rest),
         {:ok, right, rest} <- single(rest),
         {keep, rest} = keep_metric_names(rest),
         {:ok, right, rest} <- absorb(right, rest, right_binding(node.op)) do
      {:ok, %{node | right: right, keep_metric_names: keep}, rest}
    end
  end

  defp bool_modifier(%BinaryOpExpr{op: op} = node, [token | rest] = tokens) do
    cond do
      word(token) != "bool" -> {:ok, node, tokens}
      op in @comparisons -> {:ok, %{node | bool: true}, rest}
      true -> Lexer.error_at(token, "bool modifier cannot be applied to #{op}")
    end
  end

  defp group_modifier(node, [token | rest] = tokens) do
    case word(token) do
      group when group in ["on", "ignoring"] ->
        with {:ok, labels, rest} <- label_list(rest, false) do
          modifier = %Modifier{op: Map.fetch!(@modifier_ops, group), labels: labels}
          join_modifier(%{node | group_modifier: modifier}, rest)
        end

      _other ->
        {:ok, node, tokens}
    end
  end

  defp join_modifier(%BinaryOpExpr{op: op} = node, [token | rest] = tokens) do
    case word(token) do
      join when join in ["group_left", "group_right"] and op in @logical_sets ->
        Lexer.error_at(token, "#{join} cannot be applied to #{op}")

      join when join in ["group_left", "group_right"] ->
        with {:ok, labels, rest} <- join_labels(rest) do
          modifier = %Modifier{op: Map.fetch!(@modifier_ops, join), labels: labels}
          prefix(%{node | join_modifier: modifier}, rest)
        end

      _other ->
        {:ok, node, tokens}
    end
  end

  defp join_labels([{:punct, "(", _position} | _list] = tokens), do: label_list(tokens, true)
  defp join_labels(tokens), do: {:ok, [], tokens}

  defp prefix(node, [token | rest] = tokens) do
    if word(token) == "prefix" do
      with {:ok, value, rest} <- Selector.string_value(rest),
           do: {:ok, %{node | join_prefix: value}, rest}
    else
      {:ok, node, tokens}
    end
  end

  defp fills(%BinaryOpExpr{op: op} = node, [token | rest] = tokens) do
    case {word(token), rest} do
      {fill, _rest} when fill in @fills and op in @set_ops ->
        Lexer.error_at(token, "#{fill} cannot be applied to #{op}")

      {fill, [{:punct, "(", _position} | rest]} when fill in @fills ->
        with {:ok, value, rest} <- fill_value(rest), do: fills(fill(node, fill, value), rest)

      _other ->
        {:ok, node, tokens}
    end
  end

  defp fill(node, "fill", value), do: %{node | fill_left: value, fill_right: value}
  defp fill(node, "fill_left", value), do: %{node | fill_left: value}
  defp fill(node, "fill_right", value), do: %{node | fill_right: value}

  defp fill_value([{:op, "-", _position} | rest]) do
    with {:ok, number, rest} <- fill_number(rest) do
      {:ok, %Number{value: negate(number.value), text: "-" <> number.text}, rest}
    end
  end

  defp fill_value(tokens), do: fill_number(tokens)

  defp fill_number([token | rest]) do
    with {:ok, number} <- number(token) do
      case rest do
        [{:punct, ")", _position} | rest] -> {:ok, number, rest}
        [other | _rest] -> Lexer.unexpected(other, ~s|")"|)
      end
    end
  end

  defp negate(:inf), do: :neg_inf
  defp negate(:nan), do: :nan
  defp negate(value) when is_float(value), do: -value

  defp keep_metric_names([token | rest] = tokens) do
    if word(token) == "keep_metric_names", do: {true, rest}, else: {false, tokens}
  end

  defp right_binding(:^), do: Map.fetch!(@priority, :^) - 1
  defp right_binding(op), do: Map.fetch!(@priority, op)

  defp single([{:ident, text, _position}, {:punct, "(", _paren} | _rest] = tokens) do
    if String.downcase(text) == "with",
      do: {:error, {:unsupported, "WITH templates"}},
      else: suffix(tokens)
  end

  defp single(tokens), do: suffix(tokens)

  defp suffix(tokens) do
    with {:ok, expr, rest} <- primary(tokens), do: maybe_rollup(expr, rest)
  end

  defp maybe_rollup(expr, [{:punct, mark, _position} | _rest] = tokens) when mark in ["[", "@"],
    do: rollup(expr, tokens)

  defp maybe_rollup(expr, [token | _rest] = tokens) do
    if word(token) == "offset", do: rollup(expr, tokens), else: {:ok, expr, tokens}
  end

  defp primary([{:duration, text, _position} = token | rest]) do
    with {:ok, duration} <- duration(token, text), do: {:ok, duration, rest}
  end

  defp primary([{:string, _text, _position} | _rest] = tokens) do
    with {:ok, value, rest} <- Selector.string_value(tokens),
         do: {:ok, %StringLiteral{value: value}, rest}
  end

  defp primary([{:number, _text, _position} = token | rest]) do
    with {:ok, number} <- number(token), do: {:ok, number, rest}
  end

  defp primary([{:ident, text, _position} = token | rest] = tokens) do
    if String.downcase(text) in ["inf", "nan"] do
      with {:ok, number} <- number(token), do: {:ok, number, rest}
    else
      ident_expr(tokens)
    end
  end

  defp primary([{:punct, "(", _position} | rest]), do: parens(rest)
  defp primary([{:punct, "{", _position} | _rest] = tokens), do: Selector.metric(tokens)

  defp primary([{:op, "-", _position} | rest]) do
    with {:ok, expr, rest} <- single(rest) do
      {:ok, %BinaryOpExpr{op: :-, left: %Number{value: 0.0, text: "0"}, right: expr}, rest}
    end
  end

  defp primary([{:op, "+", _position} | rest]), do: single(rest)
  defp primary([token | _rest]), do: Lexer.unexpected(token, "an expression")

  defp number({kind, text, _position} = token) when kind in [:number, :ident] do
    case Literal.number(text) do
      {:ok, value} -> {:ok, %Number{value: value, text: text}}
      {:error, message} -> Lexer.error_at(token, message)
    end
  end

  defp number(token), do: Lexer.unexpected(token, "a number")

  defp duration(token, "$__interval"), do: duration(token, "1i")

  defp duration(token, text) do
    case Durations.parse(text) do
      {:ok, {ms, steps}} -> {:ok, %Duration{text: text, ms: ms, steps: steps}}
      {:error, message} -> Lexer.error_at(token, message)
    end
  end

  defp ident_expr([{:ident, text, _position}, next | _rest] = tokens) do
    aggregate? = Functions.kind(Literal.unescape_ident(text)) == :aggregate

    case {ident_role(next), aggregate?} do
      {:modifier_or_metric, true} -> aggregation(tokens)
      {:modifier_or_metric, false} -> Selector.metric(tokens)
      {:metric, _aggregate?} -> Selector.metric(tokens)
      {:call, true} -> aggregation(tokens)
      {:call, false} -> call(tokens)
      {:none, _aggregate?} -> Lexer.unexpected(next, ~s|"(", "{", "[", ")", "," or "@"|)
    end
  end

  defp ident_role({:eof, _text, _position}), do: :metric
  defp ident_role({:punct, "(", _position}), do: :call
  defp ident_role({:punct, text, _position}) when text in @metric_followers, do: :metric

  defp ident_role({:ident, text, _position}) do
    if String.downcase(text) == "offset", do: :metric, else: :modifier_or_metric
  end

  defp ident_role({:op, _text, _position}), do: :metric
  defp ident_role(_token), do: :none

  defp call([{:ident, text, _position} | rest]) do
    name = Literal.unescape_ident(text)

    with {:ok, args, rest} <- arg_list(rest) do
      {keep, rest} = keep_metric_names(rest)
      {:ok, %FuncExpr{name: name, args: args, keep_metric_names: keep}, rest}
    end
  end

  defp aggregation([{:ident, text, _position} | rest]) do
    name = text |> Literal.unescape_ident() |> String.downcase()

    with {:ok, modifier, rest} <- leading_modifier(rest),
         {:ok, args, rest} <- arg_list(rest),
         {:ok, modifier, rest} <- trailing_modifier(modifier, rest),
         {:ok, limit, rest} <- limit(rest),
         :ok <- no_keep_metric_names(name, rest) do
      {:ok, %AggrFuncExpr{name: name, args: args, modifier: modifier, limit: limit}, rest}
    end
  end

  defp leading_modifier([{:punct, "(", _position} | _args] = tokens), do: {:ok, nil, tokens}

  defp leading_modifier([{:ident, _text, _position} = token | rest]) do
    if word(token) in ["by", "without"],
      do: aggregate_modifier(token, rest),
      else: Lexer.unexpected(token, ~s|"by" or "without"|)
  end

  defp leading_modifier([token | _rest]), do: Lexer.unexpected(token, ~s|"("|)

  defp trailing_modifier(nil, [token | rest] = tokens) do
    if word(token) in ["by", "without"],
      do: aggregate_modifier(token, rest),
      else: {:ok, nil, tokens}
  end

  defp trailing_modifier(modifier, tokens), do: {:ok, modifier, tokens}

  defp aggregate_modifier(token, rest) do
    with {:ok, labels, rest} <- label_list(rest, false) do
      {:ok, %Modifier{op: Map.fetch!(@modifier_ops, word(token)), labels: labels}, rest}
    end
  end

  defp limit([token | rest] = tokens) do
    if word(token) == "limit", do: limit_value(rest), else: {:ok, nil, tokens}
  end

  defp limit_value([{:number, text, _position} = token | rest]) do
    case Integer.parse(text) do
      {0, ""} -> {:ok, nil, rest}
      {limit, ""} when limit <= @max_int64 -> {:ok, limit, rest}
      _other -> Lexer.unexpected(token, "an integer limit")
    end
  end

  defp limit_value([token | _rest]), do: Lexer.unexpected(token, "an integer limit")

  defp no_keep_metric_names(name, [token | _rest]) do
    if word(token) == "keep_metric_names",
      do:
        Lexer.error_at(
          token,
          "keep_metric_names cannot be applied to the aggregate function #{name}()"
        ),
      else: :ok
  end

  defp arg_list([{:punct, "(", _position} | rest]), do: items(rest, [])
  defp arg_list([token | _rest]), do: Lexer.unexpected(token, ~s|"("|)

  defp items([{:punct, ")", _position} | rest], acc), do: {:ok, Enum.reverse(acc), rest}

  defp items(tokens, acc) do
    with {:ok, expr, rest} <- expr(tokens) do
      case rest do
        [{:punct, ",", _position} | rest] -> items(rest, [expr | acc])
        [{:punct, ")", _position} | rest] -> {:ok, Enum.reverse([expr | acc]), rest}
        [token | _rest] -> Lexer.unexpected(token, ~s|"," or ")"|)
      end
    end
  end

  defp parens(tokens) do
    with {:ok, exprs, rest} <- items(tokens, []) do
      case {exprs, keep_metric_names(rest)} do
        {[%BinaryOpExpr{} = only], {true, rest}} ->
          {:ok, %ParensExpr{exprs: [%{only | keep_metric_names: true}]}, rest}

        _otherwise ->
          {:ok, %ParensExpr{exprs: exprs}, rest}
      end
    end
  end

  defp label_list([{:punct, "(", _position} | rest], star?) do
    case {star?, rest} do
      {true, [{:op, "*", _star}, {:punct, ")", _close} | rest]} -> {:ok, :all, rest}
      {true, [{:op, "*", _star}, token | _rest]} -> Lexer.unexpected(token, ~s|")" after "*"|)
      _labels -> labels(rest, [])
    end
  end

  defp label_list([token | _rest], _star?), do: Lexer.unexpected(token, ~s|"("|)

  defp labels([{:punct, ")", _position} | rest], acc), do: {:ok, Enum.reverse(acc), rest}

  defp labels([token | rest], acc) do
    with {:ok, name} <- label_name(token) do
      case rest do
        [{:punct, ",", _position} | rest] -> labels(rest, [name | acc])
        [{:punct, ")", _position} | _close] -> labels(rest, [name | acc])
        [other | _rest] -> Lexer.unexpected(other, ~s|"," or ")"|)
      end
    end
  end

  defp label_name({:ident, text, _position}), do: {:ok, Literal.unescape_ident(text)}

  defp label_name({:string, text, _position} = token) do
    name = binary_part(text, 1, byte_size(text) - 2)

    if Lexer.ident_prefix?(name),
      do: {:ok, Literal.unescape_ident(name)},
      else: Lexer.unexpected(token, "a label name")
  end

  defp label_name(token), do: Lexer.unexpected(token, "a label name")

  defp rollup(expr, [{:punct, "[", _position} = open | rest]) do
    with :ok <- not_range_vector(expr, open),
         {:ok, window, rest} <- window(rest),
         {:ok, step, inherit_step, rest} <- step(rest),
         {:ok, rest} <- close_bracket(rest) do
      node = %RollupExpr{expr: expr, window: window, step: step, inherit_step: inherit_step}
      at_and_offset(node, rest)
    end
  end

  defp rollup(expr, tokens), do: at_and_offset(%RollupExpr{expr: expr}, tokens)

  defp not_range_vector(%ParensExpr{exprs: [expr]}, open), do: not_range_vector(expr, open)

  defp not_range_vector(%RollupExpr{window: %Duration{}}, open),
    do: Lexer.error_at(open, "a window or a subquery cannot be applied to a range vector")

  defp not_range_vector(_expr, _open), do: :ok

  defp window([{:ident, ":" <> _step, _position} | _rest] = tokens), do: {:ok, nil, tokens}
  defp window([{:duration, "$__interval", _position} | rest]), do: {:ok, nil, rest}
  defp window(tokens), do: positive_duration(tokens)

  defp step([{:ident, ":", _position}, {:punct, "]", _close} | _rest] = tokens),
    do: {:ok, nil, true, tl(tokens)}

  defp step([{:ident, ":", _position} | rest]) do
    with {:ok, step, rest} <- positive_duration(rest), do: {:ok, step, false, rest}
  end

  defp step([{:ident, ":" <> text, position} | rest]) do
    with {:ok, step, _rest} <- positive_duration([{:ident, text, position}]),
         do: {:ok, step, false, rest}
  end

  defp step(tokens), do: {:ok, nil, false, tokens}

  defp close_bracket([{:punct, "]", _position} | rest]), do: {:ok, rest}
  defp close_bracket([token | _rest]), do: Lexer.unexpected(token, ~s|"]"|)

  defp positive_duration([{_kind, text, _position} = token | rest]) do
    cond do
      Lexer.duration?(text) ->
        with {:ok, duration} <- duration(token, text), do: {:ok, duration, rest}

      Lexer.number_prefix?(text) ->
        seconds(token, rest)

      true ->
        Lexer.unexpected(token, "a duration")
    end
  end

  defp seconds({_kind, text, _position} = token, rest) do
    with {:ok, _number} <- number({:number, text, elem(token, 2)}) do
      case Durations.parse(text) do
        {:ok, {ms, steps}} -> {:ok, %Duration{text: text, ms: ms, steps: steps}, rest}
        {:error, message} -> Lexer.error_at(token, message)
      end
    end
  end

  defp at_and_offset(node, tokens) do
    with {:ok, node, rest} <- at(node, tokens),
         {:ok, node, rest} <- offset(node, rest),
         {:ok, node, rest} <- at(node, rest) do
      {:ok, collapse(node), rest}
    end
  end

  defp at(%RollupExpr{at: nil} = node, [{:punct, "@", _position} | rest]) do
    with {:ok, at, rest} <- primary(rest), do: {:ok, %{node | at: at}, rest}
  end

  defp at(_node, [{:punct, "@", _position} = token | _rest]),
    do: Lexer.error_at(token, "duplicate @ modifier")

  defp at(node, tokens), do: {:ok, node, tokens}

  defp offset(node, [token | rest] = tokens) do
    if word(token) == "offset" do
      with {:ok, offset, rest} <- signed_duration(rest), do: {:ok, %{node | offset: offset}, rest}
    else
      {:ok, node, tokens}
    end
  end

  defp signed_duration([{:op, "-", _position} = minus | rest]) do
    with {:ok, positive, rest} <- positive_duration(rest),
         {:ok, negative} <- duration(minus, "-" <> positive.text),
         do: {:ok, negative, rest}
  end

  defp signed_duration(tokens), do: positive_duration(tokens)

  defp collapse(%RollupExpr{
         window: nil,
         step: nil,
         inherit_step: false,
         offset: nil,
         at: nil,
         expr: expr
       }),
       do: expr

  defp collapse(node), do: node

  defp word({:ident, text, _position}), do: String.downcase(text)
  defp word(_token), do: nil

  defp unwrap(%ParensExpr{exprs: [expr]}), do: unwrap(expr)
  defp unwrap(%ParensExpr{exprs: exprs}), do: %ParensExpr{exprs: Enum.map(exprs, &unwrap/1)}

  defp unwrap(%BinaryOpExpr{} = node),
    do: %{node | left: unwrap(node.left), right: unwrap(node.right)}

  defp unwrap(%RollupExpr{at: nil} = node), do: %{node | expr: unwrap(node.expr)}
  defp unwrap(%RollupExpr{} = node), do: %{node | expr: unwrap(node.expr), at: unwrap(node.at)}
  defp unwrap(%FuncExpr{} = node), do: %{node | args: Enum.map(node.args, &unwrap/1)}
  defp unwrap(%AggrFuncExpr{} = node), do: %{node | args: Enum.map(node.args, &unwrap/1)}
  defp unwrap(leaf), do: leaf
end
