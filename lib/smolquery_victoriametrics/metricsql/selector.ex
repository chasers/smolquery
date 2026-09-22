defmodule SmolqueryVictoriaMetrics.MetricsQL.Selector do
  @moduledoc """
  Series selectors and string concatenations, the two MetricsQL forms built
  from label names and string literals (PL-70, T-563).

      name
      name{label="v", label!="v", label=~"re", label!~"re",}
      {"quoted name", "label with spaces"="v"}
      {a="1",b="2" or c="3"}

  A selector is read the way VictoriaMetrics' `metricsql` v0.87.4 reads it and
  then expands it (`parseMetricExpr`, then `expandWithExpr` on a `MetricExpr`,
  which runs even with no `WITH`): the metric name, from before the braces, a
  quoted name alone (`{"m"}`, Prometheus 3's UTF-8 form) or `__name__="m"`,
  becomes the first filter of its filter set, and naming two different
  metrics in one set is an error. When the sets of an `or` that name a metric
  all name the same one, the sets that name none are given it. A filter that
  repeats one before it in its set is dropped. A trailing comma is allowed; an
  empty set is not (`{a="1" or }`). A regular expression must compile.

  A label value, the `prefix` of `group_left`, and a string operand may be
  several string literals joined by `+`: `"a" + 'b'` is `"ab"`. An identifier
  after the `+` would be a `WITH` template reference in VictoriaMetrics, and
  is refused, unless it begins a call or a selector (`"a" + f(x)`), where the
  `+` is an operator.
  """

  alias SmolqueryVictoriaMetrics.MetricsQL.Ast.LabelFilter
  alias SmolqueryVictoriaMetrics.MetricsQL.Ast.MetricExpr
  alias SmolqueryVictoriaMetrics.MetricsQL.Lexer
  alias SmolqueryVictoriaMetrics.MetricsQL.Literal

  @filter_ops %{"=" => :eq, "!=" => :neq, "=~" => :re, "!~" => :nre}

  @type result(value) :: {:ok, value, [Lexer.token()]} | {:error, {:syntax, String.t()}}

  @doc """
  Reads a selector from `tokens`, which begin with its name or its `{`.
  """
  @spec metric([Lexer.token()]) :: result(MetricExpr.t())
  def metric([{:ident, text, _position} | [{:punct, "{", _open} | _braces] = rest]),
    do: filter_sets(rest, {:name, Literal.unescape_ident(text)})

  def metric([{:ident, text, _position} | rest]),
    do: {:ok, %MetricExpr{filter_sets: [[name_filter(Literal.unescape_ident(text))]]}, rest}

  def metric([{:punct, "{", _position} | _braces] = tokens), do: filter_sets(tokens, nil)
  def metric([token | _rest]), do: Lexer.unexpected(token, ~s|a metric name or "{"|)

  @doc """
  Reads one or more string literals joined by `+` and answers their value.
  """
  @spec string_value([Lexer.token()]) :: result(binary())
  def string_value([{:string, _text, _position} = token | rest]) do
    with {:ok, value} <- literal(token), do: concat(value, rest)
  end

  def string_value([token | _rest]), do: Lexer.unexpected(token, "a string")

  defp concat(acc, [{:op, "+", _plus}, {:string, _text, _position} = token | rest]) do
    with {:ok, value} <- literal(token), do: concat(acc <> value, rest)
  end

  defp concat(_acc, [{:op, "+", _plus}, {:ident, _text, _position} = token, next | _rest])
       when elem(next, 1) not in ["(", "{"],
       do: Lexer.unexpected(token, ~s|a string after "+"|)

  defp concat(acc, rest), do: {:ok, acc, rest}

  defp literal({:string, text, _position} = token) do
    case Literal.string(text) do
      {:ok, value} -> {:ok, value}
      {:error, message} -> Lexer.error_at(token, message)
    end
  end

  defp filter_sets([{:punct, "{", _position} = open, {:punct, "}", _close} | rest], name) do
    groups = if name, do: [[name]], else: []
    with {:ok, sets} <- normalize(groups, open), do: {:ok, %MetricExpr{filter_sets: sets}, rest}
  end

  defp filter_sets([{:punct, "{", _position} = open | rest], name) do
    with {:ok, groups, rest} <- groups(rest, name, []),
         {:ok, sets} <- normalize(groups, open) do
      {:ok, %MetricExpr{filter_sets: sets}, rest}
    end
  end

  defp groups(tokens, name, acc) do
    with {:ok, group, rest} <- filters(tokens, List.wrap(name)) do
      case rest do
        [{:punct, "}", _position} | rest] -> {:ok, Enum.reverse([group | acc]), rest}
        [_or | rest] -> groups(rest, name, [group | acc])
      end
    end
  end

  defp filters(tokens, acc) do
    with {:ok, item, rest} <- filter(tokens), do: after_filter(rest, [item | acc])
  end

  defp after_filter([{:punct, ",", _comma} | [{:punct, "}", _close} | _after] = rest], acc),
    do: {:ok, Enum.reverse(acc), rest}

  defp after_filter([{:punct, ",", _comma} | rest], acc), do: filters(rest, acc)

  defp after_filter([{:punct, "}", _close} | _after] = rest, acc),
    do: {:ok, Enum.reverse(acc), rest}

  defp after_filter([token | _after] = rest, acc) do
    if or?(token),
      do: {:ok, Enum.reverse(acc), rest},
      else: Lexer.unexpected(token, ~s|",", "or" or "}"|)
  end

  defp filter([{:string, text, _position} | rest]) do
    name = text |> binary_part(1, byte_size(text) - 2) |> Literal.unescape_ident()
    filter_body(name, true, rest)
  end

  defp filter([{:ident, text, _position} | rest]),
    do: filter_body(Literal.unescape_ident(text), false, rest)

  defp filter([token | _rest]), do: Lexer.unexpected(token, "a label name")

  defp filter_body(name, _quoted, [{kind, text, _position} = token | rest])
       when kind in [:filter_op, :op] and is_map_key(@filter_ops, text) do
    with {:ok, value, rest} <- string_value(rest),
         filter = %LabelFilter{name: name, op: Map.fetch!(@filter_ops, text), value: value},
         :ok <- valid_regex(filter, token) do
      {:ok, filter, rest}
    end
  end

  defp filter_body(name, true, [token | _after] = rest) do
    if elem(token, 1) in [",", "}"] or or?(token),
      do: {:ok, {:name, name}, rest},
      else: Lexer.unexpected(token, ~s|"=", "!=", "=~" or "!~"|)
  end

  defp filter_body(_name, false, [token | _rest]),
    do: Lexer.unexpected(token, ~s|"=", "!=", "=~" or "!~"|)

  defp or?({:ident, text, _position}), do: String.downcase(text) == "or"
  defp or?(_token), do: false

  defp valid_regex(%LabelFilter{op: op, name: name, value: value}, token)
       when op in [:re, :nre] do
    options = if String.valid?(value), do: "u", else: ""

    case Regex.compile("^(?:" <> value <> ")$", options) do
      {:ok, _regex} ->
        :ok

      {:error, {reason, _at}} ->
        Lexer.error_at(
          token,
          "invalid regexp #{Literal.quote_string(value)} for #{name}: #{reason}"
        )
    end
  end

  defp valid_regex(_filter, _token), do: :ok

  defp normalize(groups, open) do
    with {:ok, sets} <- named_sets(groups, open) do
      {:ok, sets |> give_common_name() |> Enum.map(&Enum.uniq/1) |> Enum.uniq()}
    end
  end

  defp named_sets(groups, open) do
    Enum.reduce_while(groups, {:ok, []}, fn group, {:ok, acc} ->
      case named_set(group, open) do
        {:ok, set} -> {:cont, {:ok, [set | acc]}}
        error -> {:halt, error}
      end
    end)
    |> then(fn result -> with {:ok, sets} <- result, do: {:ok, Enum.reverse(sets)} end)
  end

  defp named_set(group, open) do
    group
    |> Enum.reduce_while({:ok, nil, []}, fn item, {:ok, name, filters} ->
      case {metric_name(item), name} do
        {nil, _name} -> {:cont, {:ok, name, [item | filters]}}
        {new, nil} -> {:cont, {:ok, new, filters}}
        {same, same} -> {:cont, {:ok, same, filters}}
        {new, name} -> {:halt, twice(open, name, new)}
      end
    end)
    |> then(fn
      {:ok, nil, filters} -> {:ok, Enum.reverse(filters)}
      {:ok, name, filters} -> {:ok, [name_filter(name) | Enum.reverse(filters)]}
      error -> error
    end)
  end

  defp twice(open, name, new) do
    names = "#{Literal.quote_string(name)} or #{Literal.quote_string(new)}"
    Lexer.error_at(open, "metric name must not be set twice: #{names} in the selector")
  end

  defp metric_name({:name, name}), do: name
  defp metric_name(%LabelFilter{name: "__name__", op: :eq, value: value}), do: value
  defp metric_name(_filter), do: nil

  defp give_common_name(sets) do
    case sets |> Enum.map(&set_name/1) |> Enum.reject(&is_nil/1) |> Enum.uniq() do
      [name] -> Enum.map(sets, &name_unless_named(&1, name))
      _none_or_several -> sets
    end
  end

  defp set_name([%LabelFilter{name: "__name__", op: :eq, value: name} | _rest]) when name != "",
    do: name

  defp set_name(_set), do: nil

  defp name_unless_named(set, name) do
    if Enum.any?(set, &(&1.name == "__name__")), do: set, else: [name_filter(name) | set]
  end

  defp name_filter(name), do: %LabelFilter{name: "__name__", op: :eq, value: name}
end
