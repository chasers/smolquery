defmodule SmolqueryVictoriaMetrics.Eval.LabelFunctions do
  @moduledoc """
  MetricsQL's label functions, ported from VictoriaMetrics v1.152.0's
  `transform.go` (PL-70, T-565): `label_set`, `label_del`, `label_keep`,
  `label_copy`, `label_move`, `label_join`, `label_replace`,
  `label_transform`, `label_map`, `label_match`, `label_mismatch`,
  `label_uppercase`, `label_lowercase`, `label_value`, `labels_equal` and
  `label_graphite_group`. `alias(q, "name")` is the parser's built-in
  `label_set(q, "__name__", "name")`.

  Labels are strings and every name and value argument must be a string
  literal. A label set to `""` is removed, as VictoriaMetrics has no empty
  label. `__name__` is a label like any other here: `label_set(q,
  "__name__", "m")` names a series.

  Regular expressions are RE2 in VictoriaMetrics and PCRE here; the common
  syntax is the same. `label_replace`, `label_match` and `label_mismatch`
  anchor theirs at both ends, `label_transform` replaces every match. A
  replacement is expanded as Go's `Regexp.Expand` does: `$1`, `${1}`,
  `$name`, `${name}`, `$$`; `$1x` is the group named `1x`, which is empty.
  """

  alias SmolqueryVictoriaMetrics.Eval.Args
  alias SmolqueryVictoriaMetrics.Eval.Series
  alias SmolqueryVictoriaMetrics.Eval.Value

  @functions ~w(label_set label_del label_keep label_copy label_move label_join label_replace
    label_transform label_map label_match label_mismatch label_uppercase label_lowercase
    label_value labels_equal label_graphite_group)

  @doc "The label functions of this module."
  @spec functions() :: [String.t()]
  def functions, do: @functions

  @doc "Applies the label function `name` to its evaluated `args`."
  @spec apply(String.t(), [[Series.t()]]) :: {:ok, [Series.t()]} | {:error, Args.reason()}
  def apply("label_set", args) do
    with :ok <- Args.at_least(args, 1),
         {:ok, pairs} <- Args.pairs(tl(args)) do
      {:ok, relabel(hd(args), fn labels -> Enum.reduce(pairs, labels, &put(&2, &1)) end)}
    end
  end

  def apply(name, args) when name in ["label_del", "label_keep"] do
    with :ok <- Args.at_least(args, 1),
         {:ok, names} <- Args.strings(tl(args), 1) do
      fun = if name == "label_del", do: &Series.ignoring(&1, names), else: &Series.on(&1, names)
      {:ok, relabel(hd(args), fun)}
    end
  end

  def apply(name, args) when name in ["label_copy", "label_move"] do
    with :ok <- Args.at_least(args, 1),
         {:ok, pairs} <- Args.pairs(tl(args)) do
      {:ok, relabel(hd(args), &copy(&1, pairs, name == "label_move"))}
    end
  end

  def apply("label_join", args) do
    with :ok <- Args.at_least(args, 3),
         {:ok, [dst, separator | sources]} <- Args.strings(tl(args), 1) do
      {:ok,
       relabel(hd(args), fn labels ->
         joined = Enum.map_join(sources, separator, &Series.label(labels, &1))
         Series.put_label(labels, dst, joined)
       end)}
    end
  end

  def apply("label_replace", args) do
    with :ok <- Args.count(args, 5),
         {:ok, [dst, replacement, src, regex]} <- Args.strings(tl(args), 1),
         {:ok, compiled} <- compile(regex, true) do
      {:ok, relabel(hd(args), &replace(&1, src, compiled, dst, replacement, :whole))}
    end
  end

  def apply("label_transform", args) do
    with :ok <- Args.count(args, 4),
         {:ok, [label, regex, replacement]} <- Args.strings(tl(args), 1),
         {:ok, compiled} <- compile(regex, false) do
      {:ok, relabel(hd(args), &replace(&1, label, compiled, label, replacement, :all))}
    end
  end

  def apply("label_map", args) do
    with :ok <- Args.at_least(args, 2),
         {:ok, label} <- Args.string(Enum.at(args, 1), 1),
         {:ok, pairs} <- Args.pairs(Enum.drop(args, 2)) do
      mapping = Map.new(pairs)

      {:ok,
       relabel(hd(args), fn labels ->
         current = Series.label(labels, label)
         Series.put_label(labels, label, Map.get(mapping, current, current))
       end)}
    end
  end

  def apply(name, args) when name in ["label_match", "label_mismatch"] do
    with :ok <- Args.count(args, 3),
         {:ok, [label, regex]} <- Args.strings(tl(args), 1),
         {:ok, compiled} <- compile(regex, true) do
      keep = name == "label_match"

      {:ok,
       Enum.filter(hd(args), &(Regex.match?(compiled, Series.label(&1.labels, label)) == keep))}
    end
  end

  def apply(name, args) when name in ["label_uppercase", "label_lowercase"] do
    with :ok <- Args.at_least(args, 2),
         {:ok, names} <- Args.strings(tl(args), 1) do
      fun = if name == "label_uppercase", do: &String.upcase/1, else: &String.downcase/1

      {:ok,
       relabel(hd(args), fn labels ->
         Enum.reduce(names, labels, &Series.put_label(&2, &1, fun.(Series.label(&2, &1))))
       end)}
    end
  end

  def apply("label_value", args) do
    with :ok <- Args.count(args, 2),
         {:ok, label} <- Args.string(Enum.at(args, 1), 1) do
      {:ok,
       Enum.map(hd(args), fn series ->
         value = Value.parse(Series.label(series.labels, label))

         %{
           Series.map_values(series, &if(&1, do: value))
           | labels: Series.drop_name(series.labels)
         }
       end)}
    end
  end

  def apply("labels_equal", args) do
    with :ok <- Args.at_least(args, 3),
         {:ok, names} <- Args.strings(tl(args), 1) do
      {:ok, Enum.filter(hd(args), &identical?(&1.labels, names))}
    end
  end

  def apply("label_graphite_group", args) do
    with :ok <- Args.at_least(args, 2),
         {:ok, groups} <- group_ids(tl(args)) do
      {:ok, relabel(hd(args), &graphite_group(&1, groups))}
    end
  end

  defp relabel(series, fun), do: Enum.map(series, &%{&1 | labels: fun.(&1.labels)})

  defp put(labels, {name, value}), do: Series.put_label(labels, name, value)

  defp copy(labels, pairs, move), do: Enum.reduce(pairs, labels, &copy_one(&2, &1, move))

  defp copy_one(labels, {src, dst}, move) do
    case Series.label(labels, src) do
      "" -> labels
      value -> labels |> Series.put_label(dst, value) |> moved(src, dst, move)
    end
  end

  defp moved(labels, src, dst, true) when src != dst, do: Map.delete(labels, src)
  defp moved(labels, _src, _dst, _move), do: labels

  defp identical?(_labels, [_one]), do: true

  defp identical?(labels, [first | rest]) do
    value = Series.label(labels, first)
    Enum.all?(rest, &(Series.label(labels, &1) == value))
  end

  defp group_ids(args) do
    args
    |> Enum.with_index(1)
    |> Args.collect(fn {arg, index} -> Args.integer(arg, index) end)
  end

  defp graphite_group(labels, groups) do
    parts = labels |> Series.label("__name__") |> String.split(".") |> List.to_tuple()

    name =
      Enum.map_join(groups, ".", fn id ->
        if id >= 0 and id < tuple_size(parts), do: elem(parts, id), else: ""
      end)

    Series.put_label(labels, "__name__", name)
  end

  @doc """
  Compiles an RE2 expression as VictoriaMetrics' `metricsql.CompileRegexp`
  and `CompileRegexpAnchored` do; `anchored` wraps it in `^(?:...)$`.
  """
  @spec compile(String.t(), boolean()) :: {:ok, Regex.t()} | {:error, Args.reason()}
  def compile(regex, anchored) do
    source = if anchored, do: "^(?:" <> regex <> ")$", else: regex

    case Regex.compile(source, [:unicode, :dollar_endonly]) do
      {:ok, compiled} -> {:ok, compiled}
      {:error, _reason} -> Args.invalid("cannot compile regex #{inspect(regex)}")
    end
  end

  defp replace(labels, src, regex, dst, replacement, mode) do
    value = Series.label(labels, src)

    if Regex.match?(regex, value),
      do: Series.put_label(labels, dst, replace_all(regex, value, replacement, mode)),
      else: labels
  end

  @doc """
  Replaces the matches of `regex` in `value` with `template` expanded as
  Go's `Regexp.Expand` does: the first match with `:whole` (an anchored
  expression matches all of `value`), every match with `:all`.
  """
  @spec replace_all(Regex.t(), String.t(), String.t(), :whole | :all) :: String.t()
  def replace_all(regex, value, template, mode) do
    matches =
      case mode do
        :whole -> [Regex.run(regex, value, return: :index)]
        :all -> Regex.scan(regex, value, return: :index)
      end

    names = Regex.names(regex)

    {pieces, position} =
      Enum.reduce(matches, {[], 0}, fn [{start, length} | _groups] = groups, {acc, position} ->
        named = named_groups(regex, value, start, names)
        before = binary_part(value, position, start - position)
        {[acc, before, expand(template, value, groups, named)], start + length}
      end)

    IO.iodata_to_binary([pieces, binary_part(value, position, byte_size(value) - position)])
  end

  defp named_groups(_regex, _value, _start, []), do: %{}

  defp named_groups(regex, value, start, _names) do
    Regex.named_captures(regex, value, offset: start) || %{}
  end

  defp expand(template, value, groups, named) do
    groups = List.to_tuple(groups)
    expand(template, value, groups, named, [])
  end

  defp expand("", _value, _groups, _named, acc),
    do: acc |> Enum.reverse() |> IO.iodata_to_binary()

  defp expand("$$" <> rest, value, groups, named, acc),
    do: expand(rest, value, groups, named, ["$" | acc])

  defp expand("$" <> rest, value, groups, named, acc) do
    case reference(rest) do
      {:ok, name, rest} ->
        expand(rest, value, groups, named, [group(name, value, groups, named) | acc])

      :error ->
        expand(rest, value, groups, named, ["$" | acc])
    end
  end

  defp expand(<<char::utf8, rest::binary>>, value, groups, named, acc),
    do: expand(rest, value, groups, named, [<<char::utf8>> | acc])

  defp reference("{" <> rest) do
    case String.split(rest, "}", parts: 2) do
      [name, rest] -> if name =~ ~r/\A\w+\z/, do: {:ok, name, rest}, else: :error
      [_unclosed] -> :error
    end
  end

  defp reference(text) do
    case Regex.run(~r/\A\w+/, text) do
      [name] -> {:ok, name, binary_part(text, byte_size(name), byte_size(text) - byte_size(name))}
      nil -> :error
    end
  end

  defp group(name, value, groups, named) do
    case Integer.parse(name) do
      {index, ""} -> indexed(index, value, groups)
      _name -> Map.get(named, name, "")
    end
  end

  defp indexed(index, value, groups) when index < tuple_size(groups) do
    case elem(groups, index) do
      {start, length} when start >= 0 -> binary_part(value, start, length)
      _unset -> ""
    end
  end

  defp indexed(_index, _value, _groups), do: ""
end
