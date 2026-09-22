defmodule SmolqueryVictoriaMetrics.Eval.Binary do
  @moduledoc """
  MetricsQL's binary operators over evaluated series, a port of
  VictoriaMetrics v1.152.0's `app/vmselect/promql/binary_op.go` (PL-70,
  T-565).

  ## Arithmetic and comparisons

  `+ - * / % ^ atan2` and `== != > < >= <=` pair series and compute point by
  point (`SmolqueryVictoriaMetrics.Eval.Value` holds the IEEE rules):

    * with no `on`/`ignoring` and no `group_left`/`group_right`, a scalar
      side (one series with no labels) pairs with every series of the
      other, and the result carries that series' labels;
    * otherwise series pair by their labels without `__name__` (kept with
      `keep_metric_names`), reduced by `on(...)` or `ignoring(...)`; each
      side must then hold one series per key, or two that do not overlap
      in time, which are merged; anything else is
      `duplicate time series on the left side of ...`;
    * `group_left(labels)` keeps every series of the left side and copies
      the listed labels from its match on the right (`(*)` copies all of
      them, `prefix "p"` renames them); `group_right` is its mirror;
    * `fill(v)`, `fill_left(v)`, `fill_right(v)` stand in for a missing
      series or point on that side;
    * the result drops `__name__` unless the operator is a comparison
      without `bool`, or `keep_metric_names` is given; one-to-one matching
      keeps only the `on` labels, or drops the `ignoring` ones.

  A comparison without `bool` keeps the left value where it holds and
  answers `nil` elsewhere, filtering the point; with `bool` it answers `1` or
  `0`, and `nil` where the left side has no value. Series left with no value
  are kept, so `(a > b) default 0` still sees them. `q == (1, 2)` keeps the
  points of `q` equal to any member of the union.

  ## Set operators

  `and`, `or`, `unless`, and MetricsQL's `if`, `ifnot` and `default`, match
  series by the same keys and keep or fill points: `and` keeps the left
  points with a right point at the same time, `unless` those without, `or`
  fills the left side's gaps from a matching right series and adds the
  unmatched right ones, `default` fills gaps only, `if`/`ifnot` keep left
  points where the right side has, or has not, a value; a lone scalar on
  the right of `if`, `ifnot` and `default` matches every key.
  """

  alias SmolqueryVictoriaMetrics.Eval.Args
  alias SmolqueryVictoriaMetrics.Eval.Constants
  alias SmolqueryVictoriaMetrics.Eval.Series
  alias SmolqueryVictoriaMetrics.Eval.Value
  alias SmolqueryVictoriaMetrics.MetricsQL.Ast.BinaryOpExpr
  alias SmolqueryVictoriaMetrics.MetricsQL.Ast.Modifier
  alias SmolqueryVictoriaMetrics.MetricsQL.Ast.RollupExpr

  @type reason :: {:duplicate_series, String.t()}

  @doc "Applies the operator of `node` to its evaluated sides."
  @spec apply(BinaryOpExpr.t(), [Series.t()], [Series.t()]) ::
          {:ok, [Series.t()]} | {:error, reason()}
  def apply(%BinaryOpExpr{op: op} = node, left, right) when op in [:==, :!=] do
    cond do
      Constants.union?(node.left) -> {:ok, union_compare(op, right, left)}
      Constants.union?(node.right) -> {:ok, union_compare(op, left, right)}
      true -> pointwise(node, left, right)
    end
  end

  def apply(%BinaryOpExpr{op: :and} = node, left, right),
    do: {:ok, logical_and(node, left, right)}

  def apply(%BinaryOpExpr{op: :or} = node, left, right), do: {:ok, logical_or(node, left, right)}
  def apply(%BinaryOpExpr{op: :unless} = node, left, right), do: {:ok, unless(node, left, right)}
  def apply(%BinaryOpExpr{op: :if} = node, left, right), do: {:ok, if_(node, left, right)}
  def apply(%BinaryOpExpr{op: :ifnot} = node, left, right), do: {:ok, ifnot(node, left, right)}

  def apply(%BinaryOpExpr{op: :default} = node, left, right),
    do: {:ok, default(node, left, right)}

  def apply(%BinaryOpExpr{} = node, left, right), do: pointwise(node, left, right)

  defp union_compare(:==, [], _union), do: []
  defp union_compare(:==, _series, []), do: []
  defp union_compare(:!=, [], _union), do: []
  defp union_compare(:!=, series, []), do: series

  defp union_compare(op, series, union) do
    columns = union |> Enum.map(&Series.values/1) |> Enum.zip_with(& &1)

    Enum.map(series, fn one ->
      Series.put_values(one, Enum.zip_with(Series.values(one), columns, &union_point(op, &1, &2)))
    end)
  end

  defp union_point(op, v, column) do
    member = is_float(v) and Enum.member?(column, v)
    if member == (op == :==), do: v, else: nil
  end

  defp pointwise(%BinaryOpExpr{op: op} = node, left, right) do
    comparison = Constants.comparison?(op)

    {left, right} =
      if comparison, do: {left, right}, else: {Series.drop_empty(left), Series.drop_empty(right)}

    cond do
      left == [] and right == [] -> {:ok, []}
      left == [] and node.fill_left == nil -> {:ok, []}
      right == [] and node.fill_right == nil -> {:ok, []}
      true -> with {:ok, triples} <- pair(node, left, right), do: {:ok, compute(node, triples)}
    end
  end

  defp compute(node, triples) do
    fun = operator(node)
    fill_left = fill(node.fill_left)
    fill_right = fill(node.fill_right)

    drop_nan_right =
      Constants.comparison?(node.op) and vector_comparison?(node.right) and is_nil(fill_right)

    Enum.map(triples, fn {l, r, labels} ->
      values =
        Enum.zip_with(Series.values(l), Series.values(r), fn
          nil, nil -> fun.(nil, nil)
          _a, nil when drop_nan_right -> nil
          a, b -> fun.(a || fill_left, fill_value(b, fill_right))
        end)

      %Series{labels: labels, values: Enum.zip(Series.timestamps(l), values)}
    end)
  end

  defp fill_value(nil, fill), do: fill
  defp fill_value(v, _fill), do: v

  defp fill(nil), do: nil
  defp fill(%{value: value}), do: Value.from_number(value)

  defp operator(%BinaryOpExpr{op: op, bool: bool}) do
    if Constants.comparison?(op) do
      fn a, b -> compare(op, bool, a, b) end
    else
      &Constants.arithmetic(op, &1, &2)
    end
  end

  defp compare(op, false, a, b), do: if(Constants.compare(op, a, b), do: a, else: nil)
  defp compare(_op, true, nil, _b), do: nil
  defp compare(op, true, a, b), do: if(Constants.compare(op, a, b), do: 1.0, else: 0.0)

  defp vector_comparison?(%RollupExpr{window: nil, expr: expr}), do: vector_comparison?(expr)

  defp vector_comparison?(%BinaryOpExpr{op: op, left: left, right: right}),
    do: Constants.comparison?(op) and not (Constants.scalar?(left) and Constants.scalar?(right))

  defp vector_comparison?(_expr), do: false

  defp pair(%BinaryOpExpr{group_modifier: nil, join_modifier: nil} = node, left, right) do
    cond do
      Series.scalar?(left) ->
        [scalar] = left
        {:ok, Enum.map(right, &{scalar, &1, reset_name(node, &1.labels)})}

      Series.scalar?(right) ->
        [scalar] = right
        {:ok, Enum.map(left, &{&1, scalar, reset_name(node, &1.labels)})}

      true ->
        match(node, left, right)
    end
  end

  defp pair(node, left, right), do: match(node, left, right)

  defp match(node, left, right) do
    {left_keys, left_map} = group(node, left)
    {right_keys, right_map} = group(node, right)

    keys =
      if node.fill_left,
        do: left_keys ++ Enum.reject(right_keys, &Map.has_key?(left_map, &1)),
        else: left_keys

    keys
    |> Args.collect(&pair_group(node, Map.get(left_map, &1), Map.get(right_map, &1)))
    |> concat()
  end

  defp concat({:ok, lists}), do: {:ok, Enum.concat(lists)}
  defp concat(error), do: error

  defp pair_group(%BinaryOpExpr{fill_right: nil}, _left, nil), do: {:ok, []}

  defp pair_group(node, nil, [first | _rest] = right),
    do: pair_group(node, [fill_series(node, first)], right)

  defp pair_group(node, [first | _rest] = left, nil),
    do: pair_group(node, left, [fill_series(node, first)])

  defp pair_group(%BinaryOpExpr{join_modifier: %Modifier{op: :group_left}} = node, left, right) do
    with {:ok, pairs} <- group_join("right", node, left, right),
         do: {:ok, Enum.map(pairs, fn {l, r} -> {l, r, l.labels} end)}
  end

  defp pair_group(%BinaryOpExpr{join_modifier: %Modifier{op: :group_right}} = node, left, right) do
    with {:ok, pairs} <- group_join("left", node, right, left),
         do: {:ok, Enum.map(pairs, fn {r, l} -> {l, r, r.labels} end)}
  end

  defp pair_group(node, left, right) do
    with {:ok, l} <- single("left", node, left),
         {:ok, r} <- single("right", node, right) do
      {:ok, [{l, r, one_to_one_labels(node, l.labels)}]}
    end
  end

  defp one_to_one_labels(node, labels) do
    labels = reset_name(node, labels)

    case node.group_modifier do
      %Modifier{op: :on, labels: names} ->
        names = if node.keep_metric_names, do: names ++ ["__name__"], else: names
        Series.on(labels, names)

      %Modifier{op: :ignoring, labels: names} ->
        Series.ignoring(labels, names)

      nil ->
        labels
    end
  end

  defp fill_series(node, %Series{labels: labels} = source) do
    labels = if node.keep_metric_names, do: labels, else: Series.drop_name(labels)
    labels = group_labels(node, labels)
    %{Series.map_values(source, fn _v -> nil end) | labels: labels}
  end

  defp group_labels(%BinaryOpExpr{group_modifier: %Modifier{op: :on, labels: names}}, labels),
    do: Series.on(labels, names)

  defp group_labels(
         %BinaryOpExpr{group_modifier: %Modifier{op: :ignoring, labels: names}},
         labels
       ),
       do: Series.ignoring(labels, names)

  defp group_labels(_node, labels), do: labels

  defp single(_side, _node, [one]), do: {:ok, one}

  defp single(side, node, [first | rest]) do
    last = List.last(rest)

    case merge(first, last) do
      {:ok, merged} ->
        single(side, node, [merged | Enum.drop(rest, -1)])

      :error ->
        duplicate(
          "duplicate time series on the #{side} side of #{op(node)} #{modifier(node.group_modifier)}: " <>
            "#{Series.describe_tags(first.labels)} and #{Series.describe_tags(last.labels)}"
        )
    end
  end

  defp group_join(side, node, many, others) do
    join_tags = node.join_modifier.labels
    prefix = node.join_prefix || ""

    skip =
      case node.group_modifier do
        %Modifier{op: :on, labels: names} -> names
        _other -> []
      end

    many
    |> Args.collect(fn one ->
      join_one(
        side,
        node,
        %{one | labels: reset_name(node, one.labels)},
        others,
        {join_tags, prefix, skip}
      )
    end)
    |> concat()
  end

  defp join_one(_side, _node, one, [other], tags),
    do: {:ok, [{%{one | labels: set_tags(one.labels, tags, other.labels)}, other}]}

  defp join_one(side, node, one, others, tags) do
    labelled = Enum.map(others, &{set_tags(one.labels, tags, &1.labels), &1})
    grouped = Enum.group_by(labelled, &elem(&1, 0), &elem(&1, 1))

    labelled
    |> Enum.map(&elem(&1, 0))
    |> Enum.uniq()
    |> Args.collect(fn labels ->
      [first | rest] = Map.fetch!(grouped, labels)

      with {:ok, merged} <- merge_all(side, node, first, rest),
           do: {:ok, {%{one | labels: labels}, merged}}
    end)
  end

  defp merge_all(_side, _node, previous, []), do: {:ok, previous}

  defp merge_all(side, node, previous, [other | rest]) do
    case merge(previous, other) do
      {:ok, merged} ->
        merge_all(side, node, merged, rest)

      :error ->
        duplicate(
          "duplicate time series on the #{side} side of `#{op(node)} " <>
            "#{modifier(node.group_modifier)} #{modifier(node.join_modifier)}`: " <>
            "#{Series.describe_tags(previous.labels)} and #{Series.describe_tags(other.labels)}"
        )
    end
  end

  defp set_tags(labels, {:all, prefix, skip}, source) do
    source
    |> Series.drop_name()
    |> Enum.reject(fn {name, _value} -> name in skip end)
    |> Map.new(fn {name, value} -> {IO.iodata_to_binary([prefix, name]), value} end)
    |> then(&Map.merge(labels, &1))
  end

  defp set_tags(labels, {names, prefix, skip}, source) do
    names
    |> Enum.reject(&(&1 in skip))
    |> Enum.reduce(labels, fn
      "__name__", acc ->
        Series.put_label(acc, "__name__", Series.label(source, "__name__"))

      name, acc ->
        case Map.fetch(source, name) do
          {:ok, value} -> Map.put(acc, IO.iodata_to_binary([prefix, name]), value)
          :error -> Map.delete(acc, name)
        end
    end)
  end

  @doc """
  Merges `source` into `target` when they overlap at two points at most and
  hold more than two points (`mergeNonOverlappingTimeseries`).
  """
  @spec merge(Series.t(), Series.t()) :: {:ok, Series.t()} | :error
  def merge(%Series{} = target, %Series{} = source) do
    targets = Series.values(target)
    sources = Series.values(source)

    overlaps =
      Enum.zip_with(targets, sources, &(&1 != nil and &2 != nil))
      |> Enum.count(& &1)

    if overlaps > 2 or (short?(sources) and short?(targets)) do
      :error
    else
      {:ok, Series.put_values(target, Enum.zip_with(targets, sources, &(&2 || &1)))}
    end
  end

  defp short?(values), do: not match?([_, _, _ | _], values)

  defp reset_name(%BinaryOpExpr{op: op, bool: false}, labels)
       when op in [:==, :!=, :>, :<, :>=, :<=],
       do: labels

  defp reset_name(%BinaryOpExpr{keep_metric_names: true}, labels), do: labels
  defp reset_name(_node, labels), do: Series.drop_name(labels)

  defp group(node, list) do
    grouped = Series.group(list, &key(node, &1.labels))
    {Enum.map(grouped, &elem(&1, 0)), Map.new(grouped)}
  end

  defp key(node, labels) do
    labels = if node.keep_metric_names, do: labels, else: Series.drop_name(labels)
    group_labels(node, labels)
  end

  defp logical_and(node, left, right) do
    {_left_keys, left_map} = group(node, left)
    {right_keys, right_map} = group(node, right)

    Enum.flat_map(right_keys, fn key ->
      case Map.fetch(left_map, key) do
        {:ok, lefts} -> keep_where_right(lefts, Map.fetch!(right_map, key))
        :error -> []
      end
    end)
  end

  defp if_(node, left, right) do
    {left_keys, left_map} = group(node, left)
    {_right_keys, right_map} = group(node, right)

    Enum.flat_map(left_keys, fn key ->
      case by_key(right_map, key) do
        nil -> []
        rights -> keep_where_right(Map.fetch!(left_map, key), rights)
      end
    end)
  end

  defp unless(node, left, right) do
    {left_keys, left_map} = group(node, left)
    {_right_keys, right_map} = group(node, right)

    Enum.flat_map(left_keys, fn key ->
      lefts = Map.fetch!(left_map, key)

      case Map.fetch(right_map, key) do
        {:ok, rights} -> drop_where_right(lefts, rights)
        :error -> lefts
      end
    end)
  end

  defp ifnot(node, left, right) do
    {left_keys, left_map} = group(node, left)
    {_right_keys, right_map} = group(node, right)

    Enum.flat_map(left_keys, fn key ->
      lefts = Map.fetch!(left_map, key)

      case by_key(right_map, key) do
        nil -> lefts
        rights -> drop_where_right(lefts, rights)
      end
    end)
  end

  defp default(node, left, right) do
    {left_keys, left_map} = group(node, left)
    {right_keys, right_map} = group(node, right)

    if left_keys == [] do
      Enum.flat_map(right_keys, &Map.fetch!(right_map, &1))
    else
      Enum.flat_map(left_keys, &default_group(Map.fetch!(left_map, &1), by_key(right_map, &1)))
    end
  end

  defp default_group(lefts, nil), do: lefts
  defp default_group(lefts, rights), do: Enum.map(lefts, &fill_from(&1, rights))

  defp logical_or(node, left, right) do
    {left_keys, left_map} = group(node, left)
    {right_keys, right_map} = group(node, right)
    left_map = Map.new(left_map, fn {key, lefts} -> {key, Series.drop_empty(lefts)} end)

    {left_map, added} =
      Enum.reduce(right_keys, {left_map, []}, fn key, {left_map, added} ->
        {left_map, rights} = or_group(left_map, key, Map.fetch!(right_map, key))
        {left_map, [rights | added]}
      end)

    added = added |> Enum.reverse() |> Enum.concat()

    lefts = Enum.flat_map(left_keys, &Map.fetch!(left_map, &1))
    Series.sort(lefts) ++ Series.sort(added)
  end

  defp or_group(left_map, key, rights) do
    case Map.fetch(left_map, key) do
      {:ok, lefts} ->
        {lefts, rights} = fill_or_merge(lefts, rights)
        {Map.put(left_map, key, lefts), Series.drop_empty(rights)}

      :error ->
        {left_map, rights}
    end
  end

  defp by_key(map, key) do
    case Map.fetch(map, key) do
      {:ok, members} -> members
      :error -> lone_scalar(Map.values(map))
    end
  end

  defp lone_scalar([only]), do: if(Series.scalar?(only), do: only)
  defp lone_scalar(_groups), do: nil

  defp keep_where_right(lefts, rights) do
    present = present_columns(rights)

    lefts
    |> Enum.map(fn left ->
      Series.put_values(left, Enum.zip_with(Series.values(left), present, &if(&2, do: &1)))
    end)
    |> Series.drop_empty()
  end

  defp drop_where_right(lefts, rights) do
    present = present_columns(rights)

    lefts
    |> Enum.map(fn left ->
      Series.put_values(
        left,
        Enum.zip_with(Series.values(left), present, &if(&2, do: nil, else: &1))
      )
    end)
    |> Series.drop_empty()
  end

  defp present_columns(rights) do
    rights
    |> Enum.map(&Series.values/1)
    |> Enum.zip_with(fn column -> Enum.any?(column, &(&1 != nil)) end)
  end

  defp fill_from(left, rights) do
    columns = rights |> Enum.map(&Series.values/1) |> Enum.zip_with(& &1)

    values =
      Enum.zip_with(Series.values(left), columns, fn
        nil, column -> Enum.find(column, &(&1 != nil))
        v, _column -> v
      end)

    Series.put_values(left, values)
  end

  defp fill_or_merge(lefts, rights) do
    scalar_right = Series.scalar?(rights)
    scalar_left = Series.scalar?(lefts)

    Enum.map_reduce(lefts, rights, fn left, rights ->
      mergeable =
        Enum.map(rights, &if(scalar_right, do: scalar_left, else: &1.labels == left.labels))

      columns = rights |> Enum.map(&Series.values/1) |> Enum.zip_with(& &1)

      {values, columns} =
        Series.values(left)
        |> Enum.zip_with(columns, &merge_point(&1, &2, mergeable))
        |> Enum.unzip()

      rights =
        columns
        |> Enum.zip_with(& &1)
        |> Enum.zip_with(rights, fn values, right -> Series.put_values(right, values) end)

      {Series.put_values(left, values), rights}
    end)
  end

  defp merge_point(v, column, mergeable) do
    missing = v == nil

    column
    |> Enum.zip(mergeable)
    |> Enum.map_reduce(v, fn {right, merge}, current ->
      current = if missing and merge, do: right, else: current
      right = if not missing or merge, do: nil, else: right
      {right, current}
    end)
    |> then(fn {column, current} -> {current, column} end)
  end

  defp op(%BinaryOpExpr{op: op}), do: Atom.to_string(op)

  defp modifier(nil), do: "()"
  defp modifier(%Modifier{op: op, labels: :all}), do: "#{op}(*)"
  defp modifier(%Modifier{op: op, labels: labels}), do: "#{op}(#{Enum.join(labels, ",")})"

  defp duplicate(message), do: {:error, {:duplicate_series, message}}
end
