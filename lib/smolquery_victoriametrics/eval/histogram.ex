defmodule SmolqueryVictoriaMetrics.Eval.Histogram do
  @moduledoc """
  MetricsQL's histogram functions, ported from VictoriaMetrics v1.152.0's
  `transform.go` (PL-70, T-565): `histogram_quantile`,
  `histogram_quantiles`, `histogram_share`, `histogram_fraction`,
  `histogram_avg`, `histogram_stddev`, `histogram_stdvar`,
  `prometheus_buckets` and `buckets_limit`.

  Buckets are series with an `le` label, cumulative as Prometheus writes
  them; VictoriaMetrics' own `vmrange="a...b"` buckets are turned into `le`
  buckets first (`vmrangeBucketsToLE`). Buckets are grouped by their labels
  without `__name__` and `le`, and the answer carries those labels.

  At each point, as `histogram_quantile` does it there:

    * buckets with the same `le` are summed (`mergeSameLE`);
    * a bucket smaller than the one below it, or with no value, takes the
      value below it, since a cumulative count cannot fall
      (`fixBrokenBuckets`), the lowest one taking `0` for no value;
    * with no observations the answer is `nil`; `phi` below `0` is `-Inf`,
      above `1` is `+Inf`;
    * the quantile falls in the first bucket whose count reaches
      `phi` of the total and is interpolated linearly inside it; in the
      `+Inf` bucket it is the largest finite `le`;
    * a third argument, `"bounds"`, adds two series labelled
      `bounds="lower"` and `bounds="upper"` with the bucket's edges.
  """

  alias SmolqueryVictoriaMetrics.Eval.Args
  alias SmolqueryVictoriaMetrics.Eval.Binary
  alias SmolqueryVictoriaMetrics.Eval.Series
  alias SmolqueryVictoriaMetrics.Eval.Value

  @functions ~w(histogram_quantile histogram_quantiles histogram_share histogram_fraction
    histogram_avg histogram_stddev histogram_stdvar prometheus_buckets buckets_limit)

  @doc "The histogram functions of this module."
  @spec functions() :: [String.t()]
  def functions, do: @functions

  @doc "Applies the histogram function `name` to its evaluated `args`."
  @spec apply(String.t(), [[Series.t()]]) :: {:ok, [Series.t()]} | {:error, Args.reason()}
  def apply("histogram_quantile", args) do
    with :ok <- two_or_three(args),
         {:ok, phis} <- scalar_or_fail(Enum.at(args, 0), 0, "cannot parse phi"),
         {:ok, bounds} <- bounds_label(args) do
      phis = List.to_tuple(phis)
      {:ok, per_group(Enum.at(args, 1), bounds, &quantile_at(&1, &2, elem(phis, &3)))}
    end
  end

  def apply("histogram_quantiles", args) do
    with :ok <- Args.at_least(args, 3),
         {:ok, label} <- Args.string(hd(args), 0) do
      buckets = List.last(args)

      with {:ok, lists} <-
             args
             |> Enum.slice(1..-2//1)
             |> Enum.with_index()
             |> Args.collect(&labelled_quantiles(&1, buckets, label)),
           do: {:ok, Enum.concat(lists)}
    end
  end

  def apply("histogram_share", args) do
    with :ok <- two_or_three(args),
         {:ok, les} <- scalar_or_fail(Enum.at(args, 0), 0, "cannot parse le"),
         {:ok, bounds} <- bounds_label(args) do
      les = List.to_tuple(les)
      {:ok, per_group(Enum.at(args, 1), bounds, &share_at(&1, &2, elem(les, &3)))}
    end
  end

  def apply("histogram_fraction", [lower, upper, buckets]) do
    with {:ok, lowers} <- scalar_or_fail(lower, 0, "cannot parse lower le"),
         {:ok, uppers} <- scalar_or_fail(upper, 1, "cannot parse upper le"),
         :ok <- ordered(lowers, uppers) do
      edges = lowers |> Enum.zip(uppers) |> List.to_tuple()
      {:ok, per_group(buckets, "", &fraction(&1, &2, elem(edges, &3)))}
    end
  end

  def apply("histogram_fraction", args), do: Args.count(args, 3)

  def apply(name, args) when name in ["histogram_avg", "histogram_stddev", "histogram_stdvar"] do
    with :ok <- Args.count(args, 1) do
      {:ok,
       hd(args)
       |> to_le()
       |> groups()
       |> Enum.map(fn {labels, buckets} ->
         values = buckets |> columns() |> Enum.map(&moment(name, &1, les(buckets)))
         first_series(buckets, labels, values)
       end)}
    end
  end

  def apply("prometheus_buckets", args),
    do: with(:ok <- Args.count(args, 1), do: {:ok, to_le(hd(args))})

  def apply("buckets_limit", args) do
    with :ok <- Args.count(args, 2),
         {:ok, limit} <- Args.integer(hd(args), 0),
         :ok <- positive(limit) do
      {:ok, buckets_limit(Enum.at(args, 1), max(limit, 3))}
    end
  end

  defp labelled_quantiles({phi_arg, index}, buckets, label) do
    with {:ok, phis} <- scalar_or_fail(phi_arg, index, "cannot parse phi") do
      phi_label = Value.format_general(List.first(phis))
      phis = List.to_tuple(phis)

      {:ok,
       buckets
       |> per_group("", &quantile_at(&1, &2, elem(phis, &3)))
       |> Enum.map(&%{&1 | labels: Series.put_label(&1.labels, label, phi_label)})}
    end
  end

  defp two_or_three([_first, _second]), do: :ok
  defp two_or_three([_first, _second, _third]), do: :ok

  defp two_or_three(args),
    do: Args.invalid("unexpected number of args; got #{length(args)}; want 2...3")

  defp positive(limit) when limit > 0, do: :ok
  defp positive(limit), do: Args.invalid("limit must be greater than 0; got #{limit}")

  defp ordered([lower | _], [upper | _])
       when is_float(lower) and is_float(upper) and lower >= upper,
       do:
         Args.invalid(
           "lower le cannot be greater than upper le; got lower le: #{lower}, upper le: #{upper}"
         )

  defp ordered(_lowers, _uppers), do: :ok

  defp scalar_or_fail(arg, index, context) do
    case Args.scalar(arg, index) do
      {:ok, values} -> {:ok, values}
      {:error, {:invalid_argument, message}} -> Args.invalid("#{context}: #{message}")
    end
  end

  defp bounds_label([_phi, _buckets]), do: {:ok, ""}

  defp bounds_label([_phi, _buckets, label]) do
    case Args.string(label, 2) do
      {:ok, text} ->
        {:ok, text}

      {:error, {:invalid_argument, message}} ->
        Args.invalid("cannot parse boundsLabel (arg #3): #{message}")
    end
  end

  defp per_group(series, bounds, fun) do
    series
    |> to_le()
    |> groups()
    |> Enum.flat_map(fn {labels, buckets} ->
      buckets = merge_same_le(buckets)
      les = les(buckets)

      triples =
        buckets
        |> columns()
        |> Enum.with_index()
        |> Enum.map(fn {column, index} -> fun.(les, fix_broken(column), index) end)

      main = first_series(buckets, labels, Enum.map(triples, &elem(&1, 0)))

      if bounds == "" do
        [main]
      else
        [
          main,
          %{
            Series.put_values(main, Enum.map(triples, &elem(&1, 1)))
            | labels: Series.put_label(labels, bounds, "lower")
          },
          %{
            Series.put_values(main, Enum.map(triples, &elem(&1, 2)))
            | labels: Series.put_label(labels, bounds, "upper")
          }
        ]
      end
    end)
  end

  defp first_series([{_le, first} | _rest], labels, values),
    do: %{Series.put_values(first, values) | labels: labels}

  defp les(buckets), do: Enum.map(buckets, &elem(&1, 0))

  defp columns(buckets) do
    buckets
    |> Enum.map(fn {_le, series} -> Series.values(series) end)
    |> Enum.zip_with(& &1)
  end

  defp groups(series) do
    series
    |> le_groups(&(&1 |> Series.drop_name() |> Map.delete("le")))
    |> Enum.map(fn {key, members} -> {key, Enum.sort_by(members, &elem(&1, 0))} end)
  end

  defp le_groups(series, key_fun) do
    series
    |> Enum.flat_map(&with_le/1)
    |> Series.group(fn {_le, one} -> key_fun.(one.labels) end)
  end

  defp with_le(one) do
    case Value.parse(Series.label(one.labels, "le")) do
      nil -> []
      le -> [{le, one}]
    end
  end

  defp merge_same_le([first | rest]) do
    rest
    |> Enum.reduce([first], fn {le, series}, [{previous_le, previous} | done] = acc ->
      if le == previous_le do
        summed = Enum.zip_with(Series.values(previous), Series.values(series), &Value.add/2)
        [{previous_le, Series.put_values(previous, summed)} | done]
      else
        [{le, series} | acc]
      end
    end)
    |> Enum.reverse()
  end

  @doc """
  One point's bucket counts, lowest `le` first, made non-decreasing
  (`fixBrokenBuckets`).
  """
  @spec fix_broken([Value.t()]) :: [Value.t()]
  def fix_broken([_only] = column), do: column

  def fix_broken([first | rest]) do
    first = first || 0.0

    {rest, _previous} =
      Enum.map_reduce(rest, first, fn
        nil, previous -> {previous, previous}
        v, previous when previous > v -> {previous, previous}
        v, _previous -> {v, v}
      end)

    [first | rest]
  end

  @doc """
  The `phi` quantile of one point's fixed bucket counts, with the lower and
  upper edge of the bucket it falls in.
  """
  @spec quantile_at([float()], [Value.t()], Value.t()) :: {Value.t(), Value.t(), Value.t()}
  def quantile_at(_les, _column, nil), do: {nil, nil, nil}

  def quantile_at(les, column, phi) do
    last = List.last(column) || 0.0

    cond do
      last == 0.0 -> {nil, nil, nil}
      phi < 0 -> {Value.neg_inf(), Value.neg_inf(), hd(column)}
      phi > 1 -> {Value.inf(), last, Value.inf()}
      true -> find_bucket(Enum.zip(les, column), Value.mul(last, phi), 0.0, 0.0, les)
    end
  end

  defp find_bucket([], _required, _v_prev, _le_prev, les), do: last_finite(les)

  defp find_bucket([{le, v} | rest], required, v_prev, le_prev, les) do
    cond do
      Value.lte?(v, 0.0) -> find_bucket(rest, required, v_prev, le, les)
      Value.lt?(v, required) -> find_bucket(rest, required, v, le, les)
      Value.inf?(le) -> last_finite(les)
      v == v_prev -> {le_prev, le_prev, v}
      true -> interpolated(le_prev, le, required, v_prev, v)
    end
  end

  defp interpolated(le_prev, le, required, v_prev, v) do
    offset =
      Value.divide(
        Value.mul(Value.sub(le, le_prev), Value.sub(required, v_prev)),
        Value.sub(v, v_prev)
      )

    {Value.add(le_prev, offset), le_prev, le}
  end

  defp last_finite(les) do
    finite = les |> Enum.reject(&Value.inf?/1) |> List.last()
    {finite, finite, Value.inf()}
  end

  @doc """
  The share of one point's fixed bucket counts at or below `le_req`, with
  the lower and upper edge of the bucket it falls in.
  """
  @spec share_at([float()], [Value.t()], Value.t()) :: {Value.t(), Value.t(), Value.t()}
  def share_at(_les, _column, nil), do: {nil, nil, nil}
  def share_at([], _column, _le), do: {nil, nil, nil}
  def share_at(_les, _column, le) when le < 0, do: {0.0, 0.0, 0.0}

  def share_at(les, column, le_req) do
    if Value.inf?(le_req) and le_req > 0 do
      {1.0, 1.0, 1.0}
    else
      share_bucket(Enum.zip(les, column), le_req, 0.0, 0.0, List.last(column))
    end
  end

  defp share_bucket([], _le_req, _v_prev, _le_prev, _last), do: {1.0, 1.0, 1.0}

  defp share_bucket([{le, v} | rest], le_req, v_prev, le_prev, last) do
    if le_req >= le do
      share_bucket(rest, le_req, v, le, last)
    else
      lower = Value.divide(v_prev, last)

      cond do
        Value.inf?(le) and le > 0 ->
          {lower, lower, 1.0}

        le_prev == le_req ->
          {lower, lower, lower}

        true ->
          {share_interpolated(lower, v, v_prev, last, le_req, le_prev, le), lower,
           Value.divide(v, last)}
      end
    end
  end

  defp share_interpolated(lower, v, v_prev, last, le_req, le_prev, le) do
    Value.add(
      lower,
      Value.divide(
        Value.mul(Value.divide(Value.sub(v, v_prev), last), le_req - le_prev),
        Value.sub(le, le_prev)
      )
    )
  end

  defp fraction(les, column, {lower, upper}) do
    if lower == nil or upper == nil do
      {nil, nil, nil}
    else
      high = elem(share_at(les, column, upper), 0)
      low = elem(share_at(les, column, lower), 0)
      {Value.sub(high, low), nil, nil}
    end
  end

  defp moment(name, column, les) do
    {sum, sum2, total} =
      les
      |> Enum.zip(column)
      |> Enum.reject(fn {le, _v} -> Value.inf?(le) end)
      |> Enum.reduce({{0.0, 0.0, 0.0}, 0.0, 0.0}, fn {le, v},
                                                     {{sum, sum2, total}, le_prev, v_prev} ->
        n = Value.divide(Value.add(le, le_prev), 2.0)
        weight = Value.sub(v, v_prev)

        {{Value.add(sum, Value.mul(n, weight)),
          Value.add(sum2, Value.mul(Value.mul(n, n), weight)), Value.add(total, weight)}, le, v}
      end)
      |> elem(0)

    stdvar = fn ->
      avg = Value.divide(sum, total)
      variance = Value.sub(Value.divide(sum2, total), Value.mul(avg, avg))
      if Value.lt?(variance, 0.0), do: 0.0, else: variance
    end

    cond do
      total == 0.0 -> nil
      name == "histogram_avg" -> Value.divide(sum, total)
      name == "histogram_stdvar" -> stdvar.()
      true -> Value.sqrt(stdvar.())
    end
  end

  @doc """
  Turns VictoriaMetrics' `vmrange` buckets into cumulative `le` buckets
  (`vmrangeBucketsToLE`); series with an `le` label pass through, series
  with neither are dropped.
  """
  @spec to_le([Series.t()]) :: [Series.t()]
  def to_le(series) do
    {plain, ranged} =
      Enum.reduce(series, {[], []}, fn one, {plain, ranged} ->
        case {Series.label(one.labels, "vmrange"), Series.label(one.labels, "le")} do
          {"", ""} -> {plain, ranged}
          {"", _le} -> {[one | plain], ranged}
          {vmrange, _le} -> {plain, vmrange_entry(vmrange, one, ranged)}
        end
      end)

    groups =
      ranged
      |> Enum.reverse()
      |> Enum.group_by(fn {_range, one} -> one.labels end)
      |> Enum.sort_by(fn {labels, _entries} -> Enum.sort(labels) end)

    Enum.reverse(plain, Enum.flat_map(groups, fn {_labels, entries} -> convert(entries) end))
  end

  defp vmrange_entry(vmrange, one, ranged) do
    with [start_text, end_text] <- String.split(vmrange, "...", parts: 2),
         start when is_float(start) <- Value.parse(start_text),
         finish when is_float(finish) <- Value.parse(end_text) do
      labels = one.labels |> Map.delete("le") |> Map.delete("vmrange")
      [{{start_text, end_text, start, finish}, %{one | labels: labels}} | ranged]
    else
      _malformed -> ranged
    end
  end

  defp convert(entries) do
    sorted = Enum.sort_by(entries, fn {{_s, _e, _start, finish}, _one} -> finish end)
    state = %{entries: [], uniq: %{}, store: %{}, prev: nil, next: 0}

    state =
      Enum.reduce(sorted, state, fn {{start_text, end_text, start, finish}, one}, state ->
        if zero?(one) do
          state
        else
          add_bucket(state, one, start_text, end_text, start, finish)
        end
      end)

    state = close_with_inf(state)

    state.entries
    |> Enum.reverse()
    |> Enum.map(&Map.fetch!(state.store, &1))
    |> cumulate()
  end

  defp add_bucket(state, one, start_text, end_text, start, finish) do
    id = state.next
    state = %{state | next: id + 1}
    prev_end = if state.prev, do: elem(state.prev, 0), else: 0.0

    state =
      if start != prev_end and not Map.has_key?(state.uniq, start_text) do
        copy = zeroed(one, start_text)
        copy_id = state.next

        %{
          state
          | next: copy_id + 1,
            uniq: Map.put(state.uniq, start_text, id),
            store: Map.put(state.store, copy_id, copy),
            entries: [copy_id | state.entries]
        }
      else
        state
      end

    one = %{one | labels: Map.put(one.labels, "le", end_text)}
    state = %{state | store: Map.put(state.store, id, one)}

    state =
      case Map.fetch(state.uniq, end_text) do
        {:ok, previous_id} ->
          merged =
            case Binary.merge(Map.fetch!(state.store, previous_id), one) do
              {:ok, merged} -> merged
              :error -> Map.fetch!(state.store, previous_id)
            end

          %{state | store: Map.put(state.store, previous_id, merged)}

        :error ->
          %{state | entries: [id | state.entries], uniq: Map.put(state.uniq, end_text, id)}
      end

    %{state | prev: {finish, id}}
  end

  defp close_with_inf(%{prev: nil} = state), do: state

  defp close_with_inf(%{prev: {finish, id}} = state) do
    last = Map.fetch!(state.store, id)

    if (Value.inf?(finish) and finish > 0) or zero?(last) do
      state
    else
      copy_id = state.next

      %{
        state
        | next: copy_id + 1,
          store: Map.put(state.store, copy_id, zeroed(last, "+Inf")),
          entries: [copy_id | state.entries]
      }
    end
  end

  defp zeroed(one, le),
    do: %{Series.map_values(one, fn _v -> 0.0 end) | labels: Map.put(one.labels, "le", le)}

  defp zero?(one), do: one |> Series.values() |> Enum.all?(&(not Value.gt?(&1, 0.0)))

  defp cumulate([]), do: []

  defp cumulate(buckets) do
    counts =
      buckets
      |> Enum.map(&Series.values/1)
      |> Enum.zip_with(&running_total/1)
      |> Enum.zip_with(& &1)

    Enum.zip_with(buckets, counts, &Series.put_values/2)
  end

  defp running_total(column) do
    {running, _total} =
      Enum.map_reduce(column, 0.0, fn v, total ->
        total = if Value.gt?(v, 0.0), do: Value.add(total, v), else: total
        {total, total}
      end)

    running
  end

  defp buckets_limit(series, limit) do
    series
    |> to_le()
    |> le_groups(&Map.delete(&1, "le"))
    |> Enum.flat_map(fn {_key, group} -> limit_group(group, limit) end)
  end

  defp limit_group(group, limit) when length(group) <= limit, do: Enum.map(group, &elem(&1, 1))

  defp limit_group(group, limit) do
    sorted = Enum.sort_by(group, &elem(&1, 0))
    hits = hits(sorted)

    buckets =
      sorted
      |> Enum.zip(hits)
      |> Enum.map(fn {{_le, one}, hit} -> {one, hit} end)
      |> trim_empty_edges(limit)
      |> merge_smallest(limit)

    Enum.map(buckets, &elem(&1, 0))
  end

  defp hits(sorted) do
    sorted
    |> Enum.map(fn {_le, one} -> Series.values(one) end)
    |> Enum.zip_with(fn column ->
      {hits, _previous} =
        Enum.map_reduce(column, 0.0, fn v, previous -> {Value.sub(v, previous), v} end)

      hits
    end)
    |> Enum.zip_with(fn per_bucket -> Enum.reduce(per_bucket, 0.0, &Value.add(&2, &1)) end)
  end

  defp trim_empty_edges(buckets, limit) do
    buckets
    |> drop_empty_end(limit)
    |> Enum.reverse()
    |> drop_empty_end(limit)
    |> Enum.reverse()
  end

  defp drop_empty_end(buckets, limit) do
    case Enum.reverse(buckets) do
      [{_one, hit} | rest] when length(buckets) > limit and is_float(hit) and abs(hit) < 1.0e-9 ->
        drop_empty_end(Enum.reverse(rest), limit)

      _other ->
        buckets
    end
  end

  defp merge_smallest(buckets, limit) when length(buckets) <= limit, do: buckets

  defp merge_smallest(buckets, limit) do
    tuple = List.to_tuple(buckets)
    last_candidate = tuple_size(tuple) - 3

    index =
      Enum.min_by(
        1..last_candidate//1,
        fn i ->
          Value.add(elem(elem(tuple, i), 1), elem(elem(tuple, i + 1), 1))
        end,
        fn -> 1 end
      )

    {before, [{_merged, merged_hit}, {one, hit} | rest]} = Enum.split(buckets, index)
    merge_smallest(before ++ [{one, Value.add(hit, merged_hit)} | rest], limit)
  end
end
