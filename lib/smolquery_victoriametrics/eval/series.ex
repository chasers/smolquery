defmodule SmolqueryVictoriaMetrics.Eval.Series do
  @moduledoc """
  One series of an evaluated expression (PL-70, T-564, T-565): its labels,
  `__name__` among them when it is kept, and a value, or `nil` for none, at
  each point of the grid, as `{timestamp_ms, value}`.

  Labels are a map, and a label is never empty: setting one to `""` removes
  it, as VictoriaMetrics' `MetricName` has no empty tag. The helpers here
  are `MetricName`'s: `on/2` is `RemoveTagsOn`, `ignoring/2` is
  `RemoveTagsIgnoring`, `describe/1` is `stringMetricName`.

  A scalar is a series too, one with no labels at all, as VictoriaMetrics
  evaluates `1`, `time()` or `scalar(x)`: `scalar?/1` is its `isScalar`.
  A string literal evaluates to one series named by the string, with no
  value at any point (`evalString`), which is how a function reads a
  string argument (`string/1`).
  """

  alias SmolqueryVictoriaMetrics.Eval.Value

  @enforce_keys [:labels, :values]
  defstruct [:labels, :values]

  @type labels :: %{String.t() => String.t()}
  @type t :: %__MODULE__{labels: labels(), values: [{integer(), Value.t()}]}

  @doc "A series with no labels holding `value` at every point of `grid`."
  @spec constant([integer()], Value.t()) :: t()
  def constant(grid, value), do: %__MODULE__{labels: %{}, values: Enum.map(grid, &{&1, value})}

  @doc "A series with no labels holding `fun.(t)` at every point `t` of `grid`."
  @spec generate([integer()], (integer() -> Value.t())) :: t()
  def generate(grid, fun), do: %__MODULE__{labels: %{}, values: Enum.map(grid, &{&1, fun.(&1)})}

  @doc "The series a string literal evaluates to (`evalString`)."
  @spec string([integer()], String.t()) :: t()
  def string(grid, text), do: %{constant(grid, nil) | labels: put_label(%{}, "__name__", text)}

  @doc "The values of `series`, in grid order."
  @spec values(t()) :: [Value.t()]
  def values(%__MODULE__{values: values}), do: Enum.map(values, &elem(&1, 1))

  @doc "The grid of `series`."
  @spec timestamps(t()) :: [integer()]
  def timestamps(%__MODULE__{values: values}), do: Enum.map(values, &elem(&1, 0))

  @doc "`series` with `values`, in grid order, in place of its own."
  @spec put_values(t(), [Value.t()]) :: t()
  def put_values(%__MODULE__{values: points} = series, values),
    do: %{series | values: Enum.zip_with(points, values, fn {t, _old}, v -> {t, v} end)}

  @doc "`series` with `fun` applied to every value."
  @spec map_values(t(), (Value.t() -> Value.t())) :: t()
  def map_values(%__MODULE__{values: points} = series, fun),
    do: %{series | values: Enum.map(points, fn {t, v} -> {t, fun.(v)} end)}

  @doc "Whether `series` has no value at any point."
  @spec empty?(t()) :: boolean()
  def empty?(%__MODULE__{values: values}), do: Enum.all?(values, &(elem(&1, 1) == nil))

  @doc "`list` without its empty series (`removeEmptySeries`)."
  @spec drop_empty([t()]) :: [t()]
  def drop_empty(list), do: Enum.reject(list, &empty?/1)

  @doc "Whether `list` is a scalar: one series with no labels (`isScalar`)."
  @spec scalar?([t()]) :: boolean()
  def scalar?([%__MODULE__{labels: labels}]), do: map_size(labels) == 0
  def scalar?(_list), do: false

  @doc "The value of the label `name`, or `\"\"`."
  @spec label(labels(), String.t()) :: String.t()
  def label(labels, name), do: Map.get(labels, name, "")

  @doc "Sets the label `name`; an empty value removes it."
  @spec put_label(labels(), String.t(), String.t()) :: labels()
  def put_label(labels, name, ""), do: Map.delete(labels, name)
  def put_label(labels, name, value), do: Map.put(labels, name, value)

  @doc "Keeps only the labels in `names`, `__name__` too only when listed."
  @spec on(labels(), [String.t()]) :: labels()
  def on(labels, names), do: Map.take(labels, names)

  @doc "Removes the labels in `names`; an empty list removes nothing."
  @spec ignoring(labels(), [String.t()]) :: labels()
  def ignoring(labels, names), do: Map.drop(labels, names)

  @doc "`labels` without `__name__`."
  @spec drop_name(labels()) :: labels()
  def drop_name(labels), do: Map.delete(labels, "__name__")

  @doc """
  Labels as VictoriaMetrics writes a series in a message
  (`stringMetricName`): `name{k="v", ...}`.
  """
  @spec describe(labels()) :: String.t()
  def describe(labels), do: Map.get(labels, "__name__", "") <> describe_tags(labels)

  @doc "The labels but `__name__` as `{k=\"v\", ...}` (`stringMetricTags`)."
  @spec describe_tags(labels()) :: String.t()
  def describe_tags(labels) do
    pairs =
      labels
      |> drop_name()
      |> Enum.sort()
      |> Enum.map_join(", ", fn {k, v} -> "#{k}=#{inspect(v)}" end)

    "{" <> pairs <> "}"
  end

  @doc """
  The order VictoriaMetrics sorts an answer in (`metricNameLess`): by name,
  then by the other labels in name order.
  """
  @spec sort_key(t()) :: {String.t(), [{String.t(), String.t()}]}
  def sort_key(%__MODULE__{labels: labels}),
    do: {Map.get(labels, "__name__", ""), labels |> drop_name() |> Enum.sort()}

  @doc """
  Groups `items` by `key_fun`, keys in the order first met and each group's
  members in their own order.
  """
  @spec group([item], (item -> key)) :: [{key, [item]}] when item: term(), key: term()
  def group(items, key_fun) do
    {keys, map} =
      Enum.reduce(items, {[], %{}}, fn item, {keys, map} ->
        key = key_fun.(item)
        keys = if Map.has_key?(map, key), do: keys, else: [key | keys]
        {keys, Map.update(map, key, [item], &[item | &1])}
      end)

    keys |> Enum.reverse() |> Enum.map(&{&1, Enum.reverse(Map.fetch!(map, &1))})
  end

  @doc "`list` sorted by `sort_key/1`."
  @spec sort([t()]) :: [t()]
  def sort(list), do: Enum.sort_by(list, &sort_key/1)
end
