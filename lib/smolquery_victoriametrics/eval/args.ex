defmodule SmolqueryVictoriaMetrics.Eval.Args do
  @moduledoc """
  Reading a function's evaluated arguments as VictoriaMetrics v1.152.0 reads
  them (PL-70, T-565): every argument, a number or a string included, has
  been evaluated to a list of series, and a function takes from it what it
  needs (`getScalar`, `getString`, `getIntNumber`, `expectTransformArgsNum`
  in `app/vmselect/promql`). A wrong one is
  `{:error, {:invalid_argument, message}}`, with VictoriaMetrics' message.
  """

  alias SmolqueryVictoriaMetrics.Eval.Series
  alias SmolqueryVictoriaMetrics.Eval.Value

  @type reason :: {:invalid_argument, String.t()}

  @doc "Exactly `expected` arguments."
  @spec count([[Series.t()]], non_neg_integer()) :: :ok | {:error, reason()}
  def count(args, expected) when length(args) == expected, do: :ok

  def count(args, expected),
    do: invalid("unexpected number of args; got #{length(args)}; want #{expected}")

  @doc "At least `minimum` arguments."
  @spec at_least([[Series.t()]], non_neg_integer()) :: :ok | {:error, reason()}
  def at_least(args, minimum) when length(args) >= minimum, do: :ok

  def at_least(args, minimum),
    do: invalid("not enough args; got #{length(args)}; want at least #{minimum}")

  @doc """
  A scalar argument's value at every point: the argument must be exactly
  one series. `index` counts from `0`; messages count from `1`.
  """
  @spec scalar([Series.t()], non_neg_integer()) :: {:ok, [Value.t()]} | {:error, reason()}
  def scalar([series], _index), do: {:ok, Series.values(series)}
  def scalar(_list, index), do: invalid("arg ##{index + 1} must be a scalar")

  @doc "A scalar argument's first value as an integer, `0` for none."
  @spec integer([Series.t()], non_neg_integer()) :: {:ok, integer()} | {:error, reason()}
  def integer(list, index) do
    with {:ok, values} <- scalar(list, index), do: {:ok, to_integer(List.first(values))}
  end

  @doc "A float as Go's `int(f)` bounded to the int64 range; `nil` is `0`."
  @spec to_integer(Value.t()) :: integer()
  def to_integer(nil), do: 0
  def to_integer(v) when v >= 9.223_372_036_854_775_807e18, do: 9_223_372_036_854_775_807
  def to_integer(v) when v <= -9.223_372_036_854_775_808e18, do: -9_223_372_036_854_775_808
  def to_integer(v), do: trunc(v)

  @doc """
  A string argument: one series with no value at any point, whose name is
  the string (`evalString`).
  """
  @spec string([Series.t()], non_neg_integer()) :: {:ok, String.t()} | {:error, reason()}
  def string([%Series{labels: labels} = series], index) do
    if Series.empty?(series),
      do: {:ok, Series.label(labels, "__name__")},
      else: invalid("arg ##{index + 1} must be a string")
  end

  def string(_list, index), do: invalid("arg ##{index + 1} must be a string")

  @doc "Every argument read as a string, the first counted as `offset`."
  @spec strings([[Series.t()]], non_neg_integer()) :: {:ok, [String.t()]} | {:error, reason()}
  def strings(args, offset) do
    args
    |> Enum.with_index(offset)
    |> collect(fn {arg, index} -> string(arg, index) end)
  end

  @doc """
  Applies `fun` to each element of `list` in order, collecting its `{:ok,
  value}` answers, and stops at the first answer that is not one.
  """
  @spec collect([item], (item -> {:ok, value} | error)) :: {:ok, [value]} | error
        when item: term(), value: term(), error: term()
  def collect(list, fun) do
    collected =
      Enum.reduce_while(list, {:ok, []}, fn item, {:ok, acc} ->
        case fun.(item) do
          {:ok, value} -> {:cont, {:ok, [value | acc]}}
          error -> {:halt, error}
        end
      end)

    with {:ok, values} <- collected, do: {:ok, Enum.reverse(values)}
  end

  @doc "String arguments in pairs (`getStringPairs`)."
  @spec pairs([[Series.t()]]) :: {:ok, [{String.t(), String.t()}]} | {:error, reason()}
  def pairs(args) do
    count = length(args)

    if rem(count, 2) == 0 do
      with {:ok, texts} <- strings(args, 0),
           do: {:ok, texts |> Enum.chunk_every(2) |> Enum.map(fn [k, v] -> {k, v} end)}
    else
      invalid("the number of string args must be even; got #{count}")
    end
  end

  @doc "An invalid argument's error."
  @spec invalid(String.t()) :: {:error, reason()}
  def invalid(message), do: {:error, {:invalid_argument, message}}
end
