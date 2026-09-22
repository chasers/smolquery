defmodule SmolqueryVictoriaMetrics.Rollup.Window do
  @moduledoc """
  What a rollup function sees at one point of the grid: VictoriaMetrics'
  `rollupFuncArg` (PL-70, T-564).

  The samples are the whole series, held as tuples, and the window is the
  index range `first..last - 1` of them: `SmolqueryVictoriaMetrics.Rollup`
  moves the two indexes forward along the grid instead of copying each
  window out. `values/1` and `timestamps/1` copy a window out for the
  functions that read all of it.

    * `prev_value`, `prev_timestamp` — the sample before the window, when it
      is close enough to count (`prevValue`); otherwise `nil` and the start
      of the window less the largest expected gap;
    * `real_prev_value` — the sample before the window, however far
      (`realPrevValue`);
    * `real_next_value` — the sample after the window (`realNextValue`);
    * `curr_timestamp` — the grid point, the window's end;
    * `window` — the window's length in milliseconds.
  """

  @enforce_keys [:values, :timestamps, :first, :last]
  defstruct [
    :values,
    :timestamps,
    :first,
    :last,
    :prev_value,
    :real_prev_value,
    :real_next_value,
    prev_timestamp: 0,
    curr_timestamp: 0,
    window: 0
  ]

  @type t :: %__MODULE__{
          values: tuple(),
          timestamps: tuple(),
          first: non_neg_integer(),
          last: non_neg_integer(),
          prev_value: float() | nil,
          prev_timestamp: integer(),
          real_prev_value: float() | nil,
          real_next_value: float() | nil,
          curr_timestamp: integer(),
          window: non_neg_integer()
        }

  @doc """
  A window over exactly `values` at `timestamps`, with the other fields
  from `opts`; how a test builds the `rollupFuncArg` it hands one function.
  """
  @spec new([float()], [integer()], keyword()) :: t()
  def new(values, timestamps, opts \\ []) do
    struct!(
      %__MODULE__{
        values: List.to_tuple(values),
        timestamps: List.to_tuple(timestamps),
        first: 0,
        last: length(values)
      },
      opts
    )
  end

  @doc """
  The window's values, oldest first.
  """
  @spec values(t()) :: [float()]
  def values(%__MODULE__{values: values} = window), do: slice(values, window)

  @doc """
  The window's timestamps, oldest first.
  """
  @spec timestamps(t()) :: [integer()]
  def timestamps(%__MODULE__{timestamps: timestamps} = window), do: slice(timestamps, window)

  defp slice(tuple, %__MODULE__{first: first, last: last}),
    do: for(index <- first..(last - 1)//1, do: elem(tuple, index))
end
