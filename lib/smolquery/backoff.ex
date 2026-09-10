defmodule Smolquery.Backoff do
  @moduledoc """
  The one exponential backoff: a base wait that doubles per consecutive
  failure up to a ceiling.

  The sealer waits this way between attempts on a table that keeps failing
  (T-293), the buffer waits this way between claims a replica keeps
  refusing (T-450), and the compactor waits this way before re-planning a
  table whose merge keeps failing (T-458). The first two used to spell the
  arithmetic out; the doubling is capped so the exponent cannot grow without
  bound, and the ceiling is the caller's — `seal_backoff_max_ms`,
  `seal_retry_ms`, and `compact_backoff_max_ms` respectively.
  """

  @doubling_cap 30

  @doc """
  The wait before attempt `consecutive + 1`, in milliseconds: `base_ms`
  doubled `consecutive - 1` times, never above `max_ms`.
  """
  @spec exponential(pos_integer(), non_neg_integer(), non_neg_integer()) :: non_neg_integer()
  def exponential(consecutive, base_ms, max_ms) when consecutive >= 1 do
    doublings = min(consecutive - 1, @doubling_cap)

    min(base_ms * Integer.pow(2, doublings), max_ms)
  end
end
