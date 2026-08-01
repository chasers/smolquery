defmodule Smolquery.BufferService.Load do
  @moduledoc """
  The write path's load meter, and the admission decision made on it (PL-9).

  `bench/results/buffer.md` proved that at saturation a caller's ack wait is
  `outstanding rows / service rate` — Little's Law — and that nothing bounds
  it: the queue lives in the `TableBuffer`'s mailbox, which the accumulator
  bounds never see. So the bound is enforced *before* the `GenServer.call`,
  by `Smolquery.BufferService.Endpoint`, against a pair of `:atomics` each
  buffer publishes as its Registry value:

    * **outstanding rows** — incremented by the endpoint when a batch is
      admitted, decremented by the buffer when rows leave the accumulator
      (commit, success or failure) and by the endpoint when a write it
      admitted is refused or errors. A caller's timeout deliberately
      decrements nothing: the rows are still in the mailbox and will still
      commit.
    * **service rate** (rows/second) — an EMA over commits, updated only by
      flushes of at least 1,000 rows. Small flushes are dominated by
      per-flush fixed costs and would underestimate capacity badly enough to
      shed bursts the buffer clears in milliseconds; under real overload
      flushes are packed, so the estimate is fresh exactly when admission
      matters. No estimate yet means admit — today's behavior.

  Drift is tolerated in one direction only: the races that lose a decrement
  (a caller dying between increment and call) leave the meter reading high,
  which sheds marginally early. A buffer restart discards its counters
  wholesale, so nothing accumulates across lifetimes.
  """

  @outstanding 1
  @rate 2

  @min_sample_rows 1_000

  @type t :: :atomics.atomics_ref()

  @doc """
  A fresh meter: nothing outstanding, no rate estimate.
  """
  @spec new() :: t()
  def new, do: :atomics.new(2, signed: true)

  @doc """
  Whether a `count`-row batch fits inside `budget_ms` of predicted wait.

  `nil` counters (a buffer registered before it published, or none running)
  and `:infinity` budgets admit unconditionally.
  """
  @spec admit(t() | nil, non_neg_integer(), timeout()) ::
          :ok | {:error, {:overloaded, non_neg_integer()}}
  def admit(nil, _count, _budget_ms), do: :ok
  def admit(_counters, _count, :infinity), do: :ok

  def admit(counters, count, budget_ms) do
    case predicted_wait_ms(counters, count) do
      :unknown -> :ok
      predicted when predicted <= budget_ms -> :ok
      predicted -> {:error, {:overloaded, predicted}}
    end
  end

  @doc """
  The Little's-law wait estimate for a `count`-row batch arriving now, or
  `:unknown` before any commit has established a rate.
  """
  @spec predicted_wait_ms(t(), non_neg_integer()) :: non_neg_integer() | :unknown
  def predicted_wait_ms(counters, count) do
    case :atomics.get(counters, @rate) do
      rate when rate <= 0 -> :unknown
      rate -> div((outstanding(counters) + count) * 1_000, rate)
    end
  end

  @doc """
  Rows admitted toward the buffer.
  """
  @spec enter(t() | nil, non_neg_integer()) :: :ok
  def enter(nil, _count), do: :ok
  def enter(counters, count), do: :atomics.add(counters, @outstanding, count)

  @doc """
  Rows that will never commit — a refused or errored write, undone by
  whoever incremented it.
  """
  @spec leave(t() | nil, non_neg_integer()) :: :ok
  def leave(nil, _count), do: :ok
  def leave(counters, count), do: :atomics.sub(counters, @outstanding, count)

  @doc """
  A flush drained `rows` from the accumulator — success or failure, they are
  no longer waiting.
  """
  @spec drained(t(), non_neg_integer()) :: :ok
  def drained(counters, rows), do: :atomics.sub(counters, @outstanding, rows)

  @doc """
  A successful commit of `rows` took `duration_us` — feed the rate EMA, when
  the flush is big enough to mean something.
  """
  @spec sample_rate(t(), non_neg_integer(), non_neg_integer()) :: :ok
  def sample_rate(counters, rows, duration_us) do
    if rows >= @min_sample_rows and duration_us > 0 do
      sample = div(rows * 1_000_000, duration_us)

      case :atomics.get(counters, @rate) do
        0 -> :atomics.put(counters, @rate, sample)
        rate -> :atomics.put(counters, @rate, div(rate * 3 + sample, 4))
      end
    end

    :ok
  end

  @doc """
  Rows admitted and not yet committed, clamped at zero.
  """
  @spec outstanding(t()) :: non_neg_integer()
  def outstanding(counters), do: max(:atomics.get(counters, @outstanding), 0)
end
