defmodule SmolqueryVictoriaMetrics.Prefetch do
  @moduledoc """
  Runs a query's reads concurrently before it is evaluated (T-633).

  `SmolqueryVictoriaMetrics.Eval` reads each selector, and runs each pushed
  aggregate, as it reaches it in the tree, so a query's time was the sum of
  its reads: `a / b` waited for `a`, then for `b`. Here the expression is
  first evaluated with a context whose `fetch` and `aggregate` read nothing
  and record what they were asked for; the recorded reads then start at
  once, at most `max_concurrency` at a time; and the evaluation proper
  waits only for the slowest.

  ## Two stages

  A read runs in two stages (`t:stages/0`). `start` runs the job and checks
  what it answered, and is what runs concurrently; it holds the job's frame,
  columnar and outside the process heap, and copies nothing into the
  process. `take` copies the frame into what `Eval` reads, in the
  evaluating process, when the evaluation reaches the read. So a query's
  sample lists are copied once and one read at a time, as before, and a
  ceiling checked in `start` refuses a read before anything is copied.

  ## What is held

  The recording pass sees no samples, so a read that depends on them, the
  time of an `@` taken from a series, is not recorded, or is recorded with
  the wrong range. A read that was not recorded starts when the evaluation
  reaches it, as it did before, so the answer is the same whatever was
  recorded; a read recorded wrongly costs a job, held to the same deadline
  and budgets. What a started read answered is held, its error too, so a
  read runs once and a query answers the error the evaluation meets first,
  except a reason `retry?` accepts, the query service's job limit, which
  starts the read again when the evaluation reaches it, once the others are
  done. The same read asked for twice runs once, and is taken twice.

  A query with one read or none starts it when the evaluation reaches it,
  as before.
  """

  alias SmolqueryVictoriaMetrics.Eval
  alias SmolqueryVictoriaMetrics.MetricsQL.Ast.MetricExpr
  alias SmolqueryVictoriaMetrics.Pushdown

  @typedoc """
  Each kind of read as `{start, take}`: `fetch` reads a selector over a
  range, `aggregate` runs a pushed aggregate.
  """
  @type stages :: %{
          fetch:
            {(MetricExpr.t(), {integer(), integer()} -> {:ok, term()} | {:error, term()}),
             (term() -> {:ok, Eval.result()} | {:error, term()})},
          aggregate:
            {(Pushdown.t() -> {:ok, term()} | {:error, term()}),
             (term() -> {:ok, Eval.result(), Eval.stats()} | {:error, term()})}
        }

  @doc """
  `base` with the `fetch` and `aggregate` that `evaluate` should run with:
  each read the recording pass of `evaluate` asked for is started at most
  `max_concurrency` at a time, and is taken from what it answered; any
  other read is started and taken when it is reached.

  Options: `max_concurrency` (required), and `retry?`, which reasons a
  started read is started again for (none by default).
  """
  @spec context(map(), stages(), keyword(), (Eval.context() -> term())) :: Eval.context()
  def context(base, stages, opts, evaluate) do
    held =
      case recorded(base, evaluate) do
        [_, _ | _] = reads -> started(stages, Enum.uniq(reads), opts)
        _one_or_none -> %{}
      end

    answering(base, stages, held)
  end

  defp recorded(base, evaluate) do
    tag = make_ref()
    me = self()

    base
    |> Map.merge(%{
      fetch: fn selector, range ->
        send(me, {tag, {:fetch, selector, range}})
        {:ok, []}
      end,
      aggregate: fn plan ->
        send(me, {tag, {:aggregate, plan}})
        {:ok, [], %{series: 0, samples: 0}}
      end
    })
    |> evaluate.()

    drain(tag, [])
  end

  defp drain(tag, reads) do
    receive do
      {^tag, read} -> drain(tag, [read | reads])
    after
      0 -> Enum.reverse(reads)
    end
  end

  defp started(stages, reads, opts) do
    retry? = Keyword.get(opts, :retry?, fn _reason -> false end)

    reads
    |> Task.async_stream(&{&1, start(stages, &1)},
      max_concurrency: Keyword.fetch!(opts, :max_concurrency),
      ordered: false,
      timeout: :infinity
    )
    |> Enum.reduce(%{}, fn {:ok, {read, answer}}, held ->
      if retried?(answer, retry?), do: held, else: Map.put(held, read, answer)
    end)
  end

  defp retried?({:error, reason}, retry?), do: retry?.(reason)
  defp retried?(_answer, _retry?), do: false

  defp start(%{fetch: {start, _take}}, {:fetch, selector, range}), do: start.(selector, range)
  defp start(%{aggregate: {start, _take}}, {:aggregate, plan}), do: start.(plan)

  defp answering(base, stages, held) do
    %{fetch: {start_fetch, take_fetch}, aggregate: {start_aggregate, take_aggregate}} = stages

    Map.merge(base, %{
      fetch: fn selector, range ->
        held
        |> Map.get_lazy({:fetch, selector, range}, fn -> start_fetch.(selector, range) end)
        |> taken(take_fetch)
      end,
      aggregate: fn plan ->
        held
        |> Map.get_lazy({:aggregate, plan}, fn -> start_aggregate.(plan) end)
        |> taken(take_aggregate)
      end
    })
  end

  defp taken({:ok, answer}, take), do: take.(answer)
  defp taken(error, _take), do: error
end
