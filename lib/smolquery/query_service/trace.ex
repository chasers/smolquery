defmodule Smolquery.QueryService.Trace do
  @moduledoc """
  Per-query phase spans: emitted always, collected only when asked.

  Every query phase — parsing, snapshot pin, schema resolution, each hot
  manifest fetch, pruning, engine work — runs inside `span/3`, which emits
  one `[:smolquery, :query, :span]` telemetry event with the phase's start
  and duration. Emission is unconditional and near-free when nothing listens;
  `phase` is a closed set, so a metrics rollup could aggregate the event
  without unbounded labels (`Smolquery.Telemetry`'s cardinality rule).

  Collection is opt-in per job (`trace: true` at submit). The runner attaches
  a collector — a telemetry handler writing to a public ETS table the runner
  owns — before it starts any work, and turns the table into an ordered span
  list when the job settles. Telemetry handlers run in the emitting process,
  and one query's phases run in three kinds of process: the runner (engine
  start), the job task, and the planner's fan-out tasks. The collector keeps
  one job's spans apart from every concurrent job's by ancestry: `Task`
  propagates `$callers`, so a span belongs to this job exactly when the
  runner's pid is the emitter or among the emitter's callers. No job id has
  to thread through the planner for that to hold.

  A collector cannot outlive its runner: the runner detaches it in
  `terminate/2`, and if the runner dies too hard for terminate to run, the
  handler detaches itself on the next span it sees after its owner is gone.
  The second path matters because the ancestry guard alone would never heal —
  a dead owner makes the guard false for every future span, so the handler
  would neither insert nor raise, and telemetry only auto-detaches a handler
  that raises.
  """

  alias Smolquery.Telemetry

  @event [:smolquery, :query, :span]

  @typedoc """
  One recorded phase: its name, start offset and duration in microseconds,
  and whatever the emitter attached (a manifest fetch carries its `url`).
  Offsets are relative to the earliest span in the job's trace — `stop/1`
  rebases them. The raw telemetry event carries `start_us` as unrebased
  `System.monotonic_time/1`, which is usually a large negative number.
  """
  @type span :: %{
          name: atom(),
          start_us: non_neg_integer(),
          duration_us: non_neg_integer(),
          meta: map()
        }

  @opaque collector :: %{id: term(), table: :ets.tid()}

  @doc """
  Runs `fun` as the phase `name`, emitting its span when it returns.

  `meta` is the span's metadata: a map, or a function of `fun`'s outcome
  that builds one — `{:raised, kind, reason}` when `fun` raised — for a
  phase whose metadata is what it found out (the `top_n` probe's outcome).

  `Smolquery.Telemetry.span/3` is the clock (T-380): the span is emitted
  whether `fun` answered `{:ok, _}` or `{:error, _}`, and when it raises —
  the phase still took its time, and a trace that ends at the failing phase
  says where the job died — before the exception continues.
  """
  @spec span(atom(), map() | (term() -> map()), (-> result)) :: result when result: var
  def span(name, meta \\ %{}, fun)

  def span(name, meta, fun) when is_map(meta),
    do: Telemetry.span(@event, Map.put(meta, :phase, name), fun)

  def span(name, describe, fun) when is_function(describe, 1),
    do: Telemetry.span(@event, &{%{}, Map.put(describe.(&1), :phase, name)}, fun)

  @doc """
  Attaches a collector for the job whose runner is `owner`.

  The caller (the runner) owns the returned collector's table; spans emitted
  by `owner` or any process it called into are recorded from this moment on.
  """
  @spec attach(String.t(), pid()) :: collector()
  def attach(job_id, owner) do
    id = {__MODULE__, job_id}
    table = :ets.new(__MODULE__, [:public, :duplicate_bag])

    :telemetry.attach(id, @event, &__MODULE__.handle_event/4, %{
      id: id,
      table: table,
      owner: owner
    })

    %{id: id, table: table}
  end

  @doc false
  @spec handle_event([atom()], map(), map(), map()) :: :ok
  def handle_event(@event, measurements, meta, %{id: id, table: table, owner: owner}) do
    cond do
      self() == owner or owner in Process.get(:"$callers", []) ->
        :ets.insert(
          table,
          {measurements.start_us, measurements.duration_us, Map.fetch!(meta, :phase),
           Map.delete(meta, :phase)}
        )

      Process.alive?(owner) ->
        :ok

      true ->
        :telemetry.detach(id)
    end

    :ok
  end

  @doc """
  Detaches `collector` and returns its spans, earliest first, with start
  offsets rebased to the earliest span.
  """
  @spec stop(collector()) :: [span()]
  def stop(%{id: id, table: table}) do
    :telemetry.detach(id)

    spans = :ets.tab2list(table)
    :ets.delete(table)

    case Enum.sort(spans) do
      [] ->
        []

      [{origin, _duration, _phase, _meta} | _rest] = ordered ->
        for {started, duration, phase, meta} <- ordered do
          %{name: phase, start_us: started - origin, duration_us: duration, meta: meta}
        end
    end
  end
end
