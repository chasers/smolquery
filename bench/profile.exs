Code.require_file("support.exs", __DIR__)
Code.require_file("otel_support.exs", __DIR__)

defmodule Bench.Profile do
  @moduledoc """
  Where the BEAM's CPU time goes under ingest load, from inside the VM.

  `bench/otel_logs.exs` prices the ingest path in rows and milliseconds; this
  script asks a different question about the same load: which threads are busy,
  which processes burn the reductions, and what *kind* of work each thread
  class is doing. It is the first stop for "why is the beam hot" — reaching
  for perf or eBPF in a Linux VM is only warranted once the answer here is
  "inside `emulator` time", the one bucket no in-VM view can open (JIT-compiled
  Erlang frames and NIF internals).

  Three views over one measured window, all OTP built-ins from
  `runtime_tools`. The `Mix.ensure_application!/1` call is load-bearing: Mix
  prunes unused OTP applications from the code path, so without it `:msacc`
  and `:scheduler` are not loadable under `mix run`.

    * **scheduler utilization** (`:scheduler.utilization/2`) — how busy each
      thread is, split by class. Normal schedulers run Erlang code (Bandit,
      JSON decode, validation); dirty CPU schedulers run the Polars encode
      and offloaded major GCs; dirty IO schedulers run file operations.
    * **reductions top** — which processes did the work. The bench's own
      writers share the VM and cost about as much as the connection handlers
      they drive, so they are labeled rather than filtered: their share *is*
      a finding about every single-node script in this directory.
    * **msacc** (`:msacc.print/0`) — per-thread microstates: `emulator`, `gc`,
      `port`, `sleep`. A stock OTP build carries only the basic states, so
      NIF execution lands inside `emulator`; the thread-class × state split
      is still the signal. The first run of this script showed dirty CPU
      schedulers spending 6× more time in `gc` than `emulator` — major
      collections of large decoded-JSON heaps, not the Polars encode — which
      is exactly the kind of misattribution this view exists to catch.

  This is a diagnostic, not a decision benchmark: it settles nothing by
  itself, so it has no `bench/results/` file. Its output is read at the commit
  it was run against, next to the change being diagnosed.

      mix run bench/profile.exs 2>/dev/null
      WRITERS=32 BATCH=4000 SECONDS=20 mix run bench/profile.exs 2>/dev/null

  `WRITERS` concurrent writers each loop `BATCH`-row inserts; `SECONDS` is the
  measured window and `WARMUP_MS` the settle time before it. Redirecting
  stderr is not cosmetic: ADBC's Explorer callback deprecation warns once per
  query.
  """

  import Bench.Otel
  import Bench.Support, except: [table: 0, schema: 0]

  def main do
    Mix.ensure_application!(:runtime_tools)
    {:ok, _apps} = Application.ensure_all_started(:runtime_tools)
    Logger.configure(level: :warning)

    config = %{
      writers: env("WRITERS", 8),
      batch: env("BATCH", 2_000),
      window_ms: env("SECONDS", 10) * 1_000,
      warmup_ms: env("WARMUP_MS", 3_000)
    }

    dir =
      Path.join(
        System.tmp_dir!(),
        "smolquery-profile-#{System.unique_integer([:positive])}"
      )

    try do
      req = boot!(dir)
      create_tables!(req, 1)
      pool = pool()

      heading(
        "BEAM CPU profile — #{config.writers} writers × #{config.batch}-row batches " <>
          "against #{base_url()}, #{config.warmup_ms} ms warmup, " <>
          "#{config.window_ms} ms measured window"
      )

      schedulers()

      writers = start_writers(req, pool, config)
      Process.sleep(config.warmup_ms)

      before_reds = reductions_snapshot()
      before_sched = :scheduler.sample_all()

      :msacc.start(config.window_ms)

      after_sched = :scheduler.sample_all()
      after_reds = reductions_snapshot()

      rows = Enum.reduce(writers, 0, fn writer, acc -> acc + stop_writer(writer) end)

      heading("Rows acked during run")
      IO.puts("  #{rows} (#{div(rows * 1_000, config.window_ms + config.warmup_ms)} rows/s)")

      heading("Scheduler utilization over the window")
      print_utilization(:scheduler.utilization(before_sched, after_sched))

      heading("Top processes by reductions over the window")
      print_reduction_top(before_reds, after_reds, MapSet.new(writers))

      heading("msacc — microstate accounting over the window")
      :msacc.print()
    after
      teardown!(dir)
    end
  end

  defp start_writers(req, pool, config) do
    parent = self()

    for writer <- 1..config.writers do
      spawn_link(fn -> write_loop(parent, req, pool, config.batch, writer, 0) end)
    end
  end

  defp write_loop(parent, req, pool, batch, writer, rows) do
    receive do
      :stop -> send(parent, {:done, self(), rows})
    after
      0 ->
        payload = body(pool, batch, rows + writer)

        result =
          Req.post(req,
            url: "/v1/datasets/#{dataset()}/tables/#{table()}/insert",
            headers: [{"content-type", "application/json"}],
            body: payload
          )

        rows =
          case result do
            {:ok, %{status: 200, body: response_body}} ->
              rows + response_body["insertedRows"]

            {:ok, _response} ->
              rows

            {:error, _reason} ->
              Process.sleep(200)
              rows
          end

        write_loop(parent, req, pool, batch, writer, rows)
    end
  end

  defp stop_writer(pid) do
    send(pid, :stop)

    receive do
      {:done, ^pid, rows} -> rows
    after
      10_000 -> 0
    end
  end

  defp reductions_snapshot do
    for pid <- Process.list(),
        {:reductions, reds} <- [Process.info(pid, :reductions)],
        into: %{},
        do: {pid, reds}
  end

  defp print_reduction_top(before_reds, after_reds, writers) do
    after_reds
    |> Enum.map(fn {pid, reds} -> {pid, reds - Map.get(before_reds, pid, 0)} end)
    |> Enum.sort_by(fn {_pid, delta} -> -delta end)
    |> Enum.take(12)
    |> Enum.each(fn {pid, delta} ->
      IO.puts("  #{pad(format_reds(delta), 10)}  #{describe(pid, writers)}")
    end)
  end

  defp format_reds(delta) when delta >= 1_000_000, do: "#{Float.round(delta / 1_000_000, 1)}M"
  defp format_reds(delta) when delta >= 1_000, do: "#{div(delta, 1_000)}k"
  defp format_reds(delta), do: "#{delta}"

  defp describe(pid, writers) do
    cond do
      MapSet.member?(writers, pid) ->
        "bench writer (this script's load generator)"

      info =
          Process.info(pid, [:registered_name, :dictionary, :initial_call, :current_function]) ->
        {mod, fun, arity} = info[:current_function]
        "#{name(info)}  now in #{inspect(mod)}.#{fun}/#{arity}"

      true ->
        "#{inspect(pid)} (exited)"
    end
  end

  defp name(info) do
    case info[:registered_name] do
      [] -> initial_call(info)
      registered -> inspect(registered)
    end
  end

  defp initial_call(info) do
    case List.keyfind(info[:dictionary], :"$initial_call", 0) do
      {_key, {mod, fun, arity}} -> "#{inspect(mod)}.#{fun}/#{arity}"
      nil -> inspect(info[:initial_call])
    end
  end

  defp print_utilization(rows) do
    Enum.each(rows, fn
      {:total, _value, text} -> IO.puts("  #{label("total", 12)} #{text}")
      {:weighted, _value, text} -> IO.puts("  #{label("weighted", 12)} #{text}")
      {kind, id, _value, text} -> IO.puts("  #{label("#{kind} #{id}", 12)} #{text}")
    end)
  end
end

Bench.Profile.main()
