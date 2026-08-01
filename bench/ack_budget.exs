Code.require_file("support.exs", __DIR__)

defmodule Bench.AckBudget do
  @moduledoc """
  The PL-9 exit criterion: does the ack budget bound the latency T-53 could
  not?

  Reproduces the overload that measured a 2.01 s p50 ack — a 20-column event
  table at the default config, hundreds of synchronous writers — and re-runs
  it at `ack_budget_ms` ∈ {infinity, 1000, 250}. Writers do not retry; every
  attempt is recorded as accepted (with its ack latency) or shed (with the
  prediction it was refused with). One write warms the rate estimate before
  the storm — a cold meter admits by design (PL-9 D3), and what this measures
  is the steady state, not the first flush of a fresh buffer's life.

  Pass = accepted-write p50 lands at or under the budget, throughput holds
  (shedding must not cost the fast path), and shed writes carry a prediction
  a client could sleep on.

      mix run bench/ack_budget.exs
      WRITERS=512 CALLS=10 ROWS=2000 mix run bench/ack_budget.exs
  """

  import Bench.Support

  alias Smolquery.BufferService
  alias Smolquery.BufferService.Client
  alias Smolquery.Schema

  @table {"analytics", "events"}

  def main do
    Logger.configure(level: :warning)

    writers = env("WRITERS", 512)
    calls = env("CALLS", 10)
    batch_rows = env("ROWS", 2_000)

    batch = %{schema: schema20(), rows: rows(batch_rows)}

    heading(
      "Ack under overload — 20-col schema, #{writers} writers × #{calls} calls × " <>
        "#{batch_rows} rows, default flush config"
    )

    IO.puts(
      label("budget", 10) <>
        pad("accepted", 10) <>
        pad("shed %", 8) <>
        pad("krows/s", 9) <>
        pad("p50 ms", 8) <> pad("p95 ms", 8) <> pad("p99 ms", 8) <> pad("shed p50 ms", 13)
    )

    for budget <- [:infinity, 1_000, 250] do
      run(budget, writers, calls, batch, batch_rows)
    end

    IO.puts("")
  end

  defp schema20 do
    extra =
      for i <- 1..16 do
        if rem(i, 2) == 0, do: {"f#{i}", :float64}, else: {"s#{i}", :string}
      end

    Schema.new!([
      {"id", :int64, nullable: false},
      {"ts", :timestamp},
      {"name", :string},
      {"amount", {:numeric, 38, 2}} | extra
    ])
  end

  defp rows(count) do
    base = ~N[2026-01-01 00:00:00]

    for i <- 1..count do
      schema20().fields
      |> Map.new(fn field -> {field.name, value(field.name, field.type, base, i)} end)
    end
  end

  defp value(_name, :int64, _base, i), do: i
  defp value(_name, :timestamp, base, i), do: NaiveDateTime.add(base, i, :second)
  defp value(name, :string, _base, i), do: "#{name}-value-#{i}"
  defp value(_name, :float64, _base, i), do: i * 1.5

  defp value(_name, {:numeric, _p, _s}, _base, i),
    do: Decimal.new("#{rem(i, 997)}.#{rem(i, 100)}")

  defp run(budget, writers, calls, batch, batch_rows) do
    with_tmp_dir("ack-budget", fn dir ->
      name = :"ack_budget_#{System.unique_integer([:positive])}"

      {:ok, supervisor} =
        BufferService.Supervisor.start_link(
          name: name,
          dir: dir,
          hot_server_port: 0,
          ack_budget_ms: budget,
          seal_max_bytes: 1_000_000_000,
          seal_max_files: 1_000_000,
          seal_max_age_ms: 3_600_000,
          seal_consumer: {BufferService.SealLog, []}
        )

      {:ok, _warm} = Client.write_batch(name, @table, batch)

      {wall_us, outcomes} =
        :timer.tc(fn ->
          1..writers
          |> Task.async_stream(
            fn _writer ->
              for _call <- 1..calls do
                started = System.monotonic_time(:millisecond)

                case Client.write_batch(name, @table, batch) do
                  {:ok, _ack} ->
                    {:ack, System.monotonic_time(:millisecond) - started}

                  {:error, {:overloaded, predicted}} ->
                    {:shed, predicted}

                  {:error, reason} ->
                    {:error, reason}
                end
              end
            end,
            max_concurrency: writers,
            timeout: 600_000,
            ordered: false
          )
          |> Enum.flat_map(fn {:ok, results} -> results end)
        end)

      report(budget, outcomes, batch_rows, wall_us)

      Supervisor.stop(supervisor)
      BufferService.Runtime.delete(name)
    end)
  end

  defp report(budget, outcomes, batch_rows, wall_us) do
    acks = for {:ack, latency} <- outcomes, do: latency
    sheds = for {:shed, predicted} <- outcomes, do: predicted
    accepted = length(acks)
    total = length(outcomes)
    krows = Float.round(accepted * batch_rows / (wall_us / 1_000_000) / 1_000, 1)
    shed_pct = Float.round(length(sheds) * 100 / total, 1)

    IO.puts(
      label(inspect(budget), 10) <>
        pad(accepted, 10) <>
        pad(shed_pct, 8) <>
        pad(krows, 9) <>
        pad(percentile(acks, 50), 8) <>
        pad(percentile(acks, 95), 8) <>
        pad(percentile(acks, 99), 8) <> pad(percentile(sheds, 50), 13)
    )
  end

  defp percentile([], _p), do: "-"

  defp percentile(values, p) do
    sorted = Enum.sort(values)

    Enum.at(sorted, min(div(length(sorted) * p, 100), length(sorted) - 1))
  end
end

Bench.AckBudget.main()
