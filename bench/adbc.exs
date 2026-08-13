Code.require_file("support.exs", __DIR__)

defmodule Bench.Adbc do
  @moduledoc """
  What ADBC costs: connecting, returning a large result, and serving concurrency.

  This is the measurement T-11 was decided on, and the baseline any second engine
  (T-15) has to be compared against. Three questions:

    * **Cold-connection latency** — what a connection pool would amortize. Nearly
      all of it turns out to be extension loading and `ATTACH`, not ADBC.
    * **Large results** — `Engine.query` converts Arrow to Elixir terms, which
      costs about a kilobyte and a couple of microseconds per row. `Engine.frame`,
      which hands the Arrow stream to Polars, does not — including all the way out
      to serialized Parquet bytes.
    * **Concurrency** — one `Engine.Connection` is a per-query mutex, so query
      throughput is flat in client count until there are more connections.

  Rows come from `range()` rather than Parquet so this measures fetch and
  materialization, not scanning.

      mix run bench/adbc.exs
      ROWS=10000000 CLIENTS=16 mix run bench/adbc.exs

  """

  import Bench.Support

  alias Explorer.DataFrame
  alias Smolquery.Catalog.DuckLake
  alias Smolquery.Engine
  alias Smolquery.Engine.Connection

  @engine __MODULE__.Lake

  def run do
    with_tmp_dir("adbc", fn dir ->
      cold_start(dir)

      catalog_dir = Path.join(dir, "lake")
      File.mkdir_p!(catalog_dir)
      start_lake!(@engine, catalog_dir)

      large_results()
      concurrency()
    end)
  end

  defp cold_start(dir) do
    heading("cold-connection latency: what a pool would amortize")
    IO.puts("(extensions already installed on disk, so this is the steady-state cost)\n")

    {db_us, {:ok, database}} =
      :timer.tc(fn -> Smolquery.DuckDB.start_link() end)

    IO.puts("  Smolquery.DuckDB.start_link       #{pad(ms(db_us), 7)} ms")

    {conn_us, {:ok, conn}} = :timer.tc(fn -> Adbc.Connection.start_link(database: database) end)
    IO.puts("  Adbc.Connection.start_link (bare) #{pad(ms(conn_us), 7)} ms")

    for extension <- [:httpfs, :json, :ducklake] do
      {us, _result} =
        :timer.tc(fn ->
          Adbc.Connection.query(conn, "INSTALL #{extension}")
          Adbc.Connection.query(conn, "LOAD #{extension}")
        end)

      IO.puts("  INSTALL + LOAD #{label(extension, 19)}#{pad(ms(us), 7)} ms")
    end

    attach =
      DuckLake.attach_statement(
        "cold",
        "sqlite:#{Path.join(dir, "cold.sqlite")}",
        Path.join(dir, "cold")
      )

    {attach_us, _result} = :timer.tc(fn -> Adbc.Connection.query(conn, attach) end)
    IO.puts("  ATTACH ducklake catalog           #{pad(ms(attach_us), 7)} ms")

    GenServer.stop(conn)
    GenServer.stop(database)

    full = timed(fn -> start_and_stop_engine(dir) end, 4)

    IO.puts(
      "\n  full engine start, end to end     #{pad(ms(full.min), 7)} ms min, #{ms(full.median)} ms median"
    )

    IO.puts("  a pool pays this once per connection instead of once per query")
  end

  defp start_and_stop_engine(dir) do
    name = Module.concat(__MODULE__, "Cold#{System.unique_integer([:positive])}")
    path = Path.join(dir, "cold-#{System.unique_integer([:positive])}")

    {:ok, pid} =
      DuckLake.start_link(
        name: name,
        metadata: "sqlite:#{path}.sqlite",
        data_path: path
      )

    Supervisor.stop(pid)
  end

  defp large_results do
    heading("large-result fetch: what it costs to get N rows out of DuckDB")
    max_rows = env("ROWS", 5_000_000)
    adbc = Connection.adbc_connection(Engine.connection_name(@engine))

    IO.puts("\n  rows    path                            wall            BEAM Δ    bytes/row")

    for rows <- Enum.filter([100_000, 1_000_000, 5_000_000, 10_000_000], &(&1 <= max_rows)) do
      sql = generator(rows)

      measure(rows, "Engine.query → Elixir terms", fn -> Engine.query!(@engine, sql).num_rows end)

      measure(rows, "Adbc.Connection.query (Arrow)", fn ->
        {:ok, result} = Adbc.Connection.query(adbc, sql)
        result.num_rows
      end)

      measure(rows, "Engine.frame → DataFrame", fn ->
        @engine |> Engine.frame!(sql) |> DataFrame.n_rows()
      end)

      measure(rows, "Engine.frame → Parquet bytes", fn ->
        @engine |> Engine.frame!(sql) |> DataFrame.dump_parquet!() |> byte_size()
      end)

      measure(rows, "count(*) only (baseline)", fn ->
        Engine.query!(@engine, "SELECT count(*) FROM (#{sql})").num_rows
      end)

      IO.puts("")
    end

    time_to_first_row(adbc, max_rows)
  end

  defp generator(rows) do
    "SELECT i AS id, TIMESTAMP '2026-01-01 00:00:00' + INTERVAL (i) SECOND AS ts, " <>
      "'row-' || i AS name, i * 1.5 AS amount FROM range(#{rows}) t(i)"
  end

  defp measure(rows, name, fun) do
    :erlang.garbage_collect()
    before = :erlang.memory(:total)
    {us, _result} = :timer.tc(fun)
    delta = :erlang.memory(:total) - before
    :erlang.garbage_collect()

    IO.puts(
      "  #{label(count(rows), 8)}#{label(name, 32)}#{pad(ms(us), 9)} ms " <>
        "#{pad(mib(delta), 9)} MiB #{pad(Float.round(delta / rows, 1), 12)}"
    )
  end

  defp time_to_first_row(adbc, rows) do
    heading("time to first row: does anything stream, or is it all-or-nothing?")
    sql = generator(rows)

    {full_us, _result} = :timer.tc(fn -> Engine.query!(@engine, sql) end)
    {limit_us, _result} = :timer.tc(fn -> Engine.query!(@engine, sql <> " LIMIT 1") end)

    {pointer_us, _result} =
      :timer.tc(fn ->
        Adbc.Connection.query_pointer(adbc, sql, [], fn %Adbc.StreamResult{} = stream ->
          stream.num_rows
        end)
      end)

    IO.puts("  Engine.query!, all #{count(rows)} rows        #{pad(ms(full_us), 9)} ms")
    IO.puts("  the same query with LIMIT 1           #{pad(ms(limit_us), 9)} ms")
    IO.puts("  query_pointer, handing back a stream  #{pad(ms(pointer_us), 9)} ms")
    IO.puts("\n  A stream exists, but consuming it means handing the pointer to something")
    IO.puts("  that reads Arrow natively. There is no cheap path to Elixir terms.")
  end

  defp concurrency do
    heading("does one connection serialize? the pooling argument")
    max_clients = env("CLIENTS", 8)
    counts = Enum.filter([1, 2, 4, 8, 16], &(&1 <= max_clients))
    sql = generator(200_000)

    IO.puts("\n  clients   one shared connection      one connection each")

    engines =
      for index <- 1..max_clients do
        name = Module.concat(__MODULE__, "Pooled#{index}")
        {:ok, _pid} = Engine.start_link(name: name, max_result_rows: :infinity)
        name
      end

    for clients <- counts do
      shared = race(List.duplicate(@engine, clients), sql)
      pooled = race(Enum.take(engines, clients), sql)

      IO.puts(
        "  #{label(clients, 10)}#{pad(ms(shared), 7)} ms total          #{pad(ms(pooled), 7)} ms total" <>
          "   (#{ms(div(pooled, clients))} ms/query)"
      )
    end
  end

  defp race(targets, sql) do
    {us, _results} =
      :timer.tc(fn ->
        targets
        |> Task.async_stream(&Engine.query!(&1, sql).num_rows,
          max_concurrency: length(targets),
          timeout: :infinity
        )
        |> Enum.to_list()
      end)

    us
  end

  defp count(rows) when rows >= 1_000_000, do: "#{div(rows, 1_000_000)}M"
  defp count(rows), do: "#{div(rows, 1000)}k"
end

Bench.Adbc.run()
