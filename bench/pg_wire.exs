Code.require_file("support.exs", __DIR__)
Code.require_file("../test/support/fixed_catalog.ex", __DIR__)
Code.require_file("../test/support/map_catalog.ex", __DIR__)
Code.require_file("../test/support/pg_client.ex", __DIR__)

defmodule Bench.PgWire do
  @moduledoc """
  How many extended-protocol queries per second the Postgres wire edge
  answers (PL-58).

  Every query is a full `Smolquery.QueryService` job — engine checkout,
  plan, execute, frame — so this measures the edge's protocol overhead *on
  top of* the job floor `bench/query.exs` priced, and how far concurrent
  connections scale past one caller's floor.

  The fleets default to 1, 4, and 8 connections — 8 is the query
  service's own `max_concurrent_jobs` default. The bound gets `+ 4` of
  headroom over the fleet so a describe-and-execute pair straddling two
  connections is never shed: shedding is not what this measures. The job
  engines load no extensions: this bench has no hot tier to read, and
  concurrent extension `LOAD`s across starting engines abort the VM in
  DuckDB's signature check (tracked separately) — also not what this
  measures.

  Three shapes, each at 1, `CONNS_MID`, and `CONNS` connections:

    * **Postgrex, no parameters** — `Postgrex.query!(conn, "SELECT 1", [])`.
      The driver sends Parse/Describe/Bind/Execute/Sync; the edge runs the
      job once at Describe and the portal serves the cached rows: one job
      per query.
    * **Postgrex, two parameters** — `SELECT $1::bigint + $2::bigint`.
      Describe must type the columns before values exist, so the edge runs
      a describe job and then the real job: two jobs per query.
    * **Raw portal** — the test client's Bind/Describe(portal)/Execute/Sync
      without a statement describe: one job per query, no driver overhead.

      SMOLQUERY_ROLES=query mix run bench/pg_wire.exs
      CONNS=64 REPS=200 mix run bench/pg_wire.exs
  """

  import Bench.Support

  alias Smolquery.QueryService
  alias Smolquery.Test.FixedCatalog
  alias Smolquery.Test.MapCatalog
  alias Smolquery.Test.PgClient

  @password "bench"

  def main do
    Logger.configure(level: :warning)

    reps = env("REPS", 100)
    conns_mid = env("CONNS_MID", 4)
    conns = env("CONNS", 8)

    query = start_query!(conns)
    port = start_edge!(query)

    heading(
      "Extended-protocol queries per second — port #{port}, " <>
        "#{reps} queries per connection, fleets of 1 / #{conns_mid} / #{conns}"
    )

    IO.puts(
      label("shape", 26) <>
        pad("conns", 6) <>
        pad("qps", 9) <> pad("p50 ms", 9) <> pad("p95 ms", 9) <> pad("p99 ms", 9)
    )

    for fleet <- [1, conns_mid, conns] do
      run("postgrex SELECT 1", fleet, reps, fn -> postgrex_worker(port, "SELECT 1", []) end)
    end

    for fleet <- [1, conns_mid, conns] do
      run(
        "postgrex 2 params",
        fleet,
        reps,
        fn -> postgrex_worker(port, "SELECT $1::bigint + $2::bigint AS sum", [40, 2]) end
      )
    end

    for fleet <- [1, conns_mid, conns] do
      run("raw portal, no describe", fleet, reps, fn -> raw_worker(port) end)
    end

    IO.puts("")
  end

  defp run(shape, fleet, reps, worker) do
    started = System.monotonic_time(:microsecond)

    samples =
      1..fleet
      |> Task.async_stream(fn _connection -> worker.().(reps) end,
        max_concurrency: fleet,
        timeout: 300_000
      )
      |> Enum.flat_map(fn {:ok, samples} -> samples end)

    elapsed_us = System.monotonic_time(:microsecond) - started
    qps = Float.round(length(samples) * 1_000_000 / elapsed_us, 1)
    sorted = Enum.sort(samples)

    IO.puts(
      label(shape, 26) <>
        pad(fleet, 6) <>
        pad(qps, 9) <>
        pad(to_ms(percentile(sorted, 0.50)), 9) <>
        pad(to_ms(percentile(sorted, 0.95)), 9) <> pad(to_ms(percentile(sorted, 0.99)), 9)
    )
  end

  defp to_ms(us), do: Float.round(us / 1_000, 2)

  defp percentile(sorted, fraction) do
    index = min(round(fraction * length(sorted)), length(sorted) - 1)

    Enum.at(sorted, index)
  end

  defp postgrex_worker(port, sql, params) do
    {:ok, conn} =
      Postgrex.start_link(
        hostname: "127.0.0.1",
        port: port,
        username: "bench",
        password: @password,
        database: "smolquery"
      )

    Postgrex.query!(conn, sql, params)

    fn reps ->
      for _rep <- 1..reps do
        started = System.monotonic_time(:microsecond)
        Postgrex.query!(conn, sql, params)

        System.monotonic_time(:microsecond) - started
      end
    end
  end

  defp raw_worker(port) do
    {:ok, socket, _params} = PgClient.connect(port, password: @password)
    %{errors: []} = PgClient.extended(socket, "SELECT 1", [])

    fn reps ->
      for _rep <- 1..reps do
        started = System.monotonic_time(:microsecond)
        %{errors: []} = PgClient.extended(socket, "SELECT 1", [])

        System.monotonic_time(:microsecond) - started
      end
    end
  end

  defp start_query!(conns) do
    name = __MODULE__.Query

    {:ok, _pid} =
      QueryService.Supervisor.start_link(
        name: name,
        catalog: FixedCatalog.new(%{snapshot: 1, schemas: %{}, segments: %{}}),
        engine_extensions: [],
        max_concurrent_jobs: conns + 4
      )

    name
  end

  defp start_edge!(query) do
    {:ok, _pid} =
      SmolqueryPg.Supervisor.start_link(
        name: __MODULE__.Edge,
        password: @password,
        auth: :cleartext,
        query_name: query,
        port: 0,
        catalog: MapCatalog.new()
      )

    {:ok, {_ip, port}} = SmolqueryPg.Supervisor.bound(__MODULE__.Edge)

    port
  end
end

Bench.PgWire.main()
