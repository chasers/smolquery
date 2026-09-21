Code.require_file("support.exs", __DIR__)

defmodule Bench.ClickHouseCatalog do
  @moduledoc """
  What a catalog statement costs on the ClickHouse edge as the lake gets
  more tables (T-529).

  HyperDX sends eleven `system.*` statements for one page. Each runs through
  `SmolqueryClickHouse.SystemCatalog` in turn, so what one costs is what
  every one behind it waits for, and an empty lake shows none of it: the
  cost that matters is the rebuild of the `system` tables, a catalog read for
  every dataset and two for every table.

  Each row is one statement against a real DuckLake of `TABLES` tables:

  - **idle** — the statement after the edge has been idle for longer than its
    check interval, which is when it asks the catalog anything;
  - **busy** — the statement straight after another;
  - **page** — eleven at once after an idle, as one page sends them: the
    slowest, which is when the page is done.

      mix run bench/clickhouse_catalog.exs
      TABLES=1,32,256 mix run bench/clickhouse_catalog.exs
  """

  import Bench.Support

  alias Smolquery.Catalog
  alias SmolqueryClickHouse.SystemCatalog

  @idle_ms 1_100
  @page 11

  @statements [
    {"system.settings", "SELECT name, value FROM system.settings"},
    {"DESCRIBE", "DESCRIBE analytics.events"}
  ]

  def main do
    heading("ClickHouse edge catalog statements by lake depth — p50 ms")

    IO.puts(
      label("tables", 8) <>
        label("statement", 18) <> pad("idle", 10) <> pad("busy", 10) <> pad("page", 10)
    )

    for tables <- sweep_env("TABLES", [1, 8, 32, 128]), do: depth(tables)
  end

  defp depth(tables) do
    with_tmp_dir("clickhouse-catalog", fn dir ->
      edge = :"bench_clickhouse_catalog_#{tables}"
      catalog = start_lake!(:"#{edge}_lake", dir)

      for n <- 2..tables//1,
          do: :ok = Catalog.create_table(catalog, {"analytics", "t#{n}"}, schema())

      {:ok, supervisor} =
        SmolqueryClickHouse.Supervisor.start_link(
          name: edge,
          password: "bench",
          port: 0,
          catalog: catalog
        )

      for {name, sql} <- @statements, do: row(edge, tables, name, sql)

      Supervisor.stop(supervisor)
    end)
  end

  defp row(edge, tables, name, sql) do
    answer(edge, sql)

    idle = for _rep <- 1..5, do: after_idle(fn -> answer(edge, sql) end)
    busy = for _rep <- 1..20, do: answer(edge, sql)

    page =
      after_idle(fn ->
        1..@page
        |> Task.async_stream(fn _n -> answer(edge, sql) end,
          max_concurrency: @page,
          timeout: :infinity
        )
        |> Enum.map(fn {:ok, us} -> us end)
        |> Enum.max()
      end)

    IO.puts(
      label(tables, 8) <>
        label(name, 18) <>
        pad(percentiles(idle).p50, 10) <>
        pad(percentiles(busy).p50, 10) <> pad(ms(page), 10)
    )
  end

  defp after_idle(fun) do
    Process.sleep(@idle_ms)
    fun.()
  end

  defp answer(edge, sql) do
    {us, {:ok, _frame}} = :timer.tc(fn -> SystemCatalog.answer(edge, sql, "default") end)
    us
  end
end

Bench.ClickHouseCatalog.main()
