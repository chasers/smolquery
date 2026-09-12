defmodule Smolquery.Allocator do
  @moduledoc """
  Keeps DuckDB's allocator returning freed pages for the whole node (T-452).

  `allocator_background_threads` is what makes DuckDB's jemalloc give back
  the pages an encode or a merge freed — without it a buffer node settled
  1.1–1.5 GiB above its start after a burst of encodes that `duckdb_memory()`
  never saw (bench/results/buffer.md). But the flag is one per OS process, and
  libduckdb owns it badly for a process with many instances: opening an
  instance applies *that instance's* configured value to the whole process,
  and closing any instance forces it off for the whole process (verified
  against 1.5.3 through jemalloc's own `mallctl`). A query pod
  opens and closes a job engine per query, a buffer pod a probe engine per
  schema change, so a `SET` at boot would be undone within seconds.

  Two things close that gap. Every instance opens with the node's value as a
  database option (`Smolquery.DuckDB.start_link/1`), so an open re-arms it;
  and this process holds one small in-memory instance of its own and
  re-asserts the flag every `interval_ms`, so a close disarms it for at most
  that long. It is `:ignore` when the node runs with the flag off — there is
  nothing to keep on.
  """

  use GenServer

  alias Smolquery.DuckDB
  alias Smolquery.Engine.Connection

  @default_interval_ms 1_000

  @doc false
  @spec start_link(keyword()) :: GenServer.on_start() | :ignore
  def start_link(opts \\ []) do
    config = Keyword.merge(Application.get_env(:smolquery, __MODULE__, []), opts)

    if Keyword.get(config, :enabled, DuckDB.allocator_background_threads?()) do
      GenServer.start_link(__MODULE__, config, name: Keyword.get(config, :name, __MODULE__))
    else
      :ignore
    end
  end

  @impl GenServer
  def init(config) do
    {:ok, database} = DuckDB.start_link()
    {:ok, connection} = Connection.start_link(database: database, extensions: [])

    state = %{
      connection: connection,
      interval_ms: Keyword.get(config, :interval_ms, @default_interval_ms)
    }

    {:ok, state |> assert_flag() |> schedule()}
  end

  @impl GenServer
  def handle_info(:assert, state), do: {:noreply, state |> assert_flag() |> schedule()}

  defp assert_flag(state) do
    {:ok, _result} =
      Connection.query(state.connection, "SET allocator_background_threads = true", [], 5_000)

    state
  end

  defp schedule(state) do
    Process.send_after(self(), :assert, state.interval_ms)

    state
  end
end
