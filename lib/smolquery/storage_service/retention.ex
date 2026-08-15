defmodule Smolquery.StorageService.Retention do
  @moduledoc """
  Drops whole sealed segments once every row in them has aged out.

  A table opts in with a policy (`Smolquery.Catalog.retention/2`): a column
  the age is measured against, and a TTL. Nothing here deletes rows — a
  `DELETE ... WHERE ts < horizon` would rewrite files smolquery treats as
  immutable — so the unit of expiry is the segment, judged by its Parquet
  footer stats: a segment is dropped only when the *maximum* of the policy
  column across its rows has passed the horizon. A segment straddling the
  boundary keeps all its rows until it ages out entirely, which makes
  retention conservative by construction and `drop_segments/3` — already
  exactly-once — the whole mechanism.

  Conservative extends to ignorance: a file whose stats for the policy column
  are missing, unparseable, or absent (a segment written before the column
  existed) is kept, never dropped. Retention that guesses is deletion.

  The stats query is chunked by `merge_inputs_per_call` (T-244), so no engine
  call carries an unbounded `parquet_metadata` input list no matter how many
  current segments a table holds.

  ## Expiry is the second half, and it runs here for every drop, not just ours

  A dropped segment is invisible to new queries and still referenced by every
  snapshot that predates the drop, so GC spares it — correctly — until those
  snapshots expire. Each sweep therefore ends with
  `Catalog.expire_snapshots/2` at `snapshot_keep_ms`, which is what lets GC's
  membership test finally say no, for retention's drops and compaction's
  alike. The T-14 spike verdict lives in the DuckLake tests: expiry is sound
  over externally-registered files — a pinned read of an expired snapshot
  fails cleanly, `known_segments/1` stops listing files only expired
  snapshots referenced, and the current snapshot is never expired.

  `snapshot_keep_ms` is the deployment's time-travel promise: it must exceed
  the longest query a reader may still have pinned, the same coupling
  `retire_grace_ms` states for the hot tier.

  ## Level-triggered by looking, like the compactor

  Policies and segments both live in the catalog, so a sweep reads and acts;
  a failed table costs nothing but waiting for the next sweep, and there is
  deliberately no per-table state here to keep honest. And like the
  compactor, each table is gated on `Smolquery.StorageService.Routing.own?/2`
  (Milestone 8 L6): the catalog hands every storage node the same table list,
  and without the gate an N-node fleet runs N identical drops per interval,
  N-1 of which exist only to lose a commit conflict and burn its retry
  backoff. Snapshot expiry stays ungated — it is one catalog-wide call, not
  per-table work, and a duplicate expiry is a no-op.
  """

  alias Smolquery.Catalog
  alias Smolquery.Engine
  alias Smolquery.StorageService.Routing
  alias Smolquery.StorageService.Runtime

  @enforce_keys [:runtime]
  defstruct [:runtime]

  use Smolquery.StorageService.Sweeper, interval: :retention_interval_ms

  require Logger

  @doc """
  Starts the retention sweeper for a runtime.
  """
  @spec start_link(Runtime.t()) :: GenServer.on_start()
  def start_link(%Runtime{} = runtime) do
    GenServer.start_link(__MODULE__, runtime, name: Runtime.retention(runtime.name))
  end

  @doc """
  Sweeps now, without waiting for the interval.

  Reports what was dropped per table, how many snapshots expired, and what
  failed.
  """
  @spec sweep(atom(), timeout()) :: {:ok, map()} | {:error, term()}
  def sweep(name, timeout \\ 60_000), do: GenServer.call(Runtime.retention(name), :sweep, timeout)

  defp run(state) do
    with {:ok, tables} <- Catalog.tables(state.runtime.catalog) do
      outcomes = Enum.map(tables, &sweep_table(state.runtime, &1))

      report = %{
        dropped: for({:ok, drop} <- outcomes, do: drop),
        failed: for({:failed, failure} <- outcomes, do: failure),
        expired_snapshots: expire(state.runtime)
      }

      :telemetry.execute(
        [:smolquery, :retention, :sweep],
        %{
          dropped: Enum.sum_by(report.dropped, &length(&1.dropped)),
          expired_snapshots: report.expired_snapshots
        },
        %{}
      )

      {:ok, report}
    end
  end

  defp sweep_table(runtime, table_ref) do
    with true <- runtime.name |> Routing.resolve() |> Routing.own?(table_ref),
         {:ok, %{column: column, ttl_ms: ttl_ms}} <-
           Catalog.retention(runtime.catalog, table_ref),
         {:ok, paths} when paths != [] <- Catalog.segments(runtime.catalog, table_ref, :current),
         {:ok, expired} <- expired_paths(runtime, paths, column, ttl_ms) do
      drop(runtime, table_ref, expired)
    else
      false -> :skip
      {:ok, nil} -> :skip
      {:ok, []} -> :skip
      {:error, reason} -> failed(table_ref, reason)
    end
  end

  defp drop(_runtime, _table_ref, []), do: :skip

  defp drop(runtime, table_ref, expired) do
    case Catalog.drop_segments(runtime.catalog, table_ref, expired) do
      {:ok, snapshot} ->
        Logger.info(fn ->
          "retention dropped #{length(expired)} segment(s) of #{inspect(table_ref)} " <>
            "at snapshot #{snapshot}"
        end)

        {:ok, %{table: table_ref, dropped: expired, snapshot: snapshot}}

      {:error, reason} ->
        failed(table_ref, reason)
    end
  end

  defp expired_paths(runtime, paths, column, ttl_ms) do
    horizon = NaiveDateTime.add(NaiveDateTime.utc_now(), -ttl_ms, :millisecond)

    with {:ok, stats} <- footer_stats(runtime, paths, column) do
      expired =
        for {path, %{max: %NaiveDateTime{} = max, unknown: false}} <- stats,
            NaiveDateTime.compare(max, horizon) == :lt,
            do: path

      {:ok, expired}
    end
  end

  defp footer_stats(runtime, paths, column) do
    paths
    |> Enum.chunk_every(runtime.merge_inputs_per_call)
    |> Enum.reduce_while({:ok, %{}}, fn chunk, {:ok, acc} ->
      case footer_stats_chunk(runtime, chunk, column) do
        {:ok, stats} -> {:cont, {:ok, Map.merge(acc, stats)}}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
  end

  defp footer_stats_chunk(runtime, paths, column) do
    sql =
      "SELECT file_name, max(TRY_CAST(stats_max_value AS TIMESTAMP)), " <>
        "bool_or(stats_max_value IS NULL OR TRY_CAST(stats_max_value AS TIMESTAMP) IS NULL) " <>
        "FROM parquet_metadata([#{placeholders(paths)}]) " <>
        "WHERE path_in_schema = $#{length(paths) + 1} GROUP BY file_name"

    case Engine.try_query(Runtime.engine(runtime.name), sql, paths ++ [column]) do
      {:ok, result} ->
        {:ok,
         Map.new(result.rows, fn [path, max, unknown] -> {path, %{max: max, unknown: unknown}} end)}

      {:error, error} ->
        {:error, {:stats_failed, Exception.message(error)}}
    end
  end

  defp expire(runtime) do
    case Catalog.expire_snapshots(runtime.catalog, runtime.snapshot_keep_ms) do
      {:ok, expired} ->
        expired

      {:error, reason} ->
        Logger.warning("snapshot expiry failed: #{inspect(reason)}")

        0
    end
  end

  defp failed(table_ref, reason) do
    Logger.warning("retention sweep of #{inspect(table_ref)} failed: #{inspect(reason)}")

    {:failed, %{table: table_ref, reason: reason}}
  end

  defp placeholders(paths), do: Enum.map_join(1..length(paths), ", ", &"$#{&1}")
end
