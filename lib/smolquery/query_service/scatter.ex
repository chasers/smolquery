defmodule Smolquery.QueryService.Scatter do
  @moduledoc """
  Distributes one planned query across several DuckDB instances (PL-49).

  PL-48 settled the shape: shard the file list, run a partial query per
  shard, merge the partials with a final query. This module is the
  coordinator. It runs inside the job's own execution task, after the
  planner's views exist on the job engine, and it either answers with the
  merged frame or says `:fallback` — never an error. Any refusal, any
  worker failure, any surprise falls back to the single-engine path the
  job would have taken anyway, so the flag cannot fail a query that works
  without it.

  ## The path

  1. `Smolquery.QueryService.Decomposer` splits the SQL, fed the `DESCRIBE`
     of the original statement from the job engine — whose views the plan
     just created, so the names and types are exactly the single-engine
     result's.
  2. The shard units are the sealed file list at the plan's pinned snapshot
     (`Smolquery.Catalog.segments/3`) plus the plan's surviving hot entry
     URLs. Fewer than `min_files` units is a refusal: the fixed costs
     (engine start per worker, partial transfer) outweigh a small scan
     (PL-48).
  3. Units go round-robin across the workers. With clustering on, the
     workers are this node plus every connected node whose
     `Smolquery.QueryService.PartialWorker` answers `available?`; without,
     `local_workers` instances on this node. Dispatch is `:erpc.call/4`
     either way, so the local path and the remote path are the same code.
  4. Each worker's parquet bytes land in the job's partials directory —
     `Runner` put it inside `allowed_directories` before lockdown — and the
     final query reads them back with `read_parquet` on the job's own
     engine, inside the same result bound as any other query.

  ## PoC limits

  Consistency holds — the sealed list is pinned to the plan's snapshot and
  the hot list is the plan's own — but sealed-tier min/max pruning is lost:
  DuckLake prunes only queries it plans itself. Shards balance by file
  count, not bytes. Partials travel as whole parquet binaries over
  `:erpc`; production wants Arrow over the segment HTTP routes. Remote
  workers must reach the same sealed store, which in practice means S3 or
  a shared disk.
  """

  require Logger

  alias Smolquery.Catalog
  alias Smolquery.Cluster
  alias Smolquery.Engine.Connection
  alias Smolquery.Identifier
  alias Smolquery.QueryService.Decomposer
  alias Smolquery.QueryService.PartialWorker
  alias Smolquery.QueryService.Plan
  alias Smolquery.QueryService.Runtime

  @default_spill_root ".tmp"

  @doc """
  Where job `job_id`'s partial files land.

  `Runner` adds this to the engine's `allowed_directories` before lockdown,
  so the merge can read what the workers returned.
  """
  @spec dir(String.t()) :: String.t()
  def dir(job_id) do
    Path.join(
      Application.get_env(:smolquery, :spill_dir, @default_spill_root),
      "partials-" <> job_id
    )
  end

  @doc """
  Executes `plan` scattered across workers, or `:fallback`.

  `:fallback` is not an error: the flag is off, the query does not
  decompose, the scan is too small, or something along the distributed
  path failed and was logged. The caller runs the normal path. A
  distributed answer carries its shard count and merged partial bytes, so
  the job can say how it was served.
  """
  @spec execute(Runtime.t(), GenServer.server(), Plan.t(), String.t()) ::
          {:ok, Explorer.DataFrame.t(),
           %{shards: pos_integer(), partial_bytes: non_neg_integer()}}
          | :fallback
  def execute(%Runtime{distributed: %{enabled: false}}, _connection, _plan, _job_id),
    do: :fallback

  def execute(%Runtime{} = runtime, connection, plan, job_id) do
    attempt(runtime, connection, plan, job_id)
  catch
    kind, reason ->
      Logger.warning("distributed query fell back: #{inspect({kind, reason})}")

      :fallback
  end

  defp attempt(runtime, connection, plan, job_id) do
    with {:ok, ref} <- single_table(plan),
         {:ok, schema} <- Catalog.table_schema(runtime.catalog, ref),
         {:ok, outputs} <- describe(connection, plan.canonical_sql),
         {:ok, decomposition} <- decompose(connection, plan, outputs, schema),
         {:ok, units} <- units(runtime, plan, ref),
         {:ok, shards} <- shards(runtime, units) do
      run(runtime, connection, decomposition, ref, schema, shards, job_id)
    else
      {:refused, reason} ->
        Logger.debug(fn -> "distributed query refused: #{inspect(reason)}" end)

        :fallback

      {:error, reason} ->
        Logger.warning("distributed query fell back: #{inspect(reason)}")

        :fallback
    end
  end

  defp single_table(%Plan{federated: true}), do: {:refused, :federated}
  defp single_table(%Plan{tables: [ref]}), do: {:ok, ref}
  defp single_table(%Plan{tables: refs}), do: {:refused, {:table_count, length(refs)}}

  defp describe(connection, canonical_sql) do
    with {:ok, result} <-
           Connection.query(connection, "DESCRIBE " <> canonical_sql, [], :infinity) do
      {:ok,
       Enum.map(result.rows, fn row ->
         fields = result.columns |> Enum.zip(row) |> Map.new()

         {fields["column_name"], fields["column_type"]}
       end)}
    end
  end

  defp decompose(connection, plan, outputs, schema) do
    columns = Enum.map(schema.fields, & &1.name)

    case Decomposer.decompose(connection, plan.canonical_sql, outputs, columns) do
      {:ok, decomposition} -> {:ok, decomposition}
      {:error, reason} -> {:refused, reason}
    end
  end

  defp units(runtime, plan, ref) do
    with {:ok, sealed} <- Catalog.segments(runtime.catalog, ref, plan.snapshot) do
      hot = plan.hot |> Map.get(ref, []) |> Enum.map(& &1["url"])
      units = sealed ++ hot

      if length(units) >= runtime.distributed.min_files do
        {:ok, units}
      else
        {:refused, {:too_few_files, length(units)}}
      end
    end
  end

  defp shards(runtime, units) do
    case workers(runtime) do
      [] ->
        {:refused, :no_workers}

      workers ->
        grouped =
          units
          |> Enum.with_index()
          |> Enum.group_by(fn {_unit, index} -> rem(index, length(workers)) end, &elem(&1, 0))

        {:ok,
         workers
         |> Enum.with_index()
         |> Enum.map(fn {worker, slot} -> {worker, Map.get(grouped, slot, [])} end)
         |> Enum.reject(fn {_worker, files} -> files == [] end)}
    end
  end

  defp workers(%Runtime{} = runtime) do
    if Cluster.enabled?() do
      Enum.filter([node() | Node.list()], &worker_available?(&1, runtime.name))
    else
      List.duplicate(node(), runtime.distributed.local_workers)
    end
  end

  defp worker_available?(peer, name) do
    :erpc.call(peer, PartialWorker, :available?, [name], 5_000) == true
  catch
    _kind, _reason -> false
  end

  defp run(runtime, connection, decomposition, ref, schema, shards, job_id) do
    partials_dir = dir(job_id)
    File.mkdir_p!(partials_dir)

    try do
      with {:ok, paths} <- gather(runtime, decomposition, ref, schema, shards, partials_dir),
           {:ok, frame} <- merge(runtime, connection, decomposition, paths) do
        measurements = %{
          shards: length(shards),
          partial_bytes: Enum.sum_by(paths, &File.stat!(&1).size)
        }

        :telemetry.execute(
          [:smolquery, :query, :scatter],
          measurements,
          %{workers: shards |> Enum.map(fn {peer, _files} -> peer end) |> Enum.uniq()}
        )

        {:ok, frame, measurements}
      else
        {:error, reason} ->
          Logger.warning("distributed query fell back: #{inspect(reason)}")

          :fallback

        :fallback ->
          :fallback
      end
    after
      File.rm_rf(partials_dir)
    end
  end

  defp gather(runtime, decomposition, ref, schema, shards, partials_dir) do
    shards
    |> Enum.with_index()
    |> Task.async_stream(
      fn {{peer, files}, index} ->
        request = %{
          statements: view_statements(ref, schema, files),
          partial_sql: decomposition.partial_sql
        }

        {index, dispatch(peer, runtime, request)}
      end,
      max_concurrency: length(shards),
      ordered: true,
      timeout: :infinity
    )
    |> Enum.reduce_while({:ok, []}, fn
      {:ok, {index, {:ok, %{parquet: parquet}}}}, {:ok, paths} ->
        path = Path.join(partials_dir, "partial-#{index}.parquet")
        File.write!(path, parquet)

        {:cont, {:ok, [path | paths]}}

      {:ok, {_index, {:error, reason}}}, _acc ->
        {:halt, {:error, reason}}

      {:exit, reason}, _acc ->
        {:halt, {:error, reason}}
    end)
    |> case do
      {:ok, paths} -> {:ok, Enum.reverse(paths)}
      {:error, reason} -> {:error, reason}
    end
  end

  defp bounded(final, :infinity), do: final
  defp bounded(final, max_rows), do: "SELECT * FROM (#{final}) LIMIT #{max_rows + 1}"

  defp dispatch(peer, runtime, request) do
    :erpc.call(peer, PartialWorker, :run, [runtime.name, request], runtime.default_timeout_ms)
  catch
    kind, reason -> {:error, {:worker_unreachable, peer, {kind, reason}}}
  end

  defp view_statements({dataset, table}, schema, files) do
    ds = Identifier.quote_name!(dataset)
    t = Identifier.quote_name!(table)
    columns = Enum.map_join(schema.fields, ", ", &Identifier.quote_name!(&1.name))
    parquet = Enum.map_join(files, ", ", &Identifier.sql_string/1)

    [
      "CREATE SCHEMA IF NOT EXISTS #{ds}",
      "CREATE VIEW #{ds}.#{t} AS SELECT #{columns} FROM " <>
        "(SELECT * FROM read_parquet([#{parquet}], union_by_name := true))"
    ]
  end

  defp merge(runtime, connection, decomposition, paths) do
    parquet = Enum.map_join(paths, ", ", &Identifier.sql_string/1)
    final = Decomposer.final_sql(decomposition, "read_parquet([#{parquet}])")

    case Connection.frame(connection, bounded(final, runtime.result_max_rows), [], :infinity) do
      {:ok, frame} ->
        {:ok, frame}

      {:error, reason} ->
        Logger.warning("distributed merge fell back: #{inspect(reason)}")

        :fallback
    end
  end
end
