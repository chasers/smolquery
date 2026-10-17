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
     (`Smolquery.Catalog.segment_files/3`, each with the snapshot it was
     registered at) plus the plan's surviving hot entries, ids included. The
     worker projects each file by its column ids (PL-62), reading a sealed
     file's ids from the file itself — see
     `Smolquery.QueryService.PartialWorker`. Fewer than `min_files` units is a refusal: the fixed costs
     (engine start per worker, partial transfer) outweigh a small scan
     (PL-48).
  3. Units are dealt over the job's targets (`targets/1`): the peers, and
     the runners the job asked for with `burst: n` on a node that leases
     them (PL-77). With clustering on, the peers are the query service's
     own `Smolquery.Cluster.PgGroup` members — the nodes whose supervisors
     joined the group, no probing; without, `local_workers` instances on
     this node. Sealed files go round-robin over every target; hot units go
     round-robin over the peers alone, because a hot unit is a URL on a
     buffer pod that a runner can neither resolve nor authenticate to, so a
     burst answer reads the hot tier exactly as a peer-only one does.
     Dispatch goes through `Smolquery.QueryService.WorkerTransport`: a
     direct call for this node, gen_rpc on its own sockets for a peer
     (T-364), `FLAME.call/3` with a self-contained request for a runner,
     each call bounded by the job's own deadline.
  4. Each shard decides for itself whether to read through its node's file
     cache (T-630): the request carries the job's cache mode, not the
     coordinator's verdict, because the shard's blocks land in its own
     node's cache, which the coordinator's index cannot see. It also
     carries the sealed files, with their `"bytes"`, of every shard on the
     same node, which all fill one cache. The answer carries `file_cache`,
     the shards' decisions combined
     (`Smolquery.QueryService.FileCache.combined/1`); a shard on a node
     older than T-630 reports none and counts as the coordinator's verdict,
     which is what it followed.
  5. Each worker's parquet bytes land in the job's partials directory —
     `Runner` put it inside `allowed_directories` before lockdown — and the
     final query reads them back with `read_parquet` on the job's own
     engine, inside the same result bound as any other query.
  6. The fan-out is traced (T-691): `gather` spans the wait for every
     shard, `merge` the final query, and each shard is a `shard` span of
     its own under `gather`, timed on the coordinator from dispatch to
     reply and carrying what the worker measured on its side (engine,
     describe, scan) with the target node, its kind, files and bytes.
     The spans ride the job's phase collection, so they reach the
     `trace: true` answer, the query UI waterfall and the OpenTelemetry
     trace without a context crossing to the worker.

  ## PoC limits

  Consistency holds — the sealed list is pinned to the plan's snapshot and
  the hot list is the plan's own — but sealed-tier min/max pruning is lost:
  DuckLake prunes only queries it plans itself. Shards balance by file
  count, not bytes. Partials travel as whole parquet binaries over gen_rpc,
  off the distribution channel (T-364). Remote workers must reach the same
  sealed store, which in practice means S3 or a shared disk.

  Cleanup is layered: the `after` here removes the partials directory on
  every path it can see, and `Runner`'s settle removes it again — a
  cancelled or timed-out job brutal-kills this task, which skips `after`
  blocks. An in-flight worker, local or remote, bounds itself with the
  deadline the request carries.
  """

  require Logger

  alias Smolquery.Catalog
  alias Smolquery.Cluster
  alias Smolquery.Cluster.PgGroup
  alias Smolquery.Engine.Connection
  alias Smolquery.QueryService.Decomposer
  alias Smolquery.QueryService.FileCache
  alias Smolquery.QueryService.PartialWorker
  alias Smolquery.QueryService.Plan
  alias Smolquery.QueryService.Runtime
  alias Smolquery.QueryService.Statistics
  alias Smolquery.QueryService.Trace
  alias Smolquery.QueryService.VariantResults
  alias Smolquery.QueryService.Views
  alias Smolquery.QueryService.WorkerTransport
  alias Smolquery.Telemetry

  @default_spill_root ".tmp"

  @typedoc """
  Where one shard runs: a peer node of this query service, or a runner
  leased from `Smolquery.Burst`'s pool when the shard is dispatched. The
  index is the shard's identity, so two runners from one pool never collapse
  onto one key the way nodes would.
  """
  @type target ::
          %{index: non_neg_integer(), kind: :peer, node: node()}
          | %{index: non_neg_integer(), kind: :runner, pool: atom()}

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
  distributed answer carries its shard count, merged partial bytes, and
  what its shards did with the file cache, so the job can say how it was
  served.
  """
  @spec execute(
          Runtime.t(),
          GenServer.server(),
          Plan.t(),
          String.t(),
          timeout(),
          [VariantResults.column()] | nil
        ) ::
          {:ok, Explorer.DataFrame.t(),
           %{
             shards: pos_integer(),
             partial_bytes: non_neg_integer(),
             file_cache: FileCache.decision()
           }}
          | :fallback
  def execute(runtime, connection, plan, job_id, timeout_ms, outputs \\ nil)

  def execute(
        %Runtime{distributed: %{enabled: false}},
        _connection,
        _plan,
        _job_id,
        _timeout_ms,
        _outputs
      ),
      do: :fallback

  def execute(%Runtime{} = runtime, connection, plan, job_id, timeout_ms, outputs) do
    attempt(runtime, connection, plan, job_id, timeout_ms, outputs)
  catch
    kind, reason ->
      Logger.warning("distributed query fell back: #{inspect({kind, reason})}")

      :fallback
  end

  defp attempt(runtime, connection, plan, job_id, timeout_ms, outputs) do
    with {:ok, ref} <- single_table(plan),
         {:ok, schema} <- planned_schema(plan, ref),
         {:ok, outputs} <- describe(connection, plan, outputs),
         {:ok, decomposition} <- decompose(connection, plan, outputs, schema),
         :ok <- value_lists(runtime, plan, decomposition),
         {:ok, units} <- units(runtime, plan, ref),
         {:ok, shards} <- shards(runtime, units) do
      run(runtime, connection, decomposition, ref, schema, shards, job_id, timeout_ms)
    else
      {:refused, reason} ->
        Logger.debug(fn -> "distributed query refused: #{inspect(reason)}" end)

        :fallback

      {:error, reason} ->
        Logger.warning("distributed query fell back: #{inspect(reason)}")

        :fallback
    end
  end

  defp planned_schema(%Plan{schemas: schemas}, ref) do
    case Map.fetch(schemas, ref) do
      {:ok, schema} -> {:ok, schema}
      :error -> {:refused, {:no_planned_schema, ref}}
    end
  end

  defp describe(_connection, _plan, outputs) when is_list(outputs), do: {:ok, outputs}

  defp describe(connection, %Plan{} = plan, nil),
    do: Connection.describe(connection, plan.canonical_sql, plan.params, :infinity)

  defp single_table(%Plan{federated: true}), do: {:refused, :federated}
  defp single_table(%Plan{tables: [ref]}), do: {:ok, ref}
  defp single_table(%Plan{tables: refs}), do: {:refused, {:table_count, length(refs)}}

  defp decompose(connection, plan, outputs, schema) do
    columns = Enum.map(schema.fields, & &1.name)

    case Decomposer.decompose(connection, plan.canonical_sql, outputs, columns, plan.params) do
      {:ok, decomposition} -> {:ok, decomposition}
      {:error, reason} -> {:refused, reason}
    end
  end

  defp value_lists(runtime, _plan, %Decomposer{rows: %{limit: limit}}) do
    if limit <= runtime.distributed.row_partial_max_rows,
      do: :ok,
      else: {:refused, {:row_partial_over, limit}}
  end

  defp value_lists(_runtime, _plan, %Decomposer{value_lists: false}), do: :ok

  defp value_lists(runtime, %Plan{statistics: %Statistics{} = statistics}, _decomposition) do
    rows = Statistics.rows_scanned(statistics)

    if rows <= runtime.distributed.value_list_max_rows,
      do: :ok,
      else: {:refused, {:value_lists_over, rows}}
  end

  defp value_lists(_runtime, _unsized_plan, _decomposition), do: {:refused, :value_lists_unsized}

  defp units(runtime, plan, ref) do
    with {:ok, sealed} <- Catalog.segment_files(runtime.catalog, ref, plan.snapshot) do
      hot = plan.hot |> Map.get(ref, []) |> Enum.map(&Map.take(&1, ["url", "field_ids"]))

      units =
        Enum.map(sealed, &%{"url" => &1.path, "snapshot" => &1.snapshot, "bytes" => &1.bytes}) ++
          hot

      if length(units) >= runtime.distributed.min_files do
        {:ok, units}
      else
        {:refused, {:too_few_files, length(units)}}
      end
    end
  end

  defp shards(runtime, units) do
    case targets(runtime) do
      [] -> {:refused, :no_workers}
      targets -> {:ok, deal(targets, units)}
    end
  end

  @doc """
  The targets a job on `runtime` scatters over: every peer, then the
  `runtime.burst.runners` runners the job asked for when this node leases
  them. Empty when there is no peer: a runner only ever works beside the
  peers, which is where the hot units go.
  """
  @spec targets(Runtime.t()) :: [target()]
  def targets(%Runtime{} = runtime) do
    case workers(runtime) do
      [] ->
        []

      peers ->
        runners = List.duplicate(%{kind: :runner, pool: runtime.burst.pool}, runners(runtime))

        Enum.map(peers, &%{kind: :peer, node: &1})
        |> Enum.concat(runners)
        |> Enum.with_index(&Map.put(&1, :index, &2))
    end
  end

  defp runners(%Runtime{burst: %{enabled: true, runners: runners}}), do: runners
  defp runners(%Runtime{}), do: 0

  defp workers(%Runtime{} = runtime) do
    if Cluster.enabled?() do
      PgGroup.nodes(Smolquery.QueryService, runtime.name, [node()])
    else
      List.duplicate(node(), runtime.distributed.local_workers)
    end
  end

  @doc """
  Deals `units` over `targets`: hot units round-robin over the peers alone,
  sealed files round-robin over every target, so a runner never holds a
  unit it cannot read (PL-77 D4). A target dealt nothing is dropped.
  """
  @spec deal([target()], [map()]) :: [{target(), [map()]}]
  def deal(targets, units) do
    {hot, sealed} = Enum.split_with(units, &hot?/1)
    peers = Enum.filter(targets, &(&1.kind == :peer))

    dealt =
      Map.merge(round_robin(hot, peers), round_robin(sealed, targets), fn _index, hot, sealed ->
        hot ++ sealed
      end)

    targets
    |> Enum.map(&{&1, Map.get(dealt, &1.index, [])})
    |> Enum.reject(fn {_target, files} -> files == [] end)
  end

  defp round_robin([], _targets), do: %{}

  defp round_robin(units, targets) do
    slots = List.to_tuple(targets)
    count = tuple_size(slots)

    units
    |> Enum.with_index()
    |> Enum.group_by(fn {_unit, index} -> elem(slots, rem(index, count)).index end, &elem(&1, 0))
  end

  defp hot?(%{"url" => url}), do: String.starts_with?(url, "http")

  defp run(runtime, connection, decomposition, ref, schema, shards, job_id, timeout_ms) do
    partials_dir = dir(job_id)
    File.mkdir_p!(partials_dir)

    try do
      with {:ok, partials} <-
             Trace.span(:gather, &phase_meta(%{shards: length(shards)}, &1), fn ->
               gather(
                 runtime,
                 decomposition,
                 ref,
                 schema,
                 shards,
                 partials_dir,
                 job_id,
                 timeout_ms
               )
             end),
           paths = Enum.map(partials, &elem(&1, 0)),
           {:ok, frame} <-
             Trace.span(:merge, &phase_meta(%{}, &1), fn ->
               merge(runtime, connection, decomposition, paths)
             end) do
        measurements = %{
          shards: length(shards),
          partial_bytes: Enum.sum_by(paths, &File.stat!(&1).size)
        }

        :telemetry.execute(
          [:smolquery, :query, :scatter],
          measurements,
          %{workers: shards |> peer_shards() |> Enum.map(&elem(&1, 0).node) |> Enum.uniq()}
        )

        {:ok, frame,
         Map.put(
           measurements,
           :file_cache,
           partials |> Enum.map(&elem(&1, 1)) |> FileCache.combined()
         )}
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

  defp gather(runtime, decomposition, ref, schema, shards, partials_dir, job_id, timeout_ms) do
    node_files = node_files(shards)

    base = %{
      table_ref: ref,
      schema: schema,
      partial_sql: decomposition.partial_sql,
      params: decomposition.params,
      timeout_ms: timeout_ms
    }

    shards
    |> Task.async_stream(
      fn {target, files} ->
        request = request(target, files, base, runtime, node_files)

        {target.index,
         Trace.span(:shard, &shard_meta(target, files, &1), fn ->
           Telemetry.timed(fn ->
             WorkerTransport.call(transport(target), runtime.name, request, job_id, timeout_ms)
           end)
         end)}
      end,
      max_concurrency: length(shards),
      ordered: true,
      timeout: :infinity
    )
    |> Enum.reduce_while({:ok, []}, fn
      {:ok, {index, {_elapsed_us, {:ok, %{parquet: parquet} = reply}}}}, {:ok, partials} ->
        path = Path.join(partials_dir, "partial-#{index}.parquet")
        File.write!(path, parquet)

        decision = Map.get(reply, :file_cache, runtime.file_cache.decision)

        {:cont, {:ok, [{path, decision} | partials]}}

      {:ok, {_index, {_elapsed_us, {:error, reason}}}}, _acc ->
        {:halt, {:error, reason}}

      {:exit, reason}, _acc ->
        {:halt, {:error, reason}}
    end)
    |> case do
      {:ok, partials} -> {:ok, Enum.reverse(partials)}
      {:error, reason} -> {:error, reason}
    end
  end

  defp request(%{kind: :peer, node: node}, files, base, runtime, node_files) do
    Map.merge(base, %{
      files: files,
      file_cache: runtime.file_cache.decision,
      file_cache_mode: runtime.file_cache.mode,
      node_files: Map.fetch!(node_files, node),
      allowed_paths: hot_urls(files)
    })
  end

  defp request(%{kind: :runner}, files, base, runtime, _node_files) do
    Map.merge(base, %{
      engine: PartialWorker.engine_spec(runtime),
      files: files,
      allowed_paths: hot_urls(files)
    })
  end

  defp hot_urls(files), do: files |> Enum.filter(&hot?/1) |> Enum.map(& &1["url"])

  defp transport(%{kind: :peer, node: node}), do: node
  defp transport(%{kind: :runner, pool: pool}), do: %{kind: :runner, pool: pool}

  defp shard_meta(target, files, outcome) do
    base = %{
      shard: target.index,
      target: target_name(target),
      kind: target.kind,
      files: length(files),
      hot_files: Enum.count(files, &hot?/1),
      sealed_bytes: Enum.sum_by(files, &Map.get(&1, "bytes", 0))
    }

    case outcome do
      {elapsed_us, {:ok, %{rows: rows, timings: timings} = reply}} ->
        base
        |> Map.merge(timings)
        |> Map.merge(%{
          rows: rows,
          node: Map.get(reply, :node, target_name(target)),
          wait_us: max(elapsed_us - Map.get(timings, :worker_us, elapsed_us), 0)
        })

      {_elapsed_us, {:ok, _older_worker}} ->
        base

      {_elapsed_us, failed} ->
        Map.put(base, :error, error_text(failed))

      raised ->
        Map.put(base, :error, error_text(raised))
    end
  end

  defp target_name(%{kind: :peer, node: node}), do: node
  defp target_name(%{kind: :runner, pool: pool}), do: pool

  defp phase_meta(base, outcome) do
    case Telemetry.outcome(outcome) do
      :ok -> Map.put(base, :outcome, :ok)
      :error -> base |> Map.put(:outcome, :error) |> Map.put(:error, error_text(outcome))
    end
  end

  defp error_text({:error, reason}), do: inspect(reason, limit: 20, printable_limit: 200)

  defp error_text({:raised, kind, reason}),
    do: inspect({kind, reason}, limit: 20, printable_limit: 200)

  defp error_text(other), do: inspect(other, limit: 20, printable_limit: 200)

  defp node_files(shards) do
    shards
    |> peer_shards()
    |> Enum.group_by(&elem(&1, 0).node, fn {_target, files} ->
      for %{"bytes" => bytes, "url" => url} <- files, do: %{"url" => url, "bytes" => bytes}
    end)
    |> Map.new(fn {node, sealed} -> {node, Enum.concat(sealed)} end)
  end

  defp peer_shards(shards), do: Enum.filter(shards, &(elem(&1, 0).kind == :peer))

  defp bounded(final, :infinity), do: final
  defp bounded(final, max_rows), do: "SELECT * FROM (#{final}) LIMIT #{max_rows + 1}"

  defp merge(runtime, connection, decomposition, paths) do
    final = Decomposer.final_sql(decomposition, Views.read_parquet(paths))

    case Connection.frame(connection, bounded(final, runtime.result_max_rows), [], :infinity) do
      {:ok, frame} ->
        {:ok, frame}

      {:error, reason} ->
        Logger.warning("distributed merge fell back: #{inspect(reason)}")

        :fallback
    end
  end
end
