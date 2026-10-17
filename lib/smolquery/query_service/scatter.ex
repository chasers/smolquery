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
     (T-364), a task placed on a leased runner with a self-contained request
     for a runner shard, each call bounded by what is left of the job's own
     deadline. A runner shard that fails, at the lease or on the runner, is
     retried once on a peer while more than the retry floor of the deadline
     remains (`Smolquery.Burst.Policy`); otherwise the job falls back.
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

  alias Smolquery.Burst.Policy
  alias Smolquery.Catalog
  alias Smolquery.Cluster
  alias Smolquery.Cluster.PgGroup
  alias Smolquery.Engine.Connection
  alias Smolquery.Identifier
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
  alias Smolquery.Segments.Store
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

  @typedoc """
  How a scattered job was answered: its shard count and merged partial
  bytes, how many shards ran on peers and how many on runners, the longest
  wait a runner shard spent outside its worker, why burst was refused when
  the job asked for it, and what the shards did with the file cache.
  """
  @type answer :: %{
          shards: pos_integer(),
          partial_bytes: non_neg_integer(),
          peers: non_neg_integer(),
          burst: non_neg_integer(),
          burst_wait_ms: non_neg_integer(),
          burst_refused: Policy.refusal() | :runners_unreachable | :too_few_files | nil,
          backend: String.t() | nil,
          file_cache: FileCache.decision()
        }

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
        ) :: {:ok, Explorer.DataFrame.t(), answer()} | :fallback
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
         {:ok, dealt} <- shards(runtime, units) do
      run(runtime, connection, decomposition, ref, schema, dealt, job_id, timeout_ms)
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
      hot =
        plan.hot
        |> Map.get(ref, [])
        |> Enum.map(&(&1 |> Map.take(["url", "field_ids"]) |> Map.put("hot", true)))

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
    scan_bytes = units |> Enum.reject(&hot?/1) |> Enum.sum_by(&Map.get(&1, "bytes", 0))

    {runners, refused} =
      case Policy.runners(runtime.burst.request, scan_bytes, runtime.burst) do
        {:ok, runners} -> {runners, nil}
        {:none, :off} -> {0, nil}
        {:none, reason} -> {0, reason}
      end

    case targets(runtime, runners) do
      [] ->
        {:refused, :no_workers}

      targets ->
        shards = deal(targets, units)
        runner_shards = Enum.count(shards, &(elem(&1, 0).kind == :runner))

        {:ok,
         %{
           shards: shards,
           runner_shards: runner_shards,
           refused: refused || too_few_files(runners, runner_shards)
         }}
    end
  end

  defp too_few_files(runners, 0) when runners > 0, do: :too_few_files
  defp too_few_files(_runners, _runner_shards), do: nil

  @doc """
  The targets a job on `runtime` scatters over: every peer, then `runners`
  runners from the node's pool, as `Smolquery.Burst.Policy` sized them.
  Empty when there is no peer: a runner only ever works beside the peers,
  which is where the hot units go.
  """
  @spec targets(Runtime.t(), non_neg_integer()) :: [target()]
  def targets(%Runtime{} = runtime, runners) do
    case workers(runtime) do
      [] ->
        []

      peers ->
        runners = List.duplicate(%{kind: :runner, pool: runtime.burst.pool}, runners)

        Enum.map(peers, &%{kind: :peer, node: &1})
        |> Enum.concat(runners)
        |> Enum.with_index(&Map.put(&1, :index, &2))
    end
  end

  defp workers(%Runtime{} = runtime) do
    if Cluster.enabled?() do
      PgGroup.nodes(Smolquery.QueryService, runtime.name, [node()])
    else
      List.duplicate(node(), runtime.distributed.local_workers)
    end
  end

  @doc """
  Deals `units` over `targets`: hot units (tagged `"hot"` by `units/3`)
  round-robin over the peers alone, sealed files round-robin over every
  target, so a runner never holds a unit it cannot read (PL-77 D4). A
  target dealt nothing is dropped. Hot units with no peer to take them are
  an `ArgumentError`: `targets/1` never produces that list, and dropping a
  hot unit would answer short.
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

  defp round_robin(_units, []),
    do: raise(ArgumentError, "hot units need a peer target; runners cannot read the hot tier")

  defp round_robin(units, targets) do
    slots = List.to_tuple(targets)
    count = tuple_size(slots)

    units
    |> Enum.with_index()
    |> Enum.group_by(fn {_unit, index} -> elem(slots, rem(index, count)).index end, &elem(&1, 0))
  end

  defp hot?(unit), do: Map.get(unit, "hot", false)

  defp run(runtime, connection, decomposition, ref, schema, dealt, job_id, timeout_ms) do
    %{shards: shards} = dealt
    partials_dir = dir(job_id)
    File.mkdir_p!(partials_dir)

    try do
      gathered =
        Trace.span(:gather, &phase_meta(%{shards: length(shards)}, &1), fn ->
          gather(runtime, decomposition, ref, schema, dealt, partials_dir, job_id, timeout_ms)
        end)

      case gathered do
        {:ok, partials} ->
          answer(runtime, connection, decomposition, dealt, partials, job_id)

        {:error, reason} ->
          delete_spilled(runtime, spill_keys(runtime, dealt, job_id))
          Logger.warning("distributed query fell back: #{inspect(reason)}")

          :fallback
      end
    catch
      kind, reason ->
        delete_spilled(runtime, spill_keys(runtime, dealt, job_id))
        :erlang.raise(kind, reason, __STACKTRACE__)
    after
      File.rm_rf(partials_dir)
    end
  end

  defp answer(runtime, connection, decomposition, dealt, partials, job_id) do
    merged =
      Trace.span(:merge, &phase_meta(%{}, &1), fn ->
        merge(runtime, connection, decomposition, partials)
      end)

    delete_spilled(runtime, cleanup_keys(runtime, dealt, partials, job_id))

    case merged do
      {:ok, frame} ->
        %{shards: shards} = dealt

        measurements = %{
          shards: length(shards),
          partial_bytes: Enum.sum_by(partials, & &1.bytes),
          peers: Enum.count(partials, &(&1.kind == :peer)),
          burst: Enum.count(partials, &(&1.kind == :runner))
        }

        burst = %{
          burst_wait_ms: burst_wait_ms(partials),
          burst_refused: refused(dealt, measurements.burst),
          backend: backend(runtime)
        }

        :telemetry.execute(
          [:smolquery, :query, :scatter],
          Map.put(measurements, :burst_wait_ms, burst.burst_wait_ms),
          %{
            workers: shards |> peer_shards() |> Enum.map(&elem(&1, 0).node) |> Enum.uniq(),
            burst_refused: Policy.reason(burst.burst_refused),
            backend: burst.backend
          }
        )

        {:ok, frame,
         measurements
         |> Map.merge(burst)
         |> Map.put(:file_cache, partials |> Enum.map(& &1.decision) |> FileCache.combined())}

      :fallback ->
        :fallback
    end
  end

  defp spill_key(%Runtime{burst: %{spill_prefix: prefix}}, job_id, index),
    do: "#{prefix}/#{job_id}/#{index}.parquet"

  defp spill_keys(%Runtime{store: nil}, _dealt, _job_id), do: []

  defp spill_keys(runtime, %{shards: shards}, job_id),
    do:
      for(
        {%{kind: :runner, index: index}, _files} <- shards,
        do: spill_key(runtime, job_id, index)
      )

  defp cleanup_keys(%Runtime{store: nil}, _dealt, _partials, _job_id), do: []

  defp cleanup_keys(runtime, %{shards: shards}, partials, job_id) do
    reported = for %{spilled: key} <- partials, is_binary(key), do: key
    answered = for %{kind: :runner, index: index} <- partials, do: index

    silent =
      for {%{kind: :runner, index: index}, _files} <- shards,
          index not in answered,
          do: spill_key(runtime, job_id, index)

    reported ++ silent
  end

  defp delete_spilled(_runtime, []), do: :ok

  defp delete_spilled(%Runtime{store: store}, keys) do
    keys
    |> Task.async_stream(&{&1, Store.delete(store, &1)},
      max_concurrency: 16,
      ordered: false,
      timeout: 30_000,
      on_timeout: :kill_task
    )
    |> Enum.each(fn
      {:ok, {_key, :ok}} ->
        :ok

      {:ok, {key, {:error, reason}}} ->
        Logger.warning("spill #{key} not deleted: #{inspect(reason)}")

      {:exit, reason} ->
        Logger.warning("spill delete exited: #{inspect(reason)}")
    end)
  end

  defp gather(runtime, decomposition, ref, schema, dealt, partials_dir, job_id, timeout_ms) do
    %{shards: shards, runner_shards: runner_shards} = dealt

    base = %{
      table_ref: ref,
      schema: schema,
      partial_sql: decomposition.partial_sql,
      params: decomposition.params
    }

    context = %{
      runtime: runtime,
      job_id: job_id,
      deadline_ms: deadline_ms(timeout_ms),
      base: base,
      runner_base: runner_base(runner_shards, base, runtime),
      node_files: node_files(shards),
      peers: shards |> peer_shards() |> Enum.map(&elem(&1, 0))
    }

    shards
    |> Task.async_stream(fn {target, files} -> shard(target, files, context) end,
      max_concurrency: concurrency(shards, runner_shards, runtime),
      ordered: false,
      timeout: :infinity
    )
    |> Enum.reduce_while({:ok, []}, fn
      {:ok, {target, wait_us, {:ok, reply}}}, {:ok, partials} ->
        partial =
          reply
          |> landed(partials_dir, target.index)
          |> Map.merge(%{
            decision: Map.get(reply, :file_cache, runtime.file_cache.decision),
            kind: target.kind,
            index: target.index,
            wait_us: wait_us
          })

        {:cont, {:ok, [partial | partials]}}

      {:ok, {_target, _wait_us, {:error, reason}}}, _acc ->
        {:halt, {:error, reason}}

      {:exit, reason}, _acc ->
        {:halt, {:error, reason}}
    end)
    |> case do
      {:ok, partials} -> {:ok, Enum.reverse(partials)}
      {:error, reason} -> {:error, reason}
    end
  end

  defp landed(%{spilled: %{location: location, key: key, bytes: bytes}}, _dir, _index),
    do: %{path: location, spilled: key, bytes: bytes}

  defp landed(%{parquet: parquet}, partials_dir, index) do
    path = Path.join(partials_dir, "partial-#{index}.parquet")
    File.write!(path, parquet)

    %{path: path, spilled: nil, bytes: byte_size(parquet)}
  end

  defp concurrency(shards, runner_shards, %Runtime{burst: %{max_inflight: max_inflight}}),
    do: max(length(shards) - runner_shards + min(runner_shards, max_inflight), 1)

  defp deadline_ms(:infinity), do: :infinity
  defp deadline_ms(timeout_ms), do: System.monotonic_time(:millisecond) + timeout_ms

  defp shard(target, files, context) do
    {elapsed_us, result} = dispatch(target, files, context)

    case {target, result} do
      {%{kind: :runner}, {:error, _reason}} ->
        retry_on_peer(target, files, context, {elapsed_us, result})

      _answered_or_peer_failed ->
        {target, wait_us(elapsed_us, result), result}
    end
  end

  defp retry_on_peer(target, files, context, {elapsed_us, result}) do
    case retry_target(target, context) do
      nil ->
        {target, wait_us(elapsed_us, result), result}

      peer ->
        {retry_us, retried} = dispatch(peer, files, context)
        {peer, wait_us(retry_us, retried), retried}
    end
  end

  defp retry_target(%{index: index}, %{peers: [_ | _] = peers} = context) do
    if remaining_ms(context.deadline_ms) > context.runtime.burst.retry_floor_ms do
      peers
      |> Enum.at(rem(index, length(peers)))
      |> Map.merge(%{index: index, retry: true})
    end
  end

  defp retry_target(_target, _no_peers), do: nil

  defp remaining_ms(:infinity), do: :infinity
  defp remaining_ms(deadline_ms), do: deadline_ms - System.monotonic_time(:millisecond)

  defp dispatch(target, files, context) do
    %{runtime: runtime, job_id: job_id} = context
    remaining_ms = remaining_ms(context.deadline_ms)
    request = target |> request(files, context) |> Map.put(:timeout_ms, remaining_ms)

    Trace.span(:shard, &shard_meta(target, files, &1), fn ->
      Telemetry.timed(fn ->
        WorkerTransport.call(
          transport(target, runtime),
          runtime.name,
          request,
          job_id,
          remaining_ms
        )
      end)
    end)
  end

  defp runner_base(0, _base, _runtime), do: nil

  defp runner_base(_runner_shards, base, runtime),
    do: Map.put(base, :engine, PartialWorker.engine_spec(runtime))

  defp request(%{kind: :peer, node: node} = target, files, context) do
    %{runtime: runtime, base: base, node_files: node_files} = context

    Map.merge(base, %{
      files: files,
      file_cache: runtime.file_cache.decision,
      file_cache_mode: runtime.file_cache.mode,
      node_files: Map.fetch!(node_files, node) ++ retried_sealed(target, files),
      allowed_paths: files |> Enum.filter(&hot?/1) |> Enum.map(& &1["url"])
    })
  end

  defp request(%{kind: :runner, index: index}, files, %{runner_base: runner_base} = context) do
    runner_base
    |> Map.merge(%{files: files, allowed_paths: []})
    |> with_spill(context, index)
  end

  defp with_spill(request, %{runtime: %Runtime{store: nil}}, _index), do: request

  defp with_spill(request, %{runtime: runtime, job_id: job_id}, index) do
    Map.put(request, :spill, %{
      key: spill_key(runtime, job_id, index),
      inline_max_bytes: runtime.burst.spill_inline_max_bytes
    })
  end

  defp retried_sealed(%{retry: true}, files),
    do: for(%{"bytes" => bytes, "url" => url} <- files, do: %{"url" => url, "bytes" => bytes})

  defp retried_sealed(_first_dispatch, _files), do: []

  defp transport(%{kind: :peer, node: node}, _runtime), do: node

  defp transport(%{kind: :runner, pool: pool}, %Runtime{burst: %{wait_ms: wait_ms}}),
    do: %{kind: :runner, pool: pool, wait_ms: wait_ms}

  defp wait_us(elapsed_us, {:ok, %{timings: %{worker_us: worker_us}}}),
    do: max(elapsed_us - worker_us, 0)

  defp wait_us(_elapsed_us, _failed_or_older_worker), do: 0

  defp burst_wait_ms(partials) do
    partials
    |> Enum.filter(&(&1.kind == :runner))
    |> Enum.map(& &1.wait_us)
    |> Enum.max(fn -> 0 end)
    |> div(1000)
  end

  defp refused(%{refused: nil, runner_shards: runner_shards}, 0) when runner_shards > 0,
    do: :runners_unreachable

  defp refused(%{refused: refused}, _burst), do: refused

  defp backend(%Runtime{burst: %{enabled: true, backend: backend}}), do: backend
  defp backend(%Runtime{}), do: nil

  defp shard_meta(target, files, outcome) do
    base = %{
      shard: target.index,
      target: target_name(target),
      kind: target.kind,
      files: length(files),
      hot_files: Enum.count(files, &hot?/1),
      sealed_bytes: Enum.sum_by(files, &Map.get(&1, "bytes", 0))
    }

    base = if Map.get(target, :retry), do: Map.put(base, :retry, true), else: base

    case outcome do
      {elapsed_us, {:ok, %{rows: rows, timings: timings} = reply}} ->
        base
        |> Map.merge(timings)
        |> Map.merge(%{
          rows: rows,
          partial_bytes: partial_bytes(reply),
          spilled: match?(%{spilled: %{}}, reply),
          node: Map.get(reply, :node, target_name(target)),
          wait_us: wait_us(elapsed_us, {:ok, reply})
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
  defp target_name(%{kind: :runner, pool: pool}), do: inspect(pool)

  defp partial_bytes(%{spilled: %{bytes: bytes}}), do: bytes
  defp partial_bytes(%{parquet: parquet}) when is_binary(parquet), do: byte_size(parquet)
  defp partial_bytes(_older_worker), do: 0

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

  defp merge(runtime, connection, decomposition, partials) do
    paths = Enum.map(partials, & &1.path)
    final = Decomposer.final_sql(decomposition, Views.read_parquet(paths))

    with :ok <- uncached_spill(runtime, connection, partials),
         {:ok, frame} <-
           Connection.frame(connection, bounded(final, runtime.result_max_rows), [], :infinity) do
      {:ok, frame}
    else
      {:error, reason} ->
        Logger.warning("distributed merge fell back: #{inspect(reason)}")

        :fallback
    end
  end

  defp uncached_spill(%Runtime{file_cache: %{directory: nil}}, _connection, _partials), do: :ok

  defp uncached_spill(%Runtime{} = runtime, connection, partials) do
    if Enum.any?(partials, &is_binary(&1.spilled)) do
      prefix = Store.location(runtime.store, runtime.burst.spill_prefix)
      pattern = Identifier.sql_string("^" <> Regex.escape(prefix))

      case Connection.query(
             connection,
             "SELECT cache_httpfs_add_exclusion_regex(#{pattern})",
             [],
             :infinity
           ) do
        {:ok, _result} -> :ok
        {:error, reason} -> {:error, {:spill_cache_exclusion, reason}}
      end
    else
      :ok
    end
  end
end
