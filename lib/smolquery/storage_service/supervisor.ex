defmodule Smolquery.StorageService.Supervisor do
  @moduledoc """
  Top-level subtree for the `:storage` role.

  Started only on nodes whose roles include `:storage` (see `Smolquery.Roles`).
  Holds the sealed tier's workers: a catalog to commit through, an engine to merge
  through, a task supervisor the merges run under, the sealer that answers seal
  signals, the compactor that re-merges undersized sealed segments, the retention
  sweeper that drops segments past their table's TTL and expires old snapshots,
  and the GC that deletes uploads whose commit never followed.

  The catalog, the merge, and compaction run on separate engines deliberately.
  An `Adbc.Connection` serializes the queries it is given, so sharing one would
  put every catalog commit behind whatever multi-gigabyte `COPY` happened to be
  in flight — and a timed-out compaction merge, whose statement keeps running
  after the caller gave up, would starve every seal and sizing call behind it
  (T-259). On its own engine, the compactor can also recycle the connection
  after a timeout without aborting a healthy in-flight seal. All three engines
  need the sealed tier's `CREATE SECRET` when the store is S3: the merge and
  compaction engines write and re-read segments, and the catalog engine reads
  each just-uploaded segment back when a commit registers it (DuckLake's
  add-data-files collects the file's footer stats).

  The merge engine is sized to the pod it runs in: its memory limit resolves
  per `Smolquery.StorageService.Runtime.engine_memory_limit/2` — an explicit
  knob, else half the cgroup memory limit, else `Smolquery.Engine`'s
  application config — and the resolved value is logged at boot, because it
  is exactly the number an operator cannot read off their own configuration
  (T-250). The compaction engine resolves the same way one rung down —
  explicit knob, else a quarter of the cgroup limit — per
  `Smolquery.StorageService.Runtime.compact_engine_memory_limit/2`. Merge
  and compaction sessions also set `preserve_insertion_order = false`,
  DuckDB's named mitigation for out-of-memory `COPY`: rows nobody asked to
  order may come out reordered, which no reader of a segment depends on,
  while the clustering sort is an explicit `ORDER BY` the setting leaves
  alone.

  The strategy is `rest_for_one`, in that order, because the dependency runs one
  way. A seal attempt commits through the catalog and merges through the engine and
  runs as a task, so all three must be up before the sealer accepts a signal; the
  sealer crashing disturbs none of them. Losing the catalog restarts everything
  above it, which is what abandons in-flight attempts — safe, because a
  level-triggered re-signal brings every unsealed table back and a claim fixes the
  input set.

  A deployment that commits through a catalog it manages elsewhere passes a
  `%Smolquery.Catalog{}` in configuration, and then this subtree starts none.

  Nothing here holds durable state. The catalog and the sealed store do, which is
  what makes a storage node disposable: it can die mid-seal and another node (or
  this one, restarted) reconciles from what the catalog says.

  Ring membership is the first child, a `Smolquery.Cluster.PgGroup.Member`
  (Milestone 8 L6, PL-11 D6) — a no-op when clustering is off, and otherwise
  what puts this node in the storage ring at all. Its pid dies with this
  subtree, so an ungraceful crash removes the node from the ring the same way
  it does for the buffer side — and it re-asserts membership after a `:pg`
  scope restart, which a one-shot join from `init/1` would silently lose.
  """

  use Supervisor

  require Logger

  alias Smolquery.Catalog.DuckLake
  alias Smolquery.Cluster.PgGroup
  alias Smolquery.Engine
  alias Smolquery.EngineSecrets
  alias Smolquery.StorageService.Compactor
  alias Smolquery.StorageService.GC
  alias Smolquery.StorageService.Retention
  alias Smolquery.StorageService.Runtime
  alias Smolquery.StorageService.Sealer

  @doc """
  Starts the storage service.

  Takes any `Smolquery.StorageService.Runtime` option; application config supplies
  whatever is not passed.
  """
  @spec start_link(keyword()) :: Supervisor.on_start()
  def start_link(opts \\ []) do
    runtime = Runtime.new(opts)

    Supervisor.start_link(__MODULE__, runtime, name: Runtime.supervisor(runtime.name))
  end

  @impl Supervisor
  def init(%Runtime{} = runtime) do
    Runtime.put(runtime)

    children =
      [{PgGroup.Member, {Smolquery.StorageService, runtime.name}}] ++
        DuckLake.children(catalog_opts(runtime), Runtime.catalog_engine(runtime.name)) ++
        [
          {Engine, engine_opts(runtime)},
          Supervisor.child_spec({Engine, compact_engine_opts(runtime)},
            id: Runtime.compact_engine(runtime.name)
          ),
          {Task.Supervisor, name: Runtime.seals(runtime.name)},
          {Sealer, runtime},
          {Compactor, runtime},
          {Retention, runtime},
          {GC, runtime}
        ]

    Supervisor.init(children, strategy: :rest_for_one)
  end

  defp engine_opts(%Runtime{} = runtime) do
    merge_limit =
      memory_limit(
        "storage merge engine",
        Runtime.engine_memory_limit(runtime),
        runtime.engine_memory_limit,
        "half the cgroup memory limit"
      )

    [{:name, Runtime.engine(runtime.name)} | shared_engine_opts(runtime)] ++ merge_limit
  end

  defp compact_engine_opts(%Runtime{} = runtime) do
    compact_limit =
      memory_limit(
        "storage compaction engine",
        Runtime.compact_engine_memory_limit(runtime),
        runtime.compact_engine_memory_limit,
        "a quarter of the cgroup memory limit"
      )

    [{:name, Runtime.compact_engine(runtime.name)} | shared_engine_opts(runtime)] ++
      compact_limit
  end

  defp shared_engine_opts(%Runtime{} = runtime) do
    [
      extensions: engine_extensions(runtime),
      statements: ["SET preserve_insertion_order = false" | engine_secrets(runtime)]
    ]
  end

  defp memory_limit(_label, nil, _configured, _derivation), do: []

  defp memory_limit(label, limit, configured, derivation) do
    source = if configured, do: "configured", else: derivation
    Logger.info("#{label} memory_limit=#{limit} (#{source})")

    [memory_limit: limit]
  end

  defp engine_secrets(%Runtime{} = runtime) do
    EngineSecrets.hot_tier(runtime.engine_extensions, runtime.buffer_base_url) ++
      EngineSecrets.sealed_tier(runtime.store)
  end

  defp engine_extensions(%Runtime{} = runtime) do
    EngineSecrets.sealed_tier_extensions(runtime.store, runtime.engine_extensions)
  end

  defp catalog_opts(%Runtime{catalog_opts: nil}), do: nil

  defp catalog_opts(%Runtime{catalog_opts: opts} = runtime) do
    case EngineSecrets.sealed_tier(runtime.store) do
      [] ->
        opts

      secrets ->
        opts
        |> Keyword.update(:statements, secrets, &(&1 ++ secrets))
        |> Keyword.put(:store, runtime.store)
    end
  end
end
