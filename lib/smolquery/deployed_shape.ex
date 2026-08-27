defmodule Smolquery.DeployedShape do
  @moduledoc """
  Says out loud which path a node actually came up on.

  Every performance trap this codebase has shipped had the same shape: a
  slower path selected by a default, with nothing announcing it. The buffer
  defaulting to the Polars writer, an interrupted deploy leaving gen_rpc TLS
  on, a caller posting JSON arrays while the NDJSON passthrough sat inert —
  each was configuration doing exactly what it was told, invisibly.

  A benchmark cannot see any of them. Server-side metrics report the path
  being taken, not the faster path that was not, so a node on the slow path
  looks healthy in every panel. The comparison rig spent most of a day inside
  that blind spot, measuring CPU, disk, memory, schedulers, codecs and thread
  counts, before finding the answer in one column of its own CSV.

  So this module resolves the shape once, at boot, and states it twice: as a
  log line an operator reads, and as an `_info` gauge a dashboard can diff
  against the shape that produced the last good number. Known-slow
  combinations are warnings, not debug lines, because "you are running 3-4x
  under what this deployment can do" is operational news.

  It measures nothing and decides nothing. Adding a knob here would defeat
  the point: this is the thing that tells you what the knobs did.
  """

  require Logger

  alias Smolquery.BufferService.Runtime, as: BufferRuntime
  alias Smolquery.IngestService.Runtime, as: IngestRuntime
  alias Smolquery.Segments.Store
  alias Smolquery.StorageService.Runtime, as: StorageRuntime
  alias Smolquery.Telemetry

  @doc """
  Logs a service's resolved shape and registers it for `GET /metrics`.

  Takes the runtime struct the service actually booted with, not the
  configuration it was asked for — the two differ exactly when it matters.
  """
  @spec announce(BufferRuntime.t() | StorageRuntime.t() | IngestRuntime.t()) :: :ok
  def announce(%BufferRuntime{} = runtime) do
    # The pool's per-member budget is stated resolved, not as configured: the
    # thread count is usually a division nothing wrote down, and the memory
    # limit is usually an inheritance from `Smolquery.Engine` that multiplies
    # by the pool size. Those are exactly the two values an operator cannot
    # read off their own configuration, which is this module's whole test for
    # what belongs on the shape line. The claim valves pass the same test: what
    # one seal claim may freeze is a product of two settings, and T-335 spent a
    # day reading it out of failure messages.
    budget = BufferRuntime.write_engine_budget(runtime)

    labels = [
      compression: runtime.compression,
      flush_max_bytes: runtime.flush_max_bytes,
      flush_interval_ms: runtime.flush_interval_ms,
      flush_idle_interval_ms: runtime.flush_idle_interval_ms,
      commit_siblings: runtime.commit_siblings,
      encode_concurrency: runtime.encode_concurrency,
      write_pool_size: runtime.write_pool_size,
      write_engine_threads: budget[:threads],
      write_engine_memory_limit: budget[:memory_limit] || engine_memory_limit(),
      seal_max_bytes: runtime.seal_max_bytes,
      seal_max_files: runtime.seal_max_files,
      claim_valve_factor: runtime.claim_valve_factor,
      claim_max_bytes: runtime.seal_max_bytes * runtime.claim_valve_factor,
      claim_max_files: runtime.seal_max_files * runtime.claim_valve_factor,
      transport_tls: transport_tls?()
    ]

    Logger.info("buffer shape: #{describe(labels)}")
    Telemetry.put_info("smolquery_buffer_shape_info", labels)

    warn_slow(
      transport_tls?(),
      "gen_rpc is running over TLS. It costs roughly 26% of ingest throughput. " <>
        "Intended for production; if this is a dev or benchmark deployment, " <>
        "check GEN_RPC_TLS."
    )

    :ok
  end

  # The seal side's counterpart to the buffer's claim valves: what one claim
  # may freeze is stated over there, and what this node can merge it inside is
  # stated here. T-335 needed both, and read all of it out of failure
  # messages. The two engine limits are stated resolved, like the write pool's
  # budget above — each derives from the cgroup limit when nothing configures
  # one, so neither reads off an operator's own configuration (T-250).
  def announce(%StorageRuntime{} = runtime) do
    labels = [
      store: store_impl(runtime.store),
      compression: runtime.compression,
      seal_row_group_size: runtime.seal_row_group_size,
      max_concurrent_seals: runtime.max_concurrent_seals,
      seal_backoff_base_ms: runtime.seal_backoff_base_ms,
      seal_backoff_max_ms: runtime.seal_backoff_max_ms,
      merge_engine_memory_limit:
        StorageRuntime.engine_memory_limit(runtime) || engine_memory_limit(),
      compact_engine_memory_limit:
        StorageRuntime.compact_engine_memory_limit(runtime) || engine_memory_limit(),
      merge_inputs_per_call: runtime.merge_inputs_per_call,
      merge_copy_timeout_ms: runtime.merge_copy_timeout_ms,
      merge_staging_timeout_ms: runtime.merge_staging_timeout_ms,
      merge_describe_timeout_ms: runtime.merge_describe_timeout_ms
    ]

    Logger.info("storage shape: #{describe(labels)}")
    Telemetry.put_info("smolquery_storage_shape_info", labels)

    :ok
  end

  def announce(%IngestRuntime{} = runtime) do
    labels = [
      write_partitions: runtime.write_partitions,
      schema_cache_ttl_ms: runtime.schema_cache_ttl_ms
    ]

    Logger.info("ingest shape: #{describe(labels)}")
    Telemetry.put_info("smolquery_ingest_shape_info", labels)

    :ok
  end

  @doc """
  Whether Erlang distribution and gen_rpc are running over TLS.

  Read from the resolved `:gen_rpc` application environment rather than from
  the environment variable that set it, for the same reason the rest of this
  module reads runtimes: the variable says what was asked for.
  """
  @spec transport_tls?() :: boolean()
  def transport_tls?, do: Application.get_env(:gen_rpc, :default_client_driver) == :ssl

  # What one pool member inherits when `:write_engine_memory_limit` is unset —
  # `Smolquery.Engine`'s own limit, whole. Read from the application config the
  # engine reads, so the line states what the member got, not what the buffer
  # config said. A bare DuckDB default (no limit configured anywhere) is named
  # rather than left blank.
  defp engine_memory_limit do
    :smolquery
    |> Application.get_env(Smolquery.Engine, [])
    |> Keyword.get(:memory_limit, :duckdb_default)
  end

  # Which store the sealed tier came up on, by module rather than by the
  # configuration that selected it: `SMOLQUERY_S3_BUCKET` decides it, and a
  # deployment that meant to be on S3 and is quietly on local disk is the
  # same class of invisible mistake as the rest of this module.
  defp store_impl(%Store{impl: impl}), do: inspect(impl)

  defp warn_slow(false, _message), do: :ok
  defp warn_slow(true, message), do: Logger.warning("slow path: " <> message)

  defp describe(labels), do: Enum.map_join(labels, " ", fn {k, v} -> "#{k}=#{v}" end)
end
