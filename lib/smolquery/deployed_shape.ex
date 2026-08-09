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
  alias Smolquery.Telemetry

  @doc """
  Logs a service's resolved shape and registers it for `GET /metrics`.

  Takes the runtime struct the service actually booted with, not the
  configuration it was asked for — the two differ exactly when it matters.
  """
  @spec announce(BufferRuntime.t() | IngestRuntime.t()) :: :ok
  def announce(%BufferRuntime{} = runtime) do
    labels = [
      flush_writer: runtime.flush_writer,
      compression: runtime.compression,
      flush_max_bytes: runtime.flush_max_bytes,
      flush_interval_ms: runtime.flush_interval_ms,
      encode_concurrency: runtime.encode_concurrency,
      write_pool_size: runtime.write_pool_size,
      transport_tls: transport_tls?()
    ]

    Logger.info("buffer shape: #{describe(labels)}")
    Telemetry.put_info("smolquery_buffer_shape_info", labels)

    warn_slow(
      runtime.flush_writer == :polars,
      "buffer is on the :polars flush writer. The :duckdb writer is the path " <>
        "the NDJSON passthrough is built around, and selecting :polars disables " <>
        "that passthrough with it. Set SMOLQUERY_FLUSH_WRITER=duckdb."
    )

    warn_slow(
      transport_tls?(),
      "gen_rpc is running over TLS. It costs roughly 26% of ingest throughput. " <>
        "Intended for production; if this is a dev or benchmark deployment, " <>
        "check GEN_RPC_TLS."
    )

    :ok
  end

  def announce(%IngestRuntime{} = runtime) do
    labels = [
      ndjson_passthrough: runtime.ndjson_passthrough,
      write_partitions: runtime.write_partitions,
      schema_cache_ttl_ms: runtime.schema_cache_ttl_ms
    ]

    Logger.info("ingest shape: #{describe(labels)}")
    Telemetry.put_info("smolquery_ingest_shape_info", labels)

    warn_slow(
      not runtime.ndjson_passthrough,
      "the NDJSON passthrough is off, so every insert is parsed and cast row " <>
        "by row at this edge. Set SMOLQUERY_FLUSH_WRITER=duckdb."
    )

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

  defp warn_slow(false, _message), do: :ok
  defp warn_slow(true, message), do: Logger.warning("slow path: " <> message)

  defp describe(labels), do: Enum.map_join(labels, " ", fn {k, v} -> "#{k}=#{v}" end)
end
