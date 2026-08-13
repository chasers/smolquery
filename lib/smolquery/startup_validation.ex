defmodule Smolquery.StartupValidation do
  @moduledoc """
  Validates resolved application configuration before service supervision starts.
  """

  alias Smolquery.BufferService.Replicator
  alias Smolquery.Roles

  @doc """
  Raises `ArgumentError` when resolved startup configuration is contradictory.
  """
  @spec validate!() :: :ok
  def validate! do
    buffer = config(Smolquery.BufferService)
    ingest = config(Smolquery.IngestService)
    query = config(Smolquery.QueryService)
    storage = config(Smolquery.StorageService)

    validate_buffer_capacity!(buffer)
    validate_replication!(buffer)
    validate_partitions!(ingest, query)
    validate_hot_ports!(buffer, query, storage)
    validate_expected_node_names!(buffer)

    :ok
  end

  defp validate_buffer_capacity!(buffer) do
    flush_max_bytes = Keyword.get(buffer, :flush_max_bytes, 2_000_000)
    max_buffered_bytes = Keyword.get(buffer, :max_buffered_bytes, 64_000_000)

    if not (is_integer(max_buffered_bytes) and is_integer(flush_max_bytes) and
              max_buffered_bytes > flush_max_bytes) do
      raise ArgumentError,
            "SMOLQUERY_MAX_BUFFERED_BYTES must be greater than " <>
              "SMOLQUERY_FLUSH_MAX_BYTES (got #{inspect(max_buffered_bytes)} <= " <>
              "#{inspect(flush_max_bytes)})"
    end
  end

  defp validate_replication!(buffer) do
    case Keyword.get(buffer, :replicator, {Replicator.None, []}) do
      {Smolquery.BufferService.Replicator.SegmentShipping, opts} ->
        factor = Keyword.get(opts, :replication_factor, 2)

        if not (is_integer(factor) and factor >= 2) do
          raise ArgumentError,
                "SMOLQUERY_BUFFER_REPLICATION must be at least 2 for SegmentShipping " <>
                  "(got #{inspect(factor)})"
        end

      _other ->
        :ok
    end
  end

  defp validate_partitions!(ingest, query) do
    if Roles.enabled?(:ingest) and Roles.enabled?(:query) do
      ingest_partitions = Keyword.get(ingest, :write_partitions, 1)
      query_partitions = Keyword.get(query, :write_partitions, 1)

      if ingest_partitions != query_partitions do
        raise ArgumentError,
              "SMOLQUERY_WRITE_PARTITIONS must match for enabled ingest and query roles " <>
                "(ingest=#{ingest_partitions}, query=#{query_partitions})"
      end
    end
  end

  defp validate_hot_ports!(buffer, query, storage) do
    ports =
      [
        {:buffer, Roles.enabled?(:buffer), Keyword.get(buffer, :hot_server_port, 4001)},
        {:query, Roles.enabled?(:query), Keyword.get(query, :buffer_hot_port, 4001)},
        {:storage, Roles.enabled?(:storage), Keyword.get(storage, :buffer_hot_port, 4001)}
      ]
      |> Enum.filter(fn {_role, enabled, _port} -> enabled end)

    case Enum.uniq_by(ports, &elem(&1, 2)) do
      [_single] ->
        :ok

      [] ->
        :ok

      _different ->
        values = Enum.map_join(ports, ", ", fn {role, _enabled, port} -> "#{role}=#{port}" end)
        raise ArgumentError, "SMOLQUERY_HOT_SERVER_PORT must agree across roles (#{values})"
    end
  end

  defp validate_expected_node_names!(buffer) do
    if Smolquery.Cluster.enabled?() and Keyword.has_key?(buffer, :expected_node_names) and
         Keyword.get(buffer, :expected_node_names) == [] do
      raise ArgumentError,
            "SMOLQUERY_BUFFER_NODES must not be empty when clustering is enabled"
    end
  end

  defp config(application), do: Application.get_env(:smolquery, application, [])
end
