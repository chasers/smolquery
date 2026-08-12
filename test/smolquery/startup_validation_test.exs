defmodule Smolquery.StartupValidationTest do
  use ExUnit.Case, async: false

  alias Smolquery.StartupValidation

  setup do
    applications = [
      :roles,
      Smolquery.Cluster,
      Smolquery.BufferService,
      Smolquery.IngestService,
      Smolquery.QueryService,
      Smolquery.StorageService
    ]

    previous = Map.new(applications, &{&1, Application.fetch_env(:smolquery, &1)})
    on_exit(fn -> Enum.each(previous, &restore/1) end)

    Application.put_env(:smolquery, :roles, [:ingest, :query, :buffer, :storage])
    Application.put_env(:smolquery, Smolquery.Cluster, enabled: false)
    Application.put_env(:smolquery, Smolquery.BufferService, valid_buffer())
    Application.put_env(:smolquery, Smolquery.IngestService, write_partitions: 1)
    Application.put_env(:smolquery, Smolquery.QueryService, write_partitions: 1)
    Application.put_env(:smolquery, Smolquery.StorageService, buffer_hot_port: 4001)

    :ok
  end

  test "accepts a valid resolved configuration" do
    assert StartupValidation.validate!() == :ok
  end

  test "rejects a buffer capacity at or below the flush trigger" do
    put_buffer(max_buffered_bytes: 2_000_000)

    assert_raise ArgumentError, ~r/SMOLQUERY_MAX_BUFFERED_BYTES.*greater than.*2000000/, fn ->
      StartupValidation.validate!()
    end
  end

  test "rejects an invalid segment-shipping replication factor" do
    put_buffer(
      replicator: {Smolquery.BufferService.Replicator.SegmentShipping, replication_factor: 1}
    )

    assert_raise ArgumentError, ~r/SMOLQUERY_BUFFER_REPLICATION.*at least 2.*1/, fn ->
      StartupValidation.validate!()
    end
  end

  test "accepts the none replicator" do
    put_buffer(replicator: {Smolquery.BufferService.Replicator.None, []})

    assert StartupValidation.validate!() == :ok
  end

  test "rejects mismatched ingest and query partitions" do
    Application.put_env(:smolquery, Smolquery.QueryService, write_partitions: 2)

    assert_raise ArgumentError, ~r/SMOLQUERY_WRITE_PARTITIONS.*ingest=1, query=2/, fn ->
      StartupValidation.validate!()
    end
  end

  test "does not compare partitions when one role is disabled" do
    Application.put_env(:smolquery, :roles, [:ingest, :buffer, :storage])
    Application.put_env(:smolquery, Smolquery.QueryService, write_partitions: 2)

    assert StartupValidation.validate!() == :ok
  end

  test "rejects mismatched hot server ports" do
    Application.put_env(:smolquery, Smolquery.StorageService, buffer_hot_port: 4002)

    assert_raise ArgumentError, ~r/SMOLQUERY_HOT_SERVER_PORT.*buffer=4001.*storage=4002/, fn ->
      StartupValidation.validate!()
    end
  end

  test "does not compare hot ports for disabled roles" do
    Application.put_env(:smolquery, :roles, [:ingest, :query])
    Application.put_env(:smolquery, Smolquery.StorageService, buffer_hot_port: 4002)

    assert StartupValidation.validate!() == :ok
  end

  test "rejects an explicitly empty expected node list in a cluster" do
    Application.put_env(:smolquery, Smolquery.Cluster, enabled: true)
    put_buffer(expected_node_names: [])

    assert_raise ArgumentError, ~r/SMOLQUERY_BUFFER_NODES.*must not be empty/, fn ->
      StartupValidation.validate!()
    end
  end

  test "allows an absent expected node list in a cluster" do
    Application.put_env(:smolquery, Smolquery.Cluster, enabled: true)
    put_buffer(Keyword.delete(valid_buffer(), :expected_node_names))

    assert StartupValidation.validate!() == :ok
  end

  defp valid_buffer do
    [
      flush_max_bytes: 2_000_000,
      max_buffered_bytes: 64_000_000,
      hot_server_port: 4001,
      replicator: {Smolquery.BufferService.Replicator.SegmentShipping, replication_factor: 2},
      expected_node_names: ["buffer@host"]
    ]
  end

  defp put_buffer(options) do
    Application.put_env(
      :smolquery,
      Smolquery.BufferService,
      Keyword.merge(Application.get_env(:smolquery, Smolquery.BufferService, []), options)
    )
  end

  defp restore({application, {:ok, value}}),
    do: Application.put_env(:smolquery, application, value)

  defp restore({application, :error}), do: Application.delete_env(:smolquery, application)
end
