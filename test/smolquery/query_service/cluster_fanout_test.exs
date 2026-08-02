defmodule Smolquery.QueryService.ClusterFanoutTest do
  @moduledoc """
  Milestone 8 L5: `Planner.base_url/2` derives an owner's `HotServer` URL
  from the owning node's own name once clustering is on, instead of the
  single-node `buffer_base_url` — the "one function" change the L1-era
  moduledoc called out ("the ring is `[self]`; Milestone 8 replaces that
  one function with ring lookup").

  This proves the derivation reaches a real, live `HotServer` through a
  real (if loopback) node name rather than the static config string —
  distribution starts with a connectable name (`@127.0.0.1`) so
  `node_host/1` has something real to compute from. It does not attempt a
  *second* real node on the same machine: two peers sharing one loopback
  host cannot both bind the one port every real deployment's buffer nodes
  share (`hot_server_port`), which is exactly the assumption `base_url/2`
  relies on — different real hosts, same port. Genuine cross-host fan-out
  (distinct pod IPs, same port) is what the kind cluster (Milestone 8 L7)
  exists to prove for real; T-77 already scoped it as "the harness for
  ring drain and fan-out testing".

  "An unreachable owner fails the whole plan" is not new here and already
  has unit coverage (`planner_test.exs`, "an unreachable hot tier fails
  the plan") — that path is `manifests/2`'s `Task.async_stream` halting on
  any error, which L5 does not touch.
  """

  use ExUnit.Case, async: false

  alias Smolquery.BufferService
  alias Smolquery.BufferService.Client
  alias Smolquery.Catalog
  alias Smolquery.Catalog.DuckLake
  alias Smolquery.Cluster
  alias Smolquery.Engine
  alias Smolquery.QueryService.Planner
  alias Smolquery.QueryService.Runtime
  alias Smolquery.Schema

  @moduletag :integration
  @moduletag :tmp_dir

  @lake __MODULE__.Lake
  @job __MODULE__.Job
  @table {"analytics", "events"}

  setup context do
    ensure_distributed()
    ensure_pg_scope!()

    previous_cluster = Application.fetch_env(:smolquery, Cluster)
    Application.put_env(:smolquery, Cluster, enabled: true, postgres: [])
    on_exit(fn -> restore_cluster(previous_cluster) end)

    metadata = "sqlite:#{Path.join(context.tmp_dir, "catalog.sqlite")}"
    data_path = Path.join(context.tmp_dir, "data")

    start_supervised!({DuckLake, name: @lake, metadata: metadata, data_path: data_path})
    catalog = DuckLake.new(engine: @lake)
    :ok = Catalog.create_dataset(catalog, "analytics")
    :ok = Catalog.create_table(catalog, @table, schema())

    buffer = :"cluster_fanout_buffer_#{:erlang.unique_integer([:positive])}"

    start_supervised!(
      {BufferService.Supervisor,
       name: buffer,
       dir: Path.join(context.tmp_dir, "buffer"),
       flush_interval_ms: 25,
       hot_server_port: 0,
       ring: [node()]},
      id: buffer
    )

    on_exit(fn -> BufferService.Runtime.delete(buffer) end)

    {:ok, _ack} =
      Client.write_batch(buffer, @table, %{
        schema: schema(),
        rows: [%{"id" => 1, "name" => "local"}]
      })

    hot_port = hot_server_port(buffer)

    start_supervised!(
      {Engine,
       name: @job,
       extensions: [:ducklake, :httpfs],
       statements: [
         Smolquery.InternalSecret.create_secret_statement("http://"),
         DuckLake.attach_statement(DuckLake.default_catalog(), metadata, data_path)
       ]}
    )

    runtime =
      Runtime.new(
        name: :"cluster_fanout_#{:erlang.unique_integer([:positive])}",
        catalog: catalog,
        buffer_name: buffer,
        buffer_hot_port: hot_port,
        buffer_timeout_ms: 2_000
      )

    %{runtime: runtime}
  end

  defp schema, do: Schema.new!([{"id", :int64, nullable: false}, {"name", :string}])

  defp hot_server_port(buffer) do
    %{port: port} = buffer |> BufferService.HotServer.base_url() |> URI.parse()
    port
  end

  test "reads a table through its real owner's node-derived HotServer URL", %{runtime: runtime} do
    assert {:ok, plan} =
             Planner.plan(runtime, Engine.connection_name(@job), "SELECT * FROM analytics.events")

    assert [_entry] = plan.hot[@table]
  end

  defp ensure_distributed do
    case Node.start(:"cluster_fanout_primary@127.0.0.1", :longnames) do
      {:ok, _pid} -> :ok
      {:error, {:already_started, _pid}} -> :ok
    end
  end

  defp ensure_pg_scope! do
    case :pg.start_link(Cluster.pg_scope()) do
      {:ok, _pid} -> :ok
      {:error, {:already_started, _pid}} -> :ok
    end
  end

  defp restore_cluster({:ok, value}), do: Application.put_env(:smolquery, Cluster, value)
  defp restore_cluster(:error), do: Application.delete_env(:smolquery, Cluster)
end
