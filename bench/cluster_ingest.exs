Code.require_file("support.exs", __DIR__)
Code.require_file("cluster_ingest_peer.exs", __DIR__)

defmodule Bench.ClusterIngest do
  @moduledoc """
  Milestone 8's exit-criterion number (PL-11 L8): does aggregate ingest scale
  with buffer-node count, or is it flat?

  A real fleet of peer BEAMs runs the `:buffer` role, joined into one ownership
  ring through the same `:pg` membership production uses — so ownership is
  resolved by `Smolquery.BufferService.Routing` and cross-node writes take
  gen_rpc exactly as they would in the cluster. Node count sweeps 1..`NODES`,
  and at each count two topologies are measured, because they answer different
  questions:

    * **edge** — one driver node fans out to the whole fleet. This is the kind
      cluster's shape (api ×1, buffer ×3) and the number a deployment actually
      gets; it is also where the edge's own serialization cost can become the
      ceiling instead of the fleet's.
    * **fleet** — a driver on every buffer node, writing only to tables that
      node owns. No fan-out, no remote transport: the fleet's raw ceiling.

  Offered load scales with the fleet (`WRITERS` is *per buffer node*), so
  per-node throughput holding steady while the aggregate climbs is what
  scaling looks like, and per-node throughput falling off is where it stops.

  `BATCHES` defaults high enough to keep every timed window several seconds
  long. At this throughput a smaller default finishes in under a second, where
  scheduling and warmup noise are a large fraction of the measurement.

  Not run against the kind cluster on purpose: that cluster lives in a 4 GB
  Docker VM with every pod on a shrunken request, so its numbers would measure
  VM contention rather than the fleet. kind is where L8's *correctness* clauses
  are proven (`scripts/kind-smoke.sh`); this is where the throughput claim is.

  ## How the fleet is stood up

  The driver node runs no buffer subtree of its own — it resolves ownership from
  `:pg` alone, the way a query or ingest node does. Clustering is switched on
  after boot, because `mix run` already started the application with it off, so
  the driver's `:pg` scope is started here rather than under
  `Smolquery.Cluster.children/0`. Each peer sets an empty `:libcluster`
  topology list, which keeps that same child from reaching for the Postgres
  discovery it does not need — the peers are already connected by the `:erpc`
  calls that configure them.

      mix run bench/cluster_ingest.exs
      NODES=4 WRITERS=16 ROWS=2000 mix run bench/cluster_ingest.exs
  """

  import Bench.Support

  alias Smolquery.BufferService.Client
  alias Smolquery.BufferService.Ring
  alias Smolquery.BufferService.Routing
  alias Smolquery.Cluster
  alias Smolquery.Cluster.PgGroup

  @buffer_name :cluster_ingest_bench
  @primary_gen_rpc_port 5_369
  @peer_gen_rpc_port_base 5_380
  @cookie :smolquery_bench_cookie

  def main do
    Logger.configure(level: :warning)

    nodes = env("NODES", 3)
    writers = env("WRITERS", 8)
    batches = env("BATCHES", 200)
    rows = env("ROWS", 1_000)
    per_node = env("TABLES", 4)

    start_primary!()

    batch = %{schema: schema(), rows: rows_for(rows)}

    heading(
      "Aggregate ingest — #{writers} writers/node × #{batches} batches × #{rows} rows, " <>
        "#{per_node} tables/node"
    )

    IO.puts(
      label("topology", 10) <>
        pad("nodes", 7) <>
        pad("rows", 12) <> pad("krows/s", 12) <> pad("per node", 12) <> pad("scale", 8)
    )

    try do
      sweep(nodes, writers, batches, rows, per_node, batch)
    after
      stop_fleet!()
      IO.puts("")
    end
  end

  defp sweep(nodes, writers, batches, rows, per_node, batch) do
    Enum.reduce(1..nodes, %{}, fn count, baseline ->
      peers = grow_fleet!(count)
      members = await_members!(count)
      tables = tables_by_owner(members, per_node)

      Enum.reduce([:edge, :fleet], baseline, fn topology, baseline ->
        total = count * writers * batches * rows

        run(topology, peers, tables, writers, 1, batch)

        {us, :ok} = :timer.tc(fn -> run(topology, peers, tables, writers, batches, batch) end)

        krows = Float.round(total / (us / 1_000_000) / 1_000, 1)
        base = Map.get(baseline, topology, krows)

        IO.puts(
          label(to_string(topology), 10) <>
            pad(count, 7) <>
            pad(total, 12) <>
            pad(krows, 12) <>
            pad(Float.round(krows / count, 1), 12) <>
            pad(Float.round(krows / base, 2), 8)
        )

        Map.put_new(baseline, topology, krows)
      end)
    end)
  end

  # ── topologies ────────────────────────────────────────────────────────

  defp run(:edge, peers, tables, writers, batches, batch) do
    flat = Enum.flat_map(peers, fn peer -> Map.fetch!(tables, peer.node) end)
    total_writers = length(peers) * writers

    drive(total_writers, flat, batches, fn table ->
      {:ok, _ack} = Client.write_batch(@buffer_name, table, batch)
    end)
  end

  defp run(:fleet, peers, tables, writers, batches, batch) do
    peers
    |> Task.async_stream(
      fn peer ->
        :ok =
          :erpc.call(
            peer.node,
            Bench.ClusterIngest.Driver,
            :run,
            [@buffer_name, Map.fetch!(tables, peer.node), writers, batches, batch],
            600_000
          )
      end,
      max_concurrency: length(peers),
      timeout: 600_000,
      ordered: false
    )
    |> Stream.run()

    :ok
  end

  defp drive(writers, tables, batches, fun) do
    1..writers
    |> Task.async_stream(
      fn writer ->
        table = Enum.at(tables, rem(writer - 1, length(tables)))

        for _ <- 1..batches, do: fun.(table)
      end,
      max_concurrency: writers,
      timeout: 600_000,
      ordered: false
    )
    |> Stream.run()

    :ok
  end

  # ── fixtures ──────────────────────────────────────────────────────────

  defp rows_for(count) do
    base = ~N[2026-01-01 00:00:00]

    for i <- 1..count do
      %{
        "id" => i,
        "ts" => NaiveDateTime.add(base, i, :second),
        "name" => "row-#{i}",
        "amount" => Decimal.new("#{rem(i, 997)}.#{rem(i, 100)}")
      }
    end
  end

  defp tables_by_owner(members, per_node) do
    ring = Ring.new!(members)
    empty = Map.new(members, &{&1, []})

    owned =
      Enum.reduce_while(1..100_000, empty, fn i, acc ->
        table = {"analytics", "bench_t#{i}"}
        owner = Ring.owner(ring, table)
        held = Map.fetch!(acc, owner)

        acc = if length(held) < per_node, do: %{acc | owner => [table | held]}, else: acc

        if complete?(acc, per_node), do: {:halt, acc}, else: {:cont, acc}
      end)

    case complete?(owned, per_node) do
      true -> owned
      false -> raise "no #{per_node} tables per node found for #{inspect(members)}"
    end
  end

  defp complete?(owned, per_node),
    do: Enum.all?(owned, fn {_node, tables} -> length(tables) == per_node end)

  # ── fleet bootstrap ───────────────────────────────────────────────────

  defp start_primary! do
    case Node.start(:"cluster_ingest_bench@127.0.0.1", :longnames) do
      {:ok, _pid} -> :ok
      {:error, {:already_started, _pid}} -> :ok
    end

    Node.set_cookie(@cookie)

    Process.put(:base_buffer, Application.get_env(:smolquery, Smolquery.BufferService, []))
    Process.put(:base_env, Application.get_all_env(:smolquery))

    Application.put_env(:smolquery, Cluster, enabled: true)

    case :pg.start_link(Cluster.pg_scope()) do
      {:ok, _pid} -> :ok
      {:error, {:already_started, _pid}} -> :ok
    end

    Application.put_env(:smolquery, Smolquery.BufferService,
      name: @buffer_name,
      ring: [],
      write_timeout_ms: 120_000,
      control_timeout_ms: 120_000
    )

    Routing.forget(@buffer_name)
  end

  defp grow_fleet!(count) do
    peers = Process.get(:peers, [])

    if length(peers) >= count do
      Enum.take(peers, count)
    else
      peers = peers ++ [start_peer!(length(peers) + 1)]
      Process.put(:peers, peers)
      mesh!(peers)

      grow_fleet!(count)
    end
  end

  defp start_peer!(index) do
    port = @peer_gen_rpc_port_base + index

    {:ok, peer, node} =
      :peer.start(%{
        name: :"cluster_ingest_peer_#{index}",
        host: ~c"127.0.0.1",
        longnames: true,
        args: [
          ~c"-setcookie",
          Atom.to_charlist(@cookie),
          ~c"-kernel",
          ~c"prevent_overlapping_partitions",
          ~c"false"
        ]
      })

    dir =
      Path.join(
        System.tmp_dir!(),
        "smolquery-bench-cluster-ingest-#{index}-#{System.unique_integer([:positive])}"
      )

    :erpc.call(node, :code, :add_paths, [:code.get_path()])
    {:ok, _elixir} = :erpc.call(node, :application, :ensure_all_started, [:elixir])
    :ok = :erpc.call(node, Logger, :configure, [[level: :warning]])

    for {key, value} <- Process.get(:base_env, []) do
      :erpc.call(node, Application, :put_env, [:smolquery, key, value])
    end

    :erpc.call(node, Application, :put_env, [:smolquery, :roles, [:buffer]])
    :erpc.call(node, Application, :put_env, [:smolquery, Cluster, [enabled: true]])

    buffer_options =
      Keyword.merge(Process.get(:base_buffer, []),
        name: @buffer_name,
        dir: dir,
        flush_interval_ms: env("FLUSH_MS", 25),
        hot_server_port: 0,
        ring: [node],
        write_timeout_ms: 120_000,
        control_timeout_ms: 120_000
      )

    :erpc.call(node, Application, :put_env, [:smolquery, Smolquery.BufferService, buffer_options])

    for {key, value} <- [
          tcp_server_port: port,
          tcp_client_port: port,
          rpc_module_control: :whitelist,
          rpc_module_list: [Smolquery.BufferService.Endpoint]
        ] do
      :erpc.call(node, :application, :set_env, [:gen_rpc, key, value, [persistent: true]])
    end

    :erpc.call(node, Code, :require_file, [Path.expand("cluster_ingest_peer.exs", __DIR__)])

    :ok =
      :erpc.call(node, Bench.ClusterIngest.Driver, :boot, [Cluster.pg_scope(), buffer_options])

    :erpc.call(node, Routing, :forget, [@buffer_name])

    %{peer: peer, node: node, port: port, dir: dir}
  end

  defp mesh!(peers) do
    ports = Map.new(peers, &{&1.node, &1.port})

    :application.set_env(:gen_rpc, :client_config_per_node, {:internal, ports}, persistent: true)

    for peer <- peers do
      others = ports |> Map.delete(peer.node) |> Map.put(node(), @primary_gen_rpc_port)

      :erpc.call(peer.node, :application, :set_env, [
        :gen_rpc,
        :client_config_per_node,
        {:internal, others},
        [persistent: true]
      ])
    end

    :ok
  end

  defp stop_fleet! do
    Enum.each(Process.get(:peers, []), &stop_peer!/1)

    Process.put(:peers, [])
  end

  defp stop_peer!(peer) do
    :peer.stop(peer.peer)
    File.rm_rf!(peer.dir)
  catch
    :exit, _reason -> File.rm_rf!(peer.dir)
  end

  defp await_members!(count) do
    members = wait_until(fn -> length(live_members()) == count end, &live_members/0)

    case length(members) do
      ^count -> members
      _ -> raise "ring never reached #{count} members, saw #{inspect(members)}"
    end
  end

  defp live_members, do: PgGroup.nodes(Smolquery.BufferService, @buffer_name, [])

  defp wait_until(condition, value, attempts \\ 200)
  defp wait_until(_condition, value, 0), do: value.()

  defp wait_until(condition, value, attempts) do
    if condition.() do
      value.()
    else
      Process.sleep(50)
      wait_until(condition, value, attempts - 1)
    end
  end
end

Bench.ClusterIngest.main()
