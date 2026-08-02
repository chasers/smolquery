defmodule Bench.ClusterIngest.Driver do
  @moduledoc """
  The peer side of `bench/cluster_ingest.exs`, loaded on every buffer node with
  `Code.require_file/1` and invoked over `:erpc`.

  ## Why `boot/2` and not `Application.ensure_all_started(:smolquery)`

  Starting the whole application on a peer pulls in the asset-pipeline
  applications a dev build carries (`:esbuild`), which expect Mix to be running
  and crash the boot on a peer that has no project. So this starts exactly the
  buffer role's subtree instead — the same `Smolquery.BufferService.Supervisor`
  the application's `:buffer` role starts, over the `:pg` scope
  `Smolquery.Cluster.PgGroup` joins.

  Both are started with `start_link/1` and then unlinked, the way
  `Bench.IngestTransport.Peer` starts its listener: an `:erpc` call runs in a
  transient worker that exits the moment the call returns, and a link to it
  would take the subtree down with it.

  ## `run/5`

  The fleet-topology driver writes only to tables the node it runs on already
  owns, so every write takes `Smolquery.BufferService.Transport.Local` and the
  measurement excludes the ingest edge's fan-out entirely — the fleet's own
  ceiling, against which the edge topology's number is the cost of getting
  there.
  """

  alias Smolquery.BufferService.Client

  @apps [:telemetry, :gen_rpc, :explorer, :bandit]

  def boot(scope, buffer_options) do
    for app <- @apps do
      {:ok, _started} = Application.ensure_all_started(app)
    end

    unlinked(fn -> :pg.start_link(scope) end)
    unlinked(fn -> Smolquery.BufferService.Supervisor.start_link(buffer_options) end)

    :ok
  end

  def run(name, tables, writers, batches, batch) do
    1..writers
    |> Task.async_stream(
      fn writer ->
        table = Enum.at(tables, rem(writer - 1, length(tables)))

        for _ <- 1..batches do
          {:ok, _ack} = Client.write_batch(name, table, batch)
        end
      end,
      max_concurrency: writers,
      timeout: 600_000,
      ordered: false
    )
    |> Stream.run()

    :ok
  end

  defp unlinked(fun) do
    case fun.() do
      {:ok, pid} -> Process.unlink(pid)
      {:error, {:already_started, _pid}} -> :ok
    end

    :ok
  end
end
