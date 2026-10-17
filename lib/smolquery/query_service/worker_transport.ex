defmodule Smolquery.QueryService.WorkerTransport do
  @moduledoc """
  How a scattered shard reaches its worker node (T-364).

  This node runs its own shards as a direct call, the way
  `Smolquery.BufferService.Transport.Local` does for an owned table: no
  socket, no serialization, and an exit mapped to the same `{:badrpc, _}`
  shapes a remote failure has. A peer is reached over `:gen_rpc` on its own
  sockets, the way `Smolquery.BufferService.Transport.GenRpc` carries buffer
  traffic: a
  partial comes back as one parquet binary, and on the distribution
  connection that binary would sit in front of heartbeats and every other
  cluster call while it transfers.

  ## Channels

  The destination is `{peer, {:scatter, slot}}`. The job id hashes into one
  of `channels/0` slots, so concurrent jobs spread over the pool. Partials do
  not share sockets with forward-batches: a reply on `:bulk` would queue
  behind the batches bound the same way. The pool has its own size,
  `GEN_RPC_SCATTER_CHANNELS`, because partials and forward-batches have
  different shapes: one large reply per job per peer against a stream of
  writes.

      config :smolquery, Smolquery.QueryService.WorkerTransport, channels: 4

  ## What gen_rpc does not do

  Neither path kills a worker when this caller gives up: gen_rpc cannot, and
  a direct call has no deadline of its own. The request therefore carries
  the job's deadline, and `Smolquery.QueryService.PartialWorker` bounds its
  own query with it, so an abandoned shard costs at most one deadline. The
  rpc itself waits that deadline plus a margin, the same rule the buffer
  transport follows.

  ## Runners (PL-77)

  A burst shard goes to a runner leased from a `FLAME.Pool`: `FLAME.call/3`
  runs `Smolquery.QueryService.PartialWorker.run_standalone/1` on whichever
  runner the pool checks out, so the request carries the engine the shard
  builds, and the reply comes back over that runner's own distribution
  connection, which carries nothing else. `FLAME.call/3` does not return
  errors, it exits: on a checkout that waits past the timeout, on a call
  that runs past it, and when the runner dies under the shard. A pool that
  is not running raises instead, from the lookup of its table. The runner
  clause catches every one of those, because `Smolquery.QueryService.Scatter`
  runs its shards under `Task.async_stream/3` with no timeout and linked to
  the job, where an uncaught exit kills the job, skips the single-engine
  fallback and skips the partials cleanup. The call is made with
  `link: false` for the same reason: linked, the remote process would send
  the shard's task an exit *signal* when the runner died, which no `catch`
  sees; unlinked, FLAME's own monitor turns that death into the exit the
  clause catches, and the runner's terminator still ends the remote work
  when this node goes away.

  ## Errors

  A peer outside `Node.list/0` is refused before any connect attempt. Every
  transport failure becomes `{:error, {:worker_unreachable, target, reason}}`,
  where `target` is the peer node or `{:runner, pool}`, which
  `Smolquery.QueryService.Scatter` turns into a fallback. A peer timeout
  also hands the destination to `Smolquery.Cluster.RpcClients.check/2`, so a
  channel whose socket went dead is redialed instead of reused (T-613).
  """

  alias Smolquery.Cluster.RpcClients
  alias Smolquery.QueryService.PartialWorker

  @rpc_margin_ms 5_000
  @default_channels 4

  @typedoc """
  Where a shard runs: a peer node, or a runner leased from a pool, with the
  longest a lease may wait for a checkout (`wait_ms`, default: the shard's
  own timeout).
  """
  @type target ::
          node()
          | %{
              required(:kind) => :runner,
              required(:pool) => atom(),
              optional(:wait_ms) => timeout()
            }

  @doc """
  Runs `request` on `target`'s `PartialWorker` for job `job_id`: `run/2` on
  a peer node, `run_standalone/1` on a runner leased from the pool.

  A runner shard runs under two clocks (PL-77 D7). The lease waits
  `wait_ms` for a checkout, booting a runner when the pool has room; the
  shard itself then has `timeout_ms`, the job's own deadline, so a lease
  that waits never eats the budget the shard needs to run. The shard is a
  `Task` placed on the runner (`FLAME.place_child/3`), which holds the
  runner checked out for exactly as long as it lives, and its answer comes
  back as a message over the runner's own distribution connection. A
  lease that times out is `{:checkout, :timeout}`; a shard that outruns
  its deadline is killed on the runner and answers `:timeout`; a runner
  that dies under its shard answers with the exit reason.
  """
  @spec call(
          target(),
          atom(),
          PartialWorker.request() | PartialWorker.standalone_request(),
          String.t(),
          timeout()
        ) ::
          {:ok, PartialWorker.reply()} | {:error, term()}
  def call(target, name, request, job_id, timeout_ms)

  def call(%{kind: :runner, pool: pool} = target, _name, request, _job_id, timeout_ms) do
    caller = self()
    ref = make_ref()
    shard = {Task, fn -> send(caller, {ref, PartialWorker.run_standalone(request)}) end}

    case FLAME.place_child(pool, shard,
           timeout: Map.get(target, :wait_ms, timeout_ms),
           link: false
         ) do
      {:ok, pid} -> await(pid, ref, timeout_ms, pool)
      {:error, reason} -> {:error, {:worker_unreachable, {:runner, pool}, {:checkout, reason}}}
    end
  rescue
    ArgumentError -> {:error, {:worker_unreachable, {:runner, pool}, :noproc}}
  catch
    :exit, reason ->
      {:error, {:worker_unreachable, {:runner, pool}, {:checkout, exit_reason(reason)}}}
  end

  def call(peer, name, request, _job_id, _timeout_ms) when peer == node() do
    PartialWorker.run(name, request)
  catch
    :exit, {:timeout, _call} -> {:error, {:worker_unreachable, peer, {:badrpc, :timeout}}}
    :exit, reason -> {:error, {:worker_unreachable, peer, {:badrpc, {:EXIT, reason}}}}
  end

  def call(peer, name, request, job_id, timeout_ms) do
    if peer in Node.list() do
      remote(peer, name, request, job_id, timeout_ms)
    else
      {:error, {:worker_unreachable, peer, :nodedown}}
    end
  end

  @doc """
  The gen_rpc destination job `job_id`'s shard travels on to `peer`.
  """
  @spec destination(node(), String.t()) :: {node(), {:scatter, pos_integer()}}
  def destination(peer, job_id),
    do: {peer, {:scatter, :erlang.phash2(job_id, channels()) + 1}}

  @doc """
  How many `{:scatter, _}` connections this node opens to each peer.
  """
  @spec channels() :: pos_integer()
  def channels do
    :smolquery
    |> Application.get_env(__MODULE__, [])
    |> Keyword.get(:channels, @default_channels)
  end

  defp remote(peer, name, request, job_id, timeout_ms) do
    destination = destination(peer, job_id)

    case :gen_rpc.call(destination, PartialWorker, :run, [name, request], deadline(timeout_ms)) do
      {:badrpc, :timeout} ->
        RpcClients.check(destination)
        {:error, {:worker_unreachable, peer, {:badrpc, :timeout}}}

      {:badrpc, reason} ->
        {:error, {:worker_unreachable, peer, {:badrpc, reason}}}

      {:badtcp, reason} ->
        {:error, {:worker_unreachable, peer, {:badtcp, reason}}}

      result ->
        result
    end
  end

  defp deadline(:infinity), do: :infinity
  defp deadline(timeout_ms), do: timeout_ms + @rpc_margin_ms

  defp await(pid, ref, timeout_ms, pool) do
    monitor = Process.monitor(pid)

    receive do
      {^ref, result} ->
        Process.demonitor(monitor, [:flush])
        result

      {:DOWN, ^monitor, :process, ^pid, reason} ->
        {:error, {:worker_unreachable, {:runner, pool}, reason}}
    after
      timeout_ms ->
        Process.exit(pid, :kill)
        Process.demonitor(monitor, [:flush])
        {:error, {:worker_unreachable, {:runner, pool}, :timeout}}
    end
  end

  defp exit_reason({reason, {FLAME.Pool, _fun, _args}}), do: reason
  defp exit_reason(reason), do: reason
end
