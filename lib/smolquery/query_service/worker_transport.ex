defmodule Smolquery.QueryService.WorkerTransport do
  @moduledoc """
  How a scattered shard reaches its worker node (T-364).

  This node runs its own shards through `:erpc.call/4`, which spawns the
  worker in-process and kills it if this caller dies. A peer is reached over
  `:gen_rpc` on its own sockets, the way
  `Smolquery.BufferService.Transport.GenRpc` carries buffer traffic: a
  partial comes back as one parquet binary, and on the distribution
  connection that binary would sit in front of heartbeats and every other
  cluster call while it transfers.

  ## Channels

  The destination is `{peer, {:scatter, slot}}`. The job id hashes into one
  of `Transport.GenRpc.bulk_channels/0` slots, so concurrent jobs spread over
  the pool. Partials do not share sockets with forward-batches: a reply on
  `:bulk` would queue behind the batches bound the same way.

  ## What gen_rpc does not do

  gen_rpc does not kill the remote worker when this caller dies or gives up.
  The request therefore carries the job's deadline, and
  `Smolquery.QueryService.PartialWorker` bounds its own query with it, so an
  abandoned shard costs the peer at most one deadline. The rpc itself waits
  that deadline plus a margin, the same rule the buffer transport follows.

  ## Errors

  A peer outside `Node.list/0` is refused before any connect attempt. Every
  transport failure becomes `{:error, {:worker_unreachable, peer, reason}}`,
  which `Smolquery.QueryService.Scatter` turns into a fallback.
  """

  alias Smolquery.BufferService.Transport.GenRpc
  alias Smolquery.QueryService.PartialWorker

  @rpc_margin_ms 5_000

  @doc """
  Runs `request` on `peer`'s `PartialWorker` for job `job_id`.
  """
  @spec call(node(), atom(), PartialWorker.request(), String.t(), timeout()) ::
          {:ok, %{parquet: binary(), rows: non_neg_integer()}} | {:error, term()}
  def call(peer, name, request, job_id, timeout_ms)

  def call(peer, name, request, _job_id, timeout_ms) when peer == node() do
    :erpc.call(peer, PartialWorker, :run, [name, request], timeout_ms)
  catch
    kind, reason -> {:error, {:worker_unreachable, peer, {kind, reason}}}
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
    do: {peer, {:scatter, :erlang.phash2(job_id, GenRpc.bulk_channels()) + 1}}

  defp remote(peer, name, request, job_id, timeout_ms) do
    destination = destination(peer, job_id)

    case :gen_rpc.call(destination, PartialWorker, :run, [name, request], deadline(timeout_ms)) do
      {:badrpc, reason} -> {:error, {:worker_unreachable, peer, {:badrpc, reason}}}
      {:badtcp, reason} -> {:error, {:worker_unreachable, peer, {:badtcp, reason}}}
      result -> result
    end
  end

  defp deadline(:infinity), do: :infinity
  defp deadline(timeout_ms), do: timeout_ms + @rpc_margin_ms
end
