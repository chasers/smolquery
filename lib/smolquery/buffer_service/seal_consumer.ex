defmodule Smolquery.BufferService.SealConsumer do
  @moduledoc """
  Who to tell when a table's hot tier is ready to seal.

  The buffer service cannot name its consumer. `StorageService.Sealer` is the real
  one, and a dependency in that direction is forbidden precisely so the buffer
  stays deployable on its own — so the consumer is configuration, resolved at
  runtime:

      config :smolquery, Smolquery.BufferService,
        seal_consumer: {Smolquery.BufferService.SealLog, []}

  ## The signal is level-triggered

  `seal_ready/3` is called when a table crosses `seal_max_bytes`,
  `seal_max_files`, or `seal_max_age_ms`, and called *again* every
  `seal_retry_ms` while it stays crossed. A sealer that dies mid-handoff must not
  leave a table's tail parked forever, so a lost signal costs a retry interval
  rather than everything. Consumers should therefore expect repeats for the same
  segments and treat them as a statement of the current state, not an event.

  ## It must not block

  The call happens in the owning `TableBuffer`, so a slow consumer is a slow write
  path for that table. Making the hop asynchronous is the consumer's job, not this
  module's: whether reaching the sealer is a local cast or a network round trip is
  a transport detail, and transport belongs to the client that owns it.

  A consumer that raises takes the buffer down with it. That is deliberate — it is
  a misconfiguration, and it should be loud. It is also safe: signalling happens
  after a commit has replied to its callers, so no acked write can be lost to it,
  and the buffer recovers its manifest on restart.
  """

  alias Smolquery.Segments.Store

  @doc """
  Reports that `ids` are ready to be sealed for `table_ref`.
  """
  @callback seal_ready(config :: term(), Store.table_ref(), [String.t()]) :: :ok

  @doc """
  Dispatches a signal to a configured consumer.
  """
  @spec seal_ready({module(), term()}, Store.table_ref(), [String.t()]) :: :ok
  def seal_ready({impl, config}, table_ref, ids), do: impl.seal_ready(config, table_ref, ids)
end
