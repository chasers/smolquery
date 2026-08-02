defmodule Smolquery.BufferService.SealConsumer do
  @moduledoc """
  Who to tell when a table's hot tier is ready to seal.

  The buffer service cannot name its consumer. `StorageService.Sealer` is the real
  one, and a dependency in that direction is forbidden precisely so the buffer
  stays deployable on its own — so the consumer is configuration, resolved at
  runtime:

      config :smolquery, Smolquery.BufferService,
        seal_consumer: {Smolquery.BufferService.SealLog, []}

  ## The signal is level-triggered, and carries a frozen claim

  `seal_ready/3` is called when a table crosses `seal_max_bytes`,
  `seal_max_files`, or `seal_max_age_ms`, and called *again* every
  `seal_retry_ms` until the claim it names is retired. A sealer that dies
  mid-handoff must not leave a table's tail parked forever, so a lost signal costs
  a retry interval rather than everything.

  Consumers should therefore expect repeats — and can rely on them being
  *identical*. The claim's id set is frozen in the manifest log before the first
  signal goes out, so every repeat names the same micro-segments and the same
  sealed segment key, however much has been written since and whichever side
  crashed in between. That is what a consumer's idempotence can be built on; see
  `Smolquery.BufferService.HotManifest` for the claim itself.

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

  alias Smolquery.BufferService.HotManifest
  alias Smolquery.Segments.Store

  @typedoc """
  The frozen set to seal, the sealed segment key(s) it becomes, and the node
  whose hot tier holds the claimed micro-segments.

  `:origin` is stamped per signal rather than frozen in the manifest log: it
  names where the bytes are *now*, which a consumer on another node needs to
  pull the manifest and segments from (Milestone 8 L6) — the buffer ring's
  current owner is the wrong node exactly when a ring change moved ownership
  away from the data. A claim without it (an old log, a direct test call)
  falls back to the consumer's configured `buffer_base_url`.
  """
  @type claim :: %{
          required(:ids) => [String.t()],
          required(:keys) => [String.t()],
          optional(:origin) => node()
        }

  @doc """
  Reports that `claim` is ready to be sealed for `table_ref`.
  """
  @callback seal_ready(config :: term(), Store.table_ref(), claim()) :: :ok

  @doc """
  Dispatches a signal to a configured consumer, stamped with this node as the
  claim's `:origin`.
  """
  @spec seal_ready({module(), term()}, Store.table_ref(), HotManifest.claim()) :: :ok
  def seal_ready({impl, config}, table_ref, claim),
    do: impl.seal_ready(config, table_ref, Map.put(claim, :origin, node()))
end
