defmodule Smolquery.BufferService.Replicator do
  @moduledoc """
  What must be true, beyond this node's own disk, before a group commit may
  ack (PL-13, feeding T-96).

  `Smolquery.BufferService.TableBuffer` commits a flush locally — segment
  into the store, entry fsynced into the manifest log — and then, before any
  waiter is told its rows are durable, hands the commit here. The
  implementation decides what "durable" means beyond the local copy:

    * `Smolquery.BufferService.Replicator.None` — nothing; today's
      single-copy hot tier, unchanged
    * segment shipping (T-96) — the encoded segment and its manifest record
      on `replication_factor - 1` followers' disks, fsynced, before the ack
    * a shared store (T-26) needs no segment replication at all —
      `Smolquery.Segments.Store.shared?/1` already moves the bytes off-node,
      and an implementation covers only the manifest record

  The seam sits *after* the local commit deliberately: a follower's disk must
  be indistinguishable from a crashed owner's, so the owner's own
  store-put + manifest-append is also the template for what ships. Whatever
  the implementation does, the invariant it must keep is that every copy it
  reports `:ok` for is `Smolquery.BufferService.Adopter`-replayable — that
  symmetry is the entire failover story (PL-5).

  `{:error, reason}` fails the flush: every waiter gets the error even
  though the local copy exists. That is the ack-all rule PL-5 chose — fail
  honestly rather than ack a copy count the caller was not promised. A
  retried batch dedups on its `:batch_id` against the local manifest, so
  the failure is safe to retry through.

  ## Configuration

      config :smolquery, Smolquery.BufferService,
        replicator: {Smolquery.BufferService.Replicator.None, []}

  """

  alias Smolquery.BufferService.HotManifest.Entry
  alias Smolquery.Segments.Segment
  alias Smolquery.Segments.Store

  @enforce_keys [:impl, :config]
  defstruct [:impl, :config]

  @type t :: %__MODULE__{impl: module(), config: term()}

  @typedoc """
  One committed flush, as the implementation sees it: the instance and table
  it belongs to, the segment the store holds, the manifest entry that made it
  durable here, and the batch ids the entry carries.
  """
  @type commit :: %{
          name: atom(),
          table_ref: Store.table_ref(),
          store: Store.t(),
          segment: Segment.t(),
          entry: Entry.t(),
          batch_ids: [String.t()]
        }

  @doc """
  Resolves implementation-specific options into the `config` term the other
  callbacks receive.
  """
  @callback new(keyword()) :: term()

  @typedoc """
  A manifest mutation that is not a commit: a claim freeze, a retire, or a
  grace-period drop. These replicate too — a follower whose manifest drifts
  from its owner's freezes *different* claims, and two different claims over
  the same rows double-commit them to the catalog. `args` is the mutation's
  own vocabulary: `%{ids: ids, keys: keys}` for `:claim`,
  `%{ids: ids, snapshot: snapshot}` for `:retire`, `%{ids: ids}` for `:drop`.
  """
  @type mutation :: %{
          name: atom(),
          table_ref: Store.table_ref(),
          op: :claim | :retire | :drop,
          args: map()
        }

  @doc """
  Makes `commit` durable to this implementation's standard, or says why it
  could not. Runs inside the group commit, once per flush — its latency is
  paid by every waiter of that flush.
  """
  @callback commit(term(), commit()) :: :ok | {:error, term()}

  @doc """
  Applies `mutation` wherever replicas live, *before* the owner applies it
  locally — the reverse of `commit/2`'s order, and deliberately so. All
  three mutations are idempotent and deterministic, so a follower that is
  ahead of its owner is harmless (the owner's retry re-ships the same
  mutation), while a follower left behind freezes divergent claims.
  """
  @callback append(term(), mutation()) :: :ok | {:error, term()}

  @doc """
  How many copies exist beyond the owner's, once `commit/2` has said `:ok`.

  What the read side may lean on: a planner can tolerate this many absent
  buffer nodes and still answer completely, because every acked row an
  absent node held exists on that many other disks (T-97).
  """
  @callback redundancy(term()) :: non_neg_integer()

  @doc """
  A replicator from `{impl, opts}` configuration.
  """
  @spec new({module(), keyword()} | t()) :: t()
  def new(%__MODULE__{} = replicator), do: replicator
  def new({impl, opts}), do: %__MODULE__{impl: impl, config: impl.new(opts)}

  @doc """
  Runs `commit` through the configured implementation.
  """
  @spec commit(t(), commit()) :: :ok | {:error, term()}
  def commit(%__MODULE__{impl: impl, config: config}, commit) do
    impl.commit(config, commit)
  end

  @doc """
  Runs `mutation` through the configured implementation.
  """
  @spec append(t(), mutation()) :: :ok | {:error, term()}
  def append(%__MODULE__{impl: impl, config: config}, mutation) do
    impl.append(config, mutation)
  end

  @doc """
  The configured implementation's copy count beyond the owner.
  """
  @spec redundancy(t()) :: non_neg_integer()
  def redundancy(%__MODULE__{impl: impl, config: config}) do
    impl.redundancy(config)
  end
end
