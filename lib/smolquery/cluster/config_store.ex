defmodule Smolquery.Cluster.ConfigStore do
  @moduledoc """
  The strongly consistent home of a ring's configuration: an epoch, the member
  list it names, and the member list it replaced.

  `:pg` answers *who is here right now*, per node, eventually — which is the
  wrong authority for fencing, because during a partition two nodes can hold
  different member lists and each believe the ring names them owner of the
  same table (T-92). This store is the tie-breaker: one row per ring, advanced
  by compare-and-swap on the epoch, so concurrent proposals serialize and
  exactly one member list is *the* configuration at any epoch. PacificA calls
  this role the configuration manager; PL-13 is the reasoning for adopting
  that shape rather than a consensus log.

  A configuration answers with its `age_ms` — how long ago it last changed —
  computed by the store's own clock. Lease math must never compare one node's
  clock against another's; every consumer works in "store time ago" plus its
  own monotonic clock, which assumes only bounded clock *rate* drift.

  `prev_members` exists for the settling rule: a node that just acquired a
  table must wait out the previous owner's lease before accepting writes for
  it, and it can only know who that was if the store remembers.

  Implementations: `Smolquery.Cluster.ConfigStore.Postgres` (deployments —
  the same Postgres the catalog and node discovery already require) and an
  ETS-backed test double in `test/support`.
  """

  @type server :: term()
  @type scope :: String.t()

  @type config :: %{
          epoch: non_neg_integer(),
          members: [node()],
          prev_members: [node()] | nil,
          age_ms: non_neg_integer()
        }

  @doc """
  Starts whatever process the implementation needs; the returned pid is the
  `server` every other callback takes.
  """
  @callback start_link(keyword()) :: GenServer.on_start()

  @doc """
  Prepares the store's schema. Idempotent, and safe to retry until it
  succeeds — a caller booting before the store is reachable keeps calling.
  """
  @callback setup(server()) :: :ok | {:error, term()}

  @doc """
  The current configuration for `scope`.
  """
  @callback fetch(server(), scope()) :: {:ok, config()} | :not_found | {:error, term()}

  @doc """
  Creates `scope` at epoch 0 with `members` if it does not exist, then returns
  whatever configuration is current — the existing one wins a race.
  """
  @callback ensure(server(), scope(), [node()]) :: {:ok, config()} | {:error, term()}

  @doc """
  Advances `scope` to a new configuration naming `members`, iff the stored
  epoch is still `expected_epoch`. `{:error, :conflict}` means the epoch no
  longer matches — another node advanced first, or the scope does not exist —
  so the refetch that reconsiders it must also handle `:not_found`.
  """
  @callback advance(server(), scope(), non_neg_integer(), [node()]) ::
              {:ok, config()} | {:error, :conflict} | {:error, term()}
end
