defmodule Smolquery.QueryService.Plan do
  @moduledoc """
  An executable query plan: everything a job engine needs, resolved once.

  A plan is a value the planner produces and a runner consumes. `statements`
  are run in order on the job's engine — schema and view definitions that make
  each referenced `<dataset>.<table>` resolve to the union of its sealed tier
  (pinned at `snapshot`) and its surviving hot micro-segments — and then `sql`,
  the user's query, runs unmodified against them.

  `hot` carries the micro-segment entries that survived the membership rule
  and the pruner, keyed by table: their row counts and stats are what query
  statistics and pruning decisions are made of. `statistics` is those
  decisions counted (`Smolquery.QueryService.Statistics`) — what the plan
  reads, per tier, reported out with the finished job.
  """

  alias Smolquery.BufferService.HotClient
  alias Smolquery.Catalog
  alias Smolquery.QueryService.Statistics

  @enforce_keys [:sql, :snapshot]
  defstruct [:sql, :snapshot, :statistics, tables: [], statements: [], hot: %{}]

  @type t :: %__MODULE__{
          sql: String.t(),
          snapshot: Catalog.snapshot(),
          tables: [Catalog.table_ref()],
          statements: [String.t()],
          hot: %{Catalog.table_ref() => [HotClient.entry()]},
          statistics: Statistics.t() | nil
        }
end
