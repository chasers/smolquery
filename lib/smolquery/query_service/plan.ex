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
  statistics and pruning decisions are made of. `hot_members` is the ids of
  the entries that survived the membership rule alone, before pruning —
  the exact hot tier this plan read, keyed by table. A caller that wants a
  later plan to read the same hot tier passes it back as `hot_ids:`
  (PL-58 layer 8, T-418); it is the pruner's input, not its output, so a
  later query with a different `WHERE` still prunes from the whole set.
  `statistics` is those
  decisions counted (`Smolquery.QueryService.Statistics`) — what the plan
  reads, per tier, reported out with the finished job; `nil` when the
  catalog could not answer the sizes, because statistics are reporting and
  must not fail a query that would have run. `canonical_sql` is the
  statement's text as DuckDB's parser re-emits it — no comments, no
  trailing semicolon — which is what makes the runner's result-budget wrap
  safe to parenthesize.

  `schemas` carries each referenced table's catalog schema, keyed by table:
  what the runner reads to decide whether a result can carry a `VARIANT`
  column that needs casting before it crosses Arrow
  (`Smolquery.QueryService.VariantResults`).

  `params` are the query's bind parameters, positional for its `$n`
  placeholders (T-410). They ride the plan so every consumer that prepares
  the statement — the runner's frame, a `DESCRIBE`, a scatter partial's
  `COPY` — binds the same values, and so the pruner and the Top-N bound
  can read them where the SQL says `$n`. They never enter the job's stored
  SQL; an engine error message can still quote a bound value.

  `federated` says whether `statements` opens with `ATTACH`es for registered
  Postgres connections (T-324). The runner reads it to decide whether the job
  engine needs DuckDB's `postgres` extension — loading that extension is not
  free, and on a SQLite-metadata deployment nothing else would pull it in.
  """

  alias Smolquery.BufferService.HotClient
  alias Smolquery.Catalog
  alias Smolquery.QueryService.Statistics

  @enforce_keys [:sql, :snapshot]
  defstruct [
    :sql,
    :canonical_sql,
    :snapshot,
    :statistics,
    tables: [],
    statements: [],
    hot: %{},
    hot_members: %{},
    schemas: %{},
    federated: false,
    params: []
  ]

  @type t :: %__MODULE__{
          sql: String.t(),
          canonical_sql: String.t() | nil,
          snapshot: Catalog.snapshot(),
          tables: [Catalog.table_ref()],
          statements: [String.t()],
          hot: %{Catalog.table_ref() => [HotClient.entry()]},
          hot_members: %{Catalog.table_ref() => [String.t()]},
          schemas: %{Catalog.table_ref() => Smolquery.Schema.t()},
          statistics: Statistics.t() | nil,
          federated: boolean(),
          params: [term()]
        }
end
