defmodule Smolquery.QueryService.Planner do
  @moduledoc """
  SQL in, an executable `Smolquery.QueryService.Plan` out.

  The planner composes one consistent table view out of two tiers that move
  underneath it: sealed segments in the catalog, micro-segments in buffer
  nodes' hot manifests. It never rewrites the user's SQL. Instead it builds a
  view per referenced table in the job engine's own default catalog —
  shadowing the attached lake in name resolution — and the user's query runs
  unmodified against those.

  ## DuckDB's parser is the only parser

  Table references come from `json_serialize_sql`, which doubles as the
  read-only gate: only a single SELECT statement serializes; DDL, DML, and
  multi-statement input fail here, before any manifest is fetched or view
  created. References must be `dataset.table` — a bare name that is not a CTE
  is unknown, and a catalog-qualified or `AT`-clause reference is refused
  rather than silently given different consistency semantics than the view
  would have.

  ## Federated references attach, they do not become views (T-324)

  A `catalog.schema.table` reference used to be refused outright. It now
  resolves against the catalog's registered connections: a catalog name that
  matches one attaches that Postgres into the job engine, and the user's SQL
  reaches it directly. A name matching nothing keeps the old error, so a typo
  is still a typo rather than a connection attempt.

  Federated tables get no view and no snapshot. There is nothing to pin — the
  remote database moves on its own, and pretending otherwise by naming a
  version would promise a consistency this cannot deliver. A query joining a
  smolquery table against a federated one therefore reads the local side at a
  pinned snapshot and the remote side as of whenever DuckDB scans it.

  A connection attaches once per plan however many of its tables the query
  names: DuckDB refuses a second `ATTACH` under a name already in use, so the
  dedupe is by connection name rather than by reference.

  The catalog is only asked about connections when a catalog-qualified
  reference actually appears. `table_schema/2` is already the hottest read in
  the system, and adding an unconditional connection lookup to every query
  would tax the queries that never federate to serve the few that do.

  ## One snapshot, and the membership rule

  The plan pins the catalog's current snapshot `S` once. Each view's sealed
  side reads `AT (VERSION => S)`; each hot manifest entry is included iff it
  carries no claim, or its claim's sealed keys were not all registered by `S`
  (`Catalog.registered_through(table, S)`). That is Milestone 4's D1 rule
  (T-27), exact at every instant of the seal handoff: the catalog commit that
  makes rows appear in the sealed tier at `S′` is the same event that excludes
  their micro-segments for every reader at `S >= S′`. Keys compare by path
  basename — claims carry store keys, the catalog absolute paths, and both
  end in `<segment id>.parquet`.

  Membership asks "registered by `S`", never "listed at `S`": compaction and
  retention *drop* sealed segments a live hot entry may still name, and a
  drop never un-commits a seal — its rows live on in whatever replaced it. The
  M7 maintenance matrix caught exactly that double-count when this read
  `segments/3`.

  Hot rows have no snapshot: an unclaimed micro-segment is always included.
  That is read-your-writes, not an inconsistency.

  ## A pinned plan (PL-58 layers 7 and 8)

  `plan/4` takes a pin. `snapshot:` reads the sealed tier at that version
  instead of the current one — the membership rule is exact for any fixed
  `S`. `hot_ids:` names, per table, the exact micro-segment ids a table's
  hot tier is: membership is then "the manifest entries with those ids",
  with no timestamp comparison, so a repeat read is a repeat read (T-418).
  A table absent from `hot_ids:` falls back to `hot_before_ms:`, which
  drops micro-segments whose ULID was stamped after the bound. Together
  they are what a wire transaction block pins: the block's first query
  supplies the bound and every table's first touch supplies its id set
  (`Plan.hot_members`), so every later statement in the block (a
  `postgres_fdw` join's second cursor, say) reads the same data.

  The bound leans on ULID timestamps, so cross-node clock skew is its
  precision — but only once per table, at first touch; after that the id
  set is exact.

  A pin has a lifetime, `Runtime.hot_pin_max_age_ms`: a micro-segment is
  reaped from the hot tier `retire_grace_ms` after its seal, and a read
  past that point — by id or by time — could come back short with no
  error. So a `hot_before_ms:` older than the lifetime is refused with
  `{:pinned_hot_expired, age_ms, max_age_ms}` before any manifest is read,
  and a pinned id the manifest no longer holds inside the lifetime fails
  with `{:pinned_hot_retired, ref, ids}` — never a silent re-read from
  another tier. A snapshot older than `SMOLQUERY_SNAPSHOT_KEEP_MS` can
  likewise expire under a long block; all three surface as a query error,
  never as wrong rows.

  Entries that survive the membership rule then pass through
  `Smolquery.QueryService.Pruner`: a micro-segment whose min-max stats prove
  the query's WHERE conjuncts unsatisfiable is dropped before its URL is
  built, saving DuckDB an HTTP footer read per file. The sealed tier prunes
  itself — DuckLake collects stats at registration (verified in PL-2).

  ## A last-N query reads the newest micro-segments (T-400)

  `ORDER BY col DESC LIMIT n` is not a conjunct, so the pruner cannot read
  it, and DuckDB applies it only after a footer read per file — hundreds of
  HTTP round trips per query under ingest, for the one or two files that
  hold the answer. `Smolquery.QueryService.TopN` closes that gap: when the
  statement is one SELECT over one table, ordered by a plain column with a
  constant LIMIT, the planner probes the newest entries (by their stats)
  through a view of the table's own name, with the user's WHERE, for the
  n-th value of `col`, and keeps only the entries whose stats can reach it.
  The probe runs on the job engine before the plan's statements do — before
  lockdown, since it reads the hot tier over HTTP — with extension autoload
  off under `lockdown`, so the user's WHERE can do nothing there that it
  could not do locked down. The runner's `CREATE OR REPLACE VIEW` then
  defines the table for real. A query that does not qualify, a WHERE that
  names a volatile function, and a probe that fails all keep every entry.

  ## Both tiers project onto the catalog's schema

  Micro-segments written before a column was added lack it; `UNION ALL BY
  NAME` pads what one side is missing with NULL, and the view's outer
  projection selects exactly the catalog's columns, in catalog order, so a
  column neither tier carries cannot leak in and one the hot tier predates
  cannot error.

  ## Failure honesty

  An unreachable buffer node fails the plan — answering from the sealed tier
  alone would be a wrong answer with a green status. Single-node,
  `buffer_base_url` is the hot tier's whole address. Clustered (Milestone 8
  L5), each table's manifest is fetched from *every* member, at URLs
  derived from node names (`Smolquery.Cluster.node_host/1`), and the entries
  merged. Asking only the ring's current owner would be cheaper but silently
  wrong across ring changes: a node that just joined answers an honest empty
  manifest while the previous owner still holds the table's unsealed,
  already-acked tail, and nothing hands that tail over until it seals. Asking
  every member makes hot rows visible wherever they physically sit, at the
  cost of one control-plane GET per member per table.

  "Every member" means `Smolquery.BufferService.Client.manifest_nodes/1`, not
  the live ring — the distinction is the whole of T-94. A node that dies leaves
  `:pg` membership immediately and indistinguishably from one that drained, so a
  fan-out over live membership alone silently stops counting a crashed owner's
  acked-but-unsealed rows: the query returns a short answer with a green status
  rather than failing, and the number changes back when the pod restarts and
  `Smolquery.BufferService.Adopter` re-registers the tail. Including the nodes
  the deployment expects to be in the ring is what turns that back into an
  unreachable-member failure. It is coarse — an expected node that is down fails
  every query, not only those over tables it held — because nothing durable
  records which nodes hold unsealed rows for which table. Under replication the
  coarseness relaxes exactly as far as the copies allow (T-97): up to
  `Smolquery.BufferService.Client.absence_tolerance/1` unreachable members —
  `replication_factor - 1` under segment shipping, zero single-copy — are
  tolerated, because every acked row an absent node held exists on that many
  reachable disks and the dedupe below counts the surviving copy exactly once.
  One more absent than the copies can cover still fails the plan.

  ## The merged manifest is deduped by segment id

  Today a micro-segment lives on exactly one node, so merging across members is a
  concatenation and this costs nothing. It exists for the moment that stops being
  true: under buffer replication (PL-5 Stage 1) the same segment sits on an owner
  *and* its followers, and concatenating would count every replicated row once
  per copy — a silently wrong `count(*)`, the same failure class as the bug
  above, arriving the day the follower protocol starts working rather than the
  day someone thinks to test it.

  Copies are not always identical, which is why this picks rather than takes the
  first. The seal handoff mutates an entry in place — `claim_keys`, then
  `sealed_at` — so a replica can lag its owner by one step of it, and the copy
  furthest along wins. A laggard would be the unsafe choice: an entry whose rows
  the catalog has already registered, presented as unclaimed, is counted twice at
  every snapshot that includes the sealed segment. The membership rule above then
  applies to the winner and decides inclusion by snapshot, which is what it is
  for.

  That ranking is a backstop, not a substitute for replicating the handoff:
  segment shipping replicates claims, retires, and drops to followers for
  exactly that reason (T-96), and the ranking covers the one-round-trip lag
  while a mutation propagates.

  ## Table functions are allowlisted (T-321)

  `refs/1` collects `BASE_TABLE` nodes. A FROM-clause table function is not
  one: `postgres_scan(...)` serializes as a `TABLE_FUNCTION` wrapping a
  `FUNCTION`, so it was never collected, never classified, and never refused.
  `Runner.lockdown/2` is what caught the rest — `SET enable_external_access =
  false` stops `read_csv('/etc/passwd')` at execution, which is why the gap
  went unnoticed.

  It does not stop every reader. DuckDB's `postgres` extension connects
  regardless of that setting, and the extension is already loaded on any
  deployment whose catalog metadata is Postgres, because the job engine's
  `ATTACH 'ducklake:postgres:...'` autoloads it before lockdown applies. Under
  full lockdown, `duckdb_databases()` then hands the user the catalog's own
  connection string — password included — and `postgres_scan` takes that
  string and reads the catalog. Both are table functions, so both sailed
  through here.

  So the parser gate, not the engine, decides which table functions may appear
  in a FROM clause. An allowlist rather than a denylist: `duckdb_databases`
  was found by looking for a credential leak, roughly thirty other `duckdb_*`
  introspection functions are unaudited, and a denylist passes whatever the
  next extension ships. `@allowed_table_functions` holds pure generators only
  — nothing that reads a file, a catalog, or a socket. A query needing more
  is asking for data, and data arrives as a table.

  The name comes from the `TABLE_FUNCTION` node's own `function` child, never
  from a nested one: `unnest([1, 2])` carries a `list_value` `FUNCTION` under
  `children`, and reading every function in the subtree would refuse it.

  The allowlist follows `Runtime.lockdown`, the flag that already answers "is
  the SQL reaching this node trusted". A deployment that turned lockdown off
  chose the trusted posture and keeps `read_csv`; it keeps this too. One flag
  for one question — a second, independent knob would let an operator set a
  contradictory pair and believe the stricter half.
  """

  require Logger

  alias Smolquery.BufferService.Client
  alias Smolquery.BufferService.HotClient
  alias Smolquery.Catalog
  alias Smolquery.Catalog.DuckLake
  alias Smolquery.Cluster
  alias Smolquery.Engine.Connection
  alias Smolquery.Federation
  alias Smolquery.Identifier
  alias Smolquery.Partitions
  alias Smolquery.QueryService.Plan
  alias Smolquery.QueryService.Pruner
  alias Smolquery.QueryService.Runtime
  alias Smolquery.QueryService.Statistics
  alias Smolquery.QueryService.TopN
  alias Smolquery.QueryService.Trace
  alias Smolquery.QueryService.Views
  alias Smolquery.Segments.Id

  @allowed_table_functions ~w(generate_series range repeat unnest)

  @doc """
  Plans `sql` against the catalog and the hot tier, as one consistent read.

  `connection` (an `Smolquery.Engine.Connection` server) is used to parse —
  one round trip runs `json_serialize_sql` for the AST and derives the
  statement's canonical text (`Plan.canonical_sql`, what the runner's result
  budget wraps) — and, for a query the Top-N bound applies to, to probe the
  hot tier (`Smolquery.QueryService.TopN.bound/6`). The runner passes its own
  job engine's connection, so neither queues behind another job's scan.
  """
  @spec plan(Runtime.t(), GenServer.server(), String.t(), keyword()) ::
          {:ok, Plan.t()} | {:error, term()}
  def plan(%Runtime{} = runtime, connection, sql, pin \\ []) do
    with {:ok, ast, canonical} <- Trace.span(:serialize, fn -> serialize(connection, sql) end),
         {:ok, statement} <- gate(ast),
         :ok <- gate_table_functions(statement, runtime.lockdown),
         {:ok, refs, federated} <- classified(statement),
         {:ok, attaches} <- Trace.span(:federated, fn -> federated(runtime, federated) end),
         {:ok, snapshot} <-
           Trace.span(:snapshot, fn -> pinned_snapshot(runtime, Keyword.get(pin, :snapshot)) end),
         {:ok, tables} <- Trace.span(:resolve, fn -> resolve(runtime, refs, snapshot) end),
         :ok <- fresh_pin(runtime, Keyword.get(pin, :hot_before_ms)),
         {:ok, manifests} <-
           Trace.span(:manifests, fn -> manifests(runtime, refs, tables) end),
         {:ok, members} <-
           Trace.span(:members, fn -> members(refs, tables, manifests, pin) end) do
      pruned = Trace.span(:prune, fn -> pruned(members, Pruner.conjuncts(statement, refs)) end)
      hot = bounded(runtime, connection, statement, refs, tables, pruned)

      {:ok,
       Trace.span(:build, fn ->
         build(sql, canonical, snapshot, refs, tables, members, hot, attaches)
       end)}
    end
  end

  defp pinned_snapshot(runtime, nil), do: Catalog.current_snapshot(runtime.catalog)
  defp pinned_snapshot(_runtime, snapshot), do: {:ok, snapshot}

  defp fresh_pin(_runtime, nil), do: :ok

  defp fresh_pin(%Runtime{hot_pin_max_age_ms: max_age_ms}, hot_before_ms) do
    age_ms = System.system_time(:millisecond) - hot_before_ms

    if age_ms <= max_age_ms, do: :ok, else: {:error, {:pinned_hot_expired, age_ms, max_age_ms}}
  end

  defp members(refs, tables, manifests, pin) do
    hot_ids = Keyword.get(pin, :hot_ids, %{})
    hot_before_ms = Keyword.get(pin, :hot_before_ms)

    Enum.reduce_while(refs, {:ok, %{}}, fn ref, {:ok, acc} ->
      case ref_members(ref, manifests[ref], tables[ref].sealed, hot_ids, hot_before_ms) do
        {:ok, entries} -> {:cont, {:ok, Map.put(acc, ref, entries)}}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
  end

  defp ref_members(ref, manifest, sealed, hot_ids, hot_before_ms) do
    case Map.fetch(hot_ids, ref) do
      {:ok, ids} ->
        pinned_members(ref, manifest, ids)

      :error ->
        {:ok,
         Enum.filter(
           manifest,
           &(include?(&1, sealed) and stamped_before?(&1, hot_before_ms))
         )}
    end
  end

  defp pinned_members(ref, manifest, ids) do
    pinned = MapSet.new(ids)
    entries = Enum.filter(manifest, &MapSet.member?(pinned, &1["id"]))

    missing = MapSet.difference(pinned, MapSet.new(entries, & &1["id"]))

    if MapSet.size(missing) == 0,
      do: {:ok, entries},
      else: {:error, {:pinned_hot_retired, ref, Enum.sort(missing)}}
  end

  defp stamped_before?(_entry, nil), do: true

  defp stamped_before?(entry, hot_before_ms) do
    case Id.timestamp(entry["id"]) do
      {:ok, stamped_ms} -> stamped_ms <= hot_before_ms
      :error -> true
    end
  end

  defp pruned(members, conjuncts) do
    Map.new(members, fn {ref, entries} ->
      {ref, Enum.filter(entries, &Pruner.keep?(&1, conjuncts[ref] || []))}
    end)
  end

  defp bounded(runtime, connection, statement, refs, tables, hot) do
    case TopN.spec(statement, refs) do
      nil ->
        hot

      %{ref: ref} = spec ->
        {entries, _outcome} =
          Trace.span(:top_n, &top_n_outcome/1, fn ->
            TopN.bound(
              connection,
              statement,
              spec,
              tables[ref].schema,
              hot[ref],
              budget: runtime.top_n_probe_rows,
              lockdown: runtime.lockdown
            )
          end)

        Map.put(hot, ref, entries)
    end
  end

  defp top_n_outcome({_entries, outcome}), do: outcome
  defp top_n_outcome(_raised), do: %{bounded: false, rounds: 0, candidates: 0}

  defp federated(_runtime, []), do: {:ok, []}

  defp federated(%Runtime{} = runtime, names) do
    Enum.reduce_while(names, {:ok, []}, fn {name, reference}, {:ok, acc} ->
      with {:ok, connection} <- connection(runtime, name, reference),
           {:ok, statement} <- Federation.attach_statement(connection) do
        {:cont, {:ok, [statement | acc]}}
      else
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
    |> case do
      {:ok, statements} -> {:ok, Enum.reverse(statements)}
      {:error, reason} -> {:error, reason}
    end
  end

  defp connection(%Runtime{} = runtime, name, reference) do
    case Catalog.connection(runtime.catalog, name) do
      {:ok, connection} ->
        {:ok, connection}

      {:error, reason} when reason in [:connections_unsupported] ->
        {:error, {:catalog_qualified_reference, reference}}

      {:error, {:unknown_connection, ^name}} ->
        {:error, {:catalog_qualified_reference, reference}}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp reference(node), do: "#{node["catalog_name"]}.#{node["table_name"]}"

  @doc """
  The `{dataset, table}` references in `sql`, via DuckDB's own parser.

  Serialization failing is the read-only gate doing its job: anything but a
  single SELECT statement is `{:error, {:invalid_query, message}}` or
  `{:error, :multiple_statements}`. CTE names are not references; a bare table
  name that is no CTE, a catalog-qualified name, and a reference carrying its
  own `AT` clause are each refused with a reason naming the offender.

  This applies the table-function allowlist unconditionally, where `plan/3`
  follows the runtime's `lockdown` flag: there is no runtime here to ask, and
  a caller parsing SQL in isolation wants the strict answer.
  """
  @spec table_refs(GenServer.server(), String.t()) ::
          {:ok, [Catalog.table_ref()]} | {:error, term()}
  def table_refs(connection, sql) do
    with {:ok, ast, _canonical} <- serialize(connection, sql),
         {:ok, statement} <- gate(ast),
         :ok <- gate_table_functions(statement, true) do
      refs(statement)
    end
  end

  defp serialize(connection, sql) do
    quoted = Identifier.sql_string(sql)

    with {:ok, result} <-
           Connection.query(
             connection,
             "SELECT json_serialize_sql(#{quoted}), " <>
               "CASE WHEN json_extract_string(json_serialize_sql(#{quoted}), '$.error') = 'false' " <>
               "THEN json_deserialize_sql(json_serialize_sql(#{quoted})) END"
           ),
         [[json, canonical]] <- result.rows,
         {:ok, ast} <- JSON.decode(json) do
      {:ok, ast, canonical}
    else
      {:error, reason} -> {:error, reason}
      rows when is_list(rows) -> {:error, {:invalid_query, rows}}
    end
  end

  defp gate(%{"error" => true} = ast),
    do: {:error, {:invalid_query, Map.get(ast, "error_message", "unparseable")}}

  defp gate(%{"statements" => [statement]}), do: {:ok, statement}
  defp gate(%{"statements" => _many}), do: {:error, :multiple_statements}
  defp gate(ast), do: {:error, {:invalid_query, ast}}

  defp refs(statement) do
    with {:ok, refs, federated} <- classified(statement) do
      case federated do
        [] -> {:ok, refs}
        [{_name, reference} | _rest] -> {:error, {:catalog_qualified_reference, reference}}
      end
    end
  end

  defp classified(statement) do
    ctes = collect(statement, &cte_names/1)

    statement
    |> collect(&base_table/1)
    |> Enum.uniq()
    |> Enum.reduce_while({:ok, [], []}, fn node, {:ok, refs, federated} ->
      case classify(node, ctes) do
        {:ok, ref} -> {:cont, {:ok, [ref | refs], federated}}
        {:federated, name, reference} -> {:cont, {:ok, refs, [{name, reference} | federated]}}
        :cte -> {:cont, {:ok, refs, federated}}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
    |> case do
      {:ok, refs, federated} ->
        {:ok, refs |> Enum.reverse() |> Enum.uniq(),
         federated |> Enum.reverse() |> Enum.uniq_by(&elem(&1, 0))}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp collect(node, fun), do: node |> collect(fun, []) |> Enum.reverse()

  defp collect(node, fun, acc) when is_map(node) do
    acc = node |> fun.() |> Enum.reduce(acc, &[&1 | &2])

    Enum.reduce(node, acc, fn {_key, value}, inner -> collect(value, fun, inner) end)
  end

  defp collect(node, fun, acc) when is_list(node),
    do: Enum.reduce(node, acc, &collect(&1, fun, &2))

  defp collect(_leaf, _fun, acc), do: acc

  defp cte_names(%{"cte_map" => %{"map" => entries}}) when is_list(entries),
    do: for(%{"key" => name} <- entries, do: name)

  defp cte_names(_node), do: []

  defp base_table(%{"type" => "BASE_TABLE"} = node), do: [node]
  defp base_table(_node), do: []

  defp gate_table_functions(_statement, false), do: :ok

  defp gate_table_functions(statement, true) do
    statement
    |> collect(&table_function_name/1)
    |> Enum.find(&(&1 not in @allowed_table_functions))
    |> case do
      nil -> :ok
      name -> {:error, {:unsupported_table_function, name}}
    end
  end

  defp table_function_name(%{"type" => "TABLE_FUNCTION", "function" => function}),
    do: List.wrap(function["function_name"])

  defp table_function_name(_node), do: []

  defp classify(%{"at_clause" => at} = node, _ctes) when not is_nil(at),
    do: {:error, {:unsupported_at_clause, node["table_name"]}}

  defp classify(%{"catalog_name" => catalog} = node, _ctes) when catalog != "" do
    case Identifier.validate(catalog) do
      {:ok, name} -> {:federated, name, reference(node)}
      {:error, _invalid} -> {:error, {:catalog_qualified_reference, reference(node)}}
    end
  end

  defp classify(%{"schema_name" => "", "table_name" => name}, ctes) do
    if name in ctes, do: :cte, else: {:error, {:unknown_table, name}}
  end

  defp classify(%{"schema_name" => dataset, "table_name" => table}, _ctes) do
    with {:ok, dataset} <- Identifier.validate(dataset),
         {:ok, table} <- Identifier.validate(table) do
      {:ok, {dataset, table}}
    end
  end

  defp resolve(runtime, refs, snapshot) do
    Enum.reduce_while(refs, {:ok, %{}}, fn ref, {:ok, acc} ->
      with {:ok, schema} <- Catalog.table_schema(runtime.catalog, ref),
           {:ok, paths} <- Catalog.registered_through(runtime.catalog, ref, snapshot) do
        sealed = MapSet.new(paths, &Path.basename/1)
        stats = segment_stats(runtime, ref, snapshot)

        {:cont, {:ok, Map.put(acc, ref, %{schema: schema, sealed: sealed, stats: stats})}}
      else
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
  end

  defp segment_stats(runtime, ref, snapshot) do
    case Catalog.segment_stats(runtime.catalog, ref, snapshot) do
      {:ok, stats} -> stats
      {:error, _reason} -> :unavailable
    end
  end

  defp manifests(_runtime, [], _tables), do: {:ok, %{}}

  # A partitioned table's hot tier lives under several buffer refs
  # (Smolquery.Partitions), so each table expands into its partition refs for
  # the fetch and every page gathers under the parent — the rest of the plan
  # keeps seeing one hot tier per table.
  defp manifests(runtime, refs, tables) do
    urls = manifest_urls(runtime)

    pairs =
      Enum.flat_map(refs, fn ref ->
        count = Partitions.count(tables[ref].schema.partitions, runtime.write_partitions)

        for partition <- Partitions.refs(ref, count), url <- urls do
          {ref, partition, url}
        end
      end)

    {gathered, failures} =
      pairs
      |> Task.async_stream(
        fn {parent, partition, url} ->
          {parent,
           Trace.span(:manifest_fetch, %{url: url}, fn ->
             HotClient.manifest(url, partition, timeout_ms: runtime.buffer_timeout_ms)
           end)}
        end,
        ordered: true,
        on_timeout: :kill_task,
        timeout: fetch_deadline(runtime)
      )
      |> Enum.zip(pairs)
      |> Enum.reduce({Map.new(refs, &{&1, []}), %{}}, fn
        {{:ok, {parent, {:ok, entries}}}, _pair}, {acc, failed} ->
          {Map.update!(acc, parent, &[entries | &1]), failed}

        {{:ok, {_parent, {:error, reason}}}, {_parent2, partition, url}}, {acc, failed} ->
          {acc, Map.put_new(failed, url, {partition, reason})}

        {{:exit, reason}, {_parent, partition, url}}, {acc, failed} ->
          {acc, Map.put_new(failed, url, {partition, reason})}
      end)

    if map_size(failures) <= Client.absence_tolerance(runtime.buffer_name) do
      log_tolerated(failures)

      {:ok, Map.new(gathered, fn {ref, pages} -> {ref, pages |> List.flatten() |> dedupe()} end)}
    else
      {_url, {ref, reason}} = Enum.min_by(failures, fn {url, _failure} -> url end)

      {:error, {:hot_tier_unavailable, ref, reason}}
    end
  end

  defp log_tolerated(failures) when map_size(failures) == 0, do: :ok

  defp log_tolerated(failures) do
    Logger.warning(fn ->
      absent = failures |> Map.keys() |> Enum.sort() |> Enum.join(", ")

      "hot tier read degraded: answering without #{absent} (within replica tolerance)"
    end)
  end

  defp dedupe(entries) do
    entries
    |> Enum.with_index()
    |> Enum.group_by(fn {entry, _position} -> entry["id"] end)
    |> Enum.map(fn {_id, copies} -> Enum.reduce(copies, &further_along/2) end)
    |> Enum.sort_by(fn {_entry, position} -> position end)
    |> Enum.map(fn {entry, _position} -> entry end)
  end

  defp further_along({entry, position}, {best, best_position}) do
    keep = if handoff_rank(entry) > handoff_rank(best), do: entry, else: best

    {keep, min(position, best_position)}
  end

  defp handoff_rank(entry) do
    cond do
      entry["sealed_at"] -> 2
      entry["claim_keys"] not in [nil, []] -> 1
      true -> 0
    end
  end

  defp manifest_urls(%Runtime{buffer_base_url: url} = runtime) do
    if Cluster.enabled?() do
      runtime.buffer_name
      |> Client.manifest_nodes()
      |> Enum.map(&"http://#{Cluster.node_host(&1)}:#{runtime.buffer_hot_port}")
    else
      [url]
    end
  end

  defp fetch_deadline(%Runtime{buffer_timeout_ms: :infinity}), do: :infinity
  defp fetch_deadline(%Runtime{buffer_timeout_ms: ms}), do: ms + 5_000

  defp build(sql, canonical, snapshot, refs, tables, members, hot, attaches) do
    statements = Enum.flat_map(refs, fn ref -> view(ref, snapshot, tables[ref], hot[ref]) end)

    %Plan{
      sql: sql,
      canonical_sql: canonical,
      snapshot: snapshot,
      tables: refs,
      statements: attaches ++ statements,
      federated: attaches != [],
      hot: hot,
      hot_members:
        Map.new(members, fn {ref, entries} ->
          {ref, Enum.map(entries, &:binary.copy(&1["id"]))}
        end),
      schemas: Map.new(tables, fn {ref, %{schema: schema}} -> {ref, schema} end),
      statistics: statistics(members, hot, tables)
    }
  end

  defp statistics(members, hot, tables) do
    sealed = tables |> Map.values() |> Enum.map(& &1.stats)

    if :unavailable in sealed do
      nil
    else
      surviving = hot |> Map.values() |> List.flatten()

      hot_tier =
        Statistics.tier(
          members |> Map.values() |> List.flatten() |> length(),
          length(surviving),
          Enum.sum_by(surviving, & &1["row_count"]),
          Enum.sum_by(surviving, & &1["byte_size"])
        )

      sealed_files = Enum.sum_by(sealed, & &1.files)

      sealed_tier =
        Statistics.tier(
          sealed_files,
          sealed_files,
          Enum.sum_by(sealed, & &1.rows),
          Enum.sum_by(sealed, & &1.bytes)
        )

      Statistics.new(hot_tier, sealed_tier)
    end
  end

  defp include?(entry, sealed) do
    case entry["claim_keys"] do
      keys when is_list(keys) and keys != [] ->
        not Enum.all?(keys, &MapSet.member?(sealed, Path.basename(&1)))

      _unclaimed ->
        true
    end
  end

  defp view({dataset, table} = ref, snapshot, %{schema: schema}, entries) do
    ds = Identifier.quote_name!(dataset)
    t = Identifier.quote_name!(table)
    lake = Identifier.quote_name!(DuckLake.default_catalog())

    sealed = "SELECT * FROM #{lake}.#{ds}.#{t} AT (VERSION => #{snapshot})"

    union =
      case Enum.map(entries, & &1["url"]) do
        [] -> sealed
        urls -> sealed <> " UNION ALL BY NAME SELECT * FROM " <> Views.read_parquet(urls)
      end

    Views.table_view(ref, schema, union)
  end
end
