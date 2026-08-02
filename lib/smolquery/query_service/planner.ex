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

  Entries that survive the membership rule then pass through
  `Smolquery.QueryService.Pruner`: a micro-segment whose min-max stats prove
  the query's WHERE conjuncts unsatisfiable is dropped before its URL is
  built, saving DuckDB an HTTP footer read per file. The sealed tier prunes
  itself — DuckLake collects stats at registration (verified in PL-2).

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
  records which nodes hold unsealed rows for which table.

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
  """

  alias Smolquery.BufferService.Client
  alias Smolquery.BufferService.HotClient
  alias Smolquery.Catalog
  alias Smolquery.Catalog.DuckLake
  alias Smolquery.Cluster
  alias Smolquery.Engine.Connection
  alias Smolquery.Engine.Result
  alias Smolquery.Identifier
  alias Smolquery.QueryService.Plan
  alias Smolquery.QueryService.Pruner
  alias Smolquery.QueryService.Runtime

  @doc """
  Plans `sql` against the catalog and the hot tier, as one consistent read.

  `connection` (an `Smolquery.Engine.Connection` server) is only used to
  parse — `json_serialize_sql` runs there and nothing else does. The runner
  passes its own job engine's connection, so parsing never queues behind
  another job's scan.
  """
  @spec plan(Runtime.t(), GenServer.server(), String.t()) :: {:ok, Plan.t()} | {:error, term()}
  def plan(%Runtime{} = runtime, connection, sql) do
    with {:ok, ast} <- serialize(connection, sql),
         {:ok, statement} <- gate(ast),
         {:ok, refs} <- refs(statement),
         {:ok, snapshot} <- Catalog.current_snapshot(runtime.catalog),
         {:ok, tables} <- resolve(runtime, refs, snapshot),
         {:ok, manifests} <- manifests(runtime, refs) do
      {:ok, build(sql, snapshot, refs, tables, manifests, Pruner.conjuncts(statement, refs))}
    end
  end

  @doc """
  The `{dataset, table}` references in `sql`, via DuckDB's own parser.

  Serialization failing is the read-only gate doing its job: anything but a
  single SELECT statement is `{:error, {:invalid_query, message}}` or
  `{:error, :multiple_statements}`. CTE names are not references; a bare table
  name that is no CTE, a catalog-qualified name, and a reference carrying its
  own `AT` clause are each refused with a reason naming the offender.
  """
  @spec table_refs(GenServer.server(), String.t()) ::
          {:ok, [Catalog.table_ref()]} | {:error, term()}
  def table_refs(connection, sql) do
    with {:ok, ast} <- serialize(connection, sql),
         {:ok, statement} <- gate(ast) do
      refs(statement)
    end
  end

  defp serialize(connection, sql) do
    with {:ok, result} <-
           Connection.query(
             connection,
             "SELECT json_serialize_sql(#{Identifier.sql_string(sql)})"
           ) do
      result |> Result.one!() |> JSON.decode()
    end
  end

  defp gate(%{"error" => true} = ast),
    do: {:error, {:invalid_query, Map.get(ast, "error_message", "unparseable")}}

  defp gate(%{"statements" => [statement]}), do: {:ok, statement}
  defp gate(%{"statements" => _many}), do: {:error, :multiple_statements}
  defp gate(ast), do: {:error, {:invalid_query, ast}}

  defp refs(statement) do
    ctes = collect(statement, &cte_names/1)

    statement
    |> collect(&base_table/1)
    |> Enum.uniq()
    |> Enum.reduce_while({:ok, []}, fn node, {:ok, acc} ->
      case classify(node, ctes) do
        {:ok, ref} -> {:cont, {:ok, [ref | acc]}}
        :cte -> {:cont, {:ok, acc}}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
    |> case do
      {:ok, refs} -> {:ok, refs |> Enum.reverse() |> Enum.uniq()}
      {:error, reason} -> {:error, reason}
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

  defp classify(%{"at_clause" => at} = node, _ctes) when not is_nil(at),
    do: {:error, {:unsupported_at_clause, node["table_name"]}}

  defp classify(%{"catalog_name" => catalog} = node, _ctes) when catalog != "",
    do: {:error, {:catalog_qualified_reference, "#{catalog}.#{node["table_name"]}"}}

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

        {:cont, {:ok, Map.put(acc, ref, %{schema: schema, sealed: sealed})}}
      else
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
  end

  defp manifests(_runtime, []), do: {:ok, %{}}

  defp manifests(runtime, refs) do
    urls = manifest_urls(runtime)

    refs
    |> Enum.flat_map(fn ref -> Enum.map(urls, &{ref, &1}) end)
    |> Task.async_stream(
      fn {ref, url} ->
        {ref, HotClient.manifest(url, ref, timeout_ms: runtime.buffer_timeout_ms)}
      end,
      ordered: false,
      on_timeout: :kill_task,
      timeout: fetch_deadline(runtime)
    )
    |> Enum.reduce_while({:ok, Map.new(refs, &{&1, []})}, fn
      {:ok, {ref, {:ok, entries}}}, {:ok, acc} ->
        {:cont, {:ok, Map.update!(acc, ref, &[entries | &1])}}

      {:ok, {ref, {:error, reason}}}, _acc ->
        {:halt, {:error, {:hot_tier_unavailable, ref, reason}}}

      {:exit, reason}, _acc ->
        {:halt, {:error, {:hot_tier_unavailable, reason}}}
    end)
    |> case do
      {:ok, gathered} ->
        {:ok,
         Map.new(gathered, fn {ref, pages} -> {ref, pages |> List.flatten() |> dedupe()} end)}

      {:error, reason} ->
        {:error, reason}
    end
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

  defp build(sql, snapshot, refs, tables, manifests, conjuncts) do
    hot =
      Map.new(refs, fn ref ->
        surviving =
          Enum.filter(
            manifests[ref],
            &(include?(&1, tables[ref].sealed) and Pruner.keep?(&1, conjuncts[ref] || []))
          )

        {ref, surviving}
      end)

    statements = Enum.flat_map(refs, fn ref -> view(ref, snapshot, tables[ref], hot[ref]) end)

    %Plan{sql: sql, snapshot: snapshot, tables: refs, statements: statements, hot: hot}
  end

  defp include?(entry, sealed) do
    case entry["claim_keys"] do
      keys when is_list(keys) and keys != [] ->
        not Enum.all?(keys, &MapSet.member?(sealed, Path.basename(&1)))

      _unclaimed ->
        true
    end
  end

  defp view({dataset, table}, snapshot, %{schema: schema}, entries) do
    ds = Identifier.quote_name!(dataset)
    t = Identifier.quote_name!(table)
    lake = Identifier.quote_name!(DuckLake.default_catalog())
    columns = Enum.map_join(schema.fields, ", ", &Identifier.quote_name!(&1.name))

    sealed = "SELECT * FROM #{lake}.#{ds}.#{t} AT (VERSION => #{snapshot})"

    union =
      case Enum.map(entries, & &1["url"]) do
        [] ->
          sealed

        urls ->
          parquet = Enum.map_join(urls, ", ", &Identifier.sql_string/1)

          sealed <>
            " UNION ALL BY NAME " <>
            "SELECT * FROM read_parquet([#{parquet}], union_by_name := true)"
      end

    [
      "CREATE SCHEMA IF NOT EXISTS #{ds}",
      "CREATE VIEW #{ds}.#{t} AS SELECT #{columns} FROM (#{union})"
    ]
  end
end
