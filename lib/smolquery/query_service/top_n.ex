defmodule Smolquery.QueryService.TopN do
  @moduledoc """
  The Top-N bound: an `ORDER BY <col> DESC|ASC LIMIT n` query reads only the
  hot micro-segments that can hold one of its n rows (T-400).

  A last-N query over the hot tier — `WHERE project = ? ORDER BY inserted_at
  DESC LIMIT 100` — used to open every unclaimed micro-segment the buffer
  pods hold. DuckDB applies a Top-N bound while it scans, but it discovers
  the bound from the files themselves, and that costs a footer read per file:
  hundreds of HTTP round trips per query under ingest, for a handful of
  files that matter. Each manifest entry already carries the stats DuckDB
  would read from the footer, so the planner can apply the bound itself.

  ## How the bound is found

  1. `spec/2` decides whether the statement qualifies and names the ordering
     column, the direction, and `n` (LIMIT plus OFFSET).
  2. `candidates/3` picks the newest entries — by `max(col)` for DESC, by
     `min(col)` for ASC — until their row counts cover a budget.
  3. The planner probes those candidates through the same table view, with
     the same WHERE, for the ordering column's n-th value: the bound `B`.
     `probe_ast/2` is that query, as an edit of the user's own statement.
  4. `prune/3` keeps every entry whose stats leave a chance of a row past
     the bound — `max(col) >= B` for DESC, `min(col) <= B` for ASC — through
     `Smolquery.QueryService.Pruner`, so every uncertainty keeps the entry.

  ## Why it is correct

  The probe runs the user's WHERE unchanged, so its n rows are n real rows of
  the answer set, all at or past `B`. The n rows the user asked for are the n
  best rows of that set, so every one of them is at or past `B` too, and an
  entry whose stats put all of its rows short of `B` cannot hold one. That
  argument needs the probe's rows to be a subset of the answer set — which
  is why the probe never reads more than the user's query reads, and why a
  probe over a subset of the table (the candidates alone, never the sealed
  tier) gives a looser bound, never a wrong one.

  ## What qualifies

  The bound is applied to a view every reference in the statement shares,
  and it is only sound for the one reference the ORDER BY and LIMIT belong
  to. So a statement qualifies only when it is one SELECT over exactly one
  table reference: no join, no CTE, no subquery or window expression anywhere
  (either would read the pruned view and answer differently), no GROUP BY,
  HAVING, QUALIFY, SAMPLE, or DISTINCT (DuckDB accepts `SELECT DISTINCT y …
  ORDER BY x`, and the pruned set of `x` would change which `y` survive).

  The first ORDER key must be a plain column of that table — one name, or
  two where the qualifier is the table's alias — with NULLs sorted last, the
  DuckDB default in both directions: a NULL sorts past every value and lives
  in any entry. Later keys are fine: the n-th value of the first key still
  bounds the answer. The LIMIT and any OFFSET must be constants.

  Two shapes bind the ORDER BY name to something other than the stored
  column and are refused by name: a select-list alias equal to the ordering
  column (`SELECT -x AS x … ORDER BY x` orders by the alias) and a
  `FROM t AS e(a, b)` column rename, which moves names between positions.
  A `SELECT * REPLACE (…)` is refused for the same reason.

  Every miss is `nil` — no bound, never an error.
  """

  alias Smolquery.BufferService.HotClient
  alias Smolquery.BufferService.HotManifest.Entry
  alias Smolquery.Catalog
  alias Smolquery.QueryService.Pruner

  @type direction :: :desc | :asc

  @typedoc """
  What the bound needs from the statement: the table, the ordering column,
  the direction, and `n` — the LIMIT plus any OFFSET, since the offset rows
  are read before they are skipped.
  """
  @type t :: %{
          ref: Catalog.table_ref(),
          column: String.t(),
          direction: direction(),
          limit: pos_integer()
        }

  @excluded_classes ["SUBQUERY", "WINDOW"]
  @directions %{"DESCENDING" => :desc, "ASCENDING" => :asc, "ORDER_DEFAULT" => :asc}
  @null_orders ["ORDER_DEFAULT", "NULLS LAST"]

  @doc """
  The Top-N spec of a serialized statement, or `nil` when it does not qualify.

  `refs` are the statement's resolved table references; the FROM table must
  be one of them.
  """
  @spec spec(map(), [Catalog.table_ref()]) :: t() | nil
  def spec(%{"node" => %{"type" => "SELECT_NODE"} = node} = statement, refs) do
    with {:ok, source} <- source(node["from_table"], refs),
         true <- simple?(node),
         true <- single_reference?(statement),
         {:ok, modifiers} <- modifiers(node["modifiers"]),
         {:ok, column, direction} <- order(modifiers.order, source),
         true <- stored_column?(node["select_list"], column),
         {:ok, limit} <- limit(modifiers.limit) do
      %{ref: source.ref, column: column, direction: direction, limit: limit}
    else
      _ineligible -> nil
    end
  end

  def spec(_statement, _refs), do: nil

  @doc """
  The probe statement's AST: the user's statement, selecting only the
  ordering column, ordered by that one key, limited to `n`.

  It keeps the FROM and the WHERE untouched, so `json_deserialize_sql`
  renders a query with exactly the user's predicate semantics, which the
  planner runs against a view of the same name over the candidate entries.
  """
  @spec probe_ast(map(), t()) :: map()
  def probe_ast(%{"node" => node} = statement, %{limit: limit}) do
    {:ok, modifiers} = modifiers(node["modifiers"])
    [first | _rest] = modifiers.order["orders"]

    probe =
      node
      |> Map.put("select_list", [first["expression"]])
      |> Map.put("modifiers", [
        %{"type" => "ORDER_MODIFIER", "orders" => [first]},
        %{"type" => "LIMIT_MODIFIER", "limit" => constant(limit), "offset" => nil}
      ])

    %{"error" => false, "statements" => [Map.put(statement, "node", probe)]}
  end

  @doc """
  The entries to probe: the newest by the ordering column, until their row
  counts reach `rows`.

  "Newest" is `max(col)` for DESC and `min(col)` for ASC, from each entry's
  flush-time stats. An entry without stats for the column is never a
  candidate — it has no edge to sort by — though `prune/3` still keeps it.
  The choice only affects how tight the bound is, never whether it is sound.
  """
  @spec candidates([HotClient.entry()], t(), pos_integer()) :: [HotClient.entry()]
  def candidates(entries, %{column: column, direction: direction}, rows) do
    entries
    |> Enum.flat_map(fn entry ->
      case edge(entry, column, direction) do
        nil -> []
        edge -> [{edge, entry}]
      end
    end)
    |> sort(direction)
    |> Enum.map(fn {_edge, entry} -> entry end)
    |> take_rows(rows)
  end

  @doc """
  The entries whose stats leave a chance of a row at or past `bound`.

  DESC keeps `max(col) >= bound`; ASC keeps `min(col) <= bound`. Both go
  through `Smolquery.QueryService.Pruner.keep?/2`, so an entry without stats
  for the column, or with stats of another type than the bound, is kept.
  """
  @spec prune([HotClient.entry()], t(), term()) :: [HotClient.entry()]
  def prune(entries, %{column: column, direction: direction}, bound) do
    conjunct = {column, past(direction), bound}

    Enum.filter(entries, &Pruner.keep?(&1, [conjunct]))
  end

  defp source(
         %{
           "type" => "BASE_TABLE",
           "schema_name" => dataset,
           "table_name" => table,
           "alias" => alias,
           "catalog_name" => "",
           "at_clause" => nil,
           "sample" => nil,
           "column_name_alias" => []
         },
         refs
       )
       when dataset != "" do
    ref = {dataset, table}
    name = if alias == "", do: table, else: alias

    if ref in refs, do: {:ok, %{ref: ref, name: name}}, else: :error
  end

  defp source(_from, _refs), do: :error

  defp simple?(node) do
    node["group_expressions"] == [] and Map.get(node, "group_sets", []) == [] and
      is_nil(node["having"]) and is_nil(node["qualify"]) and is_nil(node["sample"]) and
      node["aggregate_handling"] == "STANDARD_HANDLING" and
      get_in(node, ["cte_map", "map"]) in [nil, []]
  end

  defp single_reference?(statement) do
    %{tables: tables, excluded: excluded} =
      walk(statement, %{tables: 0, excluded: false}, fn node, acc ->
        %{
          tables: acc.tables + if(node["type"] == "BASE_TABLE", do: 1, else: 0),
          excluded: acc.excluded or node["class"] in @excluded_classes
        }
      end)

    tables == 1 and not excluded
  end

  defp walk(node, acc, fun) when is_map(node) do
    Enum.reduce(node, fun.(node, acc), fn {_key, value}, inner -> walk(value, inner, fun) end)
  end

  defp walk(node, acc, fun) when is_list(node),
    do: Enum.reduce(node, acc, &walk(&1, &2, fun))

  defp walk(_leaf, acc, _fun), do: acc

  defp modifiers(modifiers) when is_list(modifiers) do
    case Enum.sort_by(modifiers, & &1["type"]) do
      [%{"type" => "LIMIT_MODIFIER"} = limit, %{"type" => "ORDER_MODIFIER"} = order] ->
        {:ok, %{order: order, limit: limit}}

      _other ->
        :error
    end
  end

  defp modifiers(_modifiers), do: :error

  defp order(%{"orders" => [first | _rest]}, source) do
    with {:ok, direction} <- Map.fetch(@directions, first["type"]),
         true <- first["null_order"] in @null_orders,
         {:ok, column} <- column(first["expression"], source) do
      {:ok, column, direction}
    end
  end

  defp order(_order, _source), do: :error

  defp column(%{"class" => "COLUMN_REF", "column_names" => [column]}, _source),
    do: {:ok, column}

  defp column(%{"class" => "COLUMN_REF", "column_names" => [qualifier, column]}, %{
         name: qualifier
       }),
       do: {:ok, column}

  defp column(_expression, _source), do: :error

  defp stored_column?(select_list, column) do
    Enum.all?(select_list, fn item ->
      item["alias"] != column and Map.get(item, "replace_list", []) == [] and
        Map.get(item, "rename_list", []) == []
    end)
  end

  defp limit(%{"limit" => limit, "offset" => offset}) do
    with {:ok, n} when n >= 1 <- integer(limit),
         {:ok, skip} when skip >= 0 <- integer(offset) do
      {:ok, n + skip}
    else
      _not_constant -> :error
    end
  end

  defp integer(nil), do: {:ok, 0}

  defp integer(%{"class" => "CONSTANT", "value" => %{"is_null" => false, "value" => value}})
       when is_integer(value),
       do: {:ok, value}

  defp integer(_expression), do: :error

  defp constant(value) do
    %{
      "class" => "CONSTANT",
      "type" => "VALUE_CONSTANT",
      "alias" => "",
      "query_location" => 0,
      "value" => %{
        "type" => %{"id" => "BIGINT", "type_info" => nil},
        "is_null" => false,
        "value" => value
      }
    }
  end

  defp edge(entry, column, direction) do
    entry |> Map.get("stats", %{}) |> Entry.decode_stats() |> Map.get(column) |> edge(direction)
  end

  defp edge(%{max: max}, :desc), do: max
  defp edge(%{min: min}, :asc), do: min
  defp edge(_missing, _direction), do: nil

  defp sort([], _direction), do: []

  defp sort([{edge, _entry} | _rest] = keyed, direction) do
    Enum.sort_by(keyed, fn {edge, _entry} -> edge end, sorter(direction, edge))
  end

  defp sorter(direction, %NaiveDateTime{}), do: {direction, NaiveDateTime}
  defp sorter(direction, %Date{}), do: {direction, Date}
  defp sorter(direction, _plain), do: direction

  defp take_rows(entries, rows) do
    entries
    |> Enum.reduce_while({[], 0}, fn entry, {taken, covered} ->
      if covered >= rows do
        {:halt, {taken, covered}}
      else
        {:cont, {[entry | taken], covered + entry["row_count"]}}
      end
    end)
    |> elem(0)
    |> Enum.reverse()
  end

  defp past(:desc), do: :ge
  defp past(:asc), do: :le
end
