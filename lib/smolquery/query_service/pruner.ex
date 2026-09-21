defmodule Smolquery.QueryService.Pruner do
  @moduledoc """
  Drops hot micro-segments whose stats prove no row can match — before their
  URLs are built, which is before DuckDB pays an HTTP footer read for each.

  Conservative by construction: this module only ever *keeps* what it cannot
  rule out. A predicate it cannot parse prunes nothing; a column without
  stats prunes nothing; a bound whose type does not match the literal's
  prunes nothing. Wrong answers are impossible from keeping too much — only
  from dropping too much, so every uncertainty resolves to keeping.

  ## What it reads

  Conjuncts come from the top level of a WHERE clause — the AND-chain only,
  since a row can satisfy an OR through its other branch. A conjunct is
  `column <op> literal` (either side), or BETWEEN, where the literal is a
  plain constant, a TIMESTAMP/DATE cast of one, or a ClickHouse epoch
  function over an integer — PL-1 names timestamp ranges as the pruning that
  matters. Columns resolve through the FROM clause's aliases; an unqualified
  column resolves only when the SELECT reads a single table, because
  guessing which of two tables `id` means could prune the wrong one's
  segments.

  ## Which WHERE clauses

  The statement's own, and that of every SELECT it is built from where no
  column of another SELECT is in scope (T-532): the body of a CTE, a
  subquery that is a SELECT's whole FROM, and each side of a set operation,
  to any depth. HyperDX fills its filters sidebar with

      WITH sampledData AS (SELECT cluster AS param0 FROM t WHERE ts >= ... LIMIT n)
      SELECT groupUniqArray(10000)(param0) FROM sampledData

  and read at the top level only, that statement opened every hot
  micro-segment the buffers held to aggregate one column of a window.

  A subquery in an expression (`EXISTS`, `IN`, a scalar) is not read, nor is
  one that shares its FROM with another source: either may be correlated,
  and an unqualified `id` in it may be the outer table's. A row a nested
  WHERE rejects contributes to nothing above it, so with the table read once
  the files that hold only such rows are not needed anywhere.

  ## ClickHouse's epoch functions

  `fromUnixTimestamp`, `fromUnixTimestamp64Milli` and
  `fromUnixTimestamp64Micro` over one integer are the timestamp
  `Smolquery.QueryService.ClickHouseFunctions` makes of it, which is how
  every ClickHouse client writes a time range. They are this codebase's own
  macros, which a client cannot replace, and a test holds each bound here to
  what the engine answers for the same call. `fromUnixTimestamp64Nano` is
  left alone: a bound here is a microsecond one.

  ## A table read twice

  The planner builds one view for a table, and every reference to the table
  in the statement reads it. A conjunct is one reference's: in
  `events a JOIN events b ... WHERE a.id > 5`, `b` needs the files `a` does
  not, and so does a subquery that counts the table the outer `WHERE`
  filters. So a table prunes only when the statement names it exactly once
  (T-533). The count is `Smolquery.QueryService.SingleTable.table_reads/1`'s:
  of every table reference with that name, whatever its schema and wherever
  it is, since an unqualified name beside a qualified one may be the same
  table, and counting one too many only keeps files. A statement with a
  table function in it prunes nothing: `query_table('analytics.events')`
  reads the view without naming the table.

  ## A reference that is not the table as it is

  `events AS e(ts, id)` renames the table's columns by position, so `e.id`
  is whichever column came second, and its bounds are not those of the
  column the catalog calls `id`. `events TABLESAMPLE ... REPEATABLE (42)`
  draws its sample before the WHERE, from whichever files the view lists, so
  a file pruned is a different sample for the same seed. Either makes the
  reference opaque, as `SingleTable.source/2` has it: nothing resolves
  through it. A SELECT that samples (`USING SAMPLE`) is not read at all.

  The sealed tier gets no treatment here: DuckLake collects min-max stats at
  registration and prunes on them natively — verified in the Milestone 2
  spike (PL-2), which is why this module's job ends at the hot tier.
  """

  alias Smolquery.BufferService.HotClient
  alias Smolquery.BufferService.HotManifest.Entry
  alias Smolquery.Catalog
  alias Smolquery.QueryService.SingleTable

  @type op :: :gt | :ge | :lt | :le | :eq
  @type conjunct :: {String.t(), op(), term()}

  @operators %{
    "COMPARE_GREATERTHAN" => :gt,
    "COMPARE_GREATERTHANOREQUALTO" => :ge,
    "COMPARE_LESSTHAN" => :lt,
    "COMPARE_LESSTHANOREQUALTO" => :le,
    "COMPARE_EQUAL" => :eq
  }

  @mirrored %{gt: :lt, ge: :le, lt: :gt, le: :ge, eq: :eq}

  @epoch ~N[1970-01-01 00:00:00]
  @max_epoch_microseconds 253_402_300_799_999_999
  @epoch_functions %{
    "fromunixtimestamp" => 1_000_000,
    "fromunixtimestamp64milli" => 1_000,
    "fromunixtimestamp64micro" => 1
  }

  @doc """
  The prunable conjuncts of a serialized statement, keyed by table.

  Tables appear only when at least one conjunct resolved to them; a query this
  module cannot read (an OR-rooted WHERE, a table read twice, a subquery that
  may be correlated) yields an empty map, which prunes nothing.

  A `$n` placeholder resolves to the n-th of `params` (T-410) when that value
  is a number, a string, a date, or a timestamp; any other bound value leaves
  its conjunct unread, which keeps every entry.
  """
  @spec conjuncts(map(), [Catalog.table_ref()], [term()]) ::
          %{Catalog.table_ref() => [conjunct()]}
  def conjuncts(statement, refs, params \\ [])

  def conjuncts(%{"node" => node} = statement, refs, params) when is_map(node) do
    node
    |> selects()
    |> Enum.flat_map(&select_conjuncts(&1, refs, params))
    |> read_once(statement)
    |> Enum.group_by(fn {ref, _conjunct} -> ref end, fn {_ref, conjunct} -> conjunct end)
  end

  def conjuncts(_statement, _refs, _params), do: %{}

  defp select_conjuncts(node, refs, params) do
    aliases = aliases(Map.get(node, "from_table"), refs)

    node
    |> Map.get("where_clause")
    |> split()
    |> Enum.flat_map(&parse(&1, aliases, params))
  end

  defp selects(%{"type" => "SELECT_NODE", "sample" => nil} = node),
    do: [node | Enum.flat_map(uncorrelated(node), &selects/1)]

  defp selects(%{"type" => "SET_OPERATION_NODE"} = node),
    do: Enum.flat_map(uncorrelated(node), &selects/1)

  defp selects(_another_node), do: []

  defp uncorrelated(node),
    do: [node["left"], node["right"], whole_from(node["from_table"]) | cte_bodies(node)]

  defp cte_bodies(node) do
    node
    |> get_in(["cte_map", "map"])
    |> List.wrap()
    |> Enum.map(&get_in(&1, ["value", "query", "node"]))
  end

  defp whole_from(%{"type" => "SUBQUERY", "subquery" => %{"node" => node}}), do: node
  defp whole_from(_another_source), do: nil

  @doc """
  Whether `entry`'s stats leave any chance a row matches every conjunct.

  A conjunct names a column by its catalog name today; an entry's stats are
  keyed by the name the column had in the file when it was written. `ids` —
  the catalog's `Smolquery.Schema.field_ids/1` — and the entry's own
  `"field_ids"` bridge the two (PL-62): the conjunct's column resolves to its
  id, and the id to whatever the file called it. A column whose id the file
  never carried has no stats there, so the entry is kept — nothing proves a
  match impossible, and a column of the same *name* under another id is a
  different column whose bounds must not prune this one. Without `ids`, or
  for an entry written before ids existed, the name is the key, as before.
  """
  @spec keep?(HotClient.entry(), [conjunct()], %{String.t() => pos_integer()} | nil) ::
          boolean()
  def keep?(entry, conjuncts, ids \\ nil)

  def keep?(_entry, [], _ids), do: true

  def keep?(entry, conjuncts, ids) do
    stats = entry |> Map.get("stats", %{}) |> Entry.decode_stats()
    resolve = column_resolver(ids, Map.get(entry, "field_ids"))

    not Enum.any?(conjuncts, fn {column, op, value} ->
      excludes?(stats, {resolve.(column), op, value})
    end)
  end

  defp read_once([], _statement), do: []

  defp read_once(found, statement) do
    case SingleTable.table_reads(statement) do
      :unknowable -> []
      reads -> Enum.filter(found, fn {{_dataset, table}, _conjunct} -> reads[table] == 1 end)
    end
  end

  defp column_resolver(ids, file_ids) when is_map(ids) and is_map(file_ids) do
    names_by_id = Map.new(file_ids, fn {name, id} -> {id, name} end)

    fn column ->
      case Map.fetch(ids, column) do
        {:ok, id} -> Map.get(names_by_id, id, :absent)
        :error -> column
      end
    end
  end

  defp column_resolver(_ids, _file_ids), do: & &1

  defp aliases(from, refs) do
    known = MapSet.new(refs)
    sources = sources(from)

    named =
      sources
      |> Enum.filter(fn
        {_name, ref} -> MapSet.member?(known, ref)
        :opaque -> false
      end)
      |> Enum.group_by(fn {name, _ref} -> name end, fn {_name, ref} -> ref end)
      |> Enum.flat_map(fn
        {name, [ref]} -> [{name, ref}]
        {_name, _ambiguous} -> []
      end)
      |> Map.new()

    single =
      case {sources, Map.values(named)} do
        {[_only_source], [ref]} -> ref
        _more_or_opaque -> nil
      end

    %{named: named, single: single}
  end

  defp sources(from), do: from |> sources([]) |> Enum.reverse()

  defp sources(%{"type" => "BASE_TABLE", "column_name_alias" => [_renamed | _more]}, acc),
    do: [:opaque | acc]

  defp sources(%{"type" => "BASE_TABLE", "sample" => %{}}, acc), do: [:opaque | acc]

  defp sources(%{"type" => "BASE_TABLE"} = node, acc) do
    name =
      case node["alias"] do
        "" -> node["table_name"]
        given -> given
      end

    [{name, {node["schema_name"], node["table_name"]}} | acc]
  end

  defp sources(%{"type" => "JOIN"} = node, acc),
    do: sources(node["right"], sources(node["left"], acc))

  defp sources(%{"type" => "EMPTY"}, acc), do: acc
  defp sources(nil, acc), do: acc
  defp sources(_opaque, acc), do: [:opaque | acc]

  defp split(%{"type" => "CONJUNCTION_AND", "children" => children}),
    do: Enum.flat_map(children, &split/1)

  defp split(nil), do: []
  defp split(node), do: [node]

  defp parse(%{"class" => "COMPARISON", "type" => type} = node, aliases, params) do
    case Map.fetch(@operators, type) do
      {:ok, op} -> comparison(node, op, aliases, params)
      :error -> []
    end
  end

  defp parse(%{"class" => "BETWEEN"} = node, aliases, params) do
    with {:ok, ref, name} <- column(node["input"], aliases),
         {:ok, lower} <- literal(node["lower"], params),
         {:ok, upper} <- literal(node["upper"], params) do
      [{ref, {name, :ge, lower}}, {ref, {name, :le, upper}}]
    else
      _unparseable -> []
    end
  end

  defp parse(_node, _aliases, _params), do: []

  defp comparison(node, op, aliases, params) do
    case {column(node["left"], aliases), literal(node["right"], params)} do
      {{:ok, ref, name}, {:ok, value}} -> [{ref, {name, op, value}}]
      _not_column_op_literal -> mirrored_comparison(node, op, aliases, params)
    end
  end

  defp mirrored_comparison(node, op, aliases, params) do
    case {literal(node["left"], params), column(node["right"], aliases)} do
      {{:ok, value}, {:ok, ref, name}} -> [{ref, {name, @mirrored[op], value}}]
      _unparseable -> []
    end
  end

  defp column(%{"class" => "COLUMN_REF", "column_names" => [name]}, %{single: ref})
       when not is_nil(ref),
       do: {:ok, ref, name}

  defp column(%{"class" => "COLUMN_REF", "column_names" => [qualifier, name]}, %{named: named}) do
    case Map.fetch(named, qualifier) do
      {:ok, ref} -> {:ok, ref, name}
      :error -> :error
    end
  end

  defp column(_node, _aliases), do: :error

  defp literal(%{"class" => "CONSTANT", "value" => %{"is_null" => false} = value}, _params),
    do: constant(value)

  defp literal(
         %{
           "class" => "CAST",
           "cast_type" => %{"id" => cast},
           "child" => %{"class" => "CONSTANT", "value" => %{"is_null" => false, "value" => text}}
         },
         _params
       )
       when cast in ["TIMESTAMP", "DATE"] and is_binary(text) do
    case cast do
      "TIMESTAMP" -> text |> String.replace(" ", "T") |> naive()
      "DATE" -> date(text)
    end
  end

  defp literal(%{"class" => "PARAMETER", "identifier" => identifier}, params) do
    with {index, ""} <- Integer.parse(identifier),
         {:ok, value} <- Enum.fetch(params, index - 1) do
      bound(value)
    else
      _named_or_absent -> :error
    end
  end

  defp literal(
         %{"class" => "FUNCTION", "function_name" => name, "children" => [argument]},
         params
       )
       when is_map_key(@epoch_functions, name) do
    with {:ok, n} <- integer(argument, params),
         microseconds = n * Map.fetch!(@epoch_functions, name),
         true <- abs(microseconds) <= @max_epoch_microseconds do
      {:ok, NaiveDateTime.add(@epoch, microseconds, :microsecond)}
    else
      _not_an_integer_in_range -> :error
    end
  end

  defp literal(_node, _params), do: :error

  defp integer(%{"class" => "CAST", "cast_type" => %{"id" => id}, "child" => child}, params)
       when id in ["INTEGER", "BIGINT", "UBIGINT", "HUGEINT"],
       do: integer(child, params)

  defp integer(%{"class" => class} = node, params) when class in ["CONSTANT", "PARAMETER"] do
    case literal(node, params) do
      {:ok, n} when is_integer(n) -> {:ok, n}
      _another_value -> :error
    end
  end

  defp integer(_node, _params), do: :error

  defp bound(value) when is_number(value) or is_binary(value), do: {:ok, value}
  defp bound(%NaiveDateTime{} = value), do: {:ok, value}
  defp bound(%Date{} = value), do: {:ok, value}

  defp bound(%DateTime{} = value),
    do: {:ok, NaiveDateTime.add(@epoch, DateTime.to_unix(value, :microsecond), :microsecond)}

  defp bound(_opaque), do: :error

  defp constant(%{"type" => %{"id" => id}, "value" => value})
       when id in ["TINYINT", "SMALLINT", "INTEGER", "BIGINT", "HUGEINT", "FLOAT", "DOUBLE"] and
              is_number(value),
       do: {:ok, value}

  defp constant(%{"type" => %{"id" => "VARCHAR"}, "value" => value}) when is_binary(value),
    do: {:ok, value}

  defp constant(_value), do: :error

  defp naive(text) do
    case NaiveDateTime.from_iso8601(text) do
      {:ok, naive} -> {:ok, naive}
      {:error, _reason} -> :error
    end
  end

  defp date(text) do
    case Date.from_iso8601(text) do
      {:ok, date} -> {:ok, date}
      {:error, _reason} -> :error
    end
  end

  defp excludes?(stats, {column, op, value}) do
    with %{min: min, max: max} when not is_nil(min) and not is_nil(max) <-
           Map.get(stats, column, :missing),
         {:ok, low} <- compare(min, value),
         {:ok, high} <- compare(max, value) do
      case op do
        :gt -> high in [:lt, :eq]
        :ge -> high == :lt
        :lt -> low in [:gt, :eq]
        :le -> low == :gt
        :eq -> low == :gt or high == :lt
      end
    else
      _uncertain -> false
    end
  end

  defp compare(a, b) when is_number(a) and is_number(b) do
    cond do
      a < b -> {:ok, :lt}
      a > b -> {:ok, :gt}
      true -> {:ok, :eq}
    end
  end

  defp compare(a, b) when is_binary(a) and is_binary(b) do
    cond do
      a < b -> {:ok, :lt}
      a > b -> {:ok, :gt}
      true -> {:ok, :eq}
    end
  end

  defp compare(%NaiveDateTime{} = a, %NaiveDateTime{} = b),
    do: {:ok, NaiveDateTime.compare(a, b)}

  defp compare(%Date{} = a, %Date{} = b), do: {:ok, Date.compare(a, b)}

  defp compare(_a, _b), do: :error
end
