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

  Conjuncts come from the top level of the query's WHERE clause — the
  AND-chain only, since a row can satisfy an OR through its other branch.
  A conjunct is `column <op> literal` (either side), or BETWEEN, where the
  literal is a plain constant or a TIMESTAMP/DATE cast of one — PL-1 names
  timestamp ranges as the pruning that matters. Columns resolve through the
  FROM clause's aliases; an unqualified column resolves only when the query
  reads a single table, because guessing which of two tables `id` means
  could prune the wrong one's segments.

  The sealed tier gets no treatment here: DuckLake collects min-max stats at
  registration and prunes on them natively — verified in the Milestone 2
  spike (PL-2), which is why this module's job ends at the hot tier.
  """

  alias Smolquery.BufferService.HotClient
  alias Smolquery.BufferService.HotManifest.Entry
  alias Smolquery.Catalog

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

  @doc """
  The prunable conjuncts of a serialized statement, keyed by table.

  Tables appear only when at least one conjunct resolved to them; a query this
  module cannot read (set operations, OR-rooted WHERE, subquery-derived
  tables) yields an empty map, which prunes nothing.

  A `$n` placeholder resolves to the n-th of `params` (T-410) when that value
  is a number, a string, a date, or a timestamp; any other bound value leaves
  its conjunct unread, which keeps every entry.
  """
  @spec conjuncts(map(), [Catalog.table_ref()], [term()]) ::
          %{Catalog.table_ref() => [conjunct()]}
  def conjuncts(statement, refs, params \\ [])

  def conjuncts(%{"node" => %{"type" => "SELECT_NODE"} = node}, refs, params) do
    aliases = aliases(Map.get(node, "from_table"), refs)

    node
    |> Map.get("where_clause")
    |> split()
    |> Enum.flat_map(&parse(&1, aliases, params))
    |> Enum.group_by(fn {ref, _conjunct} -> ref end, fn {_ref, conjunct} -> conjunct end)
  end

  def conjuncts(_statement, _refs, _params), do: %{}

  @doc """
  Whether `entry`'s stats leave any chance a row matches every conjunct.
  """
  @spec keep?(HotClient.entry(), [conjunct()]) :: boolean()
  def keep?(_entry, []), do: true

  def keep?(entry, conjuncts) do
    stats = entry |> Map.get("stats", %{}) |> Entry.decode_stats()

    not Enum.any?(conjuncts, &excludes?(stats, &1))
  end

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

  defp literal(_node, _params), do: :error

  @epoch ~N[1970-01-01 00:00:00]

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
