defmodule Smolquery.QueryService.Nullability do
  @moduledoc """
  Which columns of a statement's result can never be `NULL`, read off its
  parsed form and the catalog's schemas (T-510).

  DuckDB does not say. Its binder types an expression and tracks nothing about
  `NULL`, and every column it hands over Arrow is nullable. ClickHouse does
  say — a result column is `Nullable(T)` or plain `T` — and a client reads the
  difference: HyperDX's histogram looks for a date-typed column in `meta` and
  does not unwrap `Nullable` there. So the edge needs the answer ClickHouse
  would give, and ClickHouse's rule is the one applied here: nullability
  comes from the schema and propagates through an expression.

  ## The rule

  An expression is non-null when every way it could yield `NULL` is ruled out:

    * a constant that is not `NULL`
    * a column the catalog declares `nullable: false` — a regular column, or a
      materialized one that stores its type's default
      (`Smolquery.Schema.computed_expression/1`, T-515) — reached through any
      depth of subquery or CTE whose own output is non-null by this rule
    * a relation's column alias list (`FROM t AS x(a, b)`, `WITH c(a, b) AS`)
      renames its columns in order, and a name is resolved under the alias
    * a plain `CAST` of a non-null expression: a cast that fails is an error,
      not a `NULL`; `TRY_CAST` may be `NULL`
    * a function this module lists as answering `NULL` only for a `NULL`
      argument, over non-null arguments; `count` whatever its argument
    * another aggregate (`min`, `max`, `sum`, `avg`, `any_value`) of a
      non-null argument under an explicit `GROUP BY`, where no group is empty;
      without one, an empty input answers `NULL`
    * a comparison, a conjunction, `IS [NOT] NULL`, `COALESCE` with any
      non-null argument, and a `CASE` whose every branch, `ELSE` included, is
      non-null

  Everything else may be `NULL`: a function not listed, an outer join's
  columns, a `POSITIONAL JOIN`'s (the shorter side is padded with `NULL`,
  though DuckDB calls the join inner), a set operation, grouping sets, a `*` over something whose columns
  are not known. Wrong in that direction costs a `Nullable(...)` a client did
  not need; wrong in the other would put a `NULL` under a type that cannot
  hold one, so the edge also checks the rows it is about to send
  (`SmolqueryClickHouse.Query`).

  ## The answer

  `columns/2` answers one boolean per result column, in order, or `:unknown`
  when the select list cannot be laid out — a `*` it cannot expand. A caller
  uses it only when its length is the result's width.
  """

  alias Smolquery.Schema

  @null_preserving ~w(
    + - * abs ceil floor round sign sqrt exp ln log2 log10 greatest least
    lower upper length strlen trim ltrim rtrim left right substring substr replace reverse
    concat starts_with ends_with contains prefix suffix md5 sha256 hex unhex hash
    trunc to_years to_months to_weeks to_days to_hours to_minutes to_seconds to_milliseconds
    to_microseconds date_trunc time_bucket epoch epoch_ms epoch_us epoch_ns make_timestamp make_timestamp_ns
    year month day hour minute second strftime date_part datepart
    todatetime todatetime64 todate tostartofinterval tostartofsecond tostartofminute
    tostartoffiveminutes tostartoftenminutes tostartoffifteenminutes tostartofhour tostartofday
    tostartofweek tostartofmonth tostartofquarter tostartofyear
    fromunixtimestamp fromunixtimestamp64milli fromunixtimestamp64micro fromunixtimestamp64nano
    tounixtimestamp tounixtimestamp64milli tounixtimestamp64micro tounixtimestamp64nano
    toint64 toint32 touint64 touint32 touint8 tofloat64 tofloat32 tostring
    lowcardinalitykeys
  )

  @always ~w(count count_star uniq uniqexact notempty empty indexhint dynamictype
    rand rand32 rand64 randcanonical cityhash64
    clickhouse_isnull clickhouse_isnotnull toint64orzero toint32orzero touint64orzero
    touint32orzero touint8orzero tofloat64orzero tofloat32orzero tofloat64ordefault)
  @counts ~w(count count_star uniq uniqexact)

  @grouped ~w(min max sum avg any_value first last arg_min arg_max)

  @type schemas :: %{Smolquery.Catalog.table_ref() => Schema.t()}

  @doc """
  One boolean per result column of `statement` — `true` where it can never be
  `NULL` — or `:unknown`.
  """
  @spec columns(map(), schemas()) :: [boolean()] | :unknown
  def columns(%{"statements" => [%{"node" => node}]}, schemas), do: columns(node, schemas)
  def columns(%{"node" => %{} = node}, schemas), do: columns(node, schemas)

  def columns(%{"type" => "SELECT_NODE"} = node, schemas) do
    case outputs(node, schemas, %{}) do
      :unknown -> :unknown
      outputs -> Enum.map(outputs, fn {_name, non_null} -> non_null end)
    end
  end

  def columns(_statement, _schemas), do: :unknown

  defp outputs(%{"type" => "SELECT_NODE"} = node, schemas, outer_ctes) do
    ctes = Map.merge(outer_ctes, ctes(node, schemas, outer_ctes))
    scope = relations(node["from_table"], schemas, ctes)
    grouped = node["group_expressions"] not in [nil, []]

    plain =
      match?(sets when sets in [nil, []], node["group_sets"]) or
        match?([_one], node["group_sets"])

    node["select_list"]
    |> Enum.reduce_while([], fn item, reversed ->
      case laid_out(item, scope, grouped and plain) do
        :unknown -> {:halt, :unknown}
        columns -> {:cont, Enum.reverse(columns, reversed)}
      end
    end)
    |> in_order()
    |> unless_sets(plain)
  end

  defp outputs(_set_operation_or_other, _schemas, _ctes), do: :unknown

  defp in_order(:unknown), do: :unknown
  defp in_order(reversed), do: Enum.reverse(reversed)

  defp unless_sets(:unknown, _plain), do: :unknown
  defp unless_sets(columns, true), do: columns
  defp unless_sets(columns, false), do: Enum.map(columns, fn {name, _flag} -> {name, false} end)

  defp ctes(%{"cte_map" => %{"map" => entries}}, schemas, outer) when is_list(entries) do
    Enum.reduce(entries, %{}, fn %{"key" => name} = entry, known ->
      inner = get_in(entry, ["value", "query", "node"])

      columns =
        inner |> outputs(schemas, Map.merge(outer, known)) |> renamed(entry["value"]["aliases"])

      Map.put(known, String.downcase(name), columns)
    end)
  end

  defp ctes(_node, _schemas, _outer), do: %{}

  defp laid_out(%{"class" => "STAR"} = star, scope, _grouped) do
    if Map.get(star, "exclude_list", []) in [nil, []] and
         Map.get(star, "replace_list", []) in [nil, []],
       do: starred(star["relation_name"], scope),
       else: :unknown
  end

  defp laid_out(item, scope, grouped), do: [{output_name(item), non_null?(item, scope, grouped)}]

  defp starred(relation, scope) when relation in [nil, ""] do
    if Enum.any?(scope, &(&1.columns == :unknown)),
      do: :unknown,
      else: Enum.flat_map(scope, & &1.columns)
  end

  defp starred(relation, scope) do
    case Enum.filter(scope, &(&1.name == String.downcase(relation))) do
      [%{columns: columns}] when is_list(columns) -> columns
      _none_or_ambiguous -> :unknown
    end
  end

  defp output_name(%{"alias" => alias}) when alias not in [nil, ""], do: String.downcase(alias)

  defp output_name(%{"class" => "COLUMN_REF", "column_names" => names}),
    do: names |> List.last() |> String.downcase()

  defp output_name(_expression), do: nil

  defp relations(nil, _schemas, _ctes), do: []
  defp relations(%{"type" => "EMPTY"}, _schemas, _ctes), do: []

  defp relations(%{"type" => "BASE_TABLE"} = table, schemas, ctes) do
    name = String.downcase(table["table_name"])
    alias = relation_alias(table, name)

    columns =
      case {table["schema_name"], Map.fetch(ctes, name)} do
        {schema, {:ok, outputs}} when schema in [nil, ""] -> outputs
        {schema, _not_a_cte} -> declared(schemas, schema, table["table_name"])
      end

    [%{name: alias, columns: renamed(columns, table["column_name_alias"])}]
  end

  defp relations(%{"type" => "SUBQUERY"} = subquery, schemas, ctes) do
    outputs = outputs(get_in(subquery, ["subquery", "node"]), schemas, ctes)

    [
      %{
        name: relation_alias(subquery, nil),
        columns: renamed(outputs, subquery["column_name_alias"])
      }
    ]
  end

  defp relations(
         %{"type" => "JOIN", "join_type" => "INNER", "ref_type" => kind} = join,
         schemas,
         ctes
       )
       when kind in ["REGULAR", "CROSS", "ASOF"],
       do: Enum.flat_map([join["left"], join["right"]], &relations(&1, schemas, ctes))

  defp relations(%{"type" => "JOIN"} = join, schemas, ctes) do
    [join["left"], join["right"]]
    |> Enum.flat_map(&relations(&1, schemas, ctes))
    |> Enum.map(&%{&1 | columns: nullable(&1.columns)})
  end

  defp relations(_table_function_or_other, _schemas, _ctes), do: [%{name: nil, columns: :unknown}]

  defp relation_alias(%{"alias" => alias}, _default) when alias not in [nil, ""],
    do: String.downcase(alias)

  defp relation_alias(_relation, default), do: default

  defp renamed(:unknown, _aliases), do: :unknown
  defp renamed(columns, aliases) when aliases in [nil, []], do: columns

  defp renamed(columns, aliases) do
    {given, kept} = Enum.split(columns, length(aliases))

    given
    |> Enum.zip(aliases)
    |> Enum.map(fn {{_name, flag}, alias} -> {String.downcase(alias), flag} end)
    |> Enum.concat(kept)
  end

  defp nullable(:unknown), do: :unknown
  defp nullable(columns), do: Enum.map(columns, fn {name, _flag} -> {name, false} end)

  defp declared(schemas, dataset, table) do
    found =
      Enum.find(schemas, fn {{ds, t}, _schema} ->
        String.downcase(t) == String.downcase(table) and
          (dataset in [nil, ""] or String.downcase(ds) == String.downcase(dataset))
      end)

    case found do
      {_ref, %Schema{fields: fields}} ->
        Enum.map(fields, &{String.downcase(&1.name), not &1.nullable})

      nil ->
        :unknown
    end
  end

  defp non_null?(%{"class" => "CONSTANT", "value" => %{"is_null" => true}}, _scope, _grouped),
    do: false

  defp non_null?(%{"class" => "CONSTANT"}, _scope, _grouped), do: true

  defp non_null?(%{"class" => "COLUMN_REF", "column_names" => names}, scope, _grouped),
    do: resolved(Enum.map(names, &String.downcase/1), scope)

  defp non_null?(%{"class" => "CAST", "try_cast" => true}, _scope, _grouped), do: false

  defp non_null?(%{"class" => "CAST", "child" => child}, scope, grouped),
    do: non_null?(child, scope, grouped)

  defp non_null?(%{"class" => "COMPARISON", "left" => left, "right" => right}, scope, grouped),
    do: non_null?(left, scope, grouped) and non_null?(right, scope, grouped)

  defp non_null?(%{"class" => "CONJUNCTION", "children" => children}, scope, grouped),
    do: Enum.all?(children, &non_null?(&1, scope, grouped))

  defp non_null?(%{"class" => "OPERATOR", "type" => type}, _scope, _grouped)
       when type in ["OPERATOR_IS_NULL", "OPERATOR_IS_NOT_NULL"],
       do: true

  defp non_null?(%{"class" => "OPERATOR", "type" => "OPERATOR_COALESCE"} = node, scope, grouped),
    do: Enum.any?(node["children"], &non_null?(&1, scope, grouped))

  defp non_null?(%{"class" => "OPERATOR", "type" => "OPERATOR_NOT"} = node, scope, grouped),
    do: Enum.all?(node["children"], &non_null?(&1, scope, grouped))

  defp non_null?(%{"class" => "CASE"} = node, scope, grouped) do
    branches = Enum.map(node["case_checks"], & &1["then_expr"])

    match?(%{"class" => _some}, node["else_expr"]) and
      Enum.all?([node["else_expr"] | branches], &non_null?(&1, scope, grouped))
  end

  defp non_null?(%{"class" => "FUNCTION"} = call, scope, grouped) do
    name = String.downcase(call["function_name"])
    filtered = match?(%{"class" => _some}, call["filter"])

    cond do
      name in @counts -> true
      filtered -> false
      name in @always -> true
      name in @grouped -> grouped and arguments_non_null?(call, scope, grouped)
      name in @null_preserving -> arguments_non_null?(call, scope, grouped)
      true -> false
    end
  end

  defp non_null?(_other, _scope, _grouped), do: false

  defp arguments_non_null?(call, scope, grouped),
    do: Enum.all?(call["children"] || [], &non_null?(&1, scope, grouped))

  defp resolved([column], scope) do
    case for(%{columns: columns} <- scope, is_list(columns), {^column, flag} <- columns, do: flag) do
      [flag] -> flag and Enum.all?(scope, &is_list(&1.columns))
      _none_or_ambiguous -> false
    end
  end

  defp resolved([relation, column], scope) do
    case for(
           %{name: ^relation, columns: columns} <- scope,
           is_list(columns),
           {^column, flag} <- columns,
           do: flag
         ) do
      [flag] -> flag
      _none_or_ambiguous -> false
    end
  end

  defp resolved(_longer, _scope), do: false
end
