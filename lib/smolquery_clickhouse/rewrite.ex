defmodule SmolqueryClickHouse.Rewrite do
  @moduledoc """
  ClickHouse syntax the engine's parser refuses, rewritten to what it takes
  (T-494, PL-66).

  ClickHouse's functions are answered by macros
  (`Smolquery.QueryService.ClickHouseFunctions`), which need no rewrite. A
  macro cannot reach syntax, though, and the rule here is the Postgres
  edge's: text is touched only where DuckDB cannot parse at all.

  - **An alias inside `GROUP BY` or `ORDER BY`.** ClickHouse lets an
    expression be named wherever it appears, and HyperDX's histogram names
    its bucket three times: in the `SELECT`, in `GROUP BY` and in
    `ORDER BY`. The alias is dropped from the two clauses, where it names
    nothing the `SELECT` did not. Only an `AS` at the clause's own depth is
    one: `GROUP BY CAST(x AS VARCHAR)` keeps its `AS`.
  - **`CAST(x, 'Type')`.** ClickHouse's function form, which HyperDX writes
    for a number filter (`CAST('250', 'Float64')`), becomes `CAST(x AS
    type)` with the engine's name for the type. A type this module does not
    know is left as written, for the engine to refuse by name. The type's
    text is a literal, and a literal may have come from a parameter, so
    nothing of it reaches the statement's code but a name from a fixed table
    or a `DECIMAL(p, s)` whose two numbers were checked to be numbers.

  - **A parametric aggregate, `f(params)(args)`.** HyperDX's filters sidebar
    asks for `groupUniqArray(20)(x)` and its charts for `quantile(0.95)(x)`.
    The parameters move behind the arguments, `f(args, params)`, which is
    the shape a macro or the engine's own function takes. `quantile` is the
    engine's `quantile_cont`, since ClickHouse's interpolates, and
    `quantiles(0.5, 0.95, 0.99)(x)`, the service map's three percentiles in
    one call, is `quantile_cont(x, [0.5, 0.95, 0.99])`, which answers them
    as one array the way ClickHouse does, `quantilesIf` the same through
    the `quantileIf` macro (T-501). Only the names in `@parametric` are read
    this way: `f(a)(b)` means nothing else to either dialect.
  - **`LIKE` with a backslash in its pattern.** ClickHouse's `LIKE` escapes
    with a backslash, and HyperDX writes `LIKE lower('%user\_id%')` for a
    term with an underscore. The engine's `LIKE` has no escape character
    unless told, so `ESCAPE '\'` is added after such a pattern — a literal,
    or a literal inside `lower(...)` or `upper(...)`.
  - **`WITH (expr) AS alias`.** ClickHouse names an expression in a `WITH`
    list, where the engine takes only subqueries. HyperDX sends one per
    alias of a source's select list (`ServiceName as service`), so that a
    filter can say `service` in a statement whose `SELECT` does not define
    it, as its histogram's does not. The item leaves the list, the list
    leaves the statement when nothing else is in it, and the alias is
    written as its expression, in parentheses, wherever it is read: not
    where a `SELECT` defines it, with `as` or without (`ServiceName
    service`), not after a `.`, and not before a `(`. It is read only in the
    statement's own scope. Inside a subquery the name is left alone, and an
    alias a subquery defines for itself (`SELECT ServiceName AS service`) is
    not substituted anywhere, since outside the subquery that name is its
    column. A later alias may use an earlier one (`(a + 1) AS b, (b * 2) AS
    c`). Only the statement's leading `WITH` is read, and only a
    parenthesized expression: a common table expression is left as it is.
  - **`JSONExtract(json, 'Type')`.** The form with a type and no path,
    which HyperDX writes on a row click to find the row by a map or an array
    it was shown: `LogAttributes=JSONExtract('{"k":"v"}', 'Map(String,
    String)')`. It becomes a cast of the JSON to the engine's name for the
    type. A form with a path is left for the engine to refuse.
  - **`MD5(x)`.** ClickHouse's answers sixteen bytes, which HyperDX wraps as
    `lower(hex(MD5(...)))`; the engine's answers the hex text already. The
    call is renamed to a macro that answers the bytes.
  - **A bare `default.`** ClickHouse's default database is named by a word
    the engine reserves; it is quoted.
  - **`isNull(x)`, `isNotNull(x)` and `any(x)`.** `ISNULL` and `ANY` are the
    parser's own words, so the calls are renamed to macros and to
    `any_value`. An `ANY(` after a comparison operator is the quantifier both
    dialects have, `x = ANY(SELECT ...)`, and is left alone.

  - **`now()`.** ClickHouse's is a `DateTime`, a time with no zone, and a
    relative window is written `Timestamp >= now() - INTERVAL 15 MINUTE`. The
    engine's is a `TIMESTAMP WITH TIME ZONE`, which it will not compare with
    a `TIMESTAMP_NS` column at all (T-542). The call is renamed to `now64`,
    the macro that answers the clock as a plain timestamp, to the
    microsecond where ClickHouse's `now()` stops at the second.

  The rest is what HyperDX's metrics screens send (T-501), which read
  arrays, windows and tuples where its logs and traces screens read none:

  - **`lagInFrame`, `leadInFrame`, `last_value`.** Renamed to `lag`, `lead`
    and `last`: the first two are the engine's window functions of the same
    shape, and ClickHouse's `last_value` is an aggregate where the engine's
    is a window function only, so it becomes the `last` aggregate, which
    the engine also runs over a window.
  - **A lambda before its array.** ClickHouse writes `arrayExists(x -> x > 1,
    a)`; the engine writes `list_transform(a, x -> x > 1)`, and a lambda
    cannot pass through a macro. The two arguments swap, and `arrayExists`,
    `arrayAll`, `arrayFirstIndex` and `arrayCount` become a macro over the
    list the lambda answers, `arrayMap` and `arrayFilter` the engine's
    `list_transform` and `list_filter`. A form with a different arity is
    left alone.
  - **`t.1`, a tuple's element.** The engine's `list_zip` answers unnamed
    structs that read by position, `t[1]`, and does not parse `.1` at all.
    The dot form after a name, a `)` or a `]` becomes the bracket form.
  - **`* EXCEPT (...)`** is the engine's `* EXCLUDE (...)`.
  - **`group`.** HyperDX names a histogram's packed group columns `group`,
    which the engine takes as the keyword wherever `BY` does not follow.
    The name is quoted; `GROUP BY` is not.
  - **`inf` and `nan`** are the doubles the engine spells `'infinity'` and
    `'nan'`.
  - **`CAST(x AS Array(Int64))`.** ClickHouse's type names inside the
    standard cast form become the engine's, as they do in the function
    form; a name this module does not know stays as written.
  - **`counts - prev_counts`.** ClickHouse subtracts two arrays element by
    element, and HyperDX's histogram quantile relies on it. The engine has
    no such operator, so the rewrite keeps a list of the names the
    statement binds to arrays — an alias of a `CAST(... AS Array(...))`, of
    an array function such as `arrayZip` or `sumForEach`, of a list literal,
    or of `any(...)`, `multiIf(...)` and their kin passed one — and writes
    `a - b` between two of them as `clickhouse_arrayMinus(a, b)`. A minus
    between any other two names is left alone.

  The statement's quoting is standard by the time it arrives
  (`Statement.standard_quoting/1`), so literals, quoted names and comments
  are whole tokens that are never read as code.
  """

  alias Smolquery.Sql

  @piece ~r/[A-Za-z_][A-Za-z0-9_]*|[(),]/

  @clause_ends ~w(having limit union window qualify settings format intersect except offset fetch)

  @list_parametric ~w(quantiles quantilesexact quantilesif)

  @parametric %{
    "quantile" => "quantile_cont",
    "quantiles" => "quantile_cont",
    "quantileexact" => "quantile_disc",
    "quantilesexact" => "quantile_disc",
    "quantileif" => "quantileIf",
    "quantilesif" => "quantileIf",
    "median" => "median",
    "groupuniqarray" => "groupUniqArray",
    "groupuniqarrayif" => "groupUniqArrayIf",
    "groupuniqarrayarray" => "groupUniqArrayArray",
    "grouparray" => "groupArray",
    "grouparrayif" => "groupArrayIf"
  }

  @renamed %{
    "isnull" => "clickhouse_isNull",
    "isnotnull" => "clickhouse_isNotNull",
    "any" => "any_value",
    "md5" => "clickhouse_MD5",
    "now" => "now64",
    "laginframe" => "lag",
    "leadinframe" => "lead",
    "last_value" => "last"
  }

  @lambda_first %{
    "arrayexists" => {:over, "clickhouse_arrayExists"},
    "arrayall" => {:over, "clickhouse_arrayAll"},
    "arrayfirstindex" => {:over, "clickhouse_arrayFirstIndex"},
    "arraycount" => {:over, "clickhouse_arrayCount"},
    "arraymap" => {:call, "list_transform"},
    "arrayfilter" => {:call, "list_filter"}
  }

  @doubles %{"inf" => "'infinity'::DOUBLE", "nan" => "'nan'::DOUBLE"}

  @array_valued ~w(arrayzip arrayzipunaligned arraycumsum sumforeach arraywithconstant arrayconcat
                   arrayreverse arrayresize arrayslice arraymap arrayfilter range emptyarrayint64
                   emptyarrayfloat64 emptyarraystring grouparray groupuniqarray groupuniqarrayarray
                   clickhouse_arrayminus clickhouse_arrayplus)

  @array_through ~w(any anylast tonullable assumenotnull if multiif ifnull coalesce)

  @list_ends ~w(from union intersect where limit settings format)

  @types %{
    "int8" => "TINYINT",
    "int16" => "SMALLINT",
    "int32" => "INTEGER",
    "int64" => "BIGINT",
    "uint8" => "UTINYINT",
    "uint16" => "USMALLINT",
    "uint32" => "UINTEGER",
    "uint64" => "UBIGINT",
    "float32" => "FLOAT",
    "float64" => "DOUBLE",
    "string" => "VARCHAR",
    "bool" => "BOOLEAN",
    "date" => "DATE",
    "date32" => "DATE",
    "datetime" => "TIMESTAMP",
    "datetime64" => "TIMESTAMP"
  }

  @doc """
  `statement` with the constructs above rewritten, and everything else
  byte for byte.
  """
  @spec call(String.t()) :: String.t()
  def call(statement) when is_binary(statement) do
    pieces = statement |> Sql.tokens() |> Enum.flat_map(&pieces/1) |> expression_aliases()

    arrays = array_aliases(pieces)

    pieces
    |> walk(%{depth: 0, clauses: [], casts: [], last: nil, arrays: arrays}, [])
    |> IO.iodata_to_binary()
  end

  @doc """
  The engine's name for a ClickHouse type, or `:error` for one with no
  equivalent here. `Nullable(T)` and `LowCardinality(T)` read as `T`, and a
  `DateTime64(P)` or a `DateTime('zone')` as a `TIMESTAMP`.
  """
  @spec engine_type(String.t()) :: {:ok, String.t()} | :error
  def engine_type(type) when is_binary(type) do
    case type |> String.trim() |> String.downcase() do
      "nullable(" <> rest -> rest |> inside() |> engine_type()
      "lowcardinality(" <> rest -> rest |> inside() |> engine_type()
      "array(" <> rest -> array_type(inside(rest))
      "map(" <> rest -> map_type(inside(rest))
      "datetime64(" <> _precision -> {:ok, "TIMESTAMP"}
      "datetime(" <> _zone -> {:ok, "TIMESTAMP"}
      "decimal(" <> _rest = decimal -> decimal_type(decimal)
      name -> Map.fetch(@types, name)
    end
  end

  defp decimal_type(decimal) do
    if Regex.match?(~r/\Adecimal\(\s*\d+\s*(,\s*\d+\s*)?\)\z/, decimal),
      do: {:ok, String.upcase(decimal)},
      else: :error
  end

  defp inside(rest), do: String.slice(rest, 0..-2//1)

  defp array_type(element) do
    with {:ok, type} <- engine_type(element), do: {:ok, type <> "[]"}
  end

  defp map_type(inner) do
    with [key, value] <- split_types(inner),
         {:ok, key} <- engine_type(key),
         {:ok, value} <- engine_type(value) do
      {:ok, "MAP(#{key}, #{value})"}
    else
      _not_two_types -> :error
    end
  end

  defp split_types(inner), do: split_types(inner, 0, <<>>, [])

  defp split_types(<<>>, _depth, current, acc), do: Enum.reverse([current | acc])

  defp split_types(<<?,, rest::binary>>, 0, current, acc),
    do: split_types(rest, 0, <<>>, [current | acc])

  defp split_types(<<?(, rest::binary>>, depth, current, acc),
    do: split_types(rest, depth + 1, <<current::binary, ?(>>, acc)

  defp split_types(<<?), rest::binary>>, depth, current, acc),
    do: split_types(rest, depth - 1, <<current::binary, ?)>>, acc)

  defp split_types(<<char, rest::binary>>, depth, current, acc),
    do: split_types(rest, depth, <<current::binary, char>>, acc)

  defp expression_aliases(pieces) do
    {lead, rest} = Enum.split_while(pieces, &trivia?/1)

    with [{:word, _text, "with"} = with_word | list] <- rest,
         {items, body} <- with_items(list, [], [], 0),
         {aliases, tables} when aliases != [] <-
           Enum.split_with(items, &match?({:alias, _, _}, &1)) do
      names = alias_expressions(aliases, redefined(body), %{})

      lead ++ with_clause(with_word, tables) ++ substitute(body, names)
    else
      _no_expression_alias -> pieces
    end
  end

  defp alias_expressions([], _shadowed, names), do: names

  defp alias_expressions([{:alias, name, expression} | rest], shadowed, names) do
    if MapSet.member?(shadowed, name),
      do: alias_expressions(rest, shadowed, names),
      else: alias_expressions(rest, shadowed, Map.put(names, name, substitute(expression, names)))
  end

  defp trivia?({:comment, _text}), do: true
  defp trivia?({:other, text}), do: String.trim(text) == ""
  defp trivia?(_piece), do: false

  defp with_items([], _item, _items, _depth), do: :error

  defp with_items([{:word, _text, "select"} | _rest] = body, item, items, 0),
    do: {Enum.reverse([with_item(item) | items]), body}

  defp with_items([:comma | rest], item, items, 0),
    do: with_items(rest, [], [with_item(item) | items], 0)

  defp with_items([:open | rest], item, items, depth),
    do: with_items(rest, [:open | item], items, depth + 1)

  defp with_items([:close | rest], item, items, depth),
    do: with_items(rest, [:close | item], items, depth - 1)

  defp with_items([piece | rest], item, items, depth),
    do: with_items(rest, [piece | item], items, depth)

  defp with_item(reversed) do
    item = Enum.reverse(reversed)

    with [:open | rest] <- drop_space(item),
         {:ok, expression, after_expression} <- group(rest),
         [{:word, _as, "as"} | after_as] <- drop_space(after_expression),
         [name | trailing] <- drop_space(after_as),
         {:ok, name} <- alias_text(name),
         [] <- drop_space(trailing) do
      {:alias, name, expression}
    else
      _table_expression -> {:table, item}
    end
  end

  defp alias_text({:word, text, _lower}), do: {:ok, text}

  defp alias_text({:quoted, text}),
    do: {:ok, text |> String.slice(1..-2//1) |> String.replace(~s(""), ~s("))}

  defp alias_text(_piece), do: :error

  defp with_clause(_with_word, []), do: []

  defp with_clause(with_word, tables) do
    [with_word, {:other, " "}] ++
      Enum.flat_map(Enum.intersperse(tables, :comma), fn
        {:table, item} -> drop_space(item)
        :comma -> [:comma]
      end)
  end

  defp substitute(pieces, names), do: substitute(pieces, names, %{previous: nil, groups: []}, [])

  defp substitute([], _names, _scope, acc), do: Enum.reverse(acc)

  defp substitute([:open | rest], names, scope, acc) do
    group = if subquery?(rest), do: :subquery, else: :group

    substitute(rest, names, %{scope | previous: :open, groups: [group | scope.groups]}, [
      :open | acc
    ])
  end

  defp substitute([:close | rest], names, scope, acc),
    do:
      substitute(rest, names, %{scope | previous: :close, groups: Enum.drop(scope.groups, 1)}, [
        :close | acc
      ])

  defp substitute([piece | rest], names, scope, acc) do
    with {:ok, name} <- alias_text(piece),
         {:ok, expression} <- Map.fetch(names, name),
         true <- read?(scope, rest) do
      substitute(
        rest,
        names,
        %{scope | previous: piece},
        Enum.reverse([:open | expression] ++ [:close], acc)
      )
    else
      _not_a_read ->
        substitute(rest, names, %{scope | previous: significant(piece, scope.previous)}, [
          piece | acc
        ])
    end
  end

  defp subquery?(rest),
    do: match?([{:word, _text, lower} | _more] when lower in ["select", "with"], drop_space(rest))

  defp read?(%{groups: groups, previous: previous}, rest),
    do: :subquery not in groups and names_a_value?(previous) and not called?(rest)

  @before_a_value ~w(select distinct where and or not by on having when then else case in like ilike
                     between is all any prewhere using)

  defp names_a_value?({:word, _text, lower}), do: lower in @before_a_value
  defp names_a_value?({:other, text}), do: not String.ends_with?(text, ".")
  defp names_a_value?(:close), do: false
  defp names_a_value?({:quoted, _text}), do: false
  defp names_a_value?({:string, _text}), do: false
  defp names_a_value?(_open_comma_or_start), do: true

  defp called?([:open | _rest]), do: true
  defp called?([{:other, "." <> _member} | _rest]), do: true
  defp called?(_rest), do: false

  defp redefined(body) do
    body
    |> Enum.reduce({[], nil, MapSet.new()}, fn
      :open, {groups, _previous, names} ->
        {[:group | groups], :open, names}

      :close, {groups, _previous, names} ->
        {Enum.drop(groups, 1), :close, names}

      piece, {groups, previous, names} ->
        {groups, significant(piece, previous), define(piece, previous, groups, names)}
    end)
    |> elem(2)
  end

  defp define(piece, {:word, _as, "as"}, [_inside | _groups], names) do
    case alias_text(piece) do
      {:ok, name} -> MapSet.put(names, name)
      :error -> names
    end
  end

  defp define(_piece, _previous, _groups, names), do: names

  defp significant({:other, text} = piece, previous),
    do: if(String.trim(text) == "", do: previous, else: piece)

  defp significant({:comment, _text}, previous), do: previous
  defp significant(piece, _previous), do: piece

  defp pieces({:code, text}) do
    @piece
    |> Regex.split(text, include_captures: true, trim: true)
    |> Enum.map(&piece/1)
  end

  defp pieces({kind, text}), do: [{kind, text}]

  defp piece("("), do: :open
  defp piece(")"), do: :close
  defp piece(","), do: :comma

  defp piece(<<?_, _rest::binary>> = text), do: word(text)

  defp piece(<<char, _rest::binary>> = text) when char in ?a..?z or char in ?A..?Z,
    do: word(text)

  defp piece(text), do: {:other, text}

  defp word(text), do: {:word, text, String.downcase(text)}

  defp walk([], _state, acc), do: Enum.reverse(acc)

  defp walk([{:word, text, lower} | rest], state, acc) when lower in ["group", "order"] do
    case {by(rest), lower} do
      {{:ok, between, by, after_by}, _lower} ->
        walk(after_by, enter_clause(state), [by, between, text | acc])

      {:error, "group"} ->
        walk(rest, %{state | last: lower}, [group_name(text, acc) | acc])

      {:error, _order} ->
        walk(rest, %{state | last: lower}, [text | acc])
    end
  end

  defp walk([{:word, text, "except"} = word | rest], state, acc) do
    if after_star?(acc),
      do: walk(rest, %{state | last: nil}, ["EXCLUDE" | acc]),
      else: walk_word(word, text, rest, state, acc)
  end

  defp walk([{:word, text, lower}, :open | rest], state, acc)
       when is_map_key(@lambda_first, lower) do
    with {:ok, arguments, after_call} <- group(rest),
         [lambda, list] <- split_arguments(arguments) do
      list = list |> drop_space() |> walk(inner(state), [])
      lambda = lambda |> drop_space() |> walk(inner(state), [])

      call =
        case Map.fetch!(@lambda_first, lower) do
          {:over, macro} -> [macro, "(list_transform(", list, ", ", lambda, "))"]
          {:call, function} -> [function, "(", list, ", ", lambda, ")"]
        end

      walk(after_call, %{state | last: nil}, [call | acc])
    else
      _other_arity -> walk([:open | rest], %{state | last: lower}, [text | acc])
    end
  end

  defp walk([{:word, text, "as"} | rest], %{depth: depth, casts: [depth | _]} = state, acc) do
    case cast_target(rest) do
      {:ok, type, after_type} -> walk(after_type, %{state | last: nil}, [type, " ", text | acc])
      :error -> walk(rest, %{state | last: "as"}, [text | acc])
    end
  end

  defp walk(
         [{:word, left, left_lower} = word, {:other, minus}, {:word, right, right_lower} | rest] =
           pieces,
         %{arrays: arrays} = state,
         acc
       )
       when is_map_key(arrays, left_lower) and is_map_key(arrays, right_lower) do
    if String.trim(minus) == "-",
      do:
        walk(rest, %{state | last: nil}, [
          ["clickhouse_arrayMinus(", left, ", ", right, ")"] | acc
        ]),
      else: walk_word(word, left, tl(pieces), state, acc)
  end

  defp walk([{:word, text, lower} | rest], state, acc)
       when is_map_key(@doubles, lower) and not (rest != [] and hd(rest) == :open) do
    if after_member?(acc),
      do: walk(rest, %{state | last: lower}, [text | acc]),
      else: walk(rest, %{state | last: nil}, [Map.fetch!(@doubles, lower) | acc])
  end

  defp walk([{:word, _text, "default"} = word, {:other, "." <> _after} = dot | rest], state, acc),
    do: walk([dot | rest], %{state | last: "default"}, [quote_unless_member(word, state) | acc])

  defp walk([{:word, text, lower}, :open | rest], state, acc)
       when is_map_key(@parametric, lower) do
    with {:ok, params, [:open | after_params]} <- group(rest),
         {:ok, args, after_args} <- group(after_params) do
      call = [
        Map.fetch!(@parametric, lower),
        "(",
        walk(args, inner(state), []),
        ", ",
        parameters(lower, walk(params, inner(state), [])),
        ")"
      ]

      walk(after_args, %{state | last: nil}, [call | acc])
    else
      _ordinary_call -> walk([:open | rest], %{state | last: lower}, [text | acc])
    end
  end

  defp walk([{:word, text, "jsonextract"}, :open | rest], state, acc) do
    with {:ok, arguments, after_call} <- group(rest),
         {:ok, value, literal} <- value_and_type(arguments),
         {:ok, type} <- literal |> String.slice(1..-2//1) |> engine_type() do
      call = ["CAST(CAST(", walk(value, inner(state), []), " AS JSON) AS ", type, ")"]

      walk(after_call, %{state | last: nil}, [call | acc])
    else
      _a_path_form -> walk([:open | rest], %{state | last: "jsonextract"}, [text | acc])
    end
  end

  defp walk([{:word, text, lower}, :open | rest], state, acc) when is_map_key(@renamed, lower) do
    name = if quantifier?(lower, acc), do: text, else: Map.fetch!(@renamed, lower)

    walk([:open | rest], %{state | last: lower}, [name | acc])
  end

  defp walk([{:word, text, lower} | rest], state, acc) when lower in ["like", "ilike"] do
    case pattern(rest) do
      {:ok, pattern, after_pattern} ->
        walk(after_pattern, %{state | last: nil}, [" ESCAPE '\\'", pattern, text | acc])

      :error ->
        walk(rest, %{state | last: lower}, [text | acc])
    end
  end

  defp walk([{:word, text, "as"} | rest], %{depth: depth, clauses: [depth | _]} = state, acc) do
    case aliased(rest) do
      {:ok, after_alias} -> walk(after_alias, state, trim_space(acc))
      :error -> walk(rest, state, [text | acc])
    end
  end

  defp walk([{:word, text, _lower} = word | rest], state, acc),
    do: walk_word(word, text, rest, state, acc)

  defp walk([:open | rest], %{depth: depth, last: last} = state, acc) do
    casts = if last == "cast", do: [depth + 1 | state.casts], else: state.casts

    walk(rest, %{state | depth: depth + 1, casts: casts, last: nil}, ["(" | acc])
  end

  defp walk([:close | rest], %{depth: depth} = state, acc) do
    casts = Enum.drop_while(state.casts, &(&1 >= depth))
    clauses = Enum.drop_while(state.clauses, &(&1 >= depth))

    walk(rest, %{state | depth: depth - 1, casts: casts, clauses: clauses, last: nil}, [")" | acc])
  end

  defp walk([:comma | rest], %{depth: depth, casts: [depth | _]} = state, acc) do
    case cast_type(rest) do
      {:ok, type, after_type} ->
        walk(after_type, %{state | last: nil}, [type, " AS " | trim_space(acc)])

      :error ->
        walk(rest, %{state | last: nil}, ["," | acc])
    end
  end

  defp walk([:comma | rest], state, acc), do: walk(rest, %{state | last: nil}, ["," | acc])

  defp walk([{:other, text} | rest], state, acc) do
    last = if String.trim(text) == "", do: state.last, else: nil
    written = tuple_elements(text, acc)

    case postfix_cast(text, rest) do
      {:ok, type, after_type} -> walk(after_type, %{state | last: nil}, [type, written | acc])
      :error -> walk(rest, %{state | last: last}, [written | acc])
    end
  end

  defp walk([{_kind, text} | rest], state, acc),
    do: walk(rest, %{state | last: nil}, [text | acc])

  defp postfix_cast(text, rest) do
    if String.ends_with?(String.trim_trailing(text), "::"), do: cast_target(rest), else: :error
  end

  defp walk_word(
         {:word, _word, lower},
         text,
         rest,
         %{depth: depth, clauses: [depth | outer]} = state,
         acc
       )
       when lower in @clause_ends,
       do: walk(rest, %{state | clauses: outer, last: lower}, [text | acc])

  defp walk_word({:word, _word, lower}, text, rest, state, acc),
    do: walk(rest, %{state | last: lower}, [text | acc])

  defp group_name(text, acc), do: if(after_member?(acc), do: text, else: ~s("group"))

  defp after_member?(acc), do: String.ends_with?(String.trim_trailing(leading_text(acc)), ".")

  defp after_star?(acc), do: String.ends_with?(String.trim_trailing(leading_text(acc)), "*")

  defp leading_text(acc) do
    case Enum.find(acc, &(not is_binary(&1) or String.trim(&1) != "")) do
      text when is_binary(text) -> text
      _call_or_nothing -> ""
    end
  end

  defp tuple_elements(text, acc) do
    text = Regex.replace(~r/(?<=[\]\)])\.(\d+)(?![\d.])/, text, "[\\1]")

    if after_operand?(acc),
      do: Regex.replace(~r/\A\.(\d+)(?![\d.])/, text, "[\\1]"),
      else: text
  end

  defp after_operand?([head | _rest]) when is_binary(head),
    do: Regex.match?(~r/[A-Za-z0-9_"\)\]]\z/, head)

  defp after_operand?(_acc), do: false

  defp cast_target(pieces) do
    with [{:word, name, _lower} | rest] <- drop_space(pieces),
         {:ok, written, after_type} <- type_text(name, rest),
         {:ok, type} <- engine_type(written) do
      {:ok, type, after_type}
    else
      _not_a_known_type -> :error
    end
  end

  defp type_text(name, [:open | rest]) do
    with {:ok, inner, after_group} <- group(rest),
         do: {:ok, name <> "(" <> text_of(inner) <> ")", after_group}
  end

  defp type_text(name, rest), do: {:ok, name, rest}

  defp text_of(pieces) do
    Enum.map_join(pieces, fn
      :open -> "("
      :close -> ")"
      :comma -> ","
      {:word, text, _lower} -> text
      {_kind, text} -> text
    end)
  end

  defp split_arguments(pieces) do
    pieces
    |> Enum.reduce({0, [[]]}, fn
      :open, {depth, [current | done]} -> {depth + 1, [[:open | current] | done]}
      :close, {depth, [current | done]} -> {depth - 1, [[:close | current] | done]}
      :comma, {0, groups} -> {0, [[] | groups]}
      piece, {depth, [current | done]} -> {depth, [[piece | current] | done]}
    end)
    |> elem(1)
    |> Enum.map(&Enum.reverse/1)
    |> Enum.reverse()
  end

  defp array_aliases(pieces), do: pieces |> select_items([]) |> array_names(%{})

  defp array_names(items, known) do
    grown = Enum.reduce(items, known, &learned/2)

    if map_size(grown) == map_size(known), do: grown, else: array_names(items, grown)
  end

  defp learned(item, names) do
    case aliased_item(item) do
      {:ok, name, expression} ->
        if array_expression?(expression, names), do: Map.put(names, name, true), else: names

      :error ->
        names
    end
  end

  defp select_items([], acc), do: acc |> Enum.reverse() |> Enum.concat()

  defp select_items([{:word, _text, "select"} | rest], acc) do
    {items, after_list} = select_list(rest, 0, [], [])
    nested = Enum.flat_map(items, &select_items(&1, []))

    select_items(after_list, [nested, items | acc])
  end

  defp select_items([_piece | rest], acc), do: select_items(rest, acc)

  defp select_list([], _depth, current, items), do: {finish_items(current, items), []}

  defp select_list([:close | _rest] = pieces, 0, current, items),
    do: {finish_items(current, items), pieces}

  defp select_list([{:word, _text, lower} | _rest] = pieces, 0, current, items)
       when lower in @list_ends,
       do: {finish_items(current, items), pieces}

  defp select_list([{:word, _text, lower} | rest] = pieces, 0, current, items)
       when lower in ["group", "order"] do
    case by(rest) do
      {:ok, _between, _by, _after_by} -> {finish_items(current, items), pieces}
      :error -> select_list(rest, 0, [hd(pieces) | current], items)
    end
  end

  defp select_list([:comma | rest], 0, current, items),
    do: select_list(rest, 0, [], [Enum.reverse(current) | items])

  defp select_list([:open | rest], depth, current, items),
    do: select_list(rest, depth + 1, [:open | current], items)

  defp select_list([:close | rest], depth, current, items),
    do: select_list(rest, depth - 1, [:close | current], items)

  defp select_list([piece | rest], depth, current, items),
    do: select_list(rest, depth, [piece | current], items)

  defp finish_items(current, items), do: Enum.reverse([Enum.reverse(current) | items])

  defp aliased_item(item) do
    case item |> Enum.reject(&trivia?/1) |> Enum.reverse() do
      [name, {:word, _as, "as"} | expression] ->
        named(name, Enum.reverse(expression))

      [{:word, _text, lower} = name, :close | _rest] = reversed
      when lower not in ["asc", "desc"] ->
        named(name, reversed |> tl() |> Enum.reverse())

      _no_alias ->
        :error
    end
  end

  defp named({:word, _text, lower}, expression), do: {:ok, lower, expression}

  defp named({:quoted, text}, expression),
    do: {:ok, text |> String.slice(1..-2//1) |> String.downcase(), expression}

  defp named(_piece, _expression), do: :error

  defp array_expression?([{:word, _text, lower}, :open | _rest], _names)
       when lower in @array_valued,
       do: true

  defp array_expression?([{:word, _text, "cast"} | _rest] = expression, _names) do
    expression
    |> Enum.chunk_every(2, 1, :discard)
    |> Enum.any?(&match?([{:word, _array, "array"}, :open], &1))
  end

  defp array_expression?([{:word, _text, lower}, :open | rest], names)
       when lower in @array_through do
    case group(rest) do
      {:ok, arguments, _after} ->
        arguments |> split_arguments() |> Enum.any?(&array_expression?(&1, names))

      :error ->
        false
    end
  end

  defp array_expression?([{:other, text} | _rest], _names),
    do: String.starts_with?(String.trim_leading(text), "[")

  defp array_expression?([{:word, _text, lower}], names), do: is_map_key(names, lower)

  defp array_expression?([{:word, _left, left}, {:other, minus}, {:word, _right, right}], names),
    do: String.trim(minus) == "-" and is_map_key(names, left) and is_map_key(names, right)

  defp array_expression?(_expression, _names), do: false

  defp quote_unless_member({:word, text, _lower}, %{last: "."}), do: text
  defp quote_unless_member({:word, _text, _lower}, _state), do: ~s("default")

  defp quantifier?("any", acc) do
    case Enum.find(acc, &(not is_binary(&1) or String.trim(&1) != "")) do
      text when is_binary(text) -> String.ends_with?(String.trim_trailing(text), ["=", "<", ">"])
      _call_or_nothing -> false
    end
  end

  defp quantifier?(_name, _acc), do: false

  defp inner(state), do: %{state | depth: state.depth + 1, last: nil}

  defp parameters(lower, params) when lower in @list_parametric, do: ["[", params, "]"]
  defp parameters(_lower, params), do: params

  defp value_and_type(arguments) do
    {type, value} = arguments |> Enum.reverse() |> Enum.split_while(&(&1 != :comma))

    with [:comma | reversed_value] <- value,
         [{:string, literal}] <- type |> Enum.reverse() |> drop_space() |> Enum.reject(&trivia?/1),
         false <- Enum.member?(depth_zero(Enum.reverse(reversed_value)), :comma) do
      {:ok, Enum.reverse(reversed_value), literal}
    else
      _other_shape -> :error
    end
  end

  defp depth_zero(pieces) do
    pieces
    |> Enum.reduce({0, []}, fn
      :open, {depth, acc} -> {depth + 1, acc}
      :close, {depth, acc} -> {depth - 1, acc}
      piece, {0, acc} -> {0, [piece | acc]}
      _nested, state -> state
    end)
    |> elem(1)
  end

  defp group(pieces), do: group(pieces, 1, [])

  defp group([], _depth, _acc), do: :error
  defp group([:close | rest], 1, acc), do: {:ok, Enum.reverse(acc), rest}
  defp group([:close | rest], depth, acc), do: group(rest, depth - 1, [:close | acc])
  defp group([:open | rest], depth, acc), do: group(rest, depth + 1, [:open | acc])
  defp group([piece | rest], depth, acc), do: group(rest, depth, [piece | acc])

  defp pattern(pieces) do
    case drop_space(pieces) do
      [{:string, literal} | rest] ->
        escaped(literal, [" ", literal], rest)

      [{:word, text, lower}, :open | rest] when lower in ["lower", "upper"] ->
        with {:ok, literal, [:close | after_close]} <- literal_then_close(rest) do
          escaped(literal, [" ", text, "(", literal, ")"], after_close)
        end

      _expression ->
        :error
    end
  end

  defp escaped(literal, written, rest) do
    if String.contains?(literal, "\\"), do: {:ok, written, rest}, else: :error
  end

  defp enter_clause(%{depth: depth, clauses: clauses} = state),
    do: %{state | clauses: [depth | Enum.drop_while(clauses, &(&1 >= depth))], last: "by"}

  defp by([{:other, space}, {:word, text, "by"} | rest]) do
    if String.trim(space) == "", do: {:ok, space, text, rest}, else: :error
  end

  defp by(_rest), do: :error

  defp aliased([{:other, space} | rest]) do
    if String.trim(space) == "", do: alias_name(rest), else: :error
  end

  defp aliased(_rest), do: :error

  defp alias_name([{:word, _text, lower} | rest]) when lower not in ["select", "from"],
    do: {:ok, rest}

  defp alias_name([{:quoted, _text} | rest]), do: {:ok, rest}
  defp alias_name(_rest), do: :error

  defp trim_space([text | acc]) when is_binary(text) do
    if String.trim(text) == "", do: acc, else: [text | acc]
  end

  defp trim_space(acc), do: acc

  defp cast_type(rest) do
    with {:ok, literal, closing} <- literal_then_close(rest),
         {:ok, type} <- literal |> String.slice(1..-2//1) |> engine_type() do
      {:ok, type, closing}
    end
  end

  defp literal_then_close(pieces) do
    with [{:string, literal} | rest] <- drop_space(pieces),
         [:close | _after] = closing <- drop_space(rest) do
      {:ok, literal, closing}
    else
      _not_a_literal -> :error
    end
  end

  defp drop_space([{:other, text} | rest] = pieces) do
    if String.trim(text) == "", do: drop_space(rest), else: pieces
  end

  defp drop_space(pieces), do: pieces
end
