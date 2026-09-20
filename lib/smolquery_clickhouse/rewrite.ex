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
    engine's `quantile_cont`, since ClickHouse's interpolates. Only the
    names in `@parametric` are read this way: `f(a)(b)` means nothing else
    to either dialect.
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
    where a `SELECT` defines it (`... as service`), not after a `.`, and not
    before a `(`. Only the statement's leading `WITH` is read, and only a
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

  The statement's quoting is standard by the time it arrives
  (`Statement.standard_quoting/1`), so literals, quoted names and comments
  are whole tokens that are never read as code.
  """

  alias Smolquery.Sql

  @piece ~r/[A-Za-z_][A-Za-z0-9_]*|[(),]/

  @clause_ends ~w(having limit union window qualify settings format intersect except offset fetch)

  @parametric %{
    "quantile" => "quantile_cont",
    "quantileexact" => "quantile_disc",
    "quantileif" => "quantileIf",
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
    "md5" => "clickhouse_MD5"
  }

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
    statement
    |> Sql.tokens()
    |> Enum.flat_map(&pieces/1)
    |> expression_aliases()
    |> walk(%{depth: 0, clauses: [], casts: [], last: nil}, [])
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
      names = Map.new(aliases, fn {:alias, name, expression} -> {name, expression} end)

      lead ++ with_clause(with_word, tables) ++ substitute(body, names, nil, [])
    else
      _no_expression_alias -> pieces
    end
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

  defp substitute([], _names, _previous, acc), do: Enum.reverse(acc)

  defp substitute([piece | rest], names, previous, acc) do
    with {:ok, name} <- alias_text(piece),
         {:ok, expression} <- Map.fetch(names, name),
         true <- read?(previous, rest) do
      substitute(rest, names, piece, Enum.reverse([:open | expression] ++ [:close], acc))
    else
      _not_a_read -> substitute(rest, names, significant(piece, previous), [piece | acc])
    end
  end

  defp read?({:word, _text, "as"}, _rest), do: false
  defp read?({:other, text}, rest), do: not String.ends_with?(text, ".") and not called?(rest)
  defp read?(_previous, rest), do: not called?(rest)

  defp called?([:open | _rest]), do: true
  defp called?([{:other, "." <> _member} | _rest]), do: true
  defp called?(_rest), do: false

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
    case by(rest) do
      {:ok, between, by, after_by} ->
        walk(after_by, enter_clause(state), [by, between, text | acc])

      :error ->
        walk(rest, %{state | last: lower}, [text | acc])
    end
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
        walk(params, inner(state), []),
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

  defp walk([{:word, text, lower} | rest], %{depth: depth, clauses: [depth | outer]} = state, acc)
       when lower in @clause_ends,
       do: walk(rest, %{state | clauses: outer, last: lower}, [text | acc])

  defp walk([{:word, text, lower} | rest], state, acc),
    do: walk(rest, %{state | last: lower}, [text | acc])

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

    walk(rest, %{state | last: last}, [text | acc])
  end

  defp walk([{_kind, text} | rest], state, acc),
    do: walk(rest, %{state | last: nil}, [text | acc])

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
