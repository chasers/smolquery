defmodule SmolqueryPg.Sql do
  @moduledoc """
  A lexer that tells SQL code from the places SQL hides code-like text
  (PL-58).

  Two jobs need it. `SmolqueryPg.Statements` splits a simple query on the
  semicolons that are code. `SmolqueryPg.Params` replaces the `$n`
  placeholders that are code. Both must leave a string, a quoted
  identifier, a comment, and a dollar-quoted body alone. Nothing here parses
  SQL: the scanner only tracks which of those it is inside.
  """

  @type token ::
          {:code, String.t()}
          | {:string, String.t()}
          | {:quoted, String.t()}
          | {:comment, String.t()}
          | {:dollar, String.t()}

  @doc """
  `sql` as a list of tokens whose texts concatenate back to `sql`.
  """
  @spec tokens(String.t()) :: [token()]
  def tokens(sql) when is_binary(sql), do: sql |> scan([], []) |> Enum.reverse()

  @doc """
  `sql` with every `:code` token replaced by `fun.(text)`.
  """
  @spec map_code(String.t(), (String.t() -> iodata())) :: String.t()
  def map_code(sql, fun) do
    sql
    |> tokens()
    |> Enum.map(fn
      {:code, text} -> fun.(text)
      {_kind, text} -> text
    end)
    |> IO.iodata_to_binary()
  end

  defp scan(<<>>, current, tokens), do: flush(current, tokens)

  defp scan(<<?', rest::binary>>, current, tokens),
    do: quoted(rest, ?', [?'], :string, flush(current, tokens))

  defp scan(<<?", rest::binary>>, current, tokens),
    do: quoted(rest, ?", [?"], :quoted, flush(current, tokens))

  defp scan(<<"--", rest::binary>>, current, tokens),
    do: line_comment(rest, [?-, ?-], flush(current, tokens))

  defp scan(<<"/*", rest::binary>>, current, tokens),
    do: block_comment(rest, [?*, ?/], flush(current, tokens), 1)

  defp scan(<<?$, rest::binary>>, current, tokens) do
    case dollar_tag(rest) do
      {:ok, tag, rest} -> dollar(rest, tag, [tag, ?$], flush(current, tokens))
      :error -> scan(rest, [?$ | current], tokens)
    end
  end

  defp scan(<<char, rest::binary>>, current, tokens), do: scan(rest, [char | current], tokens)

  defp flush([], tokens), do: tokens
  defp flush(current, tokens), do: [{:code, text(current)} | tokens]

  defp emit(kind, current, tokens), do: [{kind, text(current)} | tokens]

  defp quoted(<<>>, _quote, current, kind, tokens), do: emit(kind, current, tokens)

  defp quoted(<<quote, quote, rest::binary>>, quote, current, kind, tokens),
    do: quoted(rest, quote, [quote, quote | current], kind, tokens)

  defp quoted(<<quote, rest::binary>>, quote, current, kind, tokens),
    do: scan(rest, [], emit(kind, [quote | current], tokens))

  defp quoted(<<char, rest::binary>>, quote, current, kind, tokens),
    do: quoted(rest, quote, [char | current], kind, tokens)

  defp line_comment(<<>>, current, tokens), do: emit(:comment, current, tokens)

  defp line_comment(<<?\n, rest::binary>>, current, tokens),
    do: scan(rest, [], emit(:comment, [?\n | current], tokens))

  defp line_comment(<<char, rest::binary>>, current, tokens),
    do: line_comment(rest, [char | current], tokens)

  defp block_comment(<<>>, current, tokens, _depth), do: emit(:comment, current, tokens)

  defp block_comment(<<"*/", rest::binary>>, current, tokens, 1),
    do: scan(rest, [], emit(:comment, [?/, ?* | current], tokens))

  defp block_comment(<<"*/", rest::binary>>, current, tokens, depth),
    do: block_comment(rest, [?/, ?* | current], tokens, depth - 1)

  defp block_comment(<<"/*", rest::binary>>, current, tokens, depth),
    do: block_comment(rest, [?*, ?/ | current], tokens, depth + 1)

  defp block_comment(<<char, rest::binary>>, current, tokens, depth),
    do: block_comment(rest, [char | current], tokens, depth)

  defp dollar_tag(rest) do
    case Regex.run(~r/^([A-Za-z_][A-Za-z0-9_]*)?\$/, rest) do
      [match, tag] -> {:ok, "#{tag}$", after_match(rest, match)}
      [match] -> {:ok, "$", after_match(rest, match)}
      nil -> :error
    end
  end

  defp after_match(rest, match),
    do: binary_part(rest, byte_size(match), byte_size(rest) - byte_size(match))

  defp dollar(<<>>, _tag, current, tokens), do: emit(:dollar, current, tokens)

  defp dollar(<<?$, rest::binary>> = all, tag, current, tokens) do
    size = byte_size(tag)

    case rest do
      <<^tag::binary-size(^size), after_tag::binary>> ->
        scan(after_tag, [], emit(:dollar, [tag, ?$ | current], tokens))

      _other ->
        <<char, rest::binary>> = all
        dollar(rest, tag, [char | current], tokens)
    end
  end

  defp dollar(<<char, rest::binary>>, tag, current, tokens),
    do: dollar(rest, tag, [char | current], tokens)

  defp text(current), do: current |> Enum.reverse() |> IO.iodata_to_binary()

  @doc """
  `sql` with every `$n` placeholder in code replaced by `fun.(n)`. A `$`
  inside a string, a comment, or a dollar-quoted body stays. Binary
  matching, no regex: this runs per `Bind`.
  """
  @spec map_placeholders(String.t(), (pos_integer() -> iodata())) :: String.t()
  def map_placeholders(sql, fun) do
    map_code(sql, fn code -> code |> placeholders(fun, []) |> IO.iodata_to_binary() end)
  end

  defp placeholders(<<>>, _fun, acc), do: Enum.reverse(acc)

  defp placeholders(<<?$, digit, rest::binary>>, fun, acc) when digit in ?0..?9,
    do: placeholder(rest, fun, acc, digit - ?0)

  defp placeholders(<<char, rest::binary>>, fun, acc), do: placeholders(rest, fun, [char | acc])

  defp placeholder(<<digit, rest::binary>>, fun, acc, n) when digit in ?0..?9,
    do: placeholder(rest, fun, acc, n * 10 + digit - ?0)

  defp placeholder(rest, fun, acc, n), do: placeholders(rest, fun, [fun.(n) | acc])

  @doc """
  The `$n` placeholders of `sql`'s code: the highest `n`, and the cast
  hint beside each (`$1::bigint` reads as a declaration). Binary matching,
  one pass.
  """
  @spec placeholder_info(String.t()) ::
          {max :: non_neg_integer(), hints :: %{pos_integer() => String.t()}}
  def placeholder_info(sql) do
    sql
    |> tokens()
    |> Enum.reduce({0, %{}}, fn
      {:code, code}, acc -> scan_info(code, acc)
      {_kind, _text}, acc -> acc
    end)
  end

  defp scan_info(<<>>, acc), do: acc

  defp scan_info(<<?$, digit, rest::binary>>, acc) when digit in ?0..?9,
    do: scan_info_number(rest, acc, digit - ?0)

  defp scan_info(<<_char, rest::binary>>, acc), do: scan_info(rest, acc)

  defp scan_info_number(<<digit, rest::binary>>, acc, n) when digit in ?0..?9,
    do: scan_info_number(rest, acc, n * 10 + digit - ?0)

  defp scan_info_number(rest, {max, hints}, n) do
    acc = {max(max, n), hints}

    case cast_hint(skip_spaces(rest)) do
      {:ok, type, rest} -> scan_info(rest, put_hint(acc, n, type))
      :none -> scan_info(rest, acc)
    end
  end

  defp put_hint({max, hints}, n, type), do: {max, Map.put(hints, n, type)}

  defp skip_spaces(<<space, rest::binary>>) when space in [?\s, ?\t, ?\n, ?\r],
    do: skip_spaces(rest)

  defp skip_spaces(rest), do: rest

  defp cast_hint(<<"::", rest::binary>>) do
    case type_words(skip_spaces(rest), []) do
      {[], _rest} -> :none
      {words, rest} -> {:ok, Enum.join(words, " "), rest}
    end
  end

  defp cast_hint(_rest), do: :none

  @type_suffixes ~w(precision varying zone time with without)

  defp type_words(rest, words) do
    case word(rest, []) do
      {"", _rest} ->
        {Enum.reverse(words), rest}

      {word, after_word} ->
        lower = String.downcase(word)

        if words == [] or lower in @type_suffixes,
          do: type_words(skip_spaces(after_word), [lower | words]),
          else: {Enum.reverse(words), rest}
    end
  end

  defp word(<<?_, rest::binary>>, acc), do: word(rest, [?_ | acc])

  defp word(<<char, rest::binary>>, acc)
       when char in ?a..?z or char in ?A..?Z or char in ?0..?9,
       do: word(rest, [char | acc])

  defp word(rest, acc), do: {acc |> Enum.reverse() |> List.to_string(), rest}

  @typedoc """
  One token of a statement: a bare word (down-cased, as Postgres folds
  unquoted identifiers), a quoted identifier (case preserved, `""`
  unescaped), a run of digits, or any other single character. The third
  element is always the untouched remainder of the input, so a caller can
  take "everything after this token" verbatim.
  """
  @type statement_token ::
          {:word, String.t(), binary()}
          | {:quoted, String.t(), binary()}
          | {:number, String.t(), binary()}
          | {:symbol, String.t(), binary()}
          | :eof

  @doc """
  The next token of `statement`, past whitespace and comments. Binary
  matching, no regex: statement parsing runs on this.
  """
  @spec next_token(binary()) :: statement_token()
  def next_token(statement), do: statement |> skip_trivia() |> token()

  defp token(<<>>), do: :eof

  defp token(<<?", rest::binary>>), do: quoted_token(rest, [])

  defp token(<<digit, _rest::binary>> = statement) when digit in ?0..?9 do
    {number, rest} = digits(statement, [])

    {:number, number, rest}
  end

  defp token(<<?_, _rest::binary>> = statement), do: word_token(statement)

  defp token(<<char, _rest::binary>> = statement)
       when char in ?a..?z or char in ?A..?Z,
       do: word_token(statement)

  defp token(<<char, rest::binary>>), do: {:symbol, <<char>>, rest}

  defp word_token(statement) do
    {word, rest} = word(statement, [])

    {:word, String.downcase(word), rest}
  end

  defp quoted_token(<<?", ?", rest::binary>>, acc), do: quoted_token(rest, [?" | acc])

  defp quoted_token(<<?", rest::binary>>, acc),
    do: {:quoted, acc |> Enum.reverse() |> List.to_string(), rest}

  defp quoted_token(<<char, rest::binary>>, acc), do: quoted_token(rest, [char | acc])
  defp quoted_token(<<>>, acc), do: {:quoted, acc |> Enum.reverse() |> List.to_string(), <<>>}

  defp digits(<<digit, rest::binary>>, acc) when digit in ?0..?9,
    do: digits(rest, [digit | acc])

  defp digits(rest, acc), do: {acc |> Enum.reverse() |> List.to_string(), rest}

  @doc """
  The remainder of `statement` past any leading words in `allowed` — how a
  parser skips optional keyword noise (`WORK`, `SAVEPOINT`, `FROM`).
  """
  @spec skip_words(binary(), [String.t()]) :: binary()
  def skip_words(rest, allowed) do
    case next_token(rest) do
      {:word, word, next} -> if word in allowed, do: skip_words(next, allowed), else: rest
      _other -> rest
    end
  end

  @doc """
  A configuration-parameter name at the head of `statement`: a word,
  optionally dotted (`app.setting`). Answers the name (down-cased) and the
  remainder, or `:error` when no word starts it.
  """
  @spec setting_name(binary()) :: {:ok, String.t(), binary()} | :error
  def setting_name(statement) do
    case next_token(statement) do
      {:word, word, rest} -> dotted(word, rest)
      _other -> :error
    end
  end

  defp dotted(name, <<?., rest::binary>>) do
    case token(rest) do
      {:word, word, rest} -> dotted(name <> "." <> word, rest)
      _other -> {:ok, name, rest}
    end
  end

  defp dotted(name, rest), do: {:ok, name, rest}

  @doc """
  The remainder of `statement` past a `(`-opened group, honouring nesting.
  """
  @spec skip_parens(binary()) :: binary()
  def skip_parens(statement), do: parens(statement, 1)

  defp parens(<<>>, _depth), do: <<>>
  defp parens(<<?), rest::binary>>, 1), do: rest
  defp parens(<<?), rest::binary>>, depth), do: parens(rest, depth - 1)
  defp parens(<<?(, rest::binary>>, depth), do: parens(rest, depth + 1)
  defp parens(<<_char, rest::binary>>, depth), do: parens(rest, depth)

  @doc """
  `statement` past leading whitespace and comments.
  """
  @spec skip_trivia(binary()) :: binary()
  def skip_trivia(<<space, rest::binary>>) when space in [?\s, ?\t, ?\n, ?\r],
    do: skip_trivia(rest)

  def skip_trivia(<<"--", rest::binary>>), do: skip_trivia(past_line(rest))
  def skip_trivia(<<"/*", rest::binary>>), do: skip_trivia(past_block(rest, 1))
  def skip_trivia(statement), do: statement

  @doc """
  The leading keyword of `statement` — past whitespace and comments, in
  lower case — or `""` when none starts it. Binary matching: this runs per
  statement.
  """
  @spec leading_keyword(String.t()) :: String.t()
  def leading_keyword(statement), do: keyword(statement)

  defp keyword(statement) do
    case skip_trivia(statement) do
      <<?(, _rest::binary>> -> "("
      trimmed -> keyword_start(trimmed)
    end
  end

  defp keyword_start(<<?_, _rest::binary>> = statement), do: keyword_word(statement)

  defp keyword_start(<<char, _rest::binary>> = statement)
       when char in ?a..?z or char in ?A..?Z,
       do: keyword_word(statement)

  defp keyword_start(_other), do: ""

  defp keyword_word(statement) do
    {word, _rest} = word(statement, [])

    String.downcase(word)
  end

  defp past_line(<<>>), do: <<>>
  defp past_line(<<?\n, rest::binary>>), do: rest
  defp past_line(<<_char, rest::binary>>), do: past_line(rest)

  defp past_block(<<>>, _depth), do: <<>>
  defp past_block(<<"*/", rest::binary>>, 1), do: rest
  defp past_block(<<"*/", rest::binary>>, depth), do: past_block(rest, depth - 1)
  defp past_block(<<"/*", rest::binary>>, depth), do: past_block(rest, depth + 1)
  defp past_block(<<_char, rest::binary>>, depth), do: past_block(rest, depth)
end
