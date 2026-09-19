defmodule SmolqueryClickHouse.Statement do
  @moduledoc """
  The `INSERT` statement a ClickHouse HTTP client sends in its `query`
  parameter, parsed as far as an insert needs (T-476).

      INSERT INTO [TABLE] [db.]table [(column, ...)] [SETTINGS name = value, ...] FORMAT name

  Keywords are case-insensitive. An identifier is bare, backquoted, or
  double-quoted; inside quotes a doubled quote or a backslash escapes the next
  character. A setting's value is a bare token or a single-quoted string.

  Nothing but whitespace and one `;` may follow the format name. ClickHouse
  would read anything more as the first bytes of the data, and a body split
  between the URL and the request is not one smolquery takes.
  """

  alias Smolquery.Identifier
  alias SmolqueryPg.Sql

  @type t :: %{
          database: String.t() | nil,
          table: String.t(),
          columns: [String.t()] | nil,
          settings: %{optional(String.t()) => String.t()},
          format: String.t()
        }

  @doc """
  Parses `query` as an `INSERT ... FORMAT` statement.

  `{:error, message}` names what was expected and where.
  """
  @spec parse(String.t()) :: {:ok, t()} | {:error, String.t()}
  def parse(query) when is_binary(query) do
    with {:ok, rest} <- keyword(query, "INSERT"),
         {:ok, rest} <- keyword(rest, "INTO"),
         rest = optional_keyword(rest, "TABLE"),
         {:ok, first, rest} <- identifier(rest),
         {:ok, database, table, rest} <- qualified(first, rest),
         {:ok, columns, rest} <- column_list(rest),
         {:ok, settings, rest} <- settings(rest),
         {:ok, rest} <- keyword(rest, "FORMAT"),
         {:ok, format, rest} <- identifier(rest),
         :ok <- finished(rest) do
      {:ok,
       %{database: database, table: table, columns: columns, settings: settings, format: format}}
    end
  end

  @format_clause ~r/\A(.*?)(?:\A|(?<=\s))FORMAT\s+([A-Za-z][A-Za-z0-9_]*)\s*\z/is

  @doc """
  Whether `query` is an `INSERT`, by its first keyword.
  """
  @spec insert?(String.t()) :: boolean()
  def insert?(query) when is_binary(query), do: match?({:ok, _rest}, keyword(query, "INSERT"))

  @doc """
  Splits a trailing `FORMAT name` off a statement (T-478).

  Answers the statement without the clause, trimmed and without a final
  `;`, and the format's name, or `nil` when the statement names none. Only
  the end of the statement's code is read (`SmolqueryPg.Sql` tells code from
  literals and comments): a `FORMAT` earlier in the statement, or inside a
  string literal or a comment, is not a clause, and a clause that follows a
  string literal is one. `ORDER BY format DESC` ends in a column named
  `format`, so `asc` and `desc` are never taken for a format's name.
  """
  @spec split_format(String.t()) :: {String.t(), String.t() | nil}
  def split_format(query) when is_binary(query) do
    statement = query |> String.trim() |> String.trim_trailing(";") |> String.trim_trailing()

    {trailing, tokens} =
      statement
      |> Sql.tokens(dialect: :clickhouse)
      |> Enum.reverse()
      |> Enum.split_while(&(not code?(&1)))

    with [{:code, tail} | before] <- tokens,
         [_match, head, format] <- Regex.run(@format_clause, tail),
         false <- String.downcase(format) in ["asc", "desc"] do
      kept = Enum.reverse([{:code, head} | before], Enum.reverse(trailing))

      {kept |> Enum.map_join(&elem(&1, 1)) |> String.trim(), format}
    else
      _no_clause -> {statement, nil}
    end
  end

  defp code?({:code, text}), do: String.trim(text) != ""
  defp code?(_literal_or_comment), do: false

  @settings_keyword ~r/(?<![\w.])SETTINGS(?=\s)/i

  @doc """
  Splits the `SETTINGS name = value, ...` clauses off a statement (T-481).

  Answers the statement without them and the settings of the trailing
  clause, the one that applies to the whole statement. A clause that ends a
  subquery — `(SELECT ... SETTINGS x = 1)`, as HyperDX writes inside a CTE —
  is dropped and its settings with it: no setting this server reads is
  scoped to a subquery.

  `SETTINGS` is a clause only where a list of `name = value` follows it to
  the end of the statement or to a closing parenthesis, and only in the
  statement's code. A column or a table named `settings`, `system.settings`,
  and the word inside a string literal or a comment are not clauses.
  """
  @spec split_settings(String.t()) :: {String.t(), %{optional(String.t()) => String.t()}}
  def split_settings(query) when is_binary(query) do
    query
    |> settings_offsets()
    |> Enum.reduce({query, %{}}, fn {start, stop}, {statement, trailing} ->
      head = binary_part(statement, 0, start)
      tail = binary_part(statement, stop, byte_size(statement) - stop)

      case setting(tail, %{}) do
        {:ok, settings, rest} ->
          without_clause(head, String.trim_leading(rest), settings, statement, trailing)

        {:error, _message} ->
          {statement, trailing}
      end
    end)
  end

  defp without_clause(head, "", settings, _statement, _trailing),
    do: {String.trim_trailing(head), settings}

  defp without_clause(head, ";", settings, _statement, _trailing),
    do: {String.trim_trailing(head), settings}

  defp without_clause(head, ")" <> _more = rest, _settings, _statement, trailing),
    do: {head <> rest, trailing}

  defp without_clause(_head, _rest, _settings, statement, trailing), do: {statement, trailing}

  defp settings_offsets(query) do
    query
    |> Sql.tokens(dialect: :clickhouse)
    |> Enum.flat_map_reduce(0, fn {kind, text}, offset ->
      {keyword_offsets(kind, text, offset), offset + byte_size(text)}
    end)
    |> elem(0)
    |> Enum.reverse()
  end

  defp keyword_offsets(:code, text, offset) do
    for [{start, size}] <- Regex.scan(@settings_keyword, text, return: :index),
        do: {offset + start, offset + start + size}
  end

  defp keyword_offsets(_kind, _text, _offset), do: []

  @doc """
  `query` with its quoting written the way the engine reads it (T-481).

  ClickHouse quotes an identifier with backticks or double quotes and
  escapes with a backslash inside either, and inside a string literal:
  `'it\\'s'`, `'a\\tb'`. The engine takes double quotes and doubled quotes
  only, and a backslash in its strings is a character. So a backquoted
  identifier becomes a double-quoted one, and a literal or an identifier
  holding a backslash is rewritten with the characters its escapes stand
  for (`unescape/1`). Anything else is left byte for byte.
  """
  @spec standard_quoting(String.t()) :: String.t()
  def standard_quoting(query) when is_binary(query) do
    query
    |> Sql.tokens(dialect: :clickhouse)
    |> Enum.map_join(fn
      {:backquoted, text} -> requote(text, ?`, &Identifier.quote_label/1)
      {:quoted, text} -> escaped(text, ?", &Identifier.quote_label/1)
      {:string, text} -> escaped(text, ?', &Identifier.sql_string/1)
      {_kind, text} -> text
    end)
  end

  defp escaped(text, mark, write) do
    if String.contains?(text, "\\"), do: requote(text, mark, write), else: text
  end

  defp requote(text, mark, write) do
    size = byte_size(text) - 2

    case text do
      <<^mark, inner::binary-size(^size), ^mark>> when size >= 0 ->
        inner |> unescape(<<>>, mark) |> write.()

      _unclosed ->
        text
    end
  end

  @doc """
  `text` with ClickHouse's backslash escapes replaced by what they stand
  for: `\\b`, `\\f`, `\\r`, `\\n`, `\\t`, `\\0`, `\\a`, `\\v`, `\\xHH`, and a
  backslash before a quote or another backslash. Any other escape keeps its
  backslash, as ClickHouse keeps it — `\\_` and `\\%` reach `LIKE` whole.
  """
  @spec unescape(String.t()) :: String.t()
  def unescape(text) when is_binary(text), do: unescape(text, <<>>, nil)

  @escapes %{?b => ?\b, ?f => ?\f, ?r => ?\r, ?n => ?\n, ?t => ?\t, ?0 => 0, ?a => ?\a, ?v => ?\v}
  @literal_escapes [?\\, ?', ?", ?`]

  defp unescape(<<>>, acc, _mark), do: acc

  defp unescape(<<?\\, ?x, high, low, rest::binary>>, acc, mark) do
    case Integer.parse(<<high, low>>, 16) do
      {byte, ""} -> unescape(rest, <<acc::binary, byte>>, mark)
      _not_hex -> unescape(<<high, low, rest::binary>>, <<acc::binary, ?\\, ?x>>, mark)
    end
  end

  defp unescape(<<?\\, char, rest::binary>>, acc, mark) when is_map_key(@escapes, char),
    do: unescape(rest, <<acc::binary, Map.fetch!(@escapes, char)>>, mark)

  defp unescape(<<?\\, char, rest::binary>>, acc, mark) when char in @literal_escapes,
    do: unescape(rest, <<acc::binary, char>>, mark)

  defp unescape(<<mark, mark, rest::binary>>, acc, mark),
    do: unescape(rest, <<acc::binary, mark>>, mark)

  defp unescape(<<char, rest::binary>>, acc, mark),
    do: unescape(rest, <<acc::binary, char>>, mark)

  defp keyword(text, word) do
    trimmed = String.trim_leading(text)
    size = byte_size(word)

    case trimmed do
      <<candidate::binary-size(^size), rest::binary>> ->
        if String.upcase(candidate) == word and boundary?(rest),
          do: {:ok, rest},
          else: expected(word, trimmed)

      _short ->
        expected(word, trimmed)
    end
  end

  defp optional_keyword(text, word) do
    case keyword(text, word) do
      {:ok, rest} -> rest
      {:error, _message} -> text
    end
  end

  defp boundary?(<<?_, _rest::binary>>), do: false

  defp boundary?(<<char, _rest::binary>>)
       when char in ?a..?z or char in ?A..?Z or char in ?0..?9,
       do: false

  defp boundary?(_rest), do: true

  defp identifier(text) do
    case String.trim_leading(text) do
      <<?`, rest::binary>> -> quoted(rest, ?`, <<>>)
      <<?", rest::binary>> -> quoted(rest, ?", <<>>)
      trimmed -> bare(trimmed, <<>>)
    end
  end

  defp quoted(<<?\\, char, rest::binary>>, mark, acc),
    do: quoted(rest, mark, <<acc::binary, char>>)

  defp quoted(<<mark, mark, rest::binary>>, mark, acc),
    do: quoted(rest, mark, <<acc::binary, mark>>)

  defp quoted(<<mark, rest::binary>>, mark, acc), do: {:ok, acc, rest}
  defp quoted(<<char, rest::binary>>, mark, acc), do: quoted(rest, mark, <<acc::binary, char>>)
  defp quoted(<<>>, _mark, _acc), do: {:error, "a quoted name or value is never closed"}

  defp bare(<<?_, rest::binary>>, acc), do: bare(rest, <<acc::binary, ?_>>)

  defp bare(<<char, rest::binary>>, acc)
       when char in ?a..?z or char in ?A..?Z or char in ?0..?9,
       do: bare(rest, <<acc::binary, char>>)

  defp bare(rest, <<>>), do: {:error, "expected a name at #{excerpt(rest)}"}
  defp bare(rest, acc), do: {:ok, acc, rest}

  defp qualified(first, <<?., rest::binary>>) do
    with {:ok, table, remaining} <- identifier(rest), do: {:ok, first, table, remaining}
  end

  defp qualified(first, rest), do: {:ok, nil, first, rest}

  defp column_list(text) do
    case String.trim_leading(text) do
      "(" <> rest -> columns(rest, [])
      trimmed -> {:ok, nil, trimmed}
    end
  end

  defp columns(text, acc) do
    with {:ok, name, rest} <- identifier(text) do
      case String.trim_leading(rest) do
        "," <> remaining -> columns(remaining, [name | acc])
        ")" <> remaining -> {:ok, Enum.reverse([name | acc]), remaining}
        other -> {:error, "expected , or ) in the column list at #{excerpt(other)}"}
      end
    end
  end

  defp settings(text) do
    case keyword(text, "SETTINGS") do
      {:ok, rest} -> setting(rest, %{})
      {:error, _message} -> {:ok, %{}, text}
    end
  end

  defp setting(text, acc) do
    with {:ok, name, rest} <- identifier(text),
         {:ok, rest} <- equals(rest),
         {:ok, value, rest} <- setting_value(String.trim_leading(rest)) do
      case String.trim_leading(rest) do
        "," <> remaining -> setting(remaining, Map.put(acc, name, value))
        remaining -> {:ok, Map.put(acc, name, value), remaining}
      end
    end
  end

  defp equals(text) do
    case String.trim_leading(text) do
      "=" <> rest -> {:ok, rest}
      other -> expected("=", other)
    end
  end

  defp setting_value(<<?', rest::binary>>), do: quoted(rest, ?', <<>>)

  defp setting_value(text) do
    case Regex.run(~r/\A[A-Za-z0-9_.+-]+/, text) do
      [value] ->
        {:ok, value, binary_part(text, byte_size(value), byte_size(text) - byte_size(value))}

      nil ->
        {:error, "expected a setting value at #{excerpt(text)}"}
    end
  end

  defp finished(rest) do
    case String.trim(rest) do
      "" -> :ok
      ";" -> :ok
      other -> {:error, "unexpected text after the format name: #{excerpt(other)}"}
    end
  end

  defp expected(word, text), do: {:error, "expected #{word} at #{excerpt(text)}"}

  defp excerpt(""), do: "the end of the query"
  defp excerpt(text), do: inspect(String.slice(text, 0, 40))
end
