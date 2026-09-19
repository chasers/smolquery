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
      statement |> Sql.tokens() |> Enum.reverse() |> Enum.split_while(&(not code?(&1)))

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
