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
end
