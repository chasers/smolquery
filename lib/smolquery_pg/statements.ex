defmodule SmolqueryPg.Statements do
  @moduledoc """
  Splits a simple-query string into its statements (PL-58).

  The simple query protocol lets a client send `BEGIN; SELECT 1; COMMIT` as
  one message, and every driver's connection setup does. The split must
  ignore a semicolon inside a string, a quoted identifier, a comment, or a
  dollar-quoted body — the shapes SQL hides one in. Nothing here parses
  SQL; the lexer only tracks which of those it is inside.

  An empty statement (`;;`, or trailing whitespace after the last
  semicolon) is dropped: Postgres treats it as nothing, not as an error.
  """

  @doc """
  The statements of `sql`, in order, each trimmed and without its
  terminating semicolon.
  """
  @spec split(String.t()) :: [String.t()]
  def split(sql) when is_binary(sql) do
    sql
    |> scan([], [])
    |> Enum.map(&String.trim/1)
    |> Enum.reject(&(&1 == ""))
  end

  defp scan(<<>>, current, done), do: Enum.reverse([finish(current) | done])

  defp scan(<<?;, rest::binary>>, current, done), do: scan(rest, [], [finish(current) | done])

  defp scan(<<?', rest::binary>>, current, done),
    do: quoted(rest, ?', [?' | current], done)

  defp scan(<<?", rest::binary>>, current, done),
    do: quoted(rest, ?", [?" | current], done)

  defp scan(<<"--", rest::binary>>, current, done),
    do: line_comment(rest, [?-, ?- | current], done)

  defp scan(<<"/*", rest::binary>>, current, done),
    do: block_comment(rest, [?*, ?/ | current], done, 1)

  defp scan(<<?$, rest::binary>>, current, done) do
    case dollar_tag(rest) do
      {:ok, tag, rest} -> dollar(rest, tag, [tag, ?$ | current], done)
      :error -> scan(rest, [?$ | current], done)
    end
  end

  defp scan(<<char, rest::binary>>, current, done), do: scan(rest, [char | current], done)

  defp quoted(<<>>, _quote, current, done), do: scan(<<>>, current, done)

  defp quoted(<<quote, quote, rest::binary>>, quote, current, done),
    do: quoted(rest, quote, [quote, quote | current], done)

  defp quoted(<<quote, rest::binary>>, quote, current, done),
    do: scan(rest, [quote | current], done)

  defp quoted(<<char, rest::binary>>, quote, current, done),
    do: quoted(rest, quote, [char | current], done)

  defp line_comment(<<>>, current, done), do: scan(<<>>, current, done)
  defp line_comment(<<?\n, rest::binary>>, current, done), do: scan(rest, [?\n | current], done)

  defp line_comment(<<char, rest::binary>>, current, done),
    do: line_comment(rest, [char | current], done)

  defp block_comment(<<>>, current, done, _depth), do: scan(<<>>, current, done)

  defp block_comment(<<"*/", rest::binary>>, current, done, 1),
    do: scan(rest, [?/, ?* | current], done)

  defp block_comment(<<"*/", rest::binary>>, current, done, depth),
    do: block_comment(rest, [?/, ?* | current], done, depth - 1)

  defp block_comment(<<"/*", rest::binary>>, current, done, depth),
    do: block_comment(rest, [?*, ?/ | current], done, depth + 1)

  defp block_comment(<<char, rest::binary>>, current, done, depth),
    do: block_comment(rest, [char | current], done, depth)

  defp dollar_tag(rest) do
    case Regex.run(~r/^([A-Za-z_][A-Za-z0-9_]*)?\$/, rest) do
      [match, tag] ->
        {:ok, "#{tag}$", binary_part(rest, byte_size(match), byte_size(rest) - byte_size(match))}

      [match] ->
        {:ok, "$", binary_part(rest, byte_size(match), byte_size(rest) - byte_size(match))}

      nil ->
        :error
    end
  end

  defp dollar(<<>>, _tag, current, done), do: scan(<<>>, current, done)

  defp dollar(<<?$, rest::binary>> = all, tag, current, done) do
    size = byte_size(tag)

    case rest do
      <<^tag::binary-size(^size), after_tag::binary>> ->
        scan(after_tag, [tag, ?$ | current], done)

      _other ->
        <<char, rest::binary>> = all
        dollar(rest, tag, [char | current], done)
    end
  end

  defp dollar(<<char, rest::binary>>, tag, current, done),
    do: dollar(rest, tag, [char | current], done)

  defp finish(current), do: current |> Enum.reverse() |> IO.iodata_to_binary()
end
