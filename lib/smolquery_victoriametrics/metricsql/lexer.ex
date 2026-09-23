defmodule SmolqueryVictoriaMetrics.MetricsQL.Lexer do
  @moduledoc """
  Splits a MetricsQL query into tokens, as `lexer.go` of VictoriaMetrics'
  `metricsql` v0.87.4 does (PL-70, T-563).

  A token is `{kind, text, {line, column}}`: `text` is the source spelling,
  untouched, and the position is where it starts, both counted from 1 and the
  column in characters. The list always ends with one `:eof` token, whose
  position is the end of the query, so a parser can name where it ran out.

  | kind         | what                                                          |
  |--------------|---------------------------------------------------------------|
  | `:ident`     | `[letter _ :]` then `[letter digit _ : .]`, with `\\` escapes  |
  | `:string`    | `"..."`, `'...'` or `` `...` `` with its quotes               |
  | `:number`    | `1`, `1.5e3`, `.5`, `0x1f`, `0o17`, `0b101`, `017`, `12Ki`, `3M`|
  | `:duration`  | `5m`, `1h30m`, `1.5d`, `2i`, `3h-5m`, `$__interval`           |
  | `:op`        | `+ - * / % ^ == != > < >= <=`                                 |
  | `:filter_op` | `=`, `=~`, `!~` (a label filter's `!=` is an `:op`)            |
  | `:punct`     | `{ } [ ] ( ) , @`                                             |

  The order the kinds are tried in is VictoriaMetrics', and it decides what a
  run of characters is: `5m` is a duration of five minutes and `5M` the number
  five million, `45mi` is 45 MiB, and `1h-5m` is one duration, fifty-five
  minutes, while `1h - 5m` is a subtraction. Words (`and`, `offset`, `by`,
  `inf`) are identifiers; what one means is the parser's business, because
  every one of them is also a valid metric name. `:` starts an identifier, so
  the step of `m[5m:1m]` arrives as the identifier `:1m`.

  `$__rate_interval` becomes `$__interval`, as Grafana's two variables do in
  VictoriaMetrics. `#` starts a comment that runs to the end of the line.

  The query is checked to be UTF-8 once, and every token is then read by
  matching its own bytes, so tokenizing costs the length of the query, not
  its length times the number of tokens.
  """

  @type position :: {pos_integer(), pos_integer()}
  @type kind :: :ident | :string | :number | :duration | :op | :filter_op | :punct | :eof
  @type token :: {kind(), String.t(), position()}

  @letter ~r/\A\p{L}\z/u
  @unescapable ~r/\A[\p{C}\p{Z}]\z/u
  @duration_part ~S"\d+(?:\.\d*)?(?:[mM][sS]|m(?![iIbB])|[sShHdDwWyYiI])"
  @duration Regex.compile!("\\A#{@duration_part}(?:-?#{@duration_part})*")
  @space ~c" \t\n\v\f\r"
  @digits_or_dot ~c"0123456789."
  @ops ["==", "!=", ">=", "<=", "+", "-", "*", "/", "%", "^", ">", "<"]
  @filter_ops ["=~", "!~", "="]
  @punct ~c"{}[](),@"
  @intervals [{"$__rate_interval", "$__interval"}, {"$__interval", "$__interval"}]

  @doc """
  Tokenizes `query`.

  `{:error, {:syntax, message}}` names the text that is not a token, a string
  that never closes, or a number with no exponent after its `e`.
  """
  @spec tokenize(String.t()) :: {:ok, [token()]} | {:error, {:syntax, String.t()}}
  def tokenize(query) when is_binary(query) do
    if String.valid?(query) do
      scan(query, {1, 1}, [])
    else
      {:error, {:syntax, "the query is not valid UTF-8"}}
    end
  end

  @doc """
  Whether `text` is an identifier from its first character, as VictoriaMetrics'
  `isIdentPrefix` decides it.
  """
  @spec ident_prefix?(String.t()) :: boolean()
  def ident_prefix?(text), do: ident_length(text, 0, :first) > 0

  @doc """
  Whether all of `text` is one duration: `5m`, `1h30m`, `3.5d-10s`, `2i`.
  """
  @spec duration?(String.t()) :: boolean()
  def duration?("$__interval"), do: true
  def duration?(text), do: match?([^text], Regex.run(@duration, text))

  @doc """
  Whether `text` begins as a positive number does: a digit, or `.` and a digit.
  """
  @spec number_prefix?(String.t()) :: boolean()
  def number_prefix?(<<digit, _rest::binary>>) when digit in ?0..?9, do: true
  def number_prefix?(<<?., digit, _rest::binary>>) when digit in ?0..?9, do: true
  def number_prefix?(_text), do: false

  @doc """
  The `{:error, {:syntax, message}}` for meeting `token` where `want` was
  expected, worded as VictoriaMetrics words it.
  """
  @spec unexpected(token(), String.t()) :: {:error, {:syntax, String.t()}}
  def unexpected({:eof, _text, position}, want),
    do: {:error, {:syntax, "unexpected end of query at #{at(position)}; want #{want}"}}

  def unexpected({_kind, text, position}, want),
    do:
      {:error, {:syntax, "unexpected token #{quote_text(text)} at #{at(position)}; want #{want}"}}

  @doc """
  A `{:error, {:syntax, message}}` that places `message` at `token`.
  """
  @spec error_at(token(), String.t()) :: {:error, {:syntax, String.t()}}
  def error_at({_kind, _text, position}, message),
    do: {:error, {:syntax, "#{message} at #{at(position)}"}}

  defp at({line, column}), do: "#{line}:#{column}"

  defp quote_text(text), do: ~s("#{String.replace(text, ~s("), ~s(\\"))}")

  defp scan("", position, acc), do: {:ok, Enum.reverse([{:eof, "", position} | acc])}

  defp scan(<<"#", _rest::binary>> = query, position, acc) do
    case :binary.split(query, "\n") do
      [comment, rest] -> scan(rest, advance(position, comment <> "\n"), acc)
      [comment] -> scan("", advance(position, comment), acc)
    end
  end

  defp scan(<<char, _rest::binary>> = query, position, acc) when char in @space do
    length = space_length(query, 0)
    scan(rest_at(query, length), advance(position, binary_part(query, 0, length)), acc)
  end

  defp scan(query, position, acc), do: token(query, position, acc)

  defp space_length(<<char, rest::binary>>, length) when char in @space,
    do: space_length(rest, length + 1)

  defp space_length(_query, length), do: length

  defp token(query, position, acc) do
    case next(query) do
      {:ok, kind, text, consumed} ->
        rest = binary_part(query, byte_size(consumed), byte_size(query) - byte_size(consumed))
        scan(rest, advance(position, consumed), [{kind, text, position} | acc])

      {:error, message} ->
        {:error, {:syntax, "#{message} at #{at(position)}"}}
    end
  end

  defp next(<<char, _rest::binary>>) when char in @punct,
    do: {:ok, :punct, <<char>>, <<char>>}

  defp next(query) do
    with :none <- ident(query),
         :none <- string(query),
         :none <- prefix(query, @ops, :op),
         :none <- prefix(query, @filter_ops, :filter_op),
         :none <- duration(query),
         :none <- number(query),
         :none <- interval(query) do
      {:error, "cannot recognize #{quote_text(String.slice(query, 0, 32))}"}
    end
  end

  defp ident(query) do
    case ident_length(query, 0, :first) do
      0 ->
        :none

      length ->
        text = binary_part(query, 0, length)
        {:ok, :ident, text, text}
    end
  end

  defp ident_length(<<char, rest::binary>>, length, _place)
       when char in ?a..?z or char in ?A..?Z or char in [?_, ?:],
       do: ident_length(rest, length + 1, :rest)

  defp ident_length(<<char, rest::binary>>, length, :rest) when char in @digits_or_dot,
    do: ident_length(rest, length + 1, :rest)

  defp ident_length(<<?\\, rest::binary>>, length, _place) do
    case escape_length(rest) do
      0 -> length
      escape -> ident_length(rest_at(rest, escape), length + 1 + escape, :rest)
    end
  end

  defp ident_length(<<char::utf8, rest::binary>>, length, _place) when char >= 0x80 do
    if Regex.match?(@letter, <<char::utf8>>),
      do: ident_length(rest, length + byte_size(<<char::utf8>>), :rest),
      else: length
  end

  defp ident_length(_query, length, _place), do: length

  defp escape_length(<<x, a, b, _rest::binary>>) when x in [?x, ?X] do
    if hex?(a) and hex?(b), do: 3, else: 0
  end

  defp escape_length(<<u, a, b, c, d, _rest::binary>>) when u in [?u, ?U] do
    if Enum.all?([a, b, c, d], &hex?/1), do: 5, else: 0
  end

  defp escape_length(<<letter, _rest::binary>>) when letter in [?x, ?X, ?u, ?U], do: 0
  defp escape_length(<<?\s, _rest::binary>>), do: 1

  defp escape_length(<<char::utf8, _rest::binary>>) do
    if Regex.match?(@unescapable, <<char::utf8>>), do: 0, else: byte_size(<<char::utf8>>)
  end

  defp escape_length(<<>>), do: 0

  defp hex?(char), do: char in ?0..?9 or char in ?a..?f or char in ?A..?F

  defp string(<<mark, rest::binary>>) when mark in [?", ?', ?`] do
    case closing(rest, mark, 0) do
      {:ok, length} ->
        text = <<mark, binary_part(rest, 0, length + 1)::binary>>
        {:ok, :string, text, text}

      :error ->
        {:error, "cannot find the closing quote #{<<mark>>} of the string"}
    end
  end

  defp string(_query), do: :none

  defp closing(rest, mark, from) when from < byte_size(rest) do
    case :binary.match(rest, [<<mark>>, "\\"], scope: {from, byte_size(rest) - from}) do
      {index, 1} when binary_part(rest, index, 1) == "\\" -> closing(rest, mark, index + 2)
      {index, 1} -> {:ok, index}
      :nomatch -> :error
    end
  end

  defp closing(_rest, _mark, _from), do: :error

  defp prefix(query, candidates, kind) do
    case Enum.find(candidates, &String.starts_with?(query, &1)) do
      nil -> :none
      text -> {:ok, kind, text, text}
    end
  end

  defp duration(query) do
    case Regex.run(@duration, query) do
      [text] -> {:ok, :duration, text, text}
      nil -> :none
    end
  end

  defp interval(query) do
    Enum.find_value(@intervals, :none, fn {spelling, text} ->
      if String.starts_with?(query, spelling), do: {:ok, :duration, text, spelling}
    end)
  end

  defp number(query) do
    if number_prefix?(query), do: scan_number(query), else: :none
  end

  defp scan_number(query) do
    case special_prefix(query) do
      {:hex, length} -> found(query, length + hex_digits(query, length))
      {:other, length} -> mantissa(query, length + decimals(query, length))
    end
  end

  defp special_prefix(<<?0, x, _rest::binary>>) when x in [?x, ?X], do: {:hex, 2}
  defp special_prefix(<<?0, o, _rest::binary>>) when o in [?o, ?O, ?b, ?B], do: {:other, 2}
  defp special_prefix(<<?0, digit, _rest::binary>>) when digit in ?0..?9, do: {:other, 1}
  defp special_prefix(_query), do: {:other, 0}

  defp mantissa(query, length) do
    case {rest_at(query, length), multiplier(rest_at(query, length))} do
      {_rest, size} when size > 0 -> found(query, length + size)
      {<<?., _rest::binary>>, 0} -> fraction(query, length + 1 + decimals(query, length + 1))
      {<<e, _rest::binary>>, 0} when e in [?e, ?E] -> exponent(query, length + 1)
      {_rest, 0} -> found(query, length)
    end
  end

  defp fraction(query, length) do
    case {rest_at(query, length), multiplier(rest_at(query, length))} do
      {_rest, size} when size > 0 -> found(query, length + size)
      {<<e, _rest::binary>>, 0} when e in [?e, ?E] -> exponent(query, length + 1)
      {_rest, 0} -> found(query, length)
    end
  end

  defp exponent(query, length) do
    signed =
      if match?(<<s, _::binary>> when s in [?+, ?-], rest_at(query, length)), do: 1, else: 0

    case digits(query, length + signed) do
      0 -> {:error, "missing exponent part in #{quote_text(binary_part(query, 0, length))}"}
      count -> found(query, length + signed + count)
    end
  end

  defp found(query, length) do
    text = binary_part(query, 0, length)
    {:ok, :number, text, text}
  end

  defp rest_at(query, offset), do: binary_part(query, offset, byte_size(query) - offset)

  defp hex_digits(query, from), do: run_length(rest_at(query, from), ~r/\A[0-9a-fA-F]*/)
  defp decimals(query, from), do: run_length(rest_at(query, from), ~r/\A[0-9_]*/)
  defp digits(query, from), do: run_length(rest_at(query, from), ~r/\A[0-9]*/)

  defp run_length(text, regex), do: regex |> Regex.run(text) |> hd() |> byte_size()

  @multipliers ~w(kib ki kb k mib mi mb m gib gi gb g tib ti tb t)

  defp multiplier(rest) do
    head = rest |> binary_part(0, min(3, byte_size(rest))) |> String.downcase(:ascii)

    case Enum.find(@multipliers, &String.starts_with?(head, &1)) do
      nil -> 0
      suffix -> byte_size(suffix)
    end
  end

  defp advance({line, column}, consumed) do
    case :binary.split(consumed, "\n", [:global]) do
      [same_line] -> {line, column + String.length(same_line)}
      lines -> {line + length(lines) - 1, String.length(List.last(lines)) + 1}
    end
  end
end
