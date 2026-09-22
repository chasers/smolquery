defmodule SmolqueryVictoriaMetrics.MetricsQL.Literal do
  @moduledoc """
  The values of MetricsQL's literal tokens, and their spellings back (PL-70,
  T-563): numbers, strings and escaped identifiers, each read as
  VictoriaMetrics' `metricsql` v0.87.4 reads it, which is mostly as Go's
  `strconv` does.

  ## Numbers

  A decimal (`1`, `1.`, `.5`, `1_000`, `12.e+4`) is a double, and may end in a
  multiplier: `k`, `m`, `g`, `t` for powers of 1000 and `ki`/`kib`, `mi`/`mib`,
  `gi`/`gib`, `ti`/`tib` for powers of 1024, `kb`, `mb`, `gb`, `tb` for 1000
  again, all case-insensitive. An integer with a base prefix, `0x1f`, `0o17`,
  `0b101` or a leading zero (`017`, octal), is read as Go's
  `strconv.ParseInt(s, 0, 64)` reads it and takes no multiplier. `inf` and
  `nan`, in any case, are the two specials. A value past the largest double is
  `:inf`, as it is in Go.

  ## Strings

  A double-quoted string takes Go's escapes: `\\a \\b \\f \\n \\r \\t \\v \\\\ \\"`,
  `\\xhh` and three-digit octal for a byte, `\\uhhhh` and `\\Uhhhhhhhh` for a
  character. A single-quoted string takes the same, with `\\'` for a quote
  and `"` needing no escape. A backquoted string is raw: no escapes, and
  carriage returns dropped. A string's value is bytes, not necessarily UTF-8:
  `"\\xff"` is one byte.

  ## Identifiers

  An identifier may escape any character with a backslash, `\\xhh` or
  `\\uhhhh`. Printing escapes what may not stand bare: `\\3foo`, `foo\\ bar`,
  `a\\-b`. A quoted label name (`{"a b"="c"}`) is unescaped the same way,
  inside its quotes.
  """

  import Bitwise

  @max_double 1.797_693_134_862_315_7e308
  @max_int64 9_223_372_036_854_775_807

  @multipliers [
    {"kib", 1024},
    {"ki", 1024},
    {"kb", 1000},
    {"k", 1000},
    {"mib", 1024 ** 2},
    {"mi", 1024 ** 2},
    {"mb", 1000 ** 2},
    {"m", 1000 ** 2},
    {"gib", 1024 ** 3},
    {"gi", 1024 ** 3},
    {"gb", 1000 ** 3},
    {"g", 1000 ** 3},
    {"tib", 1024 ** 4},
    {"ti", 1024 ** 4},
    {"tb", 1000 ** 4},
    {"t", 1000 ** 4}
  ]

  @decimal ~r/\A(?<int>[0-9_]*)(?:\.(?<frac>[0-9_]*))?(?:[eE](?<exp>[+-]?[0-9]+))?\z/
  @misplaced_underscore ~r/(?:\A|[^0-9])_|_(?:\z|[^0-9])/
  @printable ~r/\A[\p{L}\p{M}\p{N}\p{P}\p{S} ]\z/u
  @first_ident ~r/\A[\p{L}_:]\z/u
  @ident_char ~r/\A[\p{L}_:0-9.]\z/u

  @doc """
  The value of a number token, or of `inf`/`nan`.
  """
  @spec number(String.t()) ::
          {:ok, float() | :inf | :nan} | {:error, String.t()}
  def number(text) do
    case String.downcase(text) do
      "inf" -> {:ok, :inf}
      "nan" -> {:ok, :nan}
      lower -> positive(lower, text)
    end
  end

  defp positive(<<?0, x, _rest::binary>> = lower, text) when x in [?x, ?o, ?b],
    do: integer(lower, text)

  defp positive(<<?0, digit, _rest::binary>> = lower, text) when digit in ?0..?9,
    do: integer(lower, text)

  defp positive(lower, text) do
    {mantissa, scale} = strip_multiplier(lower)

    with {:ok, value} <- decimal(mantissa, text), do: {:ok, scale_value(value, scale)}
  end

  defp strip_multiplier(lower) do
    Enum.find_value(@multipliers, {lower, 1}, fn {suffix, scale} ->
      if String.ends_with?(lower, suffix),
        do: {binary_part(lower, 0, byte_size(lower) - byte_size(suffix)), scale}
    end)
  end

  defp scale_value(:inf, _scale), do: :inf
  defp scale_value(value, scale) when value > @max_double / scale, do: :inf
  defp scale_value(value, scale), do: value * scale

  @doc """
  A decimal float as Go's `strconv.ParseFloat` reads one, without the
  specials and without hexadecimal: `1`, `1.`, `.5`, `1_000.5`, `2e-3`.
  `:inf` past the largest double.
  """
  @spec decimal(String.t(), String.t() | nil) :: {:ok, float() | :inf} | {:error, String.t()}
  def decimal(mantissa, text \\ nil) do
    with %{"int" => int, "frac" => frac, "exp" => exp} <- named(@decimal, mantissa),
         true <- digits?(int <> frac),
         false <- Regex.match?(@misplaced_underscore, mantissa) do
      to_float(strip(int), strip(frac), exp)
    else
      _invalid -> {:error, "cannot parse number #{inspect(text || mantissa)}"}
    end
  end

  defp named(regex, text), do: Regex.named_captures(regex, text) || :no_match

  defp digits?(text), do: String.match?(text, ~r/[0-9]/)

  defp strip(digits), do: String.replace(digits, "_", "")

  defp to_float(int, frac, exp) do
    normalized = "#{zero(int)}.#{zero(frac)}e#{zero(exp)}"

    case :string.to_float(String.to_charlist(normalized)) do
      {value, []} -> {:ok, value}
      {:error, :no_float} -> {:ok, :inf}
    end
  end

  defp zero(""), do: "0"
  defp zero(digits), do: digits

  defp integer(lower, text) do
    {base, digits} = base(lower)

    with false <- Regex.match?(@misplaced_underscore, digits),
         {value, ""} when value <= @max_int64 <- Integer.parse(strip(digits), base) do
      {:ok, value * 1.0}
    else
      _invalid -> {:error, "cannot parse number #{inspect(text)}"}
    end
  end

  defp base(<<"0x", rest::binary>>), do: {16, prefixed(rest)}
  defp base(<<"0o", rest::binary>>), do: {8, prefixed(rest)}
  defp base(<<"0b", rest::binary>>), do: {2, prefixed(rest)}
  defp base(<<"0", rest::binary>>), do: {8, rest}

  defp prefixed(<<"_", rest::binary>>), do: rest
  defp prefixed(rest), do: rest

  @doc """
  The value of a string token, quotes included in `token`.
  """
  @spec string(String.t()) :: {:ok, binary()} | {:error, String.t()}
  def string(<<?`, _rest::binary>> = token) do
    body = binary_part(token, 1, byte_size(token) - 2)

    if String.contains?(body, "`"),
      do: {:error, "cannot parse string literal #{token}"},
      else: {:ok, String.replace(body, "\r", "")}
  end

  def string(<<?', _rest::binary>> = token) do
    token
    |> binary_part(1, byte_size(token) - 2)
    |> String.replace("\\'", "'")
    |> String.replace(~s("), ~s(\\"))
    |> unescape(<<>>)
    |> with_token(token)
  end

  def string(<<?", _rest::binary>> = token) do
    token |> binary_part(1, byte_size(token) - 2) |> unescape(<<>>) |> with_token(token)
  end

  defp with_token({:ok, value}, _token), do: {:ok, value}
  defp with_token(:error, token), do: {:error, "cannot parse string literal #{token}"}

  defp unescape(<<>>, acc), do: {:ok, acc}
  defp unescape(<<?", _rest::binary>>, _acc), do: :error
  defp unescape(<<?\n, _rest::binary>>, _acc), do: :error
  defp unescape(<<?\\, rest::binary>>, acc), do: escape(rest, acc)
  defp unescape(<<char, rest::binary>>, acc), do: unescape(rest, <<acc::binary, char>>)

  @simple_escapes %{
    ?a => 7,
    ?b => 8,
    ?f => 12,
    ?n => 10,
    ?r => 13,
    ?t => 9,
    ?v => 11,
    ?\\ => ?\\,
    ?" => ?"
  }

  defp escape(<<char, rest::binary>>, acc) when is_map_key(@simple_escapes, char),
    do: unescape(rest, <<acc::binary, Map.fetch!(@simple_escapes, char)>>)

  defp escape(<<?x, hex::binary-size(2), rest::binary>>, acc), do: byte(hex, 16, rest, acc)

  defp escape(<<a, b, c, rest::binary>>, acc) when a in ?0..?7 and b in ?0..?7 and c in ?0..?7,
    do: byte(<<a, b, c>>, 8, rest, acc)

  defp escape(<<?u, hex::binary-size(4), rest::binary>>, acc), do: codepoint(hex, rest, acc)
  defp escape(<<?U, hex::binary-size(8), rest::binary>>, acc), do: codepoint(hex, rest, acc)
  defp escape(_rest, _acc), do: :error

  defp byte(digits, base, rest, acc) do
    case Integer.parse(digits, base) do
      {value, ""} when value < 256 -> unescape(rest, <<acc::binary, value>>)
      _invalid -> :error
    end
  end

  defp codepoint(hex, rest, acc) do
    case Integer.parse(hex, 16) do
      {value, ""} when value < 0xD800 or value in 0xE000..0x10FFFF ->
        unescape(rest, <<acc::binary, value::utf8>>)

      _invalid ->
        :error
    end
  end

  @doc """
  `value` as Go's `strconv.Quote` writes it: double quotes, printable
  characters as they are, `\\n`-style escapes, and `\\x`, `\\u` or `\\U` for
  the rest, including bytes that are not UTF-8.
  """
  @spec quote_string(binary()) :: String.t()
  def quote_string(value), do: ~s("#{quote_chars(value, [])}")

  defp quote_chars(<<>>, acc), do: acc |> Enum.reverse() |> IO.iodata_to_binary()
  defp quote_chars(<<?", rest::binary>>, acc), do: quote_chars(rest, [~S(\") | acc])
  defp quote_chars(<<?\\, rest::binary>>, acc), do: quote_chars(rest, [~S(\\) | acc])

  defp quote_chars(<<char::utf8, rest::binary>>, acc),
    do: quote_chars(rest, [quote_char(char) | acc])

  defp quote_chars(<<byte, rest::binary>>, acc),
    do: quote_chars(rest, [hex_escape(?x, byte, 2) | acc])

  @control_escapes %{
    7 => ~S(\a),
    8 => ~S(\b),
    12 => ~S(\f),
    10 => ~S(\n),
    13 => ~S(\r),
    9 => ~S(\t),
    11 => ~S(\v)
  }

  defp quote_char(char) when is_map_key(@control_escapes, char),
    do: Map.fetch!(@control_escapes, char)

  defp quote_char(char) do
    cond do
      printable?(char) -> <<char::utf8>>
      char < 0x80 -> hex_escape(?x, char, 2)
      char < 0x10000 -> hex_escape(?u, char, 4)
      true -> hex_escape(?U, char, 8)
    end
  end

  defp printable?(char), do: Regex.match?(@printable, <<char::utf8>>)

  defp hex_escape(letter, value, width) do
    digits = value |> Integer.to_string(16) |> String.downcase() |> String.pad_leading(width, "0")
    <<?\\, letter, digits::binary>>
  end

  @doc """
  An identifier token's name: every `\\c`, `\\xhh` and `\\uhhhh` replaced by
  the character it stands for. A backslash that escapes nothing valid is kept.
  """
  @spec unescape_ident(String.t()) :: String.t()
  def unescape_ident(text), do: ident_chars(text, [])

  defp ident_chars(<<>>, acc), do: acc |> Enum.reverse() |> IO.iodata_to_binary()

  defp ident_chars(<<?\\, x, hex::binary-size(2), rest::binary>> = text, acc) when x in [?x, ?X],
    do: hex_char(hex, rest, text, acc)

  defp ident_chars(<<?\\, u, hex::binary-size(4), rest::binary>> = text, acc) when u in [?u, ?U],
    do: hex_char(hex, rest, text, acc)

  defp ident_chars(<<?\\, x, rest::binary>>, acc) when x in [?x, ?X, ?u, ?U],
    do: ident_chars(<<x, rest::binary>>, [?\\ | acc])

  defp ident_chars(<<?\\, char::utf8, rest::binary>> = text, acc) do
    if printable?(char),
      do: ident_chars(rest, [<<char::utf8>> | acc]),
      else: ident_chars(binary_part(text, 1, byte_size(text) - 1), [?\\ | acc])
  end

  defp ident_chars(<<char::utf8, rest::binary>>, acc),
    do: ident_chars(rest, [<<char::utf8>> | acc])

  defp hex_char(hex, rest, <<?\\, tail::binary>>, acc) do
    case Integer.parse(hex, 16) do
      {value, ""} when value < 0xD800 or value in 0xE000..0x10FFFF ->
        ident_chars(rest, [<<value::utf8>> | acc])

      _invalid ->
        ident_chars(tail, [?\\ | acc])
    end
  end

  @doc """
  `name` as an identifier: characters that may not stand in an identifier at
  their place are escaped with a backslash, and unprintable ones as `\\xhh` or
  `\\uhhhh`.
  """
  @spec escape_ident(String.t()) :: String.t()
  def escape_ident(name) do
    name
    |> String.codepoints()
    |> Enum.with_index()
    |> Enum.map_join(fn {char, index} -> ident_char(char, index) end)
  end

  defp ident_char(char, 0),
    do: if(Regex.match?(@first_ident, char), do: char, else: escaped(char))

  defp ident_char(char, _index),
    do: if(Regex.match?(@ident_char, char), do: char, else: escaped(char))

  defp escaped(<<char::utf8>> = text) do
    cond do
      printable?(char) -> "\\" <> text
      char < 256 -> hex_escape(?x, char, 2)
      true -> hex_escape(?u, char &&& 0xFFFF, 4)
    end
  end
end
