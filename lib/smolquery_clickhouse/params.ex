defmodule SmolqueryClickHouse.Params do
  @moduledoc """
  ClickHouse's query parameters: `{name:Type}` in the statement, filled from
  the request's `param_<name>` values (T-481).

      SELECT Body FROM {db:Identifier}.{t:Identifier}
      WHERE Timestamp >= fromUnixTimestamp64Milli({from:Int64}) LIMIT {n:Int32}

  The engine takes positional parameters only, and an `Identifier` cannot be
  bound at all, so each placeholder is replaced in the statement's text by a
  literal written from its value — a quoted identifier, a string literal, or
  a number checked to be one. Only the statement's code is read
  (`SmolqueryPg.Sql`): a placeholder inside a string literal, a quoted
  identifier or a comment stays as written. One parameter may fill many
  placeholders.

  ## Types

  | type | value | written as |
  |---|---|---|
  | `Identifier` | any text | `"text"`, quotes doubled |
  | `String`, `FixedString(N)`, `UUID`, `Enum8/16(...)`, `IPv4`, `IPv6` | text, in ClickHouse's escaped form (`\\t`, `\\n`, `\\\\`, `\\'`) | `'text'` |
  | `Int8`..`Int64`, `UInt8`..`UInt64` | an integer | the integer |
  | `Float32`, `Float64`, `Decimal(P, S)` | a number | the number |
  | `Bool` | `true`, `false`, `1`, `0` | `TRUE` or `FALSE` |
  | `Date`, `Date32` | a date | `CAST('…' AS DATE)` |
  | `DateTime`, `DateTime64(P)` | a timestamp | `CAST('…' AS TIMESTAMP)` |

  `Nullable(T)` and `LowCardinality(T)` read as `T`, and a `Nullable`
  parameter whose value is `\\N` is `NULL`. Any other type — an `Array`, a
  `Map`, a `Tuple` — is code 36 `BAD_ARGUMENTS`, as is a value its type does
  not take. A placeholder with no `param_<name>` in the request is code 456
  `UNKNOWN_QUERY_PARAMETER`, as ClickHouse answers it.

  DuckDB writes a struct literal the same way, `{a: 1}`. Braces whose second
  half is not a ClickHouse type name — it does not start with a capital — are
  left alone.
  """

  alias Smolquery.Identifier
  alias SmolqueryClickHouse.Errors
  alias SmolqueryClickHouse.Statement
  alias SmolqueryPg.Sql

  @placeholder ~r/\{\s*([A-Za-z_][A-Za-z0-9_]*)\s*:\s*([A-Z][A-Za-z0-9]*(?:\([^{}]*\))?)\s*\}/

  @integers ~w(Int8 Int16 Int32 Int64 UInt8 UInt16 UInt32 UInt64)
  @floats ~w(Float32 Float64)
  @strings ~w(String UUID IPv4 IPv6)
  @dates ~w(Date Date32)

  @doc """
  `statement` with every placeholder in its code filled from `params`, the
  request's URL parameters.
  """
  @spec substitute(String.t(), %{optional(String.t()) => String.t()}) ::
          {:ok, String.t()} | {:error, Errors.t()}
  def substitute(statement, params) when is_binary(statement) and is_map(params) do
    tokens = Sql.tokens(statement, dialect: :clickhouse)

    with {:ok, literals} <- literals(tokens, params) do
      {:ok, Enum.map_join(tokens, &fill(&1, literals))}
    end
  end

  defp literals(tokens, params) do
    placeholders =
      for {:code, code} <- tokens,
          [match, name, type] <- Regex.scan(@placeholder, code),
          uniq: true,
          do: {match, name, type}

    Enum.reduce_while(placeholders, {:ok, %{}}, fn {match, name, type}, {:ok, acc} ->
      case literal(name, type, params) do
        {:ok, text} -> {:cont, {:ok, Map.put(acc, match, text)}}
        {:error, exception} -> {:halt, {:error, exception}}
      end
    end)
  end

  defp fill({:code, code}, literals),
    do:
      Regex.replace(@placeholder, code, fn match, _name, _type -> Map.fetch!(literals, match) end)

  defp fill({_kind, text}, _literals), do: text

  defp literal(name, type, params) do
    case Map.fetch(params, "param_" <> name) do
      {:ok, value} -> write(unwrap(type), value, name)
      :error -> {:error, unknown(name)}
    end
  end

  defp unwrap("Nullable(" <> rest), do: {:nullable, rest |> String.trim_trailing(")") |> unwrap()}
  defp unwrap("LowCardinality(" <> rest), do: rest |> String.trim_trailing(")") |> unwrap()
  defp unwrap(type), do: type

  defp write({:nullable, _type}, "\\N", _name), do: {:ok, "NULL"}
  defp write({:nullable, type}, value, name), do: write(type, value, name)
  defp write("Identifier", value, _name), do: {:ok, Identifier.quote_label(value)}

  defp write(type, value, _name) when type in @strings,
    do: {:ok, Identifier.sql_string(Statement.unescape(value))}

  defp write("FixedString(" <> _size, value, _name),
    do: {:ok, Identifier.sql_string(Statement.unescape(value))}

  defp write("Enum" <> _values, value, _name),
    do: {:ok, Identifier.sql_string(Statement.unescape(value))}

  defp write(type, value, name) when type in @integers do
    case Integer.parse(value) do
      {integer, ""} -> {:ok, Integer.to_string(integer)}
      _invalid -> {:error, bad_value(name, type, value)}
    end
  end

  defp write(type, value, name) when type in @floats, do: number(type, value, name)
  defp write("Decimal" <> _precision = type, value, name), do: number(type, value, name)

  defp write("Bool", value, name) do
    case String.downcase(value) do
      truthy when truthy in ["true", "1"] -> {:ok, "TRUE"}
      falsy when falsy in ["false", "0"] -> {:ok, "FALSE"}
      _invalid -> {:error, bad_value(name, "Bool", value)}
    end
  end

  defp write(type, value, _name) when type in @dates,
    do: {:ok, "CAST(" <> Identifier.sql_string(value) <> " AS DATE)"}

  defp write("DateTime" <> _rest, value, _name),
    do: {:ok, "CAST(" <> Identifier.sql_string(value) <> " AS TIMESTAMP)"}

  defp write(type, _value, name),
    do:
      {:error,
       {400, 36, "BAD_ARGUMENTS",
        "Query parameter #{name} has type #{type}, which is not one this server substitutes", nil}}

  defp number(type, value, name) do
    if Regex.match?(~r/\A[+-]?(\d+\.?\d*|\.\d+)([eE][+-]?\d+)?\z/, value),
      do: {:ok, value},
      else: {:error, bad_value(name, type, value)}
  end

  defp unknown(name),
    do:
      {400, 456, "UNKNOWN_QUERY_PARAMETER",
       "Substitution #{name} is not set; send it as the param_#{name} URL parameter", nil}

  defp bad_value(name, type, value),
    do:
      {400, 36, "BAD_ARGUMENTS",
       "Value #{inspect(value)} cannot be parsed as #{type} for query parameter #{name}", nil}
end
