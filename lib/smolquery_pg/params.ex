defmodule SmolqueryPg.Params do
  @moduledoc """
  Bind parameters, from the wire to the SQL the query service runs (PL-58).

  `Smolquery.QueryService.Client` takes SQL only. Until it binds parameters
  natively (T-410), each `Bind` value becomes a typed SQL literal in place
  of its `$n`, through `SmolqueryPg.Sql` so a `$1` inside a string or a
  comment stays text. The literal is built from the value's *decoded* term,
  never from client bytes pasted into SQL: a text value goes through
  `Smolquery.Identifier.sql_string/1`, a number is validated as one, a
  timestamp is rendered from the parsed term. That is what keeps the
  substitution from being an injection.

  ## Parameter types

  `Parse` may declare each parameter's OID, and usually declares `0`:
  unknown, for the server to infer. This edge infers nothing from the
  query, but reads the one hint clients write themselves — `$1::bigint` —
  and otherwise calls the parameter `text`, which the engine casts where a
  comparison needs it.
  """

  alias Smolquery.Identifier
  alias SmolqueryPg.Sql
  alias SmolqueryPg.Types

  @type value :: {oid :: non_neg_integer(), format :: 0 | 1, binary() | nil}

  @doc """
  The parameter OIDs of `sql`: `declared` where `Parse` gave one, the cast
  hint beside a `$n` where the client wrote one, and `text` otherwise.
  Answers one OID per `$n` the SQL mentions, at least as many as declared.
  """
  @spec oids(String.t(), [non_neg_integer()]) :: [pos_integer()]
  def oids(sql, declared) do
    {max_placeholder, hints} = Sql.placeholder_info(sql)
    declared_count = length(declared)
    count = max(declared_count, max_placeholder)
    padded = declared ++ List.duplicate(0, count - declared_count)

    padded
    |> Enum.with_index(1)
    |> Enum.map(fn
      {0, n} -> hint_oid(hints, n)
      {oid, _n} -> oid
    end)
  end

  defp hint_oid(hints, n) do
    case Map.fetch(hints, n) do
      {:ok, type} -> Types.oid_for_type_name(type)
      :error -> 25
    end
  end

  @doc """
  `sql` with each `$n` replaced by the literal for `values`' n-th entry.

  A value the edge cannot decode is an error naming the parameter.
  """
  @spec substitute(String.t(), [value()]) :: {:ok, String.t()} | {:error, term()}
  def substitute(sql, values) do
    with {:ok, literals} <- literals(values) do
      lookup = literals |> Enum.with_index(1) |> Map.new(fn {literal, n} -> {n, literal} end)

      {:ok, Sql.map_placeholders(sql, &Map.get(lookup, &1, "$#{&1}"))}
    end
  end

  @doc """
  `sql` with each `$n` replaced by a `NULL` cast to its declared type, so
  the engine can describe the statement's columns before any value is
  bound.
  """
  @spec with_typed_nulls(String.t(), [pos_integer()]) :: String.t()
  def with_typed_nulls(sql, oids) do
    lookup = oids |> Enum.with_index(1) |> Map.new(fn {oid, n} -> {n, oid} end)

    Sql.map_placeholders(sql, fn n ->
      case Map.fetch(lookup, n) do
        {:ok, oid} -> "NULL::" <> Types.duckdb_type_for_oid(oid)
        :error -> "$#{n}"
      end
    end)
  end

  defp literals(values) do
    values
    |> Enum.with_index(1)
    |> Enum.reduce_while({:ok, []}, fn {{oid, format, bytes}, index}, {:ok, acc} ->
      case Types.decode_param(oid, format, bytes) do
        {:ok, term} -> {:cont, {:ok, [literal(term) | acc]}}
        {:error, reason} -> {:halt, {:error, {:invalid_parameter, index, reason}}}
      end
    end)
    |> case do
      {:ok, acc} -> {:ok, Enum.reverse(acc)}
      error -> error
    end
  end

  @doc """
  The SQL literal for a decoded parameter term.
  """
  @spec literal(Types.param()) :: String.t()
  def literal(nil), do: "NULL"
  def literal(true), do: "TRUE"
  def literal(false), do: "FALSE"
  def literal(int) when is_integer(int), do: Integer.to_string(int)
  def literal(float) when is_float(float), do: Float.to_string(float)
  def literal(:nan), do: "'NaN'::DOUBLE"
  def literal(:infinity), do: "'Infinity'::DOUBLE"
  def literal(:neg_infinity), do: "'-Infinity'::DOUBLE"
  def literal({:text, text}), do: Identifier.sql_string(text)
  def literal({:numeric, text}), do: text
  def literal({:timestamp, %NaiveDateTime{} = value}), do: "TIMESTAMP '#{value}'"
  def literal({:date, %Date{} = value}), do: "DATE '#{value}'"
  def literal({:json, text}), do: Identifier.sql_string(text) <> "::JSON"
  def literal({:bytea, bytes}), do: "'\\x#{Base.encode16(bytes, case: :lower)}'::BLOB"
end
