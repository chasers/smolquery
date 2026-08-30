defmodule SmolqueryPg.Params do
  @moduledoc """
  Bind parameters, from the wire to the SQL the query service runs (PL-58).

  A `Bind` value decodes by its declared OID and format into the term
  `Smolquery.QueryService.Client` binds as an engine parameter (T-410):
  the SQL keeps its `$n` and the engine prepares it, so no client value is
  ever SQL text. Only `Describe` before `Bind` still rewrites the SQL — to
  typed `NULL`s, so the engine can name the columns of a statement whose
  values do not exist yet. A `NULL` binds typed by its declared OID, so
  `1 + $1` stays arithmetic.

  ## Parameter types

  `Parse` may declare each parameter's OID, and usually declares `0`:
  unknown, for the server to infer. This edge infers nothing from the
  query, but reads the one hint clients write themselves — `$1::bigint` —
  and otherwise calls the parameter `text`, which the engine casts where a
  comparison needs it.
  """

  alias SmolqueryPg.Sql
  alias SmolqueryPg.Types

  @epoch ~N[1970-01-01 00:00:00]
  @timestamp_infinity NaiveDateTime.add(@epoch, 9_223_372_036_854_775_807, :microsecond)
  @timestamp_neg_infinity NaiveDateTime.add(@epoch, -9_223_372_036_854_775_807, :microsecond)

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
  The bind values `values` decode to, as engine parameters in `$n` order.

  Each wire value decodes by its OID and format (`SmolqueryPg.Types`) into
  the term `Smolquery.QueryService.Client` binds natively: an integer, a
  float, a boolean, a string, a `Decimal`, a `Date`, a `NaiveDateTime`, or
  an `Adbc.Column` for a blob or for a `NULL` typed by its OID. No value
  ever becomes SQL text. A value the edge cannot decode is an error naming
  the parameter.
  """
  @spec values([value()]) :: {:ok, [term()]} | {:error, term()}
  def values(values) do
    values
    |> Enum.with_index(1)
    |> Enum.reduce_while({:ok, []}, fn {{oid, format, bytes}, index}, {:ok, acc} ->
      case Types.decode_param(oid, format, bytes) do
        {:ok, term} -> {:cont, {:ok, [bindable(term, oid) | acc]}}
        {:error, reason} -> {:halt, {:error, {:invalid_parameter, index, reason}}}
      end
    end)
    |> case do
      {:ok, acc} -> {:ok, Enum.reverse(acc)}
      error -> error
    end
  end

  defp bindable(nil, oid), do: null(oid)
  defp bindable({:text, text}, _oid), do: text
  defp bindable({:json, text}, _oid), do: text
  defp bindable({:numeric, text}, _oid), do: Decimal.new(text)
  defp bindable({:timestamp, %NaiveDateTime{} = value}, _oid), do: value
  defp bindable({:timestamp, :infinity}, _oid), do: @timestamp_infinity
  defp bindable({:timestamp, :neg_infinity}, _oid), do: @timestamp_neg_infinity
  defp bindable({:date, %Date{} = value}, _oid), do: value
  defp bindable({:bytea, bytes}, _oid), do: Adbc.Column.binary([bytes])
  defp bindable(term, _oid), do: term

  @dialyzer {:nowarn_function, null: 1}
  defp null(oid) do
    case Types.duckdb_type_for_oid(oid) do
      "BIGINT" -> Adbc.Column.s64([nil])
      "DOUBLE" -> Adbc.Column.f64([nil])
      "DECIMAL" <> _precision -> Adbc.Column.f64([nil])
      "BOOLEAN" -> Adbc.Column.boolean([nil])
      "DATE" -> Adbc.Column.date32([nil])
      "TIMESTAMP" <> _zone -> Adbc.Column.timestamp([nil], :microseconds, "")
      _text_or_blob -> nil
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
end
