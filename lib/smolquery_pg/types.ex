defmodule SmolqueryPg.Types do
  @moduledoc """
  How a result frame's columns and values cross the Postgres wire (PL-58).

  A `RowDescription` names each column's Postgres type by OID, and a
  `DataRow` carries each value in that type's text form. Both derive from
  the frame's Explorer dtype — the only type information a
  `Smolquery.QueryService` result carries.

  ## The mapping

  | smolquery | Explorer dtype | Postgres | OID |
  |---|---|---|---|
  | `INT64` | `{:s, 64}` | `bigint` | 20 |
  | `FLOAT64` | `{:f, 64}` | `double precision` | 701 |
  | `STRING` | `:string` | `text` | 25 |
  | `BOOL` | `:boolean` | `boolean` | 16 |
  | `TIMESTAMP` | `{:naive_datetime, _}` | `timestamp` | 1114 |
  | `DATE` | `:date` | `date` | 1082 |
  | `NUMERIC(p,s)` | `{:decimal, p, s}` | `numeric(p,s)` | 1700 |
  | `MAP(STRING, STRING)` | list of key/value structs | `jsonb` | 3802 |
  | `VARIANT` | JSON text (`job.json_columns`) | `jsonb` | 3802 |

  Smaller integers arrive as `bigint` — their values are the same terms.
  A `{:datetime, _, _}` answers `timestamptz`, a `{:time, _}` answers
  `time`, a `:binary` answers `bytea`. A list or struct has no Postgres
  scalar; it answers `jsonb`, and a value that JSON cannot carry answers its
  inspected form as `text`.

  ## Text forms

  The text is what Postgres's own input functions read, so a driver or a
  `postgres_fdw` casts it without help: `t`/`f` for a boolean, ISO 8601
  with a space for a timestamp, a plain decimal string for a numeric,
  `NaN`/`Infinity` for a non-finite double.
  """

  import Bitwise

  @type dtype :: term()

  @map_dtype {:list, {:struct, [{"key", :string}, {"value", :string}]}}

  @doc """
  The Postgres type of an Explorer `dtype`: `{oid, typlen, typmod}`.

  A `json?` column carries JSON text whatever its dtype says.
  """
  @spec describe(dtype(), boolean()) :: {pos_integer(), integer(), integer()}
  def describe(_dtype, true), do: {3802, -1, -1}
  def describe(dtype, false), do: describe(dtype)

  defp describe(:boolean), do: {16, 1, -1}
  defp describe({:s, _bits}), do: {20, 8, -1}
  defp describe({:u, _bits}), do: {20, 8, -1}
  defp describe({:f, _bits}), do: {701, 8, -1}
  defp describe(:string), do: {25, -1, -1}
  defp describe(:category), do: {25, -1, -1}
  defp describe(:binary), do: {17, -1, -1}
  defp describe(:date), do: {1082, 4, -1}
  defp describe({:time, _precision}), do: {1083, 8, -1}
  defp describe({:naive_datetime, _precision}), do: {1114, 8, -1}
  defp describe({:datetime, _precision, _zone}), do: {1184, 8, -1}
  defp describe({:decimal, precision, scale}), do: {1700, -1, numeric_typmod(precision, scale)}
  defp describe(@map_dtype), do: {3802, -1, -1}
  defp describe({:list, _inner}), do: {3802, -1, -1}
  defp describe({:struct, _fields}), do: {3802, -1, -1}
  defp describe(_other), do: {25, -1, -1}

  defp numeric_typmod(precision, scale) when is_integer(precision) and is_integer(scale),
    do: (precision <<< 16 ||| scale) + 4

  defp numeric_typmod(_precision, _scale), do: -1

  @doc """
  The text form of `value`, a term `Smolquery.Engine.Frame.to_rows/2`
  answered for a column of `dtype`. `nil` stays `nil`: the row encoder
  writes SQL NULL for it.
  """
  @spec encode_text(dtype(), boolean(), term()) :: iodata() | nil
  def encode_text(_dtype, _json?, nil), do: nil
  def encode_text(_dtype, true, value), do: JSON.encode!(value)
  def encode_text(:boolean, false, true), do: "t"
  def encode_text(:boolean, false, false), do: "f"
  def encode_text({:f, _bits}, false, value), do: float(value)
  def encode_text(:binary, false, value), do: ["\\x", Base.encode16(value, case: :lower)]

  def encode_text({:naive_datetime, _precision}, false, %NaiveDateTime{} = value),
    do: NaiveDateTime.to_string(value)

  def encode_text({:datetime, _precision, _zone}, false, %DateTime{} = value),
    do: [DateTime.to_naive(value) |> NaiveDateTime.to_string(), "+00"]

  def encode_text(:date, false, %Date{} = value), do: Date.to_string(value)
  def encode_text({:time, _precision}, false, %Time{} = value), do: Time.to_string(value)

  def encode_text({:decimal, _precision, _scale}, false, %Decimal{} = value),
    do: Decimal.to_string(value, :normal)

  def encode_text(@map_dtype, false, value) when is_map(value), do: JSON.encode!(value)
  def encode_text({:list, _inner}, false, value), do: json_or_inspect(value)
  def encode_text({:struct, _fields}, false, value), do: json_or_inspect(value)
  def encode_text(_dtype, false, value) when is_binary(value), do: value
  def encode_text(_dtype, false, value) when is_integer(value), do: Integer.to_string(value)
  def encode_text(_dtype, false, value) when is_float(value), do: float(value)
  def encode_text(_dtype, false, %Decimal{} = value), do: Decimal.to_string(value, :normal)
  def encode_text(_dtype, false, value), do: inspect(value)

  defp float(:nan), do: "NaN"
  defp float(:infinity), do: "Infinity"
  defp float(:neg_infinity), do: "-Infinity"
  defp float(value) when is_float(value), do: Float.to_string(value)
  defp float(value) when is_integer(value), do: Integer.to_string(value)
  defp float(value), do: inspect(value)

  defp json_or_inspect(value) do
    JSON.encode!(value)
  rescue
    Protocol.UndefinedError -> inspect(value)
  end
end
