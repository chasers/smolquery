defmodule Smolquery.Engine.Params do
  @moduledoc """
  Binds query parameters to the Arrow types smolquery's columns actually have.

  ADBC infers a parameter's Arrow type from its Elixir value, and for timestamps
  that inference disagrees with `Smolquery.Schema`: a `NaiveDateTime` is inferred
  as `{:timestamp, :microseconds, "UTC"}`, which DuckDB sees as `TIMESTAMP WITH
  TIME ZONE`, while a `:timestamp` column is a plain `TIMESTAMP`.

  The disagreement is invisible in results and expensive in practice. Comparing
  the two types is correct, so the right rows come back, but DuckDB will not push
  a `TIMESTAMP`-vs-`TIMESTAMPTZ` filter down to DuckLake's file statistics — so
  every time-range query reads every segment. Measured over 300 registered
  segments, a range covering one of them read 300 files in 76.7 ms bound as an
  inferred parameter, against 1 file in 7.8 ms bound as a plain `TIMESTAMP`.
  Since the timestamp is the column a time-series query prunes on, leaving the
  inference alone means no query prunes.

  Timestamps are therefore bound explicitly rather than inferred, once here,
  instead of by a cast every call site has to remember. A `DateTime` is converted
  to the same UTC-relative `TIMESTAMP`; ADBC cannot infer a type for one at all,
  so passing one would otherwise raise rather than degrade.

  Every other value is left to ADBC, whose inference already matches the type
  table: integers bind as `BIGINT`, `Decimal` as `DECIMAL`, `Date` as `DATE`.
  """

  @epoch ~N[1970-01-01 00:00:00]

  @doc """
  Normalizes a positional parameter list for `Adbc.Connection.query/4`.
  """
  @spec normalize([term()]) :: [term()]
  def normalize(params) when is_list(params), do: Enum.map(params, &bind/1)

  defp bind(%NaiveDateTime{} = value), do: timestamp(value)

  defp bind(%DateTime{} = value) do
    @epoch
    |> NaiveDateTime.add(DateTime.to_unix(value, :microsecond), :microsecond)
    |> timestamp()
  end

  defp bind(value), do: value

  defp timestamp(value), do: Adbc.Column.timestamp([value], :microseconds, "")
end
