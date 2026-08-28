defmodule SmolqueryPg.Statements do
  @moduledoc """
  Splits a simple-query string into its statements (PL-58).

  The simple query protocol lets a client send `BEGIN; SELECT 1; COMMIT` as
  one message, and every driver's connection setup does. The split ignores
  a semicolon inside a string, a quoted identifier, a comment, or a
  dollar-quoted body — `SmolqueryPg.Sql` tells those from code.

  An empty statement (`;;`, or trailing whitespace after the last
  semicolon) is dropped: Postgres treats it as nothing, not as an error.
  """

  alias SmolqueryPg.Sql

  @doc """
  The statements of `sql`, in order, each trimmed and without its
  terminating semicolon.
  """
  @spec split(String.t()) :: [String.t()]
  def split(sql) when is_binary(sql) do
    sql
    |> Sql.tokens()
    |> Enum.reduce({[], []}, &absorb/2)
    |> finish()
    |> Enum.map(&String.trim/1)
    |> Enum.reject(&(&1 == ""))
  end

  defp absorb({:code, text}, {current, done}) do
    [first | rest] = String.split(text, ";")

    Enum.reduce(rest, {[first | current], done}, fn piece, {current, done} ->
      {[piece], [text_of(current) | done]}
    end)
  end

  defp absorb({_kind, text}, {current, done}), do: {[text | current], done}

  defp finish({current, done}), do: Enum.reverse([text_of(current) | done])

  defp text_of(current), do: current |> Enum.reverse() |> IO.iodata_to_binary()
end
