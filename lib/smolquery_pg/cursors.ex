defmodule SmolqueryPg.Cursors do
  @moduledoc """
  The cursor statements `postgres_fdw` scans with (PL-58 layer 4).

  Every foreign scan is `DECLARE c1 CURSOR FOR <select>`, repeated
  `FETCH 100 FROM c1`, and `CLOSE c1`, inside a `REPEATABLE READ`
  transaction block. This module parses the three statements (plus `MOVE`,
  and `CLOSE ALL`); `SmolqueryPg.Session` owns the cursors themselves —
  a cursor is the same held outcome a portal is, paged by offset.

  These parse with regex on purpose: they are cold statements (one
  `DECLARE` per scan), and the shapes carry optional keyword noise
  (`BINARY`, `SCROLL`, `WITH HOLD`) that a hand scanner would restate.
  """

  @declare ~r/\ADECLARE\s+("[^"]+"|[A-Za-z_]\w*)\s+(?:BINARY\s+|INSENSITIVE\s+|ASENSITIVE\s+|NO\s+SCROLL\s+|SCROLL\s+)*CURSOR\s+(?:WITH\s+HOLD\s+|WITHOUT\s+HOLD\s+)?FOR\s+(.+)\z/is
  @fetch ~r/\A(FETCH|MOVE)\s+(?:(?:FORWARD|NEXT)\s+)?(ALL\s+|\d+\s+)?(?:FROM\s+|IN\s+)?("[^"]+"|[A-Za-z_]\w*)\z/is
  @close ~r/\ACLOSE\s+(ALL|"[^"]+"|[A-Za-z_]\w*)\z/is

  @doc """
  `DECLARE <name> ... CURSOR ... FOR <query>`.
  """
  @spec parse_declare(String.t()) :: {:ok, String.t(), String.t()} | :error
  def parse_declare(statement) do
    case Regex.run(@declare, String.trim(statement), capture: :all_but_first) do
      [name, query] -> {:ok, unquote_name(name), String.trim(query)}
      nil -> :error
    end
  end

  @doc """
  `FETCH [FORWARD | NEXT] [ALL | n] [FROM | IN] <name>`, and `MOVE` in the
  same shapes. A bare `FETCH <name>` fetches one row, as Postgres does.
  """
  @spec parse_fetch(String.t()) ::
          {:ok, :fetch | :move, :all | pos_integer(), String.t()} | :error
  def parse_fetch(statement) do
    case Regex.run(@fetch, String.trim(statement), capture: :all_but_first) do
      [verb, count, name] ->
        {:ok, verb(verb), count(count), unquote_name(name)}

      nil ->
        :error
    end
  end

  @doc """
  `CLOSE <name>` or `CLOSE ALL`.
  """
  @spec parse_close(String.t()) :: {:ok, :all | String.t()} | :error
  def parse_close(statement) do
    case Regex.run(@close, String.trim(statement), capture: :all_but_first) do
      [name] -> {:ok, close_target(name)}
      nil -> :error
    end
  end

  defp close_target(name) do
    if String.downcase(name) == "all", do: :all, else: unquote_name(name)
  end

  defp verb(verb) do
    case String.downcase(verb) do
      "fetch" -> :fetch
      "move" -> :move
    end
  end

  defp count(""), do: 1

  defp count(count) do
    case count |> String.trim() |> String.downcase() do
      "all" -> :all
      digits -> String.to_integer(digits)
    end
  end

  defp unquote_name(<<?", _rest::binary>> = quoted),
    do: quoted |> String.trim("\"") |> String.replace("\"\"", "\"")

  defp unquote_name(name), do: name
end
