defmodule SmolqueryPg.Cursors do
  @moduledoc """
  The cursor statements `postgres_fdw` scans with (PL-58 layer 4).

  Every foreign scan is `DECLARE c1 CURSOR FOR <select>`, repeated
  `FETCH 100 FROM c1`, and `CLOSE c1`, inside a `REPEATABLE READ`
  transaction block. This module parses the three statements (plus `MOVE`,
  and `CLOSE ALL`) on `SmolqueryPg.Sql`'s statement tokens — binary
  matching, no regex; `SmolqueryPg.Session` owns the cursors themselves —
  a cursor is the same held outcome a portal is, paged by offset.
  """

  alias SmolqueryPg.Sql

  @modifiers ~w(binary insensitive asensitive scroll no)
  @directions ~w(forward next)

  @doc """
  `DECLARE <name> ... CURSOR ... FOR <query>`.
  """
  @spec parse_declare(String.t()) :: {:ok, String.t(), String.t()} | :error
  def parse_declare(statement) do
    with {:word, "declare", rest} <- Sql.next_token(statement),
         {kind, name, rest} when kind in [:word, :quoted] <- Sql.next_token(rest),
         {:word, "cursor", rest} <- rest |> Sql.skip_words(@modifiers) |> Sql.next_token(),
         {:word, "for", rest} <- rest |> skip_hold() |> Sql.next_token(),
         query when query != "" <- Sql.skip_trivia(rest) do
      {:ok, name, String.trim(query)}
    else
      _malformed -> :error
    end
  end

  defp skip_hold(rest) do
    case Sql.next_token(rest) do
      {:word, with_or_without, next} when with_or_without in ["with", "without"] ->
        case Sql.next_token(next) do
          {:word, "hold", after_hold} -> after_hold
          _other -> rest
        end

      _other ->
        rest
    end
  end

  @doc """
  `FETCH [FORWARD | NEXT] [ALL | n] [FROM | IN] <name>`, and `MOVE` in the
  same shapes. A bare `FETCH <name>` fetches one row, as Postgres does.
  """
  @spec parse_fetch(String.t()) ::
          {:ok, :fetch | :move, :all | pos_integer(), String.t()} | :error
  def parse_fetch(statement) do
    with {:word, verb, rest} when verb in ["fetch", "move"] <- Sql.next_token(statement),
         rest = Sql.skip_words(rest, @directions),
         {count, rest} <- fetch_count(rest),
         {kind, name, rest} when kind in [:word, :quoted] <-
           rest |> Sql.skip_words(["from", "in"]) |> Sql.next_token(),
         :eof <- Sql.next_token(rest) do
      {:ok, String.to_existing_atom(verb), count, name}
    else
      _malformed -> :error
    end
  end

  defp fetch_count(rest) do
    case Sql.next_token(rest) do
      {:number, digits, next} -> {String.to_integer(digits), next}
      {:word, "all", next} -> {:all, next}
      _none -> {1, rest}
    end
  end

  @doc """
  `CLOSE <name>` or `CLOSE ALL`.
  """
  @spec parse_close(String.t()) :: {:ok, :all | String.t()} | :error
  def parse_close(statement) do
    with {:word, "close", rest} <- Sql.next_token(statement),
         {kind, name, rest} when kind in [:word, :quoted] <- Sql.next_token(rest),
         :eof <- Sql.next_token(rest) do
      case {kind, name} do
        {:word, "all"} -> {:ok, :all}
        _named -> {:ok, name}
      end
    else
      _malformed -> :error
    end
  end
end
