defmodule Smolquery.Identifier do
  @moduledoc """
  Validation and quoting for SQL identifiers.

  DuckDB takes no parameters for identifiers, so dataset, table, and column
  names can only reach the engine by interpolation. Every name is validated
  against a conservative pattern and then double-quoted, which keeps a hostile
  name from becoming SQL and preserves case through DuckDB's
  case-insensitive-unquoted rules.

  Values are a different matter — they bind as `$1..$n` parameters, or go
  through `sql_string/1` where a table function forces a literal.
  """

  @pattern ~r/^[A-Za-z_][A-Za-z0-9_]{0,62}$/

  @doc """
  Whether `name` is a usable identifier: a letter or underscore followed by up
  to 62 letters, digits, or underscores.
  """
  @spec valid?(term()) :: boolean()
  def valid?(name) when is_binary(name), do: Regex.match?(@pattern, name)
  def valid?(_name), do: false

  @doc """
  Returns `name` when it is a usable identifier.
  """
  @spec validate(term()) :: {:ok, String.t()} | {:error, {:invalid_identifier, term()}}
  def validate(name) do
    if valid?(name), do: {:ok, name}, else: {:error, {:invalid_identifier, name}}
  end

  @doc """
  A double-quoted identifier, raising when `name` would not be safe to quote.
  """
  @spec quote_name!(term()) :: String.t()
  def quote_name!(name) do
    case validate(name) do
      {:ok, name} ->
        ~s("#{name}")

      {:error, {:invalid_identifier, name}} ->
        raise ArgumentError, "invalid SQL identifier: #{inspect(name)}"
    end
  end

  @doc """
  A double-quoted label for a name this module did not mint — a result
  column's name as `DESCRIBE` reports it, such as `count_star()` or
  `attrs['host']`. Embedded quotes are doubled. Nothing is validated, because
  DuckDB already produced the name and a query must be able to refer to it.
  """
  @spec quote_label(String.t()) :: String.t()
  def quote_label(name) when is_binary(name),
    do: ~s(") <> String.replace(name, ~s("), ~s("")) <> ~s(")

  @doc """
  A single-quoted SQL string literal, for the table-function arguments that
  cannot take a bound parameter.
  """
  @spec sql_string(String.t()) :: String.t()
  def sql_string(value) when is_binary(value) do
    "'" <> String.replace(value, "'", "''") <> "'"
  end
end
