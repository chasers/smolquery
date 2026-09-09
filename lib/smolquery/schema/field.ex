defmodule Smolquery.Schema.Field do
  @moduledoc """
  One column of a table schema: a name, a logical type, nullability, and —
  once the catalog has assigned one — an identity that outlives the name.

  `id` is the catalog's stable column id (DuckLake's `column_id`, PL-62). A
  name can be dropped and given to a new column; an id never is, which is
  what lets a file written under one schema be read correctly under a later
  one. `since` is the catalog snapshot the column began at, the fact that
  says whether a file registered at some snapshot could have held it. Both
  are `nil` on a field the catalog has not seen yet — a schema a client is
  creating, or one a test built by hand — and nothing that writes or reads
  a file assumes they are set.

  `materialized` is the expression a column is computed from
  (`Smolquery.Schema.Materialized`, PL-61 L4), `nil` for a column a client
  supplies values for.
  """

  alias Smolquery.Identifier
  alias Smolquery.Schema
  alias Smolquery.Schema.Materialized

  @enforce_keys [:name, :type]
  defstruct [:name, :type, :id, :since, :materialized, nullable: true]

  @type t :: %__MODULE__{
          name: String.t(),
          type: Schema.logical_type(),
          nullable: boolean(),
          id: pos_integer() | nil,
          since: non_neg_integer() | nil,
          materialized: Materialized.t() | nil
        }

  @doc """
  Builds a field, rejecting unusable names and unknown logical types.

  ## Options

    * `:nullable` — whether the column accepts nulls. Defaults to `true`,
      matching BigQuery's `NULLABLE` default mode.
    * `:id` — the catalog's column id, when the catalog is answering
    * `:since` — the catalog snapshot the column began at, likewise
    * `:materialized` — the expression the column is computed from, as text
      a client wrote or as a `Smolquery.Schema.Materialized` the catalog
      validated; a materialized column is nullable, so `nullable: false`
      beside it is refused as `{:column_must_be_nullable, name}`

  """
  @spec new(term(), term(), keyword()) :: {:ok, t()} | {:error, term()}
  def new(name, type, opts \\ []) do
    with {:ok, name} <- Identifier.validate(name),
         {:ok, type} <- Schema.validate_type(type),
         {:ok, materialized} <- materialized(Keyword.get(opts, :materialized)),
         :ok <- nullable_if_materialized(name, materialized, Keyword.get(opts, :nullable, true)) do
      {:ok,
       %__MODULE__{
         name: name,
         type: type,
         nullable: Keyword.get(opts, :nullable, true),
         id: Keyword.get(opts, :id),
         since: Keyword.get(opts, :since),
         materialized: materialized
       }}
    end
  end

  defp nullable_if_materialized(_name, nil, _nullable), do: :ok
  defp nullable_if_materialized(_name, _definition, true), do: :ok

  defp nullable_if_materialized(name, _definition, false),
    do: {:error, {:column_must_be_nullable, name}}

  defp materialized(nil), do: {:ok, nil}
  defp materialized(%Materialized{} = definition), do: {:ok, definition}

  defp materialized(expression) when is_binary(expression) do
    case String.trim(expression) do
      "" -> {:error, {:invalid_materialized, :one_expression}}
      trimmed -> {:ok, %Materialized{expression: trimmed}}
    end
  end

  defp materialized(other), do: {:error, {:invalid_materialized, {:unparseable, inspect(other)}}}

  @doc """
  Same as `new/3` but raises on an invalid name or type.
  """
  @spec new!(term(), term(), keyword()) :: t()
  def new!(name, type, opts \\ []) do
    case new(name, type, opts) do
      {:ok, field} -> field
      {:error, reason} -> raise ArgumentError, "invalid field: #{inspect(reason)}"
    end
  end
end
