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
  """

  alias Smolquery.Identifier
  alias Smolquery.Schema

  @enforce_keys [:name, :type]
  defstruct [:name, :type, :id, :since, nullable: true]

  @type t :: %__MODULE__{
          name: String.t(),
          type: Schema.logical_type(),
          nullable: boolean(),
          id: pos_integer() | nil,
          since: non_neg_integer() | nil
        }

  @doc """
  Builds a field, rejecting unusable names and unknown logical types.

  ## Options

    * `:nullable` — whether the column accepts nulls. Defaults to `true`,
      matching BigQuery's `NULLABLE` default mode.
    * `:id` — the catalog's column id, when the catalog is answering
    * `:since` — the catalog snapshot the column began at, likewise

  """
  @spec new(term(), term(), keyword()) :: {:ok, t()} | {:error, term()}
  def new(name, type, opts \\ []) do
    with {:ok, name} <- Identifier.validate(name),
         {:ok, type} <- Schema.validate_type(type) do
      {:ok,
       %__MODULE__{
         name: name,
         type: type,
         nullable: Keyword.get(opts, :nullable, true),
         id: Keyword.get(opts, :id),
         since: Keyword.get(opts, :since)
       }}
    end
  end

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
