defmodule Smolquery.Schema.Field do
  @moduledoc """
  One column of a table schema: a name, a logical type, and nullability.
  """

  alias Smolquery.Identifier
  alias Smolquery.Schema

  @enforce_keys [:name, :type]
  defstruct [:name, :type, nullable: true]

  @type t :: %__MODULE__{
          name: String.t(),
          type: Schema.logical_type(),
          nullable: boolean()
        }

  @doc """
  Builds a field, rejecting unusable names and unknown logical types.

  ## Options

    * `:nullable` — whether the column accepts nulls. Defaults to `true`,
      matching BigQuery's `NULLABLE` default mode.

  """
  @spec new(term(), term(), keyword()) :: {:ok, t()} | {:error, term()}
  def new(name, type, opts \\ []) do
    with {:ok, name} <- Identifier.validate(name),
         {:ok, type} <- Schema.validate_type(type) do
      {:ok, %__MODULE__{name: name, type: type, nullable: Keyword.get(opts, :nullable, true)}}
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
