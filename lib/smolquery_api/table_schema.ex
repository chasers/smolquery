defmodule SmolqueryApi.TableSchema do
  @moduledoc """
  Table-schema JSON, in and out.

  The wire shape is a list of field objects, BigQuery-flavored:

      [
        {"name": "id", "type": "INT64", "nullable": false},
        {"name": "amount", "type": "NUMERIC(38,2)"}
      ]

  Type names are `Smolquery.Schema`'s API vocabulary; `nullable` defaults to
  `true`, matching the `Field` default. Everything else about a field —
  identifier rules, duplicate columns, an empty list — is rejected by
  `Smolquery.Schema.new/1`, so a schema that parses here is one the catalog
  will accept.

  Clustering is a `Smolquery.Schema` field (`clustering: [String.t()]`) but
  not part of this field-list JSON: the table routes surface it beside
  `schema` / `retention` on GET/PATCH, and the catalog attaches it when
  loading `table_schema/2` so SchemaCache carries it on the write path.
  """

  alias Smolquery.Schema
  alias Smolquery.Schema.Field

  @doc """
  The JSON-shaped field list for a schema.
  """
  @spec to_json(Schema.t()) :: [map()]
  def to_json(%Schema{fields: fields}) do
    Enum.map(fields, fn %Field{} = field ->
      {:ok, type} = Schema.api_type(field.type)

      %{"name" => field.name, "type" => type, "nullable" => field.nullable}
    end)
  end

  @doc """
  Parses a JSON-decoded field list into a schema.
  """
  @spec from_json(term()) :: {:ok, Schema.t()} | {:error, term()}
  def from_json(fields) when is_list(fields) do
    fields
    |> Enum.reduce_while({:ok, []}, fn field, {:ok, specs} ->
      case field_spec(field) do
        {:ok, spec} -> {:cont, {:ok, [spec | specs]}}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
    |> case do
      {:ok, specs} -> Schema.new(Enum.reverse(specs))
      {:error, reason} -> {:error, reason}
    end
  end

  def from_json(other), do: {:error, {:invalid_schema, other}}

  defp field_spec(%{"name" => name, "type" => type} = field) do
    with {:ok, type} <- Schema.type_from_api(type),
         {:ok, nullable} <- nullable(field) do
      {:ok, {name, type, [nullable: nullable]}}
    end
  end

  defp field_spec(other), do: {:error, {:invalid_field, other}}

  defp nullable(field) do
    case Map.get(field, "nullable", true) do
      nullable when is_boolean(nullable) -> {:ok, nullable}
      other -> {:error, {:invalid_field, %{"nullable" => other}}}
    end
  end
end
