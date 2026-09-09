defmodule SmolqueryApi.TableSchema do
  @moduledoc """
  Table-schema JSON, in and out.

  The wire shape is a list of field objects, BigQuery-flavored:

      [
        {"name": "id", "type": "INT64", "nullable": false},
        {"name": "amount", "type": "NUMERIC(38,2)"}
      ]

  Type names are `Smolquery.Schema`'s API vocabulary; `nullable` defaults to
  `true`, matching the `Field` default. A `"materialized"` expression makes
  the column a computed one (`Smolquery.Schema.Materialized`, PL-61 L4),
  echoed back as the client wrote it; a field without one has no key. Everything else about a field —
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

      json = %{"name" => field.name, "type" => type, "nullable" => field.nullable}

      case field.materialized do
        nil -> json
        %{expression: expression} -> Map.put(json, "materialized", expression)
      end
    end)
  end

  @doc """
  Parses a JSON-decoded field list into a schema.
  """
  @spec from_json(term()) :: {:ok, Schema.t()} | {:error, term()}
  def from_json(fields) when is_list(fields) do
    fields
    |> Enum.reduce_while({:ok, []}, fn field, {:ok, parsed} ->
      case field_from_json(field) do
        {:ok, field} -> {:cont, {:ok, [field | parsed]}}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
    |> case do
      {:ok, parsed} -> Schema.new(Enum.reverse(parsed))
      {:error, reason} -> {:error, reason}
    end
  end

  def from_json(other), do: {:error, {:invalid_schema, other}}

  @doc """
  Parses one JSON-decoded field object — what `POST .../columns` carries,
  and what each element of a schema list is.
  """
  @spec field_from_json(term()) :: {:ok, Field.t()} | {:error, term()}
  def field_from_json(%{"name" => name, "type" => type} = field) do
    with {:ok, type} <- Schema.type_from_api(type),
         {:ok, nullable} <- nullable(field),
         {:ok, materialized} <- materialized(field) do
      Field.new(name, type, nullable: nullable, materialized: materialized)
    end
  end

  def field_from_json(other), do: {:error, {:invalid_field, other}}

  defp materialized(field) do
    case Map.get(field, "materialized") do
      nil -> {:ok, nil}
      expression when is_binary(expression) -> {:ok, expression}
      other -> {:error, {:invalid_field, %{"materialized" => other}}}
    end
  end

  defp nullable(field) do
    case Map.get(field, "nullable", true) do
      nullable when is_boolean(nullable) -> {:ok, nullable}
      other -> {:error, {:invalid_field, %{"nullable" => other}}}
    end
  end
end
