defmodule Smolquery.Schema do
  @moduledoc """
  Logical table schemas, and the one place the three type systems meet.

  A segment's life crosses three type vocabularies: smolquery's own logical
  types (what the API speaks), Explorer dtypes (what the segment writer
  encodes), and DuckDB types (what the catalog declares and the engine reads).
  The mapping lives here once, so a new type is one row in one table rather
  than a change in three modules.

  | logical | Explorer dtype | DuckDB |
  |---|---|---|
  | `:int64` | `{:s, 64}` | `BIGINT` |
  | `:float64` | `{:f, 64}` | `DOUBLE` |
  | `:string` | `:string` | `VARCHAR` |
  | `:bool` | `:boolean` | `BOOLEAN` |
  | `:timestamp` | `{:naive_datetime, :microsecond}` | `TIMESTAMP` |
  | `:date` | `:date` | `DATE` |
  | `{:numeric, p, s}` | `{:decimal, p, s}` | `DECIMAL(p,s)` |

  Milestone 1 verified that every one of these round-trips byte-identically
  through Explorer's Parquet writer, so a segment written here reads back
  through DuckDB unchanged.

  ## Usage

      {:ok, schema} =
        Smolquery.Schema.new([
          {"id", :int64, nullable: false},
          {"ts", :timestamp},
          {"amount", {:numeric, 38, 2}}
        ])

  """

  alias Smolquery.Identifier
  alias Smolquery.Schema.Field

  @enforce_keys [:fields]
  defstruct [:fields]

  @type t :: %__MODULE__{fields: [Field.t()]}

  @type logical_type ::
          :int64
          | :float64
          | :string
          | :bool
          | :timestamp
          | :date
          | {:numeric, pos_integer(), non_neg_integer()}

  @type field_spec ::
          Field.t()
          | {String.t(), logical_type()}
          | {String.t(), logical_type(), keyword()}

  @mapping [
    {:int64, {:s, 64}, "BIGINT"},
    {:float64, {:f, 64}, "DOUBLE"},
    {:string, :string, "VARCHAR"},
    {:bool, :boolean, "BOOLEAN"},
    {:timestamp, {:naive_datetime, :microsecond}, "TIMESTAMP"},
    {:date, :date, "DATE"}
  ]

  @scalar_types Enum.map(@mapping, &elem(&1, 0))
  @logical_to_explorer Map.new(@mapping, fn {logical, dtype, _duckdb} -> {logical, dtype} end)
  @explorer_to_logical Map.new(@mapping, fn {logical, dtype, _duckdb} -> {dtype, logical} end)
  @logical_to_duckdb Map.new(@mapping, fn {logical, _dtype, duckdb} -> {logical, duckdb} end)
  @duckdb_to_logical Map.new(@mapping, fn {logical, _dtype, duckdb} -> {duckdb, logical} end)

  @doc """
  Builds a schema from field specs — `Field` structs or `{name, type}` /
  `{name, type, opts}` tuples.

  An empty field list is an error: a table with no columns cannot be written
  to or read from, so it is never what the caller meant.
  """
  @spec new([field_spec()]) :: {:ok, t()} | {:error, term()}
  def new([]), do: {:error, :empty_schema}

  def new(specs) when is_list(specs) do
    with {:ok, fields} <- build_fields(specs) do
      case duplicate_names(fields) do
        [] -> {:ok, %__MODULE__{fields: fields}}
        duplicates -> {:error, {:duplicate_columns, duplicates}}
      end
    end
  end

  @doc """
  Same as `new/1` but raises on an invalid schema.
  """
  @spec new!([field_spec()]) :: t()
  def new!(specs) do
    case new(specs) do
      {:ok, schema} -> schema
      {:error, reason} -> raise ArgumentError, "invalid schema: #{inspect(reason)}"
    end
  end

  @doc """
  The schema's column names, in order.
  """
  @spec names(t()) :: [String.t()]
  def names(%__MODULE__{fields: fields}), do: Enum.map(fields, & &1.name)

  @doc """
  The field named `name`, if the schema has one.
  """
  @spec field(t(), String.t()) :: {:ok, Field.t()} | :error
  def field(%__MODULE__{fields: fields}, name) do
    case Enum.find(fields, &(&1.name == name)) do
      nil -> :error
      field -> {:ok, field}
    end
  end

  @doc """
  Returns `type` when it is a supported logical type.
  """
  @spec validate_type(term()) :: {:ok, logical_type()} | {:error, {:unsupported_type, term()}}
  def validate_type(type) when type in @scalar_types, do: {:ok, type}

  def validate_type({:numeric, precision, scale} = type)
      when is_integer(precision) and is_integer(scale) and
             precision > 0 and precision <= 38 and scale >= 0 and scale <= precision do
    {:ok, type}
  end

  def validate_type(type), do: {:error, {:unsupported_type, type}}

  @doc """
  The Explorer dtype for a logical type.
  """
  @spec explorer_dtype(logical_type()) :: {:ok, term()} | {:error, {:unsupported_type, term()}}
  def explorer_dtype({:numeric, precision, scale}), do: {:ok, {:decimal, precision, scale}}

  def explorer_dtype(type) do
    case Map.fetch(@logical_to_explorer, type) do
      {:ok, dtype} -> {:ok, dtype}
      :error -> {:error, {:unsupported_type, type}}
    end
  end

  @doc """
  The logical type for an Explorer dtype.
  """
  @spec logical_from_explorer(term()) ::
          {:ok, logical_type()} | {:error, {:unsupported_type, term()}}
  def logical_from_explorer({:decimal, precision, scale}), do: {:ok, {:numeric, precision, scale}}

  def logical_from_explorer(dtype) do
    case Map.fetch(@explorer_to_logical, dtype) do
      {:ok, type} -> {:ok, type}
      :error -> {:error, {:unsupported_type, dtype}}
    end
  end

  @doc """
  The DuckDB type name for a logical type.
  """
  @spec duckdb_type(logical_type()) :: {:ok, String.t()} | {:error, {:unsupported_type, term()}}
  def duckdb_type({:numeric, precision, scale}), do: {:ok, "DECIMAL(#{precision},#{scale})"}

  def duckdb_type(type) do
    case Map.fetch(@logical_to_duckdb, type) do
      {:ok, name} -> {:ok, name}
      :error -> {:error, {:unsupported_type, type}}
    end
  end

  @doc """
  The logical type for a DuckDB type name, as `information_schema` reports it.
  """
  @spec logical_from_duckdb(String.t()) ::
          {:ok, logical_type()} | {:error, {:unsupported_type, term()}}
  def logical_from_duckdb(name) when is_binary(name) do
    case Map.fetch(@duckdb_to_logical, String.upcase(name)) do
      {:ok, type} -> {:ok, type}
      :error -> decimal_from_duckdb(name)
    end
  end

  @doc """
  The `column_name dtype` pairs Explorer needs to build a DataFrame.
  """
  @spec explorer_dtypes(t()) :: {:ok, [{String.t(), term()}]} | {:error, term()}
  def explorer_dtypes(%__MODULE__{fields: fields}) do
    map_fields(fields, fn field ->
      with {:ok, dtype} <- explorer_dtype(field.type), do: {:ok, {field.name, dtype}}
    end)
  end

  @doc """
  The column definitions for a `CREATE TABLE` statement, quoted and ordered.
  """
  @spec column_definitions(t()) :: {:ok, String.t()} | {:error, term()}
  def column_definitions(%__MODULE__{fields: fields}) do
    with {:ok, definitions} <- map_fields(fields, &column_definition/1) do
      {:ok, Enum.join(definitions, ", ")}
    end
  end

  defp column_definition(%Field{} = field) do
    with {:ok, type} <- duckdb_type(field.type) do
      null = if field.nullable, do: "", else: " NOT NULL"
      {:ok, "#{Identifier.quote_name!(field.name)} #{type}#{null}"}
    end
  end

  @doc """
  The `SELECT` list that projects a relation carrying `columns` onto this schema.

  Every declared column appears, in the schema's own order and cast to the type
  the schema declares; a declared column `columns` does not carry becomes a typed
  `NULL`. A column not declared here is dropped, so callers that must not lose one
  check `names/1` first.

  This is what lets a writer produce a file matching a schema it does not control
  — the merge projects a claim's inputs onto the catalog's declared columns, so a
  claim whose micro-segments all predate an added column still seals to a file
  the catalog accepts.
  """
  @spec projection(t(), [String.t()]) :: {:ok, String.t()} | {:error, term()}
  def projection(%__MODULE__{fields: fields}, columns) do
    available = MapSet.new(columns)

    with {:ok, expressions} <- map_fields(fields, &projected_column(&1, available)) do
      {:ok, Enum.join(expressions, ", ")}
    end
  end

  defp projected_column(%Field{} = field, available) do
    with {:ok, type} <- duckdb_type(field.type) do
      name = Identifier.quote_name!(field.name)
      source = if MapSet.member?(available, field.name), do: name, else: "NULL"

      {:ok, "CAST(#{source} AS #{type}) AS #{name}"}
    end
  end

  defp build_fields(specs) do
    map_fields(specs, fn
      %Field{} = field -> {:ok, field}
      {name, type} -> Field.new(name, type)
      {name, type, opts} -> Field.new(name, type, opts)
      other -> {:error, {:invalid_field_spec, other}}
    end)
  end

  defp map_fields(specs, fun) do
    Enum.reduce_while(specs, {:ok, []}, fn spec, {:ok, acc} ->
      case fun.(spec) do
        {:ok, value} -> {:cont, {:ok, [value | acc]}}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
    |> case do
      {:ok, values} -> {:ok, Enum.reverse(values)}
      {:error, reason} -> {:error, reason}
    end
  end

  defp duplicate_names(fields) do
    fields
    |> Enum.frequencies_by(& &1.name)
    |> Enum.filter(fn {_name, count} -> count > 1 end)
    |> Enum.map(fn {name, _count} -> name end)
  end

  defp decimal_from_duckdb(name) do
    case Regex.run(~r/^DECIMAL\((\d+),\s*(\d+)\)$/i, String.trim(name)) do
      [_match, precision, scale] ->
        validate_type({:numeric, String.to_integer(precision), String.to_integer(scale)})

      nil ->
        {:error, {:unsupported_type, name}}
    end
  end
end
