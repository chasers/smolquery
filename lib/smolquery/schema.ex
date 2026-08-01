defmodule Smolquery.Schema do
  @moduledoc """
  Logical table schemas, and the one place the three type systems meet.

  A segment's life crosses three type vocabularies: smolquery's own logical
  types (what the API speaks), Explorer dtypes (what the segment writer
  encodes), and DuckDB types (what the catalog declares and the engine reads).
  The mapping lives here once, so a new type is one row in one table rather
  than a change in three modules.

  | logical | API name | Explorer dtype | DuckDB |
  |---|---|---|---|
  | `:int64` | `INT64` | `{:s, 64}` | `BIGINT` |
  | `:float64` | `FLOAT64` | `{:f, 64}` | `DOUBLE` |
  | `:string` | `STRING` | `:string` | `VARCHAR` |
  | `:bool` | `BOOL` | `:boolean` | `BOOLEAN` |
  | `:timestamp` | `TIMESTAMP` | `{:naive_datetime, :microsecond}` | `TIMESTAMP` |
  | `:date` | `DATE` | `:date` | `DATE` |
  | `{:numeric, p, s}` | `NUMERIC(p,s)` | `{:decimal, p, s}` | `DECIMAL(p,s)` |

  The API names are the BigQuery-flavored strings `Smolquery.Api` speaks in
  table-schema JSON.

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
    {:int64, {:s, 64}, "BIGINT", "INT64"},
    {:float64, {:f, 64}, "DOUBLE", "FLOAT64"},
    {:string, :string, "VARCHAR", "STRING"},
    {:bool, :boolean, "BOOLEAN", "BOOL"},
    {:timestamp, {:naive_datetime, :microsecond}, "TIMESTAMP", "TIMESTAMP"},
    {:date, :date, "DATE", "DATE"}
  ]

  @scalar_types Enum.map(@mapping, &elem(&1, 0))
  @logical_to_explorer Map.new(@mapping, fn {logical, dtype, _duckdb, _api} ->
                         {logical, dtype}
                       end)
  @explorer_to_logical Map.new(@mapping, fn {logical, dtype, _duckdb, _api} ->
                         {dtype, logical}
                       end)
  @logical_to_duckdb Map.new(@mapping, fn {logical, _dtype, duckdb, _api} -> {logical, duckdb} end)
  @duckdb_to_logical Map.new(@mapping, fn {logical, _dtype, duckdb, _api} -> {duckdb, logical} end)
  @logical_to_api Map.new(@mapping, fn {logical, _dtype, _duckdb, api} -> {logical, api} end)
  @api_to_logical Map.new(@mapping, fn {logical, _dtype, _duckdb, api} -> {api, logical} end)

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
  The API name for a logical type — what table-schema JSON says.
  """
  @spec api_type(logical_type()) :: {:ok, String.t()} | {:error, {:unsupported_type, term()}}
  def api_type({:numeric, precision, scale}), do: {:ok, "NUMERIC(#{precision},#{scale})"}

  def api_type(type) do
    case Map.fetch(@logical_to_api, type) do
      {:ok, name} -> {:ok, name}
      :error -> {:error, {:unsupported_type, type}}
    end
  end

  @doc """
  The logical type for an API name, case-insensitively.
  """
  @spec type_from_api(term()) :: {:ok, logical_type()} | {:error, {:unsupported_type, term()}}
  def type_from_api(name) when is_binary(name) do
    case Map.fetch(@api_to_logical, String.upcase(name)) do
      {:ok, type} -> {:ok, type}
      :error -> numeric_from_api(name)
    end
  end

  def type_from_api(name), do: {:error, {:unsupported_type, name}}

  @doc """
  The Elixir term a JSON-decoded value becomes for a logical type — what a
  streaming insert's rows pass through on the way to the segment writer.

  One table, like the type names:

  | logical | accepted JSON | becomes |
  |---|---|---|
  | `:int64` | integer, or a string of digits (JS clients lose precision past 2^53) | integer |
  | `:float64` | number, or a string that parses as one | float |
  | `:string` | string | binary |
  | `:bool` | boolean | boolean |
  | `:timestamp` | ISO 8601 string; an offset is converted to UTC | `NaiveDateTime` |
  | `:date` | ISO 8601 string | `Date` |
  | `{:numeric, p, s}` | string (preferred — floats round), integer, or number | `Decimal` |

  `nil` passes through for every type; whether a column may be null is the
  validator's question, not a value question. A value already in its native
  Elixir representation (`NaiveDateTime`, `Date`, `Decimal`) also passes
  through — batch loads parse files into natives before taking the same
  validation path streaming inserts do.
  """
  @spec value_from_json(logical_type(), term()) ::
          {:ok, term()} | {:error, {:invalid_value, logical_type(), term()}}
  def value_from_json(_type, nil), do: {:ok, nil}

  def value_from_json(:int64, value) when is_integer(value), do: {:ok, value}

  def value_from_json(:int64, value) when is_binary(value) do
    case Integer.parse(value) do
      {integer, ""} -> {:ok, integer}
      _partial_or_error -> invalid(:int64, value)
    end
  end

  def value_from_json(:float64, value) when is_float(value), do: {:ok, value}
  def value_from_json(:float64, value) when is_integer(value), do: {:ok, value * 1.0}

  def value_from_json(:float64, value) when is_binary(value) do
    case Float.parse(value) do
      {float, ""} -> {:ok, float}
      _partial_or_error -> invalid(:float64, value)
    end
  end

  def value_from_json(:string, value) when is_binary(value), do: {:ok, value}

  def value_from_json(:bool, value) when is_boolean(value), do: {:ok, value}

  def value_from_json(:timestamp, %NaiveDateTime{} = value), do: {:ok, value}

  def value_from_json(:timestamp, value) when is_binary(value) do
    case DateTime.from_iso8601(value) do
      {:ok, datetime, _offset} ->
        {:ok, DateTime.to_naive(DateTime.shift_zone!(datetime, "Etc/UTC"))}

      {:error, :missing_offset} ->
        case NaiveDateTime.from_iso8601(value) do
          {:ok, naive} -> {:ok, naive}
          {:error, _reason} -> invalid(:timestamp, value)
        end

      {:error, _reason} ->
        invalid(:timestamp, value)
    end
  end

  def value_from_json(:date, %Date{} = value), do: {:ok, value}

  def value_from_json(:date, value) when is_binary(value) do
    case Date.from_iso8601(value) do
      {:ok, date} -> {:ok, date}
      {:error, _reason} -> invalid(:date, value)
    end
  end

  def value_from_json({:numeric, _p, _s}, %Decimal{} = value), do: {:ok, value}

  def value_from_json({:numeric, _p, _s} = type, value) when is_binary(value) do
    case Decimal.parse(value) do
      {decimal, ""} -> {:ok, decimal}
      _partial_or_error -> invalid(type, value)
    end
  end

  def value_from_json({:numeric, _p, _s}, value) when is_integer(value),
    do: {:ok, Decimal.new(value)}

  def value_from_json({:numeric, _p, _s}, value) when is_float(value),
    do: {:ok, Decimal.from_float(value)}

  def value_from_json(type, value), do: invalid(type, value)

  defp invalid(type, value), do: {:error, {:invalid_value, type, value}}

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

  defp numeric_from_api(name) do
    case Regex.run(~r/^NUMERIC\((\d+),\s*(\d+)\)$/i, String.trim(name)) do
      [_match, precision, scale] ->
        validate_type({:numeric, String.to_integer(precision), String.to_integer(scale)})

      nil ->
        {:error, {:unsupported_type, name}}
    end
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
