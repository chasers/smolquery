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
  | `{:map, :string, :string}` | `MAP(STRING, STRING)` | — | `MAP(VARCHAR, VARCHAR)` |
  | `:variant` | `VARIANT` | — | `JSON` stored, `VARIANT` queried |

  The API names are the BigQuery-flavored strings `SmolqueryApi` speaks in
  table-schema JSON. `MAP(STRING, STRING)` is the one BigQuery does not have: it
  is ClickHouse's `Map(String, String)`, the shape OpenTelemetry attribute bags
  arrive in.

  ## A map has no Explorer dtype

  Explorer has no map dtype, and the closest shape it can write — a list of
  `{key, value}` structs — reaches Parquet without the `MAP` annotation, so
  DuckDB reads it back as `STRUCT[]` and refuses to union it with a sealed
  `MAP` column. A map column is therefore written only by the DuckDB flush
  writer — the one writer since PL-57 — which reads the spooled NDJSON
  straight into `MAP(VARCHAR, VARCHAR)`. `explorer_dtype/1` answers
  `{:error, {:unsupported_type, _}}` for it, and every Explorer-side path — the
  fixture writer, CSV and Parquet loads — refuses on
  that answer. On the read side a map arrives from `Smolquery.Engine.frame/3`
  as Explorer's list-of-struct; `Smolquery.Engine.Frame.to_rows/1` turns it
  back into a map.

  A map's values are strings. A JSON value that is not a string is written as
  its JSON text (`1`, `true`, `["a","b"]`, `{"k":"v"}`), the way DuckDB's
  `read_json` stringifies it on the passthrough path. The two encoders agree
  on scalars and short documents; they can differ on a float's exponent form
  and on the key order of a nested object, because this path re-encodes a
  decoded term and the passthrough keeps the client's bytes.

  ## A variant is stored as JSON and queried as VARIANT

  `VARIANT` is DuckDB's semi-structured type: any JSON value, with each
  value's type kept — `1` stays an integer, `["a","b"]` stays an array, an
  object nests. Query it with `attrs['host']::VARCHAR`,
  `TRY_CAST(attrs['n'] AS BIGINT)`, `variant_typeof(attrs)`, and `attrs::JSON`
  for the document back out.

  On disk it is `JSON` text, in both tiers. DuckDB's Parquet encoding of a
  VARIANT carries no Parquet logical type, and DuckLake's
  `ducklake_add_data_files` — the seal's zero-copy registration — refuses to
  map that file onto a VARIANT column (`Expected VARIANT, found type STRUCT`).
  A `JSON` column it registers. So `duckdb_type/1` says `JSON`, the writer
  reads the column as JSON with no cast, and `view_cast/1` says `VARIANT`:
  `Smolquery.QueryService.Views` projects `"attrs"::VARIANT` in every table
  view, so a query sees the variant type across the hot ∪ sealed union. The
  parse is paid per row that reaches a projection or filter naming the
  column, per query — DuckDB does not read a column no expression names.

  The reverse mapping, `JSON` to `:variant`, holds by construction: this
  schema has no JSON logical type, so a `JSON` column in a catalog smolquery
  manages is a variant and nothing else. The day DuckLake registers a variant
  file, `duckdb_type/1` flips for new tables, `view_cast/1` keeps casting
  both storages, and `Field` learns the stored type so the seal merge casts
  to what each table holds (T-394).

  The same writer story as a map applies: no Explorer dtype, DuckDB writes it.
  And a variant never leaves DuckDB as itself: Arrow has no VARIANT, so the
  query runner casts every VARIANT result column back to `JSON` before the
  result crosses (`Smolquery.QueryService.VariantResults`), and the API
  decodes those columns into JSON values — a variant reaches the client as the
  nested JSON it was inserted as.

  ## What a caller must know about a map or a variant

  The limits, in one list; `docs/api.md` carries the same list for API callers:

    * neither has stats bounds, so nothing prunes on a key filter
    * only DuckDB writes a segment holding them; Explorer's fixture writer
      refuses the schema, and a CSV or Parquet load cannot carry either
    * a map's values are strings — any other JSON value is stored as its text —
      and a `NULL` map reads back as `%{}`
    * a variant is JSON text on disk, parsed per scanned row per query; its
      casts are strict (`TRY_CAST` for a mixed key); a variant nested in a
      struct or list cannot be returned; a variant with no table behind it is
      not cast at the boundary and fails the export

  A schema also carries the table's `clustering` key — the column names writes
  sort by, smolquery's analog of ClickHouse's `ORDER BY`. It rides here because
  this is the one description of a table that already reaches every write point,
  from the catalog through the ingest cache to `Smolquery.Segments.Writer`, and
  because sorting is the only thing the key ever does. `clustering_columns/1`,
  not the raw field, is what a write point should sort by.

  `partitions` rides here for the same reason: the table's own write-partition
  count (`Smolquery.Partitions`, T-304), read from the catalog, reaching the
  ingest cache and the query planner with no extra round trip. `nil` means the
  catalog holds no count and the deployment's `write_partitions` applies.

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
  defstruct fields: nil, clustering: [], partitions: nil

  @type t :: %__MODULE__{
          fields: [Field.t()],
          clustering: [String.t()],
          partitions: pos_integer() | nil
        }

  @type logical_type ::
          :int64
          | :float64
          | :string
          | :bool
          | :timestamp
          | :date
          | {:numeric, pos_integer(), non_neg_integer()}
          | {:map, :string, :string}
          | :variant

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
  @map_type {:map, :string, :string}
  @map_duckdb "MAP(VARCHAR, VARCHAR)"
  @map_api "MAP(STRING, STRING)"
  @logical_to_explorer Map.new(@mapping, fn {logical, dtype, _duckdb, _api} ->
                         {logical, dtype}
                       end)
  @explorer_to_logical Map.new(@mapping, fn {logical, dtype, _duckdb, _api} ->
                         {dtype, logical}
                       end)
  @logical_to_duckdb @mapping
                     |> Map.new(fn {logical, _dtype, duckdb, _api} -> {logical, duckdb} end)
                     |> Map.put(:variant, "JSON")
  @duckdb_to_logical @mapping
                     |> Map.new(fn {logical, _dtype, duckdb, _api} -> {duckdb, logical} end)
                     |> Map.put("JSON", :variant)
  @logical_to_api @mapping
                  |> Map.new(fn {logical, _dtype, _duckdb, api} -> {logical, api} end)
                  |> Map.put(:variant, "VARIANT")
  @api_to_logical @mapping
                  |> Map.new(fn {logical, _dtype, _duckdb, api} -> {api, logical} end)
                  |> Map.put("VARIANT", :variant)

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
  The clustering key's columns that this schema actually declares, in the key's
  own order — what a write point sorts by.

  Not the same list as the `:clustering` field, and the difference is the point.
  The field is what an operator asked for, which is what the API reports back.
  This is what is safe to sort on, and the two can disagree: the key is catalog
  metadata living beside the table rather than inside it, so dropping a table
  and recreating it without a column leaves a key still naming that column.

  Sorting on the columns that remain is the only answer that keeps data moving.
  Erroring instead would fail identically on every retry, so a seal would never
  retire and the table's tail would stay in the hot tier for good — and a key is
  a pruning optimization, never a correctness property, so degrading it is free.
  """
  @spec clustering_columns(t()) :: [String.t()]
  def clustering_columns(%__MODULE__{clustering: []}), do: []

  def clustering_columns(%__MODULE__{clustering: clustering} = schema) do
    names = MapSet.new(names(schema))

    Enum.filter(clustering, &MapSet.member?(names, &1))
  end

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

  def validate_type(@map_type), do: {:ok, @map_type}
  def validate_type(:variant), do: {:ok, :variant}

  def validate_type(type), do: {:error, {:unsupported_type, type}}

  @doc """
  The Explorer dtype for a logical type.

  A map has none — see "A map has no Explorer dtype" above — so this answers
  `{:error, {:unsupported_type, _}}` for it, which is what the Explorer-side
  paths fall back on.
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
  def duckdb_type(@map_type), do: {:ok, @map_duckdb}

  def duckdb_type(type) do
    case Map.fetch(@logical_to_duckdb, type) do
      {:ok, name} -> {:ok, name}
      :error -> {:error, {:unsupported_type, type}}
    end
  end

  @doc """
  The DuckDB type a table view casts the column to, or `:none` when a query
  sees the stored type as it is.

  `VARIANT` for a variant, whatever a given table stores it as — `JSON`
  today, and `VARIANT` once DuckLake registers a variant file (T-394). A table
  created before that flip keeps its `JSON` column, so the rule is on the
  logical type, not on what `duckdb_type/1` says now: old and new tables then
  read alike, and a `VARIANT` to `VARIANT` cast costs nothing.
  `Smolquery.QueryService.Views` renders the cast.
  """
  @spec view_cast(logical_type()) :: {:cast, String.t()} | :none
  def view_cast(:variant), do: {:cast, "VARIANT"}
  def view_cast(_type), do: :none

  @doc """
  The logical type for a DuckDB type name, as `information_schema` reports it.
  """
  @spec logical_from_duckdb(String.t()) ::
          {:ok, logical_type()} | {:error, {:unsupported_type, term()}}
  def logical_from_duckdb(name) when is_binary(name) do
    case Map.fetch(@duckdb_to_logical, String.upcase(name)) do
      {:ok, type} -> {:ok, type}
      :error -> compound_from_duckdb(name)
    end
  end

  @doc """
  The API name for a logical type — what table-schema JSON says.
  """
  @spec api_type(logical_type()) :: {:ok, String.t()} | {:error, {:unsupported_type, term()}}
  def api_type({:numeric, precision, scale}), do: {:ok, "NUMERIC(#{precision},#{scale})"}
  def api_type(@map_type), do: {:ok, @map_api}

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
      :error -> compound_from_api(name)
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
  | `{:map, :string, :string}` | object; a non-string value becomes its JSON text | map of binaries |
  | `:variant` | any JSON value | the decoded term, unchanged |

  A map takes an object and nothing else — the passthrough path's `read_json`
  refuses an array or a scalar, and this path must agree with it. A map value
  that is `null` stays `nil`; a key must be a string, which JSON guarantees.

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

  def value_from_json(@map_type, value) when is_map(value) and not is_struct(value) do
    if Enum.all?(value, fn {key, _value} -> is_binary(key) end),
      do: {:ok, Map.new(value, fn {key, entry} -> {key, map_value_text(entry)} end)},
      else: invalid(@map_type, value)
  end

  def value_from_json(:variant, value) when not is_struct(value), do: {:ok, value}

  def value_from_json(type, value), do: invalid(type, value)

  defp map_value_text(nil), do: nil
  defp map_value_text(value) when is_binary(value), do: value
  defp map_value_text(value), do: JSON.encode!(value)

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
  The first field Explorer cannot write, if any.
  """
  @spec explorer_unwritable(t()) :: {:ok, Field.t()} | :none
  def explorer_unwritable(%__MODULE__{fields: fields}) do
    case Enum.find(fields, &match?({:error, _no_dtype}, explorer_dtype(&1.type))) do
      nil -> :none
      field -> {:ok, field}
    end
  end

  @doc """
  The `column_name dtype` pairs for the fields Explorer can read — what a CSV
  load parses with. A CSV cannot carry a map or a variant, so those fields
  are left out; a CSV that names one anyway parses it as text, and the row
  validator then refuses the value.
  """
  @spec readable_explorer_dtypes(t()) :: [{String.t(), term()}]
  def readable_explorer_dtypes(%__MODULE__{fields: fields}) do
    for field <- fields, {:ok, dtype} <- [explorer_dtype(field.type)], do: {field.name, dtype}
  end

  @doc """
  Whether a column of this type can serve in a clustering key.

  A map or a variant sorts, but no stats bound it, so the pruner could never
  use the key — the sort would be a tax with no return. Refused up front.
  """
  @spec clustering_type?(logical_type()) :: boolean()
  def clustering_type?(@map_type), do: false
  def clustering_type?(:variant), do: false
  def clustering_type?(_type), do: true

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

  defp compound_from_api(name) do
    name = String.trim(name)

    cond do
      Regex.match?(~r/^MAP\(\s*STRING\s*,\s*STRING\s*\)$/i, name) ->
        {:ok, @map_type}

      match = Regex.run(~r/^NUMERIC\((\d+),\s*(\d+)\)$/i, name) ->
        [_match, precision, scale] = match
        validate_type({:numeric, String.to_integer(precision), String.to_integer(scale)})

      true ->
        {:error, {:unsupported_type, name}}
    end
  end

  defp compound_from_duckdb(name) do
    name = String.trim(name)

    cond do
      Regex.match?(~r/^MAP\(\s*VARCHAR\s*,\s*VARCHAR\s*\)$/i, name) ->
        {:ok, @map_type}

      match = Regex.run(~r/^DECIMAL\((\d+),\s*(\d+)\)$/i, name) ->
        [_match, precision, scale] = match
        validate_type({:numeric, String.to_integer(precision), String.to_integer(scale)})

      true ->
        {:error, {:unsupported_type, name}}
    end
  end
end
