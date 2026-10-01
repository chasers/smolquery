defmodule Smolquery.Federation do
  @moduledoc """
  The DuckDB side of a federated connection: a Postgres database (T-322,
  T-324) or another DuckLake (T-610).

  One place builds the `ATTACH` a registered connection becomes, and one place
  scrubs what its failures say. Both the API's connectivity check and the query
  path's per-job attach go through here, so the statement a test exercises is
  the statement a query runs.

  ## `READ_ONLY` is not optional

  The planner's read-only gate already refuses anything but a single SELECT, so
  no DML should ever reach an attached database. `READ_ONLY` on the attachment
  is the second lock, in the engine rather than the parser: a gap in the first
  one cannot become a write to somebody's production database.

  ## A DuckLake connection

  A DuckLake connection attaches the other lake through its Postgres metadata
  database, read-only, with the connection's `data_path`:

      ATTACH 'ducklake:postgres:<libpq>' AS "<name>" (READ_ONLY, DATA_PATH '<data_path>')

  DuckLake accepts its own data path on an existing lake and refuses any
  other, so a wrong path fails the attach, at `probe/1` and at query time,
  rather than reading somewhere unexpected. When the connection has S3
  credentials, a `CREATE TEMPORARY SECRET` scoped to the data path comes
  first. DuckDB answers an `s3://` read with the secret whose scope is the
  longest prefix of it, and the sealed tier's secret covers its whole bucket,
  so a lake in that bucket would answer for some of the node's own segments:
  `check/2` refuses one, and the planner calls it before any attach.

  ## A DuckLake connection trusts that lake's catalog

  The query engine's lockdown (`Smolquery.QueryService.Runner`) is unchanged
  by a DuckLake connection and needs no widening: DuckLake reads an attached
  lake's files itself, and `allowed_directories` does not apply to those
  reads (measured: after lockdown a query through the attached lake reads,
  while `read_parquet` of the same files is refused). The flip side is the
  boundary to know: the files a query reads are the ones the remote catalog
  lists, wherever they are, and `data_path` does not confine a catalog whose
  entries are absolute paths. Whoever controls a registered lake's catalog
  can therefore have the node read any Parquet file it can open, the node's
  own included. Registering a connection takes the credential key, as it
  does for Postgres; register only lakes whose catalog you trust as you trust
  this deployment's.

  ## Failures are scrubbed before anyone sees them

  DuckDB reports a failed `ATTACH` by quoting the connection string back, and
  that string carries the password. The error travels into a job's `:error`
  field, an API envelope, a log line, and the job history — four places a
  credential must not reach. `scrub/2` replaces the string with the connection's
  name before the reason leaves this module, so the caller learns which
  connection failed and nothing about how to open it.
  """

  alias Smolquery.Catalog.Connection
  alias Smolquery.Engine
  alias Smolquery.Identifier

  @probe_timeout_ms 10_000

  @doc """
  The `ATTACH` that makes `connection` reachable under its own name.
  """
  @spec attach_statement(Connection.t()) :: {:ok, String.t()} | {:error, term()}
  def attach_statement(%Connection{kind: "ducklake", data_path: nil} = connection),
    do: {:error, {:missing_data_path, connection.name}}

  def attach_statement(%Connection{kind: "ducklake"} = connection) do
    with {:ok, string} <- Connection.connection_string(connection) do
      {:ok,
       "ATTACH #{Identifier.sql_string("ducklake:postgres:" <> string)} AS " <>
         "#{Identifier.quote_name!(connection.name)} " <>
         "(READ_ONLY, DATA_PATH #{Identifier.sql_string(connection.data_path)})"}
    end
  end

  def attach_statement(%Connection{} = connection) do
    with {:ok, string} <- Connection.connection_string(connection) do
      {:ok,
       "ATTACH #{Identifier.sql_string(string)} AS " <>
         "#{Identifier.quote_name!(connection.name)} (TYPE postgres, READ_ONLY)"}
    end
  end

  @doc """
  Every statement a job runs to reach `connection`, in order: for a DuckLake
  connection with S3 credentials, the storage secret, then the attach.
  """
  @spec statements(Connection.t()) :: {:ok, [String.t()]} | {:error, term()}
  def statements(%Connection{kind: "ducklake", data_path: nil} = connection),
    do: {:error, {:missing_data_path, connection.name}}

  def statements(%Connection{} = connection) do
    with {:ok, secret} <- secret_statements(connection),
         {:ok, attach} <- attach_statement(connection) do
      {:ok, secret ++ [attach]}
    end
  end

  @doc """
  Refuses a DuckLake connection whose data path lies in a bucket the sealed
  tier reads (`sealed_prefixes`, as `Smolquery.EngineSecrets.sealed_prefixes/1`
  gives them): its scoped secret would answer for the node's own segments.
  """
  @spec check(Connection.t(), [String.t()]) :: :ok | {:error, term()}
  def check(%Connection{kind: "ducklake", data_path: "s3://" <> _ = path} = connection, sealed) do
    if Enum.any?(sealed, &overlaps?(path, &1)),
      do: {:error, {:federated_path_in_sealed_bucket, connection.name}},
      else: :ok
  end

  def check(%Connection{}, _sealed), do: :ok

  defp overlaps?(path, prefix) do
    path = if String.ends_with?(path, "/"), do: path, else: path <> "/"
    String.starts_with?(path, prefix) or String.starts_with?(prefix, path)
  end

  @doc """
  The DuckDB extensions a job loads before `statements/1` will run:
  `postgres` for every kind, since a DuckLake's metadata is Postgres, and
  `ducklake` for a DuckLake.
  """
  @spec extensions(Connection.t()) :: [atom()]
  def extensions(%Connection{kind: "ducklake"}), do: [:postgres, :ducklake]
  def extensions(%Connection{}), do: [:postgres]

  defp secret_statements(%Connection{kind: "ducklake", storage: %{key_id: key_id}} = connection) do
    with {:ok, secret} <- Connection.storage_secret(connection) do
      options =
        [
          "TYPE s3",
          "KEY_ID #{Identifier.sql_string(key_id)}",
          "SECRET #{Identifier.sql_string(secret)}",
          "SCOPE #{Identifier.sql_string(connection.data_path)}"
        ] ++ region_option(connection.storage) ++ endpoint_options(connection.storage)

      name = Identifier.quote_name!("federated_" <> connection.name)
      body = Enum.join(options, ", ")

      {:ok, ["CREATE OR REPLACE TEMPORARY SECRET #{name} (#{body})"]}
    end
  end

  defp secret_statements(_connection), do: {:ok, []}

  defp region_option(%{region: region}), do: ["REGION #{Identifier.sql_string(region)}"]
  defp region_option(_storage), do: []

  defp endpoint_options(%{endpoint: endpoint} = storage) do
    uri = URI.parse(endpoint)

    {host, ssl} =
      case uri do
        %URI{scheme: scheme, host: host, port: port} when scheme in ["http", "https"] ->
          {if(port, do: "#{host}:#{port}", else: host), scheme == "https"}

        _bare ->
          {endpoint, true}
      end

    [
      "ENDPOINT #{Identifier.sql_string(host)}",
      "URL_STYLE #{Identifier.sql_string(Map.get(storage, :url_style, "path"))}",
      "USE_SSL #{ssl}"
    ]
  end

  defp endpoint_options(%{url_style: style}), do: ["URL_STYLE #{Identifier.sql_string(style)}"]
  defp endpoint_options(_storage), do: []

  @doc """
  Whether `connection` opens: attaches it in a throwaway engine and reads one
  row through it.

  The engine is private and short-lived, like a query job's. A connection that
  cannot be reached is an error the operator can act on, and never a crash —
  a bad host is the expected case here, not an exceptional one. A call that
  times out is caught for the same reason: the exit reason carries the
  `ATTACH` statement, password and all, so it must reach `scrub/2` rather
  than a crash report.
  """
  @spec probe(Connection.t()) :: :ok | {:error, term()}
  def probe(%Connection{} = connection) do
    with {:ok, _tables} <- in_probe_engine(connection, &run_probe(&1, connection)), do: :ok
  end

  @doc """
  A DuckLake connection's tables, as `{schema, table}`, read by attaching it
  in a throwaway engine as `probe/1` does. The connections page opens the
  editor on the first: a lake's catalog is not readable through the planner
  the way a Postgres database's `pg_catalog` is.
  """
  @spec tables(Connection.t()) :: {:ok, [{String.t(), String.t()}]} | {:error, term()}
  def tables(%Connection{kind: "ducklake"} = connection),
    do: in_probe_engine(connection, &lake_tables(&1, connection))

  defp in_probe_engine(connection, run) do
    with {:ok, statements} <- statements(connection) do
      name = :"federation_probe_#{:erlang.unique_integer([:positive])}"

      case Engine.start_link(name: name, extensions: probe_extensions(connection)) do
        {:ok, pid} ->
          try do
            with :ok <- run_statements(name, statements, connection), do: run.(name)
          after
            Supervisor.stop(pid, :normal)
          end

        {:error, reason} ->
          {:error, scrub(reason, connection)}
      end
    end
  end

  defp probe_extensions(%Connection{kind: "ducklake"} = connection),
    do: extensions(connection) ++ [:httpfs]

  defp probe_extensions(connection), do: extensions(connection)

  defp run_statements(name, statements, connection) do
    Enum.reduce_while(statements, :ok, fn statement, :ok ->
      case Engine.try_query(name, statement, [], @probe_timeout_ms) do
        {:ok, _result} -> {:cont, :ok}
        {:error, reason} -> {:halt, {:error, scrub(reason, connection)}}
      end
    end)
  end

  @doc """
  The SQL that ranks a connection's user tables by live rows, ten at most.

  The connections page opens the editor on it. The planner takes one SELECT,
  so a page cannot hand the editor a script that finds the largest table and
  then reads it; this is the first half, and the operator writes the second.
  `pg_stat_user_tables` is read through the attached catalog, so the statement
  runs through the planner like any federated query.
  """
  @spec discovery_query(String.t()) :: String.t()
  def discovery_query(name) when is_binary(name) do
    """
    select schemaname, relname, n_live_tup
    from #{Identifier.quote_name!(name)}.pg_catalog.pg_stat_user_tables
    order by n_live_tup desc
    limit 10;
    """
  end

  @doc """
  The SQL that reads the first rows of a DuckLake connection's `table`.
  """
  @spec table_query(String.t(), {String.t(), String.t()}) :: String.t()
  def table_query(name, {schema, table}) do
    "select *\nfrom #{Identifier.quote_name!(name)}.#{Identifier.quote_name!(schema)}." <>
      "#{Identifier.quote_name!(table)}\nlimit 100;\n"
  end

  @doc """
  Replaces `connection`'s connection string, wherever it appears in `reason`,
  with the connection's name.

  Works on the inspected form because a DuckDB error is a struct carrying the
  message as a field, and the password can sit anywhere inside it. Losing the
  original term is the point: what comes back is safe to log, to store in job
  history, and to put in an error envelope.
  """
  @spec scrub(term(), Connection.t()) :: term()
  def scrub(reason, %Connection{} = connection) do
    with {:ok, string} <- Connection.connection_string(connection),
         {:ok, secret} <- Connection.storage_secret(connection) do
      {:federation_error, connection.name, reason |> redact(string) |> redact_secret(secret)}
    else
      {:error, _unopenable} -> {:federation_error, connection.name, :unavailable}
    end
  end

  defp redact_secret(reason, nil), do: reason
  defp redact_secret(reason, secret), do: String.replace(reason, secret, "<redacted>")

  @doc """
  Redacts an `ATTACH`'s own connection string out of the error it produced.

  The runner holds the statement that failed but not the connection it came
  from, so the credential is recovered from the statement itself: the string
  literal an `attach_statement/1` puts between the first pair of quotes.
  Anything else passes through untouched, so this is safe to run over every
  failed statement rather than only the ones a caller believes are attaches.
  """
  @spec redact_statement(term(), String.t()) :: term()
  def redact_statement(reason, "ATTACH '" <> rest) do
    case literal(rest, "") do
      {:ok, "ducklake:postgres:" <> string} ->
        redact(reason, string)

      {:ok, string} ->
        redact(reason, string)

      :error ->
        reason
    end
  end

  def redact_statement(reason, "CREATE OR REPLACE TEMPORARY SECRET " <> rest) do
    case secret_literal(rest, "") do
      {:ok, secret} when secret != "" -> redact(reason, secret)
      _no_secret -> reason
    end
  end

  def redact_statement(reason, _statement), do: reason

  defp literal("\\" <> <<escaped::binary-size(1), rest::binary>>, acc),
    do: literal(rest, acc <> escaped)

  defp literal("''" <> rest, acc), do: literal(rest, acc <> "'")
  defp literal("'" <> _rest, acc), do: {:ok, acc}

  defp literal(<<char::binary-size(1), rest::binary>>, acc), do: literal(rest, acc <> char)

  defp literal("", _acc), do: :error

  defp secret_literal("'" <> rest, before) do
    with {:ok, value, after_literal} <- literal_with_rest(rest, "") do
      if String.ends_with?(before, "SECRET "),
        do: {:ok, value},
        else: secret_literal(after_literal, "")
    end
  end

  defp secret_literal(<<char::binary-size(1), rest::binary>>, before),
    do: secret_literal(rest, before <> char)

  defp secret_literal("", _before), do: :error

  defp literal_with_rest("''" <> rest, acc), do: literal_with_rest(rest, acc <> "'")
  defp literal_with_rest("'" <> rest, acc), do: {:ok, acc, rest}

  defp literal_with_rest(<<char::binary-size(1), rest::binary>>, acc),
    do: literal_with_rest(rest, acc <> char)

  defp literal_with_rest("", _acc), do: :error

  defp redact(reason, string) do
    reason
    |> inspect(limit: :infinity, printable_limit: :infinity)
    |> String.replace(string, "<redacted>")
  end

  defp run_probe(name, %Connection{kind: "ducklake"} = connection) do
    with {:ok, tables} <- lake_tables(name, connection) do
      case tables do
        [] -> {:ok, []}
        [first | _rest] -> read_first(name, connection, first)
      end
    end
  end

  defp run_probe(name, connection) do
    case Engine.try_query(
           name,
           "SELECT 1 FROM #{Identifier.quote_name!(connection.name)}.information_schema.schemata LIMIT 1",
           [],
           @probe_timeout_ms
         ) do
      {:ok, _row} -> {:ok, []}
      {:error, reason} -> {:error, scrub(reason, connection)}
    end
  end

  defp lake_tables(name, connection) do
    case Engine.try_query(
           name,
           "SELECT schema_name, table_name FROM duckdb_tables() " <>
             "WHERE database_name = $1 ORDER BY schema_name, table_name",
           [connection.name],
           @probe_timeout_ms
         ) do
      {:ok, %{rows: rows}} -> {:ok, Enum.map(rows, &List.to_tuple/1)}
      {:error, reason} -> {:error, scrub(reason, connection)}
    end
  end

  defp read_first(name, connection, {schema, table} = first) do
    sql =
      "SELECT * FROM #{Identifier.quote_name!(connection.name)}.#{Identifier.quote_name!(schema)}." <>
        "#{Identifier.quote_name!(table)} LIMIT 1"

    case Engine.try_query(name, sql, [], @probe_timeout_ms) do
      {:ok, _row} -> {:ok, [first]}
      {:error, reason} -> {:error, scrub(reason, connection)}
    end
  end
end
