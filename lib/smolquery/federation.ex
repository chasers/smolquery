defmodule Smolquery.Federation do
  @moduledoc """
  The DuckDB side of a federated Postgres connection (T-322, T-324).

  One place builds the `ATTACH` a registered connection becomes, and one place
  scrubs what its failures say. Both the API's connectivity check and the query
  path's per-job attach go through here, so the statement a test exercises is
  the statement a query runs.

  ## `READ_ONLY` is not optional

  The planner's read-only gate already refuses anything but a single SELECT, so
  no DML should ever reach an attached database. `READ_ONLY` on the attachment
  is the second lock, in the engine rather than the parser: a gap in the first
  one cannot become a write to somebody's production database.

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

  @probe_extensions [:postgres]
  @probe_timeout_ms 10_000

  @doc """
  The `ATTACH` that makes `connection` reachable under its own name.
  """
  @spec attach_statement(Connection.t()) :: {:ok, String.t()} | {:error, term()}
  def attach_statement(%Connection{} = connection) do
    with {:ok, string} <- Connection.connection_string(connection) do
      {:ok,
       "ATTACH #{Identifier.sql_string(string)} AS " <>
         "#{Identifier.quote_name!(connection.name)} (TYPE postgres, READ_ONLY)"}
    end
  end

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
    with {:ok, statement} <- attach_statement(connection) do
      name = :"federation_probe_#{:erlang.unique_integer([:positive])}"

      case Engine.start_link(name: name, extensions: @probe_extensions) do
        {:ok, pid} ->
          try do
            run_probe(name, statement, connection)
          after
            Supervisor.stop(pid, :normal)
          end

        {:error, reason} ->
          {:error, scrub(reason, connection)}
      end
    end
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
  def discovery_query(name) do
    """
    select schemaname, relname, n_live_tup
    from #{Identifier.quote_name!(name)}.pg_catalog.pg_stat_user_tables
    order by n_live_tup desc
    limit 10;
    """
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
    case Connection.connection_string(connection) do
      {:ok, string} -> {:federation_error, connection.name, redact(reason, string)}
      {:error, _unopenable} -> {:federation_error, connection.name, :unavailable}
    end
  end

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
      {:ok, string} -> redact(reason, string)
      :error -> reason
    end
  end

  def redact_statement(reason, _statement), do: reason

  defp literal("\\" <> <<escaped::binary-size(1), rest::binary>>, acc),
    do: literal(rest, acc <> escaped)

  defp literal("''" <> rest, acc), do: literal(rest, acc <> "'")
  defp literal("'" <> _rest, acc), do: {:ok, acc}

  defp literal(<<char::binary-size(1), rest::binary>>, acc), do: literal(rest, acc <> char)

  defp literal("", _acc), do: :error

  defp redact(reason, string) do
    reason
    |> inspect(limit: :infinity, printable_limit: :infinity)
    |> String.replace(string, "<redacted>")
  end

  defp run_probe(name, statement, connection) do
    with {:ok, _attached} <- Engine.try_query(name, statement, [], @probe_timeout_ms),
         {:ok, _row} <-
           Engine.try_query(
             name,
             "SELECT 1 FROM #{Identifier.quote_name!(connection.name)}.information_schema.schemata LIMIT 1",
             [],
             @probe_timeout_ms
           ) do
      :ok
    else
      {:error, reason} -> {:error, scrub(reason, connection)}
    end
  end
end
