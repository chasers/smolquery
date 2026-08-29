defmodule SmolqueryPg.Session do
  @moduledoc """
  One authenticated connection's state, and what each statement answers
  (PL-58).

  A session holds what the protocol makes stateful: the transaction status
  the prompt shows, the `SET` values a client reads back with `SHOW`, the
  key a `CancelRequest` must quote, and the extended protocol's prepared
  statements and portals. The socket is the handler's; this module only
  answers messages to send.

  ## Routing

  The planner's gate refuses everything but one `SELECT` over
  `dataset.table`, and a client sends a lot more than that on connect.
  `run/2` classifies each statement by its leading keyword before anything
  runs:

  | class | answer |
  |---|---|
  | `SELECT`, `WITH`, `VALUES`, `(` | `Smolquery.QueryService.Client`; the frame as rows |
  | `SET`, `RESET`, `SHOW` | remembered in the session; `SHOW` reads it back |
  | `BEGIN`, `START TRANSACTION`, `COMMIT`, `END`, `ROLLBACK`, `ABORT` | the transaction status only |
  | empty | `EmptyQueryResponse` |
  | anything else | `0A000 feature_not_supported` |

  A transaction pins nothing: each statement reads its own snapshot, as an
  HTTP query does. The status exists because clients track it — a `BEGIN`
  answered with an error would abort a driver's connection setup, and a
  failed statement inside a block must fail every later one until the
  block ends, or a client that expects Postgres's behaviour reads a
  half-applied batch as whole.

  ## The extended protocol

  `Parse` stores a statement with its parameter OIDs (`SmolqueryPg.Params`
  fills in the ones the client left unspecified). `Bind` substitutes the
  values into the SQL and opens a portal. `Describe` names a statement's or
  a portal's columns. `Execute` answers a portal's rows, `max_rows` at a
  time, and `PortalSuspended` when more remain.

  A `Describe` of a statement must answer column types before any value is
  bound, so it runs the query service's `describe` mode over the SQL with
  each `$n` cast to a typed `NULL`. A statement with no parameters is run
  instead, and its rows are kept for the portal that binds it next, so the
  Parse/Describe/Bind/Execute a driver sends for one query costs one job.
  A portal runs when it is first described or executed, and keeps its rows
  for the `Execute`s that follow.

  ## Cancellation

  The session registers `{backend_pid, secret_key}` in the edge's cancel
  registry, and publishes the running job's id there while a query runs.
  A `CancelRequest` on another connection looks the key up and cancels the
  job through the client module.
  """

  alias Explorer.DataFrame
  alias Smolquery.Engine.Frame
  alias Smolquery.QueryService.Client
  alias Smolquery.QueryService.Job
  alias SmolqueryPg.Errors
  alias SmolqueryPg.Params
  alias SmolqueryPg.PgCatalog
  alias SmolqueryPg.Protocol
  alias SmolqueryPg.Runtime
  alias SmolqueryPg.Statements
  alias SmolqueryPg.Types

  @server_version "14.10"

  @defaults %{
    "server_version" => @server_version,
    "server_encoding" => "UTF8",
    "client_encoding" => "UTF8",
    "DateStyle" => "ISO, MDY",
    "IntervalStyle" => "postgres",
    "TimeZone" => "UTC",
    "integer_datetimes" => "on",
    "standard_conforming_strings" => "on",
    "is_superuser" => "off",
    "application_name" => "",
    "search_path" => "public",
    "transaction_isolation" => "read committed",
    "statement_timeout" => "0",
    "extra_float_digits" => "1"
  }

  @reported ~w(server_version server_encoding client_encoding DateStyle IntervalStyle TimeZone
               integer_datetimes standard_conforming_strings is_superuser application_name
               session_authorization)

  @enforce_keys [:runtime, :user, :backend_pid, :secret_key]
  defstruct [
    :runtime,
    :user,
    :database,
    :backend_pid,
    :secret_key,
    txn: :idle,
    settings: %{},
    statements: %{},
    portals: %{}
  ]

  @typedoc """
  What one statement answered: the rows of a query, or a command tag.
  `pre` holds the notices and parameter reports to send first. `columns`
  is `nil` for a command.
  """
  @type outcome :: %{
          columns: [{String.t(), Types.dtype(), boolean()}] | nil,
          rows: [map()],
          tag: String.t(),
          pre: [iodata()]
        }

  @type prepared :: %{sql: String.t(), oids: [pos_integer()], outcome: outcome() | nil}

  @type portal :: %{
          sql: String.t(),
          formats: [0 | 1],
          outcome: outcome() | nil,
          offset: non_neg_integer()
        }

  @type t :: %__MODULE__{
          runtime: Runtime.t(),
          user: String.t(),
          database: String.t() | nil,
          backend_pid: pos_integer(),
          secret_key: pos_integer(),
          txn: Protocol.ready_status(),
          settings: %{String.t() => String.t()},
          statements: %{String.t() => prepared()},
          portals: %{String.t() => portal()}
        }

  @doc """
  A session for a client that opened with `params`, registered for
  cancellation under a fresh backend key.
  """
  @spec new(Runtime.t(), %{String.t() => String.t()}) :: t()
  def new(%Runtime{} = runtime, params) do
    user = Map.get(params, "user", "")

    session = %__MODULE__{
      runtime: runtime,
      user: user,
      database: Map.get(params, "database"),
      backend_pid: :rand.uniform(2_147_483_647),
      secret_key: :rand.uniform(2_147_483_647),
      settings:
        @defaults
        |> Map.put("application_name", Map.get(params, "application_name", ""))
        |> Map.put("session_authorization", user)
    }

    {:ok, _owner} = Registry.register(Runtime.cancels(runtime.name), cancel_key(session), nil)

    session
  end

  defp cancel_key(%__MODULE__{backend_pid: pid, secret_key: key}), do: {pid, key}

  @doc """
  Cancels the job the session identified by `{pid, key}` is running, if
  any. Called on the connection that carried the `CancelRequest`.
  """
  @spec cancel(Runtime.t(), integer(), integer()) :: :ok
  def cancel(%Runtime{} = runtime, pid, key) do
    case Registry.lookup(Runtime.cancels(runtime.name), {pid, key}) do
      [{_owner, {query_name, job_id}}] -> Client.cancel(query_name, job_id)
      _idle_or_unknown -> :ok
    end
  end

  @doc """
  What the server sends after `AuthenticationOk`: every reported parameter,
  then the backend key.
  """
  @spec startup_messages(t()) :: [iodata()]
  def startup_messages(%__MODULE__{} = session) do
    Enum.map(@reported, &Protocol.parameter_status(&1, Map.fetch!(session.settings, &1))) ++
      [Protocol.backend_key_data(session.backend_pid, session.secret_key)]
  end

  @doc """
  Runs every statement in `sql` (a simple `Query`) and answers the messages
  to send, without the closing `ReadyForQuery` — the handler adds it from
  the session's transaction status. A simple query also closes the unnamed
  statement and portal, as Postgres does.
  """
  @spec execute(t(), String.t()) :: {[iodata()], t()}
  def execute(%__MODULE__{} = session, sql) do
    session = session |> drop_statement("") |> drop_portal("")

    case Statements.split(sql) do
      [] ->
        {[Protocol.empty_query_response()], session}

      statements ->
        Enum.reduce_while(statements, {[], session}, &run_next/2)
    end
  end

  defp run_next(statement, {messages, session}) do
    case run(session, statement) do
      {:ok, outcome, session} -> {:cont, {messages ++ render(outcome, []), session}}
      {:error, error, session} -> {:halt, {messages ++ [error_message(error)], session}}
    end
  end

  defp render(%{columns: nil} = outcome, _formats),
    do: outcome.pre ++ [Protocol.command_complete(outcome.tag)]

  defp render(outcome, formats) do
    rows = Enum.map(outcome.rows, &data_row(&1, outcome.columns, formats))

    outcome.pre ++
      [Protocol.row_description(fields(outcome.columns, formats))] ++
      rows ++ [Protocol.command_complete(outcome.tag)]
  end

  defp fields(columns, formats) do
    Enum.zip_with(columns, resolved_formats(formats, length(columns)), fn
      {name, dtype, json?}, format ->
        {oid, typlen, typmod} = Types.describe(dtype, json?)

        %{name: name, oid: oid, typlen: typlen, typmod: typmod, format: format}
    end)
  end

  defp data_row(row, columns, formats) do
    columns
    |> Enum.zip_with(resolved_formats(formats, length(columns)), fn {name, dtype, json?},
                                                                    format ->
      Types.encode(dtype, json?, Map.fetch!(row, name), format)
    end)
    |> Protocol.data_row()
  end

  defp resolved_formats([], count), do: List.duplicate(0, count)
  defp resolved_formats([format], count), do: List.duplicate(format, count)

  defp resolved_formats(formats, count),
    do: formats |> Enum.concat(List.duplicate(0, count)) |> Enum.take(count)

  defp error_message({code, message}), do: Protocol.error_response(code, message)

  @doc """
  `Parse`: stores `sql` under `name` with its parameter OIDs.
  """
  @spec parse(t(), String.t(), String.t(), [non_neg_integer()]) ::
          {:ok, [iodata()], t()} | {:error, Errors.wire_error(), t()}
  def parse(%__MODULE__{} = session, name, sql, declared) do
    case Statements.split(sql) do
      [] ->
        {:ok, [Protocol.parse_complete()], put_statement(session, name, "", [])}

      [statement] ->
        oids = Params.oids(statement, declared)

        {:ok, [Protocol.parse_complete()], put_statement(session, name, statement, oids)}

      _several ->
        {:error, {"42601", "cannot insert multiple commands into a prepared statement"},
         failed(session)}
    end
  end

  defp put_statement(session, name, sql, oids) do
    statement = %{sql: sql, oids: oids, outcome: nil}

    %{session | statements: Map.put(session.statements, name, statement)}
  end

  @doc """
  `Bind`: opens portal `portal` over statement `statement` with `values`
  substituted for its parameters, answering in `formats`.
  """
  @spec bind(t(), String.t(), String.t(), [0 | 1], [binary() | nil], [0 | 1]) ::
          {:ok, [iodata()], t()} | {:error, Errors.wire_error(), t()}
  def bind(%__MODULE__{} = session, portal, statement, param_formats, values, formats) do
    with {:ok, prepared} <- fetch_statement(session, statement),
         :ok <- arity(prepared, values),
         {:ok, sql} <-
           Params.substitute(prepared.sql, typed(prepared.oids, param_formats, values)) do
      {session, outcome} = take_outcome(session, statement, prepared, values)
      opened = %{sql: sql, formats: formats, outcome: outcome, offset: 0}

      {:ok, [Protocol.bind_complete()],
       %{session | portals: Map.put(session.portals, portal, opened)}}
    else
      {:error, {:invalid_parameter, index, reason}} ->
        {:error, {"22P02", "parameter $#{index} could not be read: #{inspect(reason)}"},
         failed(session)}

      {:error, error} ->
        {:error, error, failed(session)}
    end
  end

  defp fetch_statement(session, name) do
    case Map.fetch(session.statements, name) do
      {:ok, prepared} -> {:ok, prepared}
      :error -> {:error, {"26000", ~s|prepared statement "#{name}" does not exist|}}
    end
  end

  defp fetch_portal(session, name) do
    case Map.fetch(session.portals, name) do
      {:ok, portal} -> {:ok, portal}
      :error -> {:error, {"34000", ~s|portal "#{name}" does not exist|}}
    end
  end

  defp arity(%{oids: oids}, values) when length(oids) == length(values), do: :ok

  defp arity(%{oids: oids}, values) do
    {:error,
     {"08P01",
      "bind message supplies #{length(values)} parameters, " <>
        "but prepared statement requires #{length(oids)}"}}
  end

  defp typed(oids, param_formats, values) do
    Enum.zip_with([oids, resolved_formats(param_formats, length(values)), values], fn
      [oid, format, value] -> {oid, format, value}
    end)
  end

  defp take_outcome(session, name, %{outcome: outcome} = prepared, []) when outcome != nil do
    statements = Map.put(session.statements, name, %{prepared | outcome: nil})

    {%{session | statements: statements}, outcome}
  end

  defp take_outcome(session, _name, _prepared, _values), do: {session, nil}

  @doc """
  `Describe`: a statement's parameters and columns, or a portal's columns.
  """
  @spec describe(t(), :statement | :portal, String.t()) ::
          {:ok, [iodata()], t()} | {:error, Errors.wire_error(), t()}
  def describe(%__MODULE__{} = session, :statement, name) do
    with {:ok, prepared} <- fetch_statement(session, name),
         {:ok, columns, session} <- statement_columns(session, name, prepared) do
      {:ok, [Protocol.parameter_description(prepared.oids), description(columns, [])], session}
    else
      {:error, error} -> {:error, error, failed(session)}
      {:error, error, session} -> {:error, error, session}
    end
  end

  def describe(%__MODULE__{} = session, :portal, name) do
    with {:ok, portal} <- fetch_portal(session, name),
         {:ok, portal, session} <- ensure_run(session, name, portal) do
      {:ok, [description(portal_columns(portal), portal.formats)], session}
    else
      {:error, error} -> {:error, error, failed(session)}
      {:error, error, session} -> {:error, error, session}
    end
  end

  defp portal_columns(%{outcome: nil}), do: nil
  defp portal_columns(%{outcome: outcome}), do: outcome.columns

  defp description(nil, _formats), do: Protocol.no_data()
  defp description(columns, formats), do: Protocol.row_description(fields(columns, formats))

  defp statement_columns(session, _name, %{outcome: %{columns: columns}}),
    do: {:ok, columns, session}

  defp statement_columns(session, name, %{sql: sql, oids: []} = prepared) do
    case run(session, sql) do
      {:ok, outcome, session} ->
        statements = Map.put(session.statements, name, %{prepared | outcome: outcome})

        {:ok, outcome.columns, %{session | statements: statements}}

      {:error, error, session} ->
        {:error, error, session}
    end
  end

  defp statement_columns(session, _name, %{sql: sql, oids: oids}) do
    if classify(sql) == :query,
      do: describe_query(session, Params.with_typed_nulls(sql, oids)),
      else: {:ok, nil, session}
  end

  defp describe_query(session, sql) do
    case Client.query(session.runtime.query_name, sql, describe: true) do
      {:ok, %Job{state: :done}, %DataFrame{} = frame} ->
        columns =
          frame
          |> DataFrame.to_rows()
          |> Enum.map(fn %{"column_name" => name, "column_type" => type} ->
            {name, {:duckdb, type}, false}
          end)

        {:ok, columns, session}

      {:ok, %Job{error: reason}, _frame} ->
        fail(session, reason)

      {:error, reason} ->
        fail(session, reason)
    end
  end

  @doc """
  `Execute`: up to `max_rows` of portal `name` (`0` for all), then
  `CommandComplete`, or `PortalSuspended` when rows remain.
  """
  @spec execute_portal(t(), String.t(), non_neg_integer()) ::
          {:ok, [iodata()], t()} | {:error, Errors.wire_error(), t()}
  def execute_portal(%__MODULE__{} = session, name, max_rows) do
    with {:ok, portal} <- fetch_portal(session, name),
         {:ok, portal, session} <- ensure_run(session, name, portal) do
      {messages, portal} = portal_rows(portal, max_rows)

      {:ok, messages, %{session | portals: Map.put(session.portals, name, portal)}}
    else
      {:error, error} -> {:error, error, failed(session)}
      {:error, error, session} -> {:error, error, session}
    end
  end

  defp portal_rows(%{sql: ""} = portal, _max_rows),
    do: {[Protocol.empty_query_response()], portal}

  defp portal_rows(%{outcome: %{columns: nil} = outcome} = portal, _max_rows),
    do: {outcome.pre ++ [Protocol.command_complete(outcome.tag)], portal}

  defp portal_rows(%{outcome: outcome, offset: offset} = portal, max_rows) do
    remaining = Enum.drop(outcome.rows, offset)
    {page, rest} = if max_rows == 0, do: {remaining, []}, else: Enum.split(remaining, max_rows)
    rows = Enum.map(page, &data_row(&1, outcome.columns, portal.formats))
    pre = if offset == 0, do: outcome.pre, else: []

    if rest == [] do
      {pre ++ rows ++ [Protocol.command_complete("SELECT #{length(rows)}")],
       %{portal | offset: offset + length(page)}}
    else
      {pre ++ rows ++ [Protocol.portal_suspended()], %{portal | offset: offset + length(page)}}
    end
  end

  defp ensure_run(session, _name, %{outcome: outcome} = portal) when outcome != nil,
    do: {:ok, portal, session}

  defp ensure_run(session, _name, %{sql: ""} = portal), do: {:ok, portal, session}

  defp ensure_run(session, name, portal) do
    case run(session, portal.sql) do
      {:ok, outcome, session} ->
        portal = %{portal | outcome: outcome}

        {:ok, portal, %{session | portals: Map.put(session.portals, name, portal)}}

      {:error, error, session} ->
        {:error, error, session}
    end
  end

  @doc """
  `Close`: drops a statement or a portal. Closing one that does not exist
  is not an error.
  """
  @spec close(t(), :statement | :portal, String.t()) :: {:ok, [iodata()], t()}
  def close(%__MODULE__{} = session, :statement, name),
    do: {:ok, [Protocol.close_complete()], drop_statement(session, name)}

  def close(%__MODULE__{} = session, :portal, name),
    do: {:ok, [Protocol.close_complete()], drop_portal(session, name)}

  defp drop_statement(session, name),
    do: %{session | statements: Map.delete(session.statements, name)}

  defp drop_portal(session, name), do: %{session | portals: Map.delete(session.portals, name)}

  @doc """
  Runs one statement and answers its outcome. An error marks an open
  transaction block failed.
  """
  @spec run(t(), String.t()) :: {:ok, outcome(), t()} | {:error, Errors.wire_error(), t()}
  def run(%__MODULE__{txn: :failed} = session, statement) do
    case classify(statement) do
      class when class in [:commit, :rollback] ->
        transaction(session, class)

      _blocked ->
        {:error,
         {"25P02",
          "current transaction is aborted, commands ignored until end of transaction block"},
         session}
    end
  end

  def run(%__MODULE__{} = session, statement) do
    case classify(statement) do
      :query -> query(session, statement)
      :set -> set(session, statement)
      :reset -> {:ok, command("RESET"), session}
      :show -> show(session, statement)
      class when class in [:begin, :commit, :rollback] -> transaction(session, class)
      {:unsupported, keyword} -> unsupported(session, keyword)
    end
  end

  defp command(tag, pre \\ []), do: outcome_map(nil, [], tag, pre)

  defp outcome_map(columns, rows, tag, pre),
    do: %{columns: columns, rows: rows, tag: tag, pre: pre}

  defp classify(statement) do
    case Statements.leading_keyword(statement) do
      "" -> {:unsupported, statement}
      keyword -> class(keyword)
    end
  end

  defp class(keyword) when keyword in ["select", "with", "values", "("], do: :query
  defp class("set"), do: :set
  defp class("reset"), do: :reset
  defp class("show"), do: :show
  defp class(keyword) when keyword in ["begin", "start"], do: :begin
  defp class(keyword) when keyword in ["commit", "end"], do: :commit
  defp class(keyword) when keyword in ["rollback", "abort"], do: :rollback
  defp class(keyword), do: {:unsupported, String.upcase(keyword)}

  defp query(%__MODULE__{runtime: runtime} = session, sql) do
    if PgCatalog.catalog_statement?(runtime.name, sql),
      do: catalog_query(session, sql),
      else: user_query(session, sql)
  end

  defp catalog_query(%__MODULE__{runtime: runtime} = session, sql) do
    case PgCatalog.query(runtime.name, sql, session.settings) do
      {:ok, columns, rows} ->
        columns = Enum.map(columns, &pg_array_column/1)

        {:ok, outcome_map(columns, rows, "SELECT #{length(rows)}", []), session}

      {:error, reason} ->
        fail(session, reason)
    end
  end

  defp pg_array_column({name, {:list, inner}, json?}), do: {name, {:pg_array, inner}, json?}
  defp pg_array_column(column), do: column

  defp user_query(%__MODULE__{runtime: runtime} = session, sql) do
    timeout = timeout_ms(session)

    with {:ok, job} <- Client.submit(runtime.query_name, sql, timeout_ms: timeout),
         :ok <- publish(session, {runtime.query_name, job.id}),
         awaited <- Client.await(runtime.query_name, job.id, timeout),
         :ok <- publish(session, nil) do
      case awaited do
        {:ok, %Job{state: :done} = job, %DataFrame{} = frame} ->
          {:ok, outcome(job, frame), session}

        {:ok, %Job{state: :cancelled}, _frame} ->
          fail(session, :cancelled)

        {:ok, %Job{error: reason}, _frame} ->
          fail(session, reason)

        {:error, reason} ->
          fail(session, reason)
      end
    else
      {:error, reason} -> fail(session, reason)
    end
  end

  defp publish(session, value) do
    Registry.update_value(Runtime.cancels(session.runtime.name), cancel_key(session), fn _ ->
      value
    end)

    :ok
  end

  defp timeout_ms(%__MODULE__{settings: settings, runtime: runtime}) do
    case Integer.parse(Map.get(settings, "statement_timeout", "0")) do
      {ms, _rest} when ms > 0 -> ms
      _off -> default_timeout(runtime)
    end
  end

  defp default_timeout(%Runtime{query_name: query_name}) do
    case Smolquery.QueryService.Runtime.fetch(query_name) do
      {:ok, runtime} -> runtime.default_timeout_ms
      :error -> 60_000
    end
  end

  defp outcome(%Job{json_columns: json}, frame) do
    names = DataFrame.names(frame)
    dtypes = DataFrame.dtypes(frame)
    rows = Frame.to_rows(frame, json_columns: json)

    columns = Enum.map(names, &{&1, Map.fetch!(dtypes, &1), &1 in json})

    outcome_map(columns, rows, "SELECT #{length(rows)}", [])
  end

  defp fail(session, reason), do: {:error, Errors.from_reason(reason), failed(session)}

  defp failed(%__MODULE__{txn: :transaction} = session), do: %{session | txn: :failed}
  defp failed(session), do: session

  @set ~r/^SET\s+(?:SESSION\s+|LOCAL\s+)?(?:TIME\s+ZONE\s+(?<zone>.+)|NAMES\s+(?<names>.+)|(?<name>[A-Za-z_][\w.]*)\s*(?:=|\s+TO\s+)\s*(?<value>.+))$/is

  defp set(session, statement) do
    session =
      case Regex.named_captures(@set, String.trim(statement)) do
        %{"zone" => zone} when zone != "" -> put_setting(session, "TimeZone", zone)
        %{"names" => names} when names != "" -> put_setting(session, "client_encoding", names)
        %{"name" => name, "value" => value} when name != "" -> put_setting(session, name, value)
        nil -> session
      end

    {:ok, command("SET", reported(session, statement)), session}
  end

  defp put_setting(session, name, value) do
    key = canonical(session, name)

    case unquote_value(value) do
      :default ->
        %{session | settings: Map.put(session.settings, key, Map.get(@defaults, key, ""))}

      value ->
        %{session | settings: Map.put(session.settings, key, value)}
    end
  end

  defp canonical(%__MODULE__{settings: settings}, name) do
    Enum.find(Map.keys(settings), name, &(String.downcase(&1) == String.downcase(name)))
  end

  defp unquote_value(value) do
    trimmed = String.trim(value)

    cond do
      String.downcase(trimmed) == "default" ->
        :default

      String.starts_with?(trimmed, "'") and String.ends_with?(trimmed, "'") and
          byte_size(trimmed) >= 2 ->
        trimmed |> binary_part(1, byte_size(trimmed) - 2) |> String.replace("''", "'")

      true ->
        trimmed
    end
  end

  defp reported(session, statement) do
    case Regex.named_captures(@set, String.trim(statement)) do
      %{"zone" => zone} when zone != "" ->
        [Protocol.parameter_status("TimeZone", session.settings["TimeZone"])]

      %{"names" => names} when names != "" ->
        [Protocol.parameter_status("client_encoding", session.settings["client_encoding"])]

      %{"name" => name} when name != "" ->
        key = canonical(session, name)

        if key in @reported,
          do: [Protocol.parameter_status(key, session.settings[key])],
          else: []

      nil ->
        []
    end
  end

  @show ~r/^SHOW\s+(?<name>ALL|TIME\s+ZONE|[A-Za-z_][\w.]*)$/is

  defp show(session, statement) do
    case Regex.named_captures(@show, String.trim(statement)) do
      %{"name" => all} when all in ["ALL", "all"] -> show_all(session)
      %{"name" => name} -> show_one(session, name)
      nil -> {:error, {"42601", "syntax error in SHOW"}, failed(session)}
    end
  end

  defp show_one(session, name) do
    key = canonical(session, String.replace(name, ~r/^time\s+zone$/i, "TimeZone"))

    case Map.fetch(session.settings, key) do
      {:ok, value} ->
        {:ok, text_rows([key], [%{key => value}], "SHOW"), session}

      :error ->
        {:error, {"42704", ~s|unrecognized configuration parameter "#{name}"|}, failed(session)}
    end
  end

  defp show_all(session) do
    rows =
      session.settings
      |> Enum.sort()
      |> Enum.map(fn {name, value} ->
        %{"name" => name, "setting" => value, "description" => ""}
      end)

    {:ok, text_rows(~w(name setting description), rows, "SHOW"), session}
  end

  defp text_rows(names, rows, tag),
    do: outcome_map(Enum.map(names, &{&1, :string, false}), rows, tag, [])

  defp transaction(%__MODULE__{txn: :idle} = session, :begin),
    do: {:ok, command("BEGIN"), %{session | txn: :transaction}}

  defp transaction(session, :begin) do
    notice = Protocol.notice_response("25001", "there is already a transaction in progress")

    {:ok, command("BEGIN", [notice]), session}
  end

  defp transaction(%__MODULE__{txn: :idle} = session, class) do
    notice = Protocol.notice_response("25P01", "there is no transaction in progress")

    {:ok, command(tag(class), [notice]), session}
  end

  defp transaction(%__MODULE__{txn: :failed} = session, :commit),
    do: {:ok, command("ROLLBACK"), %{session | txn: :idle}}

  defp transaction(session, class), do: {:ok, command(tag(class)), %{session | txn: :idle}}

  defp tag(:commit), do: "COMMIT"
  defp tag(:rollback), do: "ROLLBACK"

  defp unsupported(session, keyword) do
    {:error,
     {"0A000",
      "#{keyword} is not supported over the Postgres wire; smolquery serves SELECT queries here"},
     failed(session)}
  end
end
