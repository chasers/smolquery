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

  A transaction block pins its read (PL-58 layers 7 and 8): the block's
  first query captures a hot-tier time bound at submit and the snapshot
  the job ran at, and each table's first touch in the block captures the
  exact micro-segment ids that job read (`job.hot_members`). Every later
  query in the block passes all three through
  `Smolquery.QueryService.Client` — the id set as `hot_ids:` for a table
  already touched, the time bound for one not yet touched — so two
  statements in one `BEGIN`, or the several cursors of a `postgres_fdw`
  join, read the same data, whatever the buffer nodes' clocks say. The
  pin forms lazily at the first query, as Postgres's `REPEATABLE READ`
  does, and clears when the block ends (`ROLLBACK TO` keeps it; so does a
  failed block, until it ends — but a statement that fails before the pin
  has a snapshot drops the half-formed pin, so the block re-pins whole at
  its next query). `EXPLAIN` in a block pins and reads the pin like a
  query, as `postgres_fdw`'s remote estimates need. A block older than
  the query service's `hot_pin_max_age_ms`, or one whose pinned segment
  has been retired out of the hot tier, answers `72000`: the block is
  too old. Outside a block each statement reads its own snapshot, as an
  HTTP query does.

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
  alias Smolquery.QueryService.Statistics
  alias SmolqueryPg.Cursors
  alias SmolqueryPg.Errors
  alias SmolqueryPg.Params
  alias SmolqueryPg.PgCatalog
  alias SmolqueryPg.Protocol
  alias SmolqueryPg.Runtime
  alias SmolqueryPg.Sql
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
    "idle_in_transaction_session_timeout" => "300000",
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
    block: nil,
    settings: %{},
    statements: %{},
    portals: %{},
    cursors: %{}
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

  @type prepared :: %{sql: String.t(), oids: [pos_integer()]}

  @typedoc """
  One transaction block's state: its isolation level, the lazily formed
  read pin (`REPEATABLE READ` and `SERIALIZABLE` only) — the snapshot,
  the hot-tier time bound, and per table the micro-segment ids its first
  touch read — the savepoint names it holds, and the original values
  `SET LOCAL` must restore when the block ends.
  """
  @type block :: %{
          isolation: :read_committed | :repeatable_read | :serializable,
          pin:
            %{
              snapshot: term() | nil,
              hot_before_ms: pos_integer(),
              tables: %{Smolquery.Catalog.table_ref() => [String.t()]}
            }
            | nil,
          savepoints: MapSet.t(String.t()),
          locals: %{String.t() => String.t()}
        }

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
          block: block() | nil,
          settings: %{String.t() => String.t()},
          statements: %{String.t() => prepared()},
          portals: %{String.t() => portal()},
          cursors: %{String.t() => %{outcome: outcome(), offset: non_neg_integer()}}
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
        |> Map.put(
          "idle_in_transaction_session_timeout",
          Integer.to_string(runtime.idle_in_transaction_timeout_ms)
        )
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
  The session's idle-in-transaction budget in milliseconds.

  `SET idle_in_transaction_session_timeout = <ms>` may lower it, never
  raise it past the runtime's bound, and cannot disable it: the timeout
  guards the server's pinned snapshots and file descriptors, not the
  client's patience. A value of `0`, a value over the bound, or an
  unparsable one all mean the bound. Only an operator bound of `0`
  disables the timer.
  """
  @spec idle_in_transaction_timeout_ms(t()) :: non_neg_integer()
  def idle_in_transaction_timeout_ms(%__MODULE__{runtime: runtime, settings: settings}) do
    cap = runtime.idle_in_transaction_timeout_ms

    case Integer.parse(Map.get(settings, "idle_in_transaction_session_timeout", "")) do
      {ms, _rest} when ms > 0 and ms < cap -> ms
      _at_or_past_the_cap -> cap
    end
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

  defp put_statement(session, name, sql, oids),
    do: %{session | statements: Map.put(session.statements, name, %{sql: sql, oids: oids})}

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
      opened = %{sql: sql, formats: formats, outcome: nil, offset: 0}

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

  defp statement_columns(session, _name, %{sql: ""}), do: {:ok, nil, session}

  defp statement_columns(%__MODULE__{runtime: runtime} = session, _name, %{sql: sql, oids: oids}) do
    cond do
      classify(sql) != :query -> {:ok, nil, session}
      PgCatalog.catalog_statement?(runtime.name, sql) -> catalog_columns(session, sql, oids)
      true -> describe_query(session, Params.with_typed_nulls(sql, oids))
    end
  end

  defp catalog_columns(session, sql, oids) do
    case catalog_query(session, Params.with_typed_nulls(sql, oids)) do
      {:ok, outcome, session} -> {:ok, outcome.columns, session}
      {:error, error, session} -> {:error, error, session}
    end
  end

  defp describe_query(session, sql) do
    case run_job(session, sql, describe: true, timeout_ms: timeout_ms(session)) do
      {:ok, _job, %DataFrame{} = frame, session} ->
        columns =
          frame
          |> DataFrame.to_rows()
          |> Enum.map(fn %{"column_name" => name, "column_type" => type} ->
            {name, {:duckdb, type}, false}
          end)

        {:ok, columns, session}

      {:error, reason, session} ->
        {:error, Errors.from_reason(reason), failed(session)}
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
        end_block(session, class, statement)

      :rollback_to ->
        rollback_to(session, statement)

      _blocked ->
        {:error,
         {"25P02",
          "current transaction is aborted, commands ignored until end of transaction block"},
         session}
    end
  end

  def run(%__MODULE__{} = session, statement),
    do: dispatch(classify(statement), session, statement)

  defp dispatch(:query, session, statement), do: query(session, statement)
  defp dispatch(:set, session, statement), do: set(session, statement)
  defp dispatch(:reset, session, _statement), do: {:ok, command("RESET"), session}
  defp dispatch(:show, session, statement), do: show(session, statement)

  defp dispatch(:begin, session, statement), do: begin_block(session, statement)

  defp dispatch(class, session, statement) when class in [:commit, :rollback],
    do: end_block(session, class, statement)

  defp dispatch(:savepoint, session, statement), do: savepoint(session, statement)
  defp dispatch(:rollback_to, session, statement), do: rollback_to(session, statement)
  defp dispatch(:declare, session, statement), do: declare(session, statement)
  defp dispatch(:fetch, session, statement), do: fetch(session, statement)
  defp dispatch(:close_cursor, session, statement), do: close_cursor(session, statement)
  defp dispatch(:deallocate, session, statement), do: deallocate(session, statement)
  defp dispatch(:discard, session, statement), do: discard(session, statement)
  defp dispatch(:explain, session, statement), do: explain(session, statement)
  defp dispatch({:unsupported, keyword}, session, _statement), do: unsupported(session, keyword)

  defp classify(statement) do
    case Statements.leading_keyword(statement) do
      "" -> {:unsupported, statement}
      "rollback" -> rollback_class(statement)
      keyword -> class(keyword)
    end
  end

  defp rollback_class(statement) do
    {:word, "rollback", rest} = Sql.next_token(statement)
    rest = Sql.skip_words(rest, ["work", "transaction"])

    case Sql.next_token(rest) do
      {:word, "to", _rest} -> :rollback_to
      _other -> :rollback
    end
  end

  defp class(keyword) when keyword in ["select", "with", "values", "("], do: :query
  defp class("set"), do: :set
  defp class("reset"), do: :reset
  defp class("show"), do: :show
  defp class(keyword) when keyword in ["begin", "start"], do: :begin
  defp class("commit"), do: :commit
  defp class("end"), do: :commit
  defp class(keyword) when keyword in ["savepoint", "release"], do: :savepoint
  defp class("declare"), do: :declare
  defp class(keyword) when keyword in ["fetch", "move"], do: :fetch
  defp class("close"), do: :close_cursor
  defp class("deallocate"), do: :deallocate
  defp class("discard"), do: :discard
  defp class("explain"), do: :explain
  defp class("abort"), do: :rollback
  defp class(keyword), do: {:unsupported, String.upcase(keyword)}

  defp command(tag, pre \\ []), do: outcome_map(nil, [], tag, pre)

  defp outcome_map(columns, rows, tag, pre),
    do: %{columns: columns, rows: rows, tag: tag, pre: pre}

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

  defp user_query(session, sql) do
    case run_job(session, sql, timeout_ms: timeout_ms(session)) do
      {:ok, job, %DataFrame{} = frame, session} -> {:ok, outcome(job, frame), session}
      {:error, reason, session} -> fail(session, reason)
    end
  end

  defp run_job(%__MODULE__{runtime: runtime} = session, sql, opts) do
    timeout = Keyword.fetch!(opts, :timeout_ms)
    {pin, session} = block_pin(session)

    with {:ok, job} <- Client.submit(runtime.query_name, sql, opts ++ pin),
         :ok <- publish(session, {runtime.query_name, job.id}),
         awaited <- Client.await(runtime.query_name, job.id, timeout),
         :ok <- publish(session, nil) do
      case awaited do
        {:ok, %Job{state: :done} = job, frame} -> {:ok, job, frame, remember_pin(session, job)}
        {:ok, %Job{state: :cancelled}, _frame} -> {:error, :cancelled, session}
        {:ok, %Job{error: reason}, _frame} -> {:error, reason, session}
        {:error, reason} -> {:error, reason, session}
      end
    else
      {:error, reason} -> {:error, reason, session}
    end
  end

  defp block_pin(%__MODULE__{txn: :transaction, block: %{isolation: isolation} = block} = session)
       when isolation in [:repeatable_read, :serializable] do
    case block.pin do
      nil ->
        pin = %{snapshot: nil, hot_before_ms: System.system_time(:millisecond), tables: %{}}

        {[hot_before_ms: pin.hot_before_ms], %{session | block: %{block | pin: pin}}}

      pin ->
        {[hot_before_ms: pin.hot_before_ms] ++ snapshot_opt(pin) ++ tables_opt(pin), session}
    end
  end

  defp block_pin(session), do: {[], session}

  defp snapshot_opt(%{snapshot: nil}), do: []
  defp snapshot_opt(%{snapshot: snapshot}), do: [snapshot: snapshot]

  defp tables_opt(%{tables: tables}) when map_size(tables) == 0, do: []
  defp tables_opt(%{tables: tables}), do: [hot_ids: tables]

  defp remember_pin(
         %__MODULE__{txn: :transaction, block: %{pin: %{} = pin} = block} = session,
         %Job{} = job
       ) do
    pin = %{
      pin
      | snapshot: job.snapshot,
        tables: Map.merge(job.hot_members, pin.tables)
    }

    %{session | block: %{block | pin: pin}}
  end

  defp remember_pin(session, _job), do: session

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

  defp failed(%__MODULE__{txn: :transaction, block: block} = session),
    do: %{session | txn: :failed, block: drop_unformed_pin(block)}

  defp failed(session), do: session

  defp drop_unformed_pin(%{pin: %{snapshot: nil}} = block), do: %{block | pin: nil}
  defp drop_unformed_pin(block), do: block

  defp set(session, statement) do
    {:word, "set", rest} = Sql.next_token(statement)

    case Sql.next_token(rest) do
      {:word, "transaction", next} -> set_transaction(session, next)
      {:word, "session", next} -> set_session(session, next)
      {:word, "local", next} -> set_assignment(session, next, :local)
      _other -> set_assignment(session, rest, :session)
    end
  end

  defp set_session(session, rest) do
    case Sql.next_token(rest) do
      {:word, "characteristics", next} ->
        with {:word, "as", next} <- Sql.next_token(next),
             {:word, "transaction", next} <- Sql.next_token(next),
             {:ok, _isolation} <- transaction_modes(next, nil) do
          {:ok, command("SET"), session}
        else
          {:error, :read_write} -> read_write_refusal(session)
          _malformed -> set_syntax_error(session)
        end

      _other ->
        set_assignment(session, rest, :session)
    end
  end

  defp set_assignment(%__MODULE__{txn: :idle} = session, _rest, :local) do
    notice =
      Protocol.notice_response("25P01", "SET LOCAL can only be used in transaction blocks")

    {:ok, command("SET", [notice]), session}
  end

  defp set_assignment(session, rest, scope) do
    case Sql.next_token(rest) do
      {:word, "time", next} -> set_time_zone(session, next, scope)
      {:word, "names", next} -> assign(session, "client_encoding", next, scope)
      {:word, _name, _next} -> named_assignment(session, rest, scope)
      _other -> set_syntax_error(session)
    end
  end

  defp set_time_zone(session, rest, scope) do
    case Sql.next_token(rest) do
      {:word, "zone", next} -> assign(session, "TimeZone", next, scope)
      _malformed -> set_syntax_error(session)
    end
  end

  defp named_assignment(session, rest, scope) do
    {:ok, name, next} = Sql.setting_name(rest)

    case Sql.next_token(next) do
      {:symbol, "=", value} -> assign(session, name, value, scope)
      {:word, "to", value} -> assign(session, name, value, scope)
      _malformed -> set_syntax_error(session)
    end
  end

  defp assign(session, name, value_rest, scope) do
    key = canonical(session, name)

    with {:ok, value} <- present(value_rest),
         {:ok, value, notices} <- validated(session, key, value) do
      session = session |> local_backup(key, scope) |> put_setting(name, value)

      {:ok, command("SET", set_reported(session, key) ++ notices), session}
    else
      :blank -> set_syntax_error(session)
      {:error, error} -> {:error, error, failed(session)}
    end
  end

  defp present(value_rest) do
    case String.trim(value_rest) do
      "" -> :blank
      value -> {:ok, value}
    end
  end

  defp validated(session, "statement_timeout", value) do
    case duration_ms(unquote_value(value)) do
      {:ok, ms} -> {:ok, Integer.to_string(ms), []}
      :default -> {:ok, value, []}
      :error -> invalid_setting("statement_timeout", value, session)
    end
  end

  defp validated(
         %__MODULE__{runtime: runtime} = session,
         "idle_in_transaction_session_timeout",
         value
       ) do
    cap = runtime.idle_in_transaction_timeout_ms

    case duration_ms(unquote_value(value)) do
      {:ok, ms} when ms > 0 and ms < cap ->
        {:ok, Integer.to_string(ms), []}

      {:ok, _zero_or_past_the_cap} ->
        notice =
          Protocol.notice_response(
            "01000",
            "idle_in_transaction_session_timeout is capped at the server's #{cap} ms " <>
              "and cannot be disabled"
          )

        {:ok, Integer.to_string(cap), [notice]}

      :default ->
        {:ok, value, []}

      :error ->
        invalid_setting("idle_in_transaction_session_timeout", value, session)
    end
  end

  defp validated(_session, _key, value), do: {:ok, value, []}

  defp invalid_setting(key, value, _session),
    do: {:error, {"22023", ~s|invalid value for parameter "#{key}": #{value}|}}

  @duration_units %{"" => 1, "ms" => 1, "s" => 1_000, "min" => 60_000, "h" => 3_600_000}

  defp duration_ms(:default), do: :default

  defp duration_ms(text) do
    case Integer.parse(String.trim(text)) do
      {ms, unit} when ms >= 0 ->
        case Map.fetch(@duration_units, unit |> String.trim() |> String.downcase()) do
          {:ok, factor} -> {:ok, ms * factor}
          :error -> :error
        end

      _negative_or_not_a_number ->
        :error
    end
  end

  defp local_backup(%__MODULE__{block: %{locals: locals} = block} = session, key, :local) do
    original = Map.get(session.settings, key, "")

    %{session | block: %{block | locals: Map.put_new(locals, key, original)}}
  end

  defp local_backup(session, _key, _scope), do: session

  defp set_reported(session, key) do
    if key in @reported,
      do: [Protocol.parameter_status(key, session.settings[key])],
      else: []
  end

  defp set_syntax_error(session),
    do: {:error, {"42601", "syntax error in SET"}, failed(session)}

  defp set_transaction(session, rest) do
    case transaction_modes(rest, nil) do
      {:error, :read_write} -> read_write_refusal(session)
      {:error, :syntax} -> set_syntax_error(session)
      {:ok, nil} -> {:ok, command("SET"), session}
      {:ok, isolation} -> apply_block_isolation(session, isolation)
    end
  end

  defp apply_block_isolation(%__MODULE__{txn: :idle} = session, _isolation) do
    notice =
      Protocol.notice_response("25P01", "SET TRANSACTION applies inside a transaction block")

    {:ok, command("SET", [notice]), session}
  end

  defp apply_block_isolation(%__MODULE__{block: %{pin: pin}} = session, _isolation)
       when pin != nil do
    {:error,
     {"25001", "SET TRANSACTION ISOLATION LEVEL must be called before any query in the block"},
     failed(session)}
  end

  defp apply_block_isolation(session, isolation) do
    session =
      put_in_settings(
        %{session | block: %{session.block | isolation: isolation}},
        "transaction_isolation",
        isolation_name(isolation)
      )

    {:ok, command("SET"), session}
  end

  defp put_in_settings(session, key, value),
    do: %{session | settings: Map.put(session.settings, key, value)}

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

  defp show(session, statement) do
    {:word, "show", rest} = Sql.next_token(statement)

    case Sql.next_token(rest) do
      {:word, "all", next} -> show_when_done(session, "", next, &show_all/1)
      {:word, "time", next} -> show_time_zone(session, next)
      {:word, _name, _next} -> show_named(session, rest)
      _other -> show_syntax_error(session)
    end
  end

  defp show_time_zone(session, rest) do
    case Sql.next_token(rest) do
      {:word, "zone", next} ->
        show_when_done(session, "TimeZone", next, &show_one(&1, "TimeZone"))

      _malformed ->
        show_syntax_error(session)
    end
  end

  defp show_named(session, rest) do
    {:ok, name, next} = Sql.setting_name(rest)

    show_when_done(session, name, next, &show_one(&1, name))
  end

  defp show_when_done(session, _name, rest, answer) do
    if eof?(rest), do: answer.(session), else: show_syntax_error(session)
  end

  defp show_syntax_error(session),
    do: {:error, {"42601", "syntax error in SHOW"}, failed(session)}

  defp show_one(session, name) do
    key = canonical(session, name)

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

  defp begin_block(session, statement) do
    {:word, _begin_or_start, rest} = Sql.next_token(statement)
    rest = Sql.skip_words(rest, ["work", "transaction"])

    case transaction_modes(rest, :read_committed) do
      {:ok, isolation} when session.txn == :idle ->
        {:ok, command("BEGIN"), open_block(session, isolation)}

      {:ok, _isolation} ->
        notice = Protocol.notice_response("25001", "there is already a transaction in progress")

        {:ok, command("BEGIN", [notice]), session}

      {:error, :read_write} ->
        read_write_refusal(session)

      {:error, :syntax} ->
        {:error, {"42601", "syntax error in BEGIN"}, failed(session)}
    end
  end

  defp transaction_modes(rest, isolation) do
    case Sql.next_token(rest) do
      :eof -> {:ok, isolation}
      token -> transaction_mode(token, isolation)
    end
  end

  defp transaction_mode({:symbol, ",", next}, isolation), do: transaction_modes(next, isolation)

  defp transaction_mode({:word, "isolation", next}, _isolation) do
    with {:word, "level", next} <- Sql.next_token(next),
         {:ok, isolation, next} <- isolation_level(next) do
      transaction_modes(next, isolation)
    else
      _malformed -> {:error, :syntax}
    end
  end

  defp transaction_mode({:word, "read", next}, isolation) do
    case Sql.next_token(next) do
      {:word, "only", next} -> transaction_modes(next, isolation)
      {:word, "write", _next} -> {:error, :read_write}
      _malformed -> {:error, :syntax}
    end
  end

  defp transaction_mode({:word, "not", next}, isolation) do
    case Sql.next_token(next) do
      {:word, "deferrable", next} -> transaction_modes(next, isolation)
      _malformed -> {:error, :syntax}
    end
  end

  defp transaction_mode({:word, "deferrable", next}, isolation),
    do: transaction_modes(next, isolation)

  defp transaction_mode(_other, _isolation), do: {:error, :syntax}

  defp isolation_level(rest) do
    case Sql.next_token(rest) do
      {:word, "serializable", next} ->
        {:ok, :serializable, next}

      {:word, "repeatable", next} ->
        case Sql.next_token(next) do
          {:word, "read", next} -> {:ok, :repeatable_read, next}
          _malformed -> :error
        end

      {:word, "read", next} ->
        case Sql.next_token(next) do
          {:word, level, next} when level in ["committed", "uncommitted"] ->
            {:ok, :read_committed, next}

          _malformed ->
            :error
        end

      _other ->
        :error
    end
  end

  defp read_write_refusal(session) do
    {:error,
     {"25006", "smolquery is read-only over the wire; a read-write transaction is not available"},
     failed(session)}
  end

  defp open_block(session, isolation) do
    settings =
      Map.put(session.settings, "transaction_isolation", isolation_name(isolation))

    %{
      session
      | txn: :transaction,
        settings: settings,
        block: %{isolation: isolation, pin: nil, savepoints: MapSet.new(), locals: %{}}
    }
  end

  defp isolation_name(:repeatable_read), do: "repeatable read"
  defp isolation_name(:serializable), do: "serializable"
  defp isolation_name(:read_committed), do: "read committed"

  defp end_block(session, class, statement) do
    {:word, _keyword, rest} = Sql.next_token(statement)
    rest = Sql.skip_words(rest, ["work", "transaction"])

    case Sql.next_token(rest) do
      :eof ->
        finish_block(session, class, false)

      {:word, "prepared", _next} ->
        {:error, {"0A000", "prepared (two-phase) transactions are not supported"},
         failed(session)}

      {:word, "and", next} ->
        end_block_chain(session, class, next)

      _other ->
        {:error, {"42601", "syntax error in #{tag(class)}"}, failed(session)}
    end
  end

  defp end_block_chain(session, class, rest) do
    case Sql.next_token(rest) do
      {:word, "chain", _next} ->
        finish_block(session, class, true)

      {:word, "no", next} ->
        case Sql.next_token(next) do
          {:word, "chain", _next} -> finish_block(session, class, false)
          _malformed -> {:error, {"42601", "syntax error in #{tag(class)}"}, failed(session)}
        end

      _other ->
        {:error, {"42601", "syntax error in #{tag(class)}"}, failed(session)}
    end
  end

  defp finish_block(%__MODULE__{txn: :idle} = session, class, _chain?) do
    notice = Protocol.notice_response("25P01", "there is no transaction in progress")

    {:ok, command(tag(class), [notice]), session}
  end

  defp finish_block(session, class, chain?) do
    tag = if session.txn == :failed and class == :commit, do: "ROLLBACK", else: tag(class)
    isolation = session.block.isolation
    session = close_block(session)
    session = if chain?, do: open_block(session, isolation), else: session

    {:ok, command(tag), session}
  end

  defp close_block(%__MODULE__{block: block} = session) do
    settings =
      session.settings
      |> Map.merge(block_locals(block))
      |> Map.put("transaction_isolation", @defaults["transaction_isolation"])

    %{session | txn: :idle, block: nil, settings: settings}
  end

  defp block_locals(nil), do: %{}
  defp block_locals(%{locals: locals}), do: locals

  defp savepoint(%__MODULE__{txn: :idle} = session, statement) do
    verb = statement |> Statements.leading_keyword() |> String.upcase()

    {:error, {"25P01", "#{verb} can only be used in transaction blocks"}, session}
  end

  defp savepoint(%__MODULE__{block: block} = session, statement) do
    {:word, verb, rest} = Sql.next_token(statement)

    case verb do
      "savepoint" ->
        case single_identifier(rest) do
          {:ok, name} ->
            savepoints = MapSet.put(block.savepoints, name)

            {:ok, command("SAVEPOINT"), %{session | block: %{block | savepoints: savepoints}}}

          :error ->
            {:error, {"42601", "syntax error in SAVEPOINT"}, failed(session)}
        end

      "release" ->
        case single_identifier(Sql.skip_words(rest, ["savepoint"])) do
          {:ok, name} -> release_savepoint(session, block, name)
          :error -> {:error, {"42601", "syntax error in RELEASE"}, failed(session)}
        end
    end
  end

  defp single_identifier(rest) do
    with {kind, name, next} when kind in [:word, :quoted] <- Sql.next_token(rest),
         true <- eof?(next) do
      {:ok, name}
    else
      _malformed -> :error
    end
  end

  defp eof?(rest), do: Sql.next_token(rest) == :eof

  defp release_savepoint(session, block, name) do
    if MapSet.member?(block.savepoints, name) do
      savepoints = MapSet.delete(block.savepoints, name)

      {:ok, command("RELEASE"), %{session | block: %{block | savepoints: savepoints}}}
    else
      {:error, {"3B001", ~s|savepoint "#{name}" does not exist|}, failed(session)}
    end
  end

  defp rollback_to(%__MODULE__{txn: :idle} = session, _statement) do
    {:error, {"25P01", "ROLLBACK TO can only be used in transaction blocks"}, session}
  end

  defp rollback_to(%__MODULE__{block: block} = session, statement) do
    {:word, "rollback", rest} = Sql.next_token(statement)
    rest = Sql.skip_words(rest, ["work", "transaction"])
    {:word, "to", rest} = Sql.next_token(rest)

    case single_identifier(Sql.skip_words(rest, ["savepoint"])) do
      {:ok, name} ->
        if MapSet.member?(block.savepoints, name) do
          {:ok, command("ROLLBACK"), %{session | txn: :transaction}}
        else
          {:error, {"3B001", ~s|savepoint "#{name}" does not exist|}, failed(session)}
        end

      :error ->
        {:error, {"42601", "syntax error in ROLLBACK TO"}, failed(session)}
    end
  end

  defp tag(:commit), do: "COMMIT"
  defp tag(:rollback), do: "ROLLBACK"

  defp declare(%__MODULE__{} = session, statement) do
    with {:ok, name, query_sql} <- Cursors.parse_declare(statement),
         {:ok, outcome, session} <- run(session, query_sql) do
      cursors = Map.put(session.cursors, name, %{outcome: outcome, offset: 0})

      {:ok, command("DECLARE CURSOR"), %{session | cursors: cursors}}
    else
      :error -> {:error, {"42601", "syntax error in DECLARE"}, failed(session)}
      {:error, error, session} -> {:error, error, session}
    end
  end

  defp fetch(%__MODULE__{} = session, statement) do
    with {:ok, verb, count, name} <- Cursors.parse_fetch(statement),
         {:ok, cursor} <- fetch_cursor(session, name) do
      taken = page_size(cursor, count)
      page = cursor.outcome.rows |> Enum.drop(cursor.offset) |> Enum.take(taken)
      cursors = Map.put(session.cursors, name, %{cursor | offset: cursor.offset + length(page)})
      session = %{session | cursors: cursors}

      case verb do
        :move ->
          {:ok, command("MOVE #{length(page)}"), session}

        :fetch ->
          {:ok, outcome_map(cursor.outcome.columns, page, "FETCH #{length(page)}", []), session}
      end
    else
      :error -> {:error, {"42601", "syntax error in FETCH"}, failed(session)}
      {:error, error} -> {:error, error, failed(session)}
    end
  end

  defp page_size(cursor, :all), do: length(cursor.outcome.rows) - cursor.offset
  defp page_size(_cursor, count), do: count

  defp fetch_cursor(session, name) do
    case Map.fetch(session.cursors, name) do
      {:ok, %{outcome: %{columns: columns}} = cursor} when columns != nil -> {:ok, cursor}
      {:ok, _no_rows} -> {:error, {"42P01", ~s|cursor "#{name}" holds no rows|}}
      :error -> {:error, {"34000", ~s|cursor "#{name}" does not exist|}}
    end
  end

  defp close_cursor(%__MODULE__{} = session, statement) do
    case Cursors.parse_close(statement) do
      {:ok, :all} ->
        {:ok, command("CLOSE CURSOR ALL"), %{session | cursors: %{}}}

      {:ok, name} ->
        if Map.has_key?(session.cursors, name) do
          {:ok, command("CLOSE CURSOR"), %{session | cursors: Map.delete(session.cursors, name)}}
        else
          {:error, {"34000", ~s|cursor "#{name}" does not exist|}, failed(session)}
        end

      :error ->
        {:error, {"42601", "syntax error in CLOSE"}, failed(session)}
    end
  end

  defp deallocate(%__MODULE__{} = session, _statement),
    do: {:ok, command("DEALLOCATE ALL"), %{session | statements: %{}, portals: %{}}}

  defp discard(%__MODULE__{txn: txn} = session, _statement) when txn != :idle do
    {:error, {"25001", "DISCARD ALL cannot run inside a transaction block"}, failed(session)}
  end

  defp discard(%__MODULE__{} = session, _statement) do
    {:ok, command("DISCARD ALL"),
     %{
       session
       | statements: %{},
         portals: %{},
         cursors: %{},
         block: nil,
         settings: reset_settings(session)
     }}
  end

  defp reset_settings(%__MODULE__{settings: settings}) do
    Map.merge(@defaults, Map.take(settings, ["session_authorization", "application_name"]))
  end

  defp explain(session, statement) do
    sql = statement |> strip_explain() |> String.trim()

    case run_job(session, sql, explain: :plan, timeout_ms: timeout_ms(session)) do
      {:ok, job, _frame, session} ->
        rows = estimated_rows(job)

        {:ok,
         text_rows(
           ["QUERY PLAN"],
           [
             %{
               "QUERY PLAN" =>
                 "Foreign Scan  (cost=100.00..#{100 + rows}.00 rows=#{rows} width=64)"
             }
           ],
           "EXPLAIN"
         ), session}

      {:error, reason, session} ->
        fail(session, reason)
    end
  end

  defp strip_explain(statement) do
    {:word, "explain", rest} = Sql.next_token(statement)

    explain_tail(rest)
  end

  defp explain_tail(rest) do
    case Sql.next_token(rest) do
      {:symbol, "(", next} -> explain_tail(Sql.skip_parens(next))
      {:word, word, next} when word in ["analyze", "verbose"] -> explain_tail(next)
      _query -> Sql.skip_trivia(rest)
    end
  end

  defp estimated_rows(%Job{statistics: nil}), do: 1000

  defp estimated_rows(%Job{statistics: statistics}),
    do: max(Statistics.rows_scanned(statistics), 1)

  defp unsupported(session, keyword) do
    {:error,
     {"0A000",
      "#{keyword} is not supported over the Postgres wire; smolquery serves SELECT queries here"},
     failed(session)}
  end
end
