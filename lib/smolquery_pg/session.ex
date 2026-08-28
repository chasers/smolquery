defmodule SmolqueryPg.Session do
  @moduledoc """
  One authenticated connection's state, and what each statement answers
  (PL-58).

  A session holds what the protocol makes stateful: the transaction status
  the prompt shows, the `SET` values a client reads back with `SHOW`, and
  the key a `CancelRequest` must quote. The socket is the handler's; this
  module only answers messages to send.

  ## Routing

  The planner's gate refuses everything but one `SELECT` over
  `dataset.table`, and a client sends a lot more than that on connect.
  `execute/2` classifies each statement by its leading keyword before
  anything runs:

  | class | answer |
  |---|---|
  | `SELECT`, `WITH`, `VALUES`, `(` | `Smolquery.QueryService.Client.query/3`; the frame as rows |
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

  ## Multiple statements

  A simple `Query` may carry several statements. They run in order, and
  the first error stops the rest, as Postgres does.
  """

  alias Explorer.DataFrame
  alias Smolquery.Engine.Frame
  alias Smolquery.QueryService.Client
  alias Smolquery.QueryService.Job
  alias SmolqueryPg.Errors
  alias SmolqueryPg.Protocol
  alias SmolqueryPg.Runtime
  alias SmolqueryPg.Statements
  alias SmolqueryPg.Types

  @server_version "16.0"

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
  defstruct [:runtime, :user, :database, :backend_pid, :secret_key, txn: :idle, settings: %{}]

  @type t :: %__MODULE__{
          runtime: Runtime.t(),
          user: String.t(),
          database: String.t() | nil,
          backend_pid: pos_integer(),
          secret_key: pos_integer(),
          txn: Protocol.ready_status(),
          settings: %{String.t() => String.t()}
        }

  @doc """
  A session for a client that opened with `params`.
  """
  @spec new(Runtime.t(), %{String.t() => String.t()}) :: t()
  def new(%Runtime{} = runtime, params) do
    user = Map.get(params, "user", "")

    %__MODULE__{
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
  Runs every statement in `sql` and answers the messages to send, without
  the closing `ReadyForQuery` — the handler adds it from the session's
  transaction status.
  """
  @spec execute(t(), String.t()) :: {[iodata()], t()}
  def execute(%__MODULE__{} = session, sql) do
    case Statements.split(sql) do
      [] ->
        {[Protocol.empty_query_response()], session}

      statements ->
        Enum.reduce_while(statements, {[], session}, &run_next/2)
    end
  end

  defp run_next(statement, {messages, session}) do
    case run(session, statement) do
      {:ok, answer, session} -> {:cont, {messages ++ answer, session}}
      {:error, answer, session} -> {:halt, {messages ++ answer, session}}
    end
  end

  defp run(%__MODULE__{txn: :failed} = session, statement) do
    case classify(statement) do
      class when class in [:commit, :rollback] ->
        transaction(session, class)

      _blocked ->
        {:error,
         [
           Protocol.error_response(
             "25P02",
             "current transaction is aborted, commands ignored until end of transaction block"
           )
         ], session}
    end
  end

  defp run(session, statement) do
    case classify(statement) do
      :query -> query(session, statement)
      :set -> set(session, statement)
      :reset -> {:ok, [Protocol.command_complete("RESET")], session}
      :show -> show(session, statement)
      class when class in [:begin, :commit, :rollback] -> transaction(session, class)
      {:unsupported, keyword} -> unsupported(session, keyword)
    end
  end

  @leading_keyword ~r/^\s*(?:(?:--[^\n]*\n|\/\*.*?\*\/)\s*)*([A-Za-z_]+|\()/s

  defp classify(statement) do
    case Regex.run(@leading_keyword, statement, capture: :all_but_first) do
      [keyword] -> class(String.downcase(keyword))
      nil -> {:unsupported, statement}
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
    case Client.query(runtime.query_name, sql, timeout_opts(session)) do
      {:ok, %Job{state: :done} = job, %DataFrame{} = frame} ->
        {:ok, result_messages(job, frame), session}

      {:ok, %Job{state: :cancelled}, _frame} ->
        fail(session, :cancelled)

      {:ok, %Job{error: reason}, _frame} ->
        fail(session, reason)

      {:error, reason} ->
        fail(session, reason)
    end
  end

  defp timeout_opts(%__MODULE__{settings: settings}) do
    case Integer.parse(Map.get(settings, "statement_timeout", "0")) do
      {ms, _rest} when ms > 0 -> [timeout_ms: ms]
      _off -> []
    end
  end

  defp result_messages(%Job{json_columns: json}, frame) do
    names = DataFrame.names(frame)
    dtypes = DataFrame.dtypes(frame)
    columns = Enum.map(names, &{&1, Map.fetch!(dtypes, &1), &1 in json})

    fields =
      Enum.map(columns, fn {name, dtype, json?} ->
        {oid, typlen, typmod} = Types.describe(dtype, json?)
        %{name: name, oid: oid, typlen: typlen, typmod: typmod, format: 0}
      end)

    rows =
      frame
      |> Frame.to_rows(json_columns: json)
      |> Enum.map(fn row ->
        Protocol.data_row(
          Enum.map(columns, fn {name, dtype, json?} ->
            Types.encode_text(dtype, json?, Map.fetch!(row, name))
          end)
        )
      end)

    [Protocol.row_description(fields)] ++
      rows ++ [Protocol.command_complete("SELECT #{length(rows)}")]
  end

  defp fail(session, reason) do
    {code, message} = Errors.from_reason(reason)

    {:error, [Protocol.error_response(code, message)], failed(session)}
  end

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

    {:ok, reported(session, statement) ++ [Protocol.command_complete("SET")], session}
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
      %{"name" => "ALL"} -> show_all(session)
      %{"name" => "all"} -> show_all(session)
      %{"name" => name} -> show_one(session, name)
      nil -> {:error, [Protocol.error_response("42601", "syntax error in SHOW")], failed(session)}
    end
  end

  defp show_one(session, name) do
    key = canonical(session, String.replace(name, ~r/^time\s+zone$/i, "TimeZone"))

    case Map.fetch(session.settings, key) do
      {:ok, value} ->
        {:ok,
         [
           Protocol.row_description([text_field(key)]),
           Protocol.data_row([value]),
           Protocol.command_complete("SHOW")
         ], session}

      :error ->
        {:error,
         [Protocol.error_response("42704", ~s|unrecognized configuration parameter "#{name}"|)],
         failed(session)}
    end
  end

  defp show_all(session) do
    rows =
      session.settings
      |> Enum.sort()
      |> Enum.map(fn {name, value} -> Protocol.data_row([name, value, ""]) end)

    {:ok,
     [Protocol.row_description(Enum.map(~w(name setting description), &text_field/1))] ++
       rows ++ [Protocol.command_complete("SHOW")], session}
  end

  defp text_field(name), do: %{name: name, oid: 25, typlen: -1, typmod: -1, format: 0}

  defp transaction(%__MODULE__{txn: :idle} = session, :begin),
    do: {:ok, [Protocol.command_complete("BEGIN")], %{session | txn: :transaction}}

  defp transaction(session, :begin) do
    {:ok,
     [
       Protocol.notice_response("25001", "there is already a transaction in progress"),
       Protocol.command_complete("BEGIN")
     ], session}
  end

  defp transaction(%__MODULE__{txn: :idle} = session, class) do
    {:ok,
     [
       Protocol.notice_response("25P01", "there is no transaction in progress"),
       Protocol.command_complete(tag(class))
     ], session}
  end

  defp transaction(%__MODULE__{txn: :failed} = session, :commit),
    do: {:ok, [Protocol.command_complete("ROLLBACK")], %{session | txn: :idle}}

  defp transaction(session, class),
    do: {:ok, [Protocol.command_complete(tag(class))], %{session | txn: :idle}}

  defp tag(:commit), do: "COMMIT"
  defp tag(:rollback), do: "ROLLBACK"

  defp unsupported(session, keyword) do
    {:error,
     [
       Protocol.error_response(
         "0A000",
         "#{keyword} is not supported over the Postgres wire; smolquery serves SELECT queries here"
       )
     ], failed(session)}
  end
end
