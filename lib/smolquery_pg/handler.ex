defmodule SmolqueryPg.Handler do
  @moduledoc """
  One client connection to the Postgres wire edge (PL-58).

  A `ThousandIsland.Handler`, so one process per connection, and the
  process is idle between messages. The handler buffers bytes, decodes
  complete messages through `SmolqueryPg.Protocol`, and moves through the
  phases: startup, authentication, ready.

  ## Startup and TLS

  `SSLRequest` upgrades the connection when the runtime holds a
  certificate: the handler answers `S` and returns ThousandIsland's
  `:switch_transport` continuation, which runs the TLS handshake while
  the socket is passive — no window where a client byte can slip past the
  upgrade. Three refusals guard the boundary: bytes buffered alongside
  the `SSLRequest` (a plaintext-injection vector across the upgrade), a
  second `SSLRequest` inside an upgraded session, and — without a
  certificate — the request itself, declined with `N`. `GSSENCRequest`
  is always declined. A `CancelRequest` cancels the job its key names
  through `SmolqueryPg.Session.cancel/3` and closes.

  ## Authentication

  SCRAM-SHA-256 by default (`SmolqueryPg.Scram`, against the verifier the
  runtime derived at boot), or the cleartext password message when the
  runtime says `auth: :cleartext`. Either way the whole exchange — TLS
  handshake included — must finish inside the runtime's `auth_timeout_ms`:
  a timer closes the socket from outside the process, so a client that
  goes silent mid-handshake cannot pin the connection and its file
  descriptor. Authentication cancels the timer.

  ## Ready

  Every message runs through `SmolqueryPg.Session`. A simple `Query`
  answers with `ReadyForQuery` at its end. The extended protocol's
  messages each answer their completion, and `Sync` answers
  `ReadyForQuery`. After an error in the extended protocol the handler
  discards every message until `Sync`, as Postgres does, so a client's
  pipeline fails as one unit instead of desynchronising. Any message no
  phase expects answers `08P01`, never a crash.
  """

  use ThousandIsland.Handler

  require Logger

  alias SmolqueryPg.Protocol
  alias SmolqueryPg.Runtime
  alias SmolqueryPg.Scram
  alias SmolqueryPg.Session
  alias ThousandIsland.Socket

  @impl ThousandIsland.Handler
  def handle_connection(socket, %Runtime{} = runtime) do
    {:ok, timer} =
      :timer.apply_after(runtime.auth_timeout_ms, ThousandIsland.Socket, :close, [socket])

    {:continue,
     %{
       runtime: runtime,
       buffer: <<>>,
       phase: :startup,
       params: %{},
       session: nil,
       scram: nil,
       discard: false,
       tls: false,
       auth_timer: timer
     }}
  end

  @impl ThousandIsland.Handler
  def handle_data(data, socket, state) do
    drain(socket, %{state | buffer: state.buffer <> data})
  end

  defp drain(socket, %{phase: :startup} = state) do
    case Protocol.decode_startup(state.buffer) do
      {:ok, message, rest} -> startup(socket, message, %{state | buffer: rest})
      :incomplete -> {:continue, state}
      {:error, reason} -> refuse(socket, state, reason)
    end
  end

  defp drain(socket, state) do
    case Protocol.decode(state.buffer) do
      {:ok, message, rest} -> handle(socket, message, %{state | buffer: rest})
      :incomplete -> {:continue, state}
      {:error, reason} -> refuse(socket, state, reason)
    end
  end

  defp startup(socket, :ssl_request, %{tls: true} = state),
    do: refuse(socket, state, "08P01", "duplicate SSLRequest inside a TLS session")

  defp startup(socket, :ssl_request, %{buffer: buffer} = state) when buffer != <<>>,
    do: refuse(socket, state, "08P01", "unexpected data after SSLRequest")

  defp startup(socket, :ssl_request, state) do
    with true <- Runtime.tls?(state.runtime),
         :ok <- Socket.send(socket, "S") do
      {:switch_transport,
       {ThousandIsland.Transports.SSL,
        certfile: String.to_charlist(state.runtime.tls_cert),
        keyfile: String.to_charlist(state.runtime.tls_key)}, %{state | tls: true}}
    else
      false -> deny_encryption(socket, state)
      {:error, _reason} -> {:close, state}
    end
  end

  defp startup(socket, :gssenc_request, state), do: deny_encryption(socket, state)

  defp startup(_socket, {:cancel_request, pid, key}, state) do
    :ok = Session.cancel(state.runtime, pid, key)

    {:close, state}
  end

  defp startup(socket, {:startup, params}, %{runtime: %Runtime{auth: :cleartext}} = state) do
    case Socket.send(socket, Protocol.authentication_cleartext()) do
      :ok -> drain(socket, %{state | phase: :password, params: params})
      {:error, _reason} -> {:close, state}
    end
  end

  defp startup(socket, {:startup, params}, state) do
    case Socket.send(socket, Protocol.authentication_sasl([Scram.mechanism()])) do
      :ok -> drain(socket, %{state | phase: :sasl_initial, params: params})
      {:error, _reason} -> {:close, state}
    end
  end

  defp deny_encryption(socket, state) do
    case Socket.send(socket, Protocol.deny_encryption()) do
      :ok -> drain(socket, state)
      {:error, _reason} -> {:close, state}
    end
  end

  @impl GenServer
  def handle_info(:tls_upgrade, {socket, state}) do
    with :ok <- Socket.setopts(socket, active: false),
         :ok <- Socket.send(socket, "S"),
         {:ok, upgraded} <-
           Socket.upgrade(socket, ThousandIsland.Transports.SSL,
             certfile: String.to_charlist(state.runtime.tls_cert),
             keyfile: String.to_charlist(state.runtime.tls_key)
           ),
         :ok <- Socket.setopts(upgraded, active: :once) do
      {:noreply, {upgraded, state}}
    else
      error ->
        Logger.warning("pg wire: TLS upgrade failed: #{inspect(error)}")

        {:stop, {:shutdown, :tls_upgrade_failed}, {socket, state}}
    end
  end

  defp handle(socket, {:auth_response, body}, %{phase: :password} = state) do
    password = Protocol.cstring(body)

    if Plug.Crypto.secure_compare(password, state.runtime.password) do
      open_session(socket, state, [])
    else
      refuse_auth(socket, state)
    end
  end

  defp handle(socket, {:auth_response, body}, %{phase: :sasl_initial} = state) do
    with {:ok, mechanism, client_first} <- Protocol.decode_sasl_initial(body),
         true <- mechanism == Scram.mechanism(),
         {:ok, server_first, scram} <- Scram.server_first(client_first, state.runtime.scram) do
      case Socket.send(socket, Protocol.authentication_sasl_continue(server_first)) do
        :ok -> drain(socket, %{state | phase: :sasl_final, scram: scram})
        {:error, _reason} -> {:close, state}
      end
    else
      false ->
        refuse(socket, state, "28000", "unsupported SASL mechanism; use #{Scram.mechanism()}")

      {:error, reason} ->
        refuse(socket, state, "28000", reason)

      _malformed ->
        refuse(socket, state, "28000", "malformed SASL initial response")
    end
  end

  defp handle(socket, {:auth_response, body}, %{phase: :sasl_final, scram: scram} = state) do
    case Scram.server_final(body, scram) do
      {:ok, server_final} ->
        open_session(socket, state, [Protocol.authentication_sasl_final(server_final)])

      {:error, _reason} ->
        refuse_auth(socket, state)
    end
  end

  defp handle(_socket, :terminate, state), do: {:close, state}

  defp handle(socket, _message, %{phase: phase} = state)
       when phase in [:password, :sasl_initial, :sasl_final],
       do: refuse(socket, state, "08P01", "expected an authentication response")

  defp handle(socket, :sync, state) do
    case Socket.send(socket, Protocol.ready_for_query(state.session.txn)) do
      :ok -> drain(socket, %{state | discard: false})
      {:error, _reason} -> {:close, state}
    end
  end

  defp handle(socket, _message, %{discard: true} = state), do: drain(socket, state)

  defp handle(socket, {:query, sql}, state) do
    {messages, session} = Session.execute(state.session, sql)

    case Socket.send(socket, [messages, Protocol.ready_for_query(session.txn)]) do
      :ok -> drain(socket, %{state | session: session})
      {:error, _reason} -> {:close, state}
    end
  end

  defp handle(socket, :flush, state), do: drain(socket, state)

  defp handle(socket, {:parse, name, sql, oids}, state),
    do: extended(socket, state, Session.parse(state.session, name, sql, oids))

  defp handle(socket, {:bind, portal, statement, param_formats, values, formats}, state) do
    extended(
      socket,
      state,
      Session.bind(state.session, portal, statement, param_formats, values, formats)
    )
  end

  defp handle(socket, {:describe, kind, name}, state),
    do: extended(socket, state, Session.describe(state.session, kind, name))

  defp handle(socket, {:execute, portal, max_rows}, state),
    do: extended(socket, state, Session.execute_portal(state.session, portal, max_rows))

  defp handle(socket, {:close, kind, name}, state),
    do: extended(socket, state, Session.close(state.session, kind, name))

  defp handle(socket, {:unknown, tag, _body}, state) do
    message = "message type #{inspect(<<tag>>)} is not supported"

    case Socket.send(socket, Protocol.error_response("0A000", message)) do
      :ok -> drain(socket, %{state | discard: true})
      {:error, _reason} -> {:close, state}
    end
  end

  defp handle(socket, _message, state),
    do: refuse(socket, state, "08P01", "unexpected message for this phase")

  defp extended(socket, state, {:ok, messages, session}) do
    case Socket.send(socket, messages) do
      :ok -> drain(socket, %{state | session: session})
      {:error, _reason} -> {:close, state}
    end
  end

  defp extended(socket, state, {:error, {code, message}, session}) do
    case Socket.send(socket, Protocol.error_response(code, message)) do
      :ok -> drain(socket, %{state | session: session, discard: true})
      {:error, _reason} -> {:close, state}
    end
  end

  defp open_session(socket, state, prelude) do
    {:ok, :cancel} = :timer.cancel(state.auth_timer)
    session = Session.new(state.runtime, state.params)

    messages =
      prelude ++
        [Protocol.authentication_ok()] ++
        Session.startup_messages(session) ++ [Protocol.ready_for_query(:idle)]

    case Socket.send(socket, messages) do
      :ok -> drain(socket, %{state | phase: :ready, session: session})
      {:error, _reason} -> {:close, state}
    end
  end

  defp refuse_auth(socket, state) do
    user = Map.get(state.params, "user", "")
    Logger.warning("pg wire: password authentication failed for user #{inspect(user)}")

    refuse(socket, state, "28P01", ~s|password authentication failed for user "#{user}"|)
  end

  defp refuse(socket, state, {:message_too_large, length}),
    do: refuse(socket, state, "08P01", "message length #{length} exceeds the limit")

  defp refuse(socket, state, {:unsupported_protocol, major, minor}),
    do: refuse(socket, state, "0A000", "unsupported frontend protocol #{major}.#{minor}")

  defp refuse(socket, state, reason),
    do: refuse(socket, state, "08P01", "protocol violation: #{inspect(reason)}")

  defp refuse(socket, state, code, message) do
    _ = Socket.send(socket, Protocol.error_response(code, message))

    {:close, state}
  end
end
