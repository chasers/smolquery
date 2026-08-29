defmodule SmolqueryPg.Handler do
  @moduledoc """
  One client connection to the Postgres wire edge (PL-58).

  A `ThousandIsland.Handler`, so one process per connection, and the
  process is idle between messages. The handler buffers bytes, decodes
  complete messages through `SmolqueryPg.Protocol`, and moves through three
  phases:

  1. **startup** — the first packet. `SSLRequest` and `GSSENCRequest` are
     declined with `N`, and the client sends its real startup next. A
     `CancelRequest` closes the connection (layer 2 acts on it). A startup
     packet moves to the password phase.
  2. **password** — `AuthenticationCleartextPassword` was sent; the answer
     is compared in constant time against the runtime's password. A wrong
     one answers `28P01` and closes. Nothing about the user or the
     database is checked: the credential is the whole gate, as the API's
     Bearer key is.
  3. **ready** — every `Query` runs through `SmolqueryPg.Session`, and the
     answer ends with `ReadyForQuery` carrying the transaction status.

  A message of the extended query protocol answers `0A000` in this layer
  and the handler discards everything until the client's `Sync`, as
  Postgres does after an error, so a driver's connection setup fails
  cleanly instead of desynchronising.
  """

  use ThousandIsland.Handler

  require Logger

  alias SmolqueryPg.Protocol
  alias SmolqueryPg.Runtime
  alias SmolqueryPg.Session
  alias ThousandIsland.Socket

  @impl ThousandIsland.Handler
  def handle_connection(_socket, %Runtime{} = runtime) do
    {:continue,
     %{runtime: runtime, buffer: <<>>, phase: :startup, params: %{}, session: nil, discard: false}}
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

  defp startup(socket, request, state) when request in [:ssl_request, :gssenc_request] do
    case Socket.send(socket, Protocol.deny_encryption()) do
      :ok -> drain(socket, state)
      {:error, _reason} -> {:close, state}
    end
  end

  defp startup(_socket, {:cancel_request, _pid, _key}, state), do: {:close, state}

  defp startup(socket, {:startup, params}, state) do
    case Socket.send(socket, Protocol.authentication_cleartext()) do
      :ok -> drain(socket, %{state | phase: :password, params: params})
      {:error, _reason} -> {:close, state}
    end
  end

  defp handle(socket, {:password, password}, %{phase: :password} = state) do
    if Plug.Crypto.secure_compare(password, state.runtime.password) do
      session = Session.new(state.runtime, state.params)

      messages =
        [Protocol.authentication_ok()] ++
          Session.startup_messages(session) ++ [Protocol.ready_for_query(:idle)]

      case Socket.send(socket, messages) do
        :ok -> drain(socket, %{state | phase: :ready, session: session})
        {:error, _reason} -> {:close, state}
      end
    else
      user = Map.get(state.params, "user", "")
      Logger.warning("pg wire: password authentication failed for user #{inspect(user)}")

      refuse(socket, state, "28P01", ~s|password authentication failed for user "#{user}"|)
    end
  end

  defp handle(_socket, :terminate, state), do: {:close, state}

  defp handle(socket, _message, %{phase: :password} = state),
    do: refuse(socket, state, "08P01", "expected a password message")

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

  defp handle(socket, {:unknown, tag, _body}, state) do
    message =
      "message type #{inspect(<<tag>>)} is not supported; " <>
        "this edge speaks the simple query protocol only"

    case Socket.send(socket, Protocol.error_response("0A000", message)) do
      :ok -> drain(socket, %{state | discard: true})
      {:error, _reason} -> {:close, state}
    end
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
