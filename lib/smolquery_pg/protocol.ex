defmodule SmolqueryPg.Protocol do
  @moduledoc """
  The Postgres frontend/backend protocol, version 3.0, as pure functions
  over binaries (PL-58).

  Decoding never blocks: `decode_startup/1` and `decode/1` answer
  `:incomplete` when the buffer holds less than one message, and the caller
  keeps reading. Encoding answers iodata the caller sends as-is.

  The first packet a client sends has no type byte. It is a length, then a
  protocol code: `196608` opens a session and carries the startup
  parameters; the three request codes (`SSLRequest`, `GSSENCRequest`,
  `CancelRequest`) each ask for something the session does not yet exist
  for. Every later message is one type byte, an `int32` length that counts
  itself but not the type byte, and a body.

  ## Message size

  A length field is untrusted. `@max_message_bytes` bounds what a single
  message may claim, so a client cannot make the handler wait to buffer a
  gigabyte before it sees the first byte is nonsense.
  """

  import Bitwise

  @protocol_version 196_608
  @ssl_request 80_877_103
  @gssenc_request 80_877_104
  @cancel_request 80_877_102
  @max_message_bytes 64 * 1024 * 1024

  @type frontend ::
          {:query, String.t()}
          | {:password, String.t()}
          | :terminate
          | :sync
          | {:unknown, byte(), binary()}

  @type startup ::
          :ssl_request
          | :gssenc_request
          | {:cancel_request, integer(), integer()}
          | {:startup, %{String.t() => String.t()}}

  @type ready_status :: :idle | :transaction | :failed

  @type field :: %{
          name: String.t(),
          oid: pos_integer(),
          typlen: integer(),
          typmod: integer(),
          format: 0 | 1
        }

  @doc """
  The message length the codec refuses.
  """
  @spec max_message_bytes() :: pos_integer()
  def max_message_bytes, do: @max_message_bytes

  @doc """
  The first packet of a connection.
  """
  @spec decode_startup(binary()) ::
          {:ok, startup(), binary()} | :incomplete | {:error, term()}
  def decode_startup(<<length::32, _rest::binary>>) when length > @max_message_bytes,
    do: {:error, {:message_too_large, length}}

  def decode_startup(<<length::32, _rest::binary>>) when length < 8,
    do: {:error, {:invalid_length, length}}

  def decode_startup(<<length::32, rest::binary>> = buffer)
      when byte_size(rest) >= length - 4 do
    body_size = length - 8
    <<_length::32, code::32, body::binary-size(^body_size), remainder::binary>> = buffer

    with {:ok, message} <- startup_message(code, body) do
      {:ok, message, remainder}
    end
  end

  def decode_startup(_buffer), do: :incomplete

  defp startup_message(@ssl_request, _body), do: {:ok, :ssl_request}
  defp startup_message(@gssenc_request, _body), do: {:ok, :gssenc_request}

  defp startup_message(@cancel_request, <<pid::32, key::32>>),
    do: {:ok, {:cancel_request, pid, key}}

  defp startup_message(@protocol_version, body), do: {:ok, {:startup, parameters(body)}}

  defp startup_message(code, _body),
    do: {:error, {:unsupported_protocol, code >>> 16, code &&& 0xFFFF}}

  defp parameters(body) do
    body
    |> String.split(<<0>>)
    |> Enum.chunk_every(2, 2, :discard)
    |> Enum.reject(fn [key, _value] -> key == "" end)
    |> Map.new(fn [key, value] -> {key, value} end)
  end

  @doc """
  One typed message after startup.
  """
  @spec decode(binary()) :: {:ok, frontend(), binary()} | :incomplete | {:error, term()}
  def decode(<<_tag, length::32, _rest::binary>>) when length > @max_message_bytes,
    do: {:error, {:message_too_large, length}}

  def decode(<<_tag, length::32, _rest::binary>>) when length < 4,
    do: {:error, {:invalid_length, length}}

  def decode(<<tag, length::32, rest::binary>>) when byte_size(rest) >= length - 4 do
    body_size = length - 4
    <<body::binary-size(^body_size), remainder::binary>> = rest

    {:ok, message(tag, body), remainder}
  end

  def decode(_buffer), do: :incomplete

  defp message(?Q, body), do: {:query, cstring(body)}
  defp message(?p, body), do: {:password, cstring(body)}
  defp message(?X, _body), do: :terminate
  defp message(?S, _body), do: :sync
  defp message(tag, body), do: {:unknown, tag, body}

  defp cstring(body) do
    case :binary.split(body, <<0>>) do
      [string, _rest] -> string
      [string] -> string
    end
  end

  @doc """
  The one-byte answer to `SSLRequest` and `GSSENCRequest`: not here.
  """
  @spec deny_encryption() :: iodata()
  def deny_encryption, do: "N"

  @doc """
  `AuthenticationOk`.
  """
  @spec authentication_ok() :: iodata()
  def authentication_ok, do: frame(?R, <<0::32>>)

  @doc """
  `AuthenticationCleartextPassword`: the client must answer with a
  `PasswordMessage`.
  """
  @spec authentication_cleartext() :: iodata()
  def authentication_cleartext, do: frame(?R, <<3::32>>)

  @doc """
  `ParameterStatus`, one per session parameter the server reports.
  """
  @spec parameter_status(String.t(), String.t()) :: iodata()
  def parameter_status(name, value), do: frame(?S, [name, 0, value, 0])

  @doc """
  `BackendKeyData`: what a `CancelRequest` must quote back.
  """
  @spec backend_key_data(integer(), integer()) :: iodata()
  def backend_key_data(pid, key), do: frame(?K, <<pid::32, key::32>>)

  @doc """
  `ReadyForQuery`, with the transaction status the client shows in its
  prompt.
  """
  @spec ready_for_query(ready_status()) :: iodata()
  def ready_for_query(:idle), do: frame(?Z, "I")
  def ready_for_query(:transaction), do: frame(?Z, "T")
  def ready_for_query(:failed), do: frame(?Z, "E")

  @doc """
  `RowDescription` for `fields`, every column in the format the field names.
  """
  @spec row_description([field()]) :: iodata()
  def row_description(fields) do
    body = [
      <<length(fields)::16>>,
      Enum.map(fields, fn field ->
        [
          field.name,
          0,
          <<0::32, 0::16, field.oid::32, field.typlen::16-signed, field.typmod::32-signed,
            field.format::16>>
        ]
      end)
    ]

    frame(?T, body)
  end

  @doc """
  `DataRow`: one encoded value per column, `nil` for SQL NULL.
  """
  @spec data_row([iodata() | nil]) :: iodata()
  def data_row(values) do
    body = [
      <<length(values)::16>>,
      Enum.map(values, fn
        nil -> <<-1::32-signed>>
        value -> [<<IO.iodata_length(value)::32>>, value]
      end)
    ]

    frame(?D, body)
  end

  @doc """
  `CommandComplete` with `tag`, for example `SELECT 3` or `SET`.
  """
  @spec command_complete(String.t()) :: iodata()
  def command_complete(tag), do: frame(?C, [tag, 0])

  @doc """
  `EmptyQueryResponse`: the client sent no statement at all.
  """
  @spec empty_query_response() :: iodata()
  def empty_query_response, do: frame(?I, <<>>)

  @doc """
  `ErrorResponse` with an `ERROR` severity, a SQLSTATE `code`, and a message.
  """
  @spec error_response(String.t(), String.t()) :: iodata()
  def error_response(code, message), do: frame(?E, fields("ERROR", code, message))

  @doc """
  `NoticeResponse` with a `WARNING` severity.
  """
  @spec notice_response(String.t(), String.t()) :: iodata()
  def notice_response(code, message), do: frame(?N, fields("WARNING", code, message))

  defp fields(severity, code, message),
    do: [?S, severity, 0, ?V, severity, 0, ?C, code, 0, ?M, message, 0, 0]

  defp frame(tag, body), do: [tag, <<IO.iodata_length(body) + 4::32>>, body]
end
