defmodule Smolquery.Test.PgClient do
  @moduledoc """
  A simple-query-protocol client over `:gen_tcp`, for tests of the Postgres
  wire edge (PL-58).

  `Postgrex` cannot drive layer 1: it speaks the extended protocol and
  bootstraps from `pg_type`. This client sends a startup packet, answers a
  cleartext password request, and runs `Query` messages, collecting every
  backend message until `ReadyForQuery`.
  """

  @protocol_version 196_608

  @type message :: {byte(), binary()}

  @doc """
  Connects and authenticates. Answers the socket and the startup
  parameters the server reported.
  """
  @spec connect(:inet.port_number(), keyword()) ::
          {:ok, :gen_tcp.socket(), %{String.t() => String.t()}} | {:error, term()}
  def connect(port, opts \\ []) do
    user = Keyword.get(opts, :user, "smolquery")
    password = Keyword.get(opts, :password, "")
    database = Keyword.get(opts, :database, "smolquery")

    {:ok, socket} = :gen_tcp.connect(~c"127.0.0.1", port, [:binary, active: false, packet: :raw])

    :ok = :gen_tcp.send(socket, startup([{"user", user}, {"database", database}]))

    with {:ok, {?R, <<3::32>>}} <- recv(socket),
         :ok <- :gen_tcp.send(socket, frame(?p, [password, 0])),
         {:ok, {?R, <<0::32>>}} <- recv(socket) do
      {:ok, socket,
       until_ready(socket, %{params: %{}, results: [], errors: [], notices: []}).params}
    else
      {:ok, {?E, body}} -> {:error, fields(body)}
      other -> {:error, other}
    end
  end

  @doc """
  Sends `sql` as one `Query` and collects the answer.

  `results` is one entry per statement that answered rows or a command tag:
  `%{columns: [...], rows: [[text | nil]], tag: "SELECT 2"}`. `errors` and
  `notices` are the decoded field maps. `status` is the `ReadyForQuery`
  status byte, `?I`, `?T`, or `?E`.
  """
  @spec query(:gen_tcp.socket(), String.t()) :: map()
  def query(socket, sql) do
    :ok = :gen_tcp.send(socket, frame(?Q, [sql, 0]))

    until_ready(socket, %{params: %{}, results: [], errors: [], notices: []})
  end

  @doc """
  Sends raw bytes, for protocol-level tests.
  """
  @spec send_raw(:gen_tcp.socket(), iodata()) :: :ok
  def send_raw(socket, data), do: :gen_tcp.send(socket, data)

  @doc """
  One backend message: its type byte and body.
  """
  @spec recv(:gen_tcp.socket()) :: {:ok, message()} | {:error, term()}
  def recv(socket) do
    with {:ok, <<tag, length::32>>} <- :gen_tcp.recv(socket, 5, 5_000),
         {:ok, body} <- body(socket, length - 4) do
      {:ok, {tag, body}}
    end
  end

  defp body(_socket, 0), do: {:ok, <<>>}
  defp body(socket, size), do: :gen_tcp.recv(socket, size, 5_000)

  @doc """
  The startup packet for `params`.
  """
  @spec startup([{String.t(), String.t()}]) :: iodata()
  def startup(params) do
    body = [<<@protocol_version::32>>, Enum.map(params, fn {k, v} -> [k, 0, v, 0] end), 0]

    [<<IO.iodata_length(body) + 4::32>>, body]
  end

  @doc """
  A typed frontend message.
  """
  @spec frame(byte(), iodata()) :: iodata()
  def frame(tag, body), do: [tag, <<IO.iodata_length(body) + 4::32>>, body]

  @doc """
  Decodes the fields of an `ErrorResponse` or `NoticeResponse` body.
  """
  @spec fields(binary()) :: %{String.t() => String.t()}
  def fields(body) do
    body
    |> String.split(<<0>>, trim: true)
    |> Map.new(fn <<type, value::binary>> -> {<<type>>, value} end)
  end

  defp until_ready(socket, acc) do
    case recv(socket) do
      {:ok, {?Z, <<status>>}} ->
        %{acc | results: Enum.reverse(acc.results)} |> Map.put(:status, status)

      {:ok, message} ->
        until_ready(socket, absorb(message, acc))

      {:error, reason} ->
        Map.put(acc, :status, {:error, reason})
    end
  end

  defp absorb({?S, body}, acc) do
    [name, value | _rest] = String.split(body, <<0>>)

    %{acc | params: Map.put(acc.params, name, value)}
  end

  defp absorb({?K, _body}, acc), do: acc

  defp absorb({?T, body}, acc),
    do: %{acc | results: [%{columns: columns(body), rows: []} | acc.results]}

  defp absorb({?D, body}, %{results: [current | rest]} = acc),
    do: %{acc | results: [%{current | rows: [row(body) | current.rows]} | rest]}

  defp absorb({?C, body}, acc) do
    tag = body |> String.split(<<0>>) |> hd()

    %{acc | results: complete(acc.results, tag)}
  end

  defp absorb({?I, _body}, acc),
    do: %{acc | results: [%{columns: [], rows: [], tag: ""} | acc.results]}

  defp absorb({?E, body}, acc), do: %{acc | errors: acc.errors ++ [fields(body)]}
  defp absorb({?N, body}, acc), do: %{acc | notices: acc.notices ++ [fields(body)]}

  defp complete([%{tag: _tagged} | _rest] = results, tag),
    do: [%{columns: [], rows: [], tag: tag} | results]

  defp complete([current | rest], tag),
    do: [current |> Map.put(:rows, Enum.reverse(current.rows)) |> Map.put(:tag, tag) | rest]

  defp complete([], tag), do: [%{columns: [], rows: [], tag: tag}]

  defp columns(<<count::16, rest::binary>>), do: columns(rest, count, [])

  defp columns(_rest, 0, acc), do: Enum.reverse(acc)

  defp columns(rest, count, acc) do
    [name, rest] = :binary.split(rest, <<0>>)

    <<_table::32, _attnum::16, oid::32, _typlen::16, typmod::32-signed, _format::16,
      rest::binary>> = rest

    columns(rest, count - 1, [%{name: name, oid: oid, typmod: typmod} | acc])
  end

  defp row(<<count::16, rest::binary>>), do: row(rest, count, [])

  defp row(_rest, 0, acc), do: Enum.reverse(acc)
  defp row(<<-1::32-signed, rest::binary>>, count, acc), do: row(rest, count - 1, [nil | acc])

  defp row(<<size::32, value::binary-size(size), rest::binary>>, count, acc),
    do: row(rest, count - 1, [value | acc])
end
