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

    with {:ok, socket} <- authenticate(socket, password) do
      answer = query_answer(socket)
      Process.put({__MODULE__, socket}, answer.backend)

      {:ok, socket, answer.params}
    end
  end

  defp authenticate(socket, password) do
    case recv(socket) do
      {:ok, {?R, <<3::32>>}} ->
        :ok = :gen_tcp.send(socket, frame(?p, [password, 0]))
        auth_outcome(socket)

      {:ok, {?R, <<10::32, _mechanisms::binary>>}} ->
        scram(socket, password)

      {:ok, {?E, body}} ->
        {:error, fields(body)}

      other ->
        {:error, other}
    end
  end

  defp auth_outcome(socket) do
    case recv(socket) do
      {:ok, {?R, <<0::32>>}} -> {:ok, socket}
      {:ok, {?E, body}} -> {:error, fields(body)}
      other -> {:error, other}
    end
  end

  defp scram(socket, password) do
    nonce = 18 |> :crypto.strong_rand_bytes() |> Base.encode64()
    client_first_bare = "n=,r=" <> nonce
    initial = "n,," <> client_first_bare

    :ok =
      :gen_tcp.send(
        socket,
        frame(?p, ["SCRAM-SHA-256", 0, <<byte_size(initial)::32>>, initial])
      )

    case recv(socket) do
      {:ok, {?R, <<11::32, server_first::binary>>}} ->
        {message, expected_signature} = client_final(password, client_first_bare, server_first)
        :ok = :gen_tcp.send(socket, message)

        verify_server(socket, expected_signature)

      {:ok, {?E, body}} ->
        {:error, fields(body)}

      other ->
        {:error, other}
    end
  end

  defp verify_server(socket, expected_signature) do
    case recv(socket) do
      {:ok, {?R, <<12::32, "v=", signature::binary>>}} ->
        if Base.decode64!(signature) == expected_signature,
          do: auth_outcome(socket),
          else: {:error, :server_signature_mismatch}

      {:ok, {?E, body}} ->
        {:error, fields(body)}

      other ->
        {:error, other}
    end
  end

  defp client_final(password, client_first_bare, server_first) do
    %{"r" => full_nonce, "s" => salt, "i" => iterations} = scram_attributes(server_first)
    {:ok, salt} = Base.decode64(salt)
    iterations = String.to_integer(iterations)
    salted = :crypto.pbkdf2_hmac(:sha256, password, salt, iterations, 32)
    client_key = :crypto.mac(:hmac, :sha256, salted, "Client Key")
    stored_key = :crypto.hash(:sha256, client_key)
    without_proof = "c=biws,r=" <> full_nonce
    auth_message = Enum.join([client_first_bare, server_first, without_proof], ",")
    signature = :crypto.mac(:hmac, :sha256, stored_key, auth_message)
    proof = Base.encode64(:crypto.exor(client_key, signature))
    server_key = :crypto.mac(:hmac, :sha256, salted, "Server Key")
    expected_signature = :crypto.mac(:hmac, :sha256, server_key, auth_message)

    {frame(?p, [without_proof, ",p=", proof]), expected_signature}
  end

  defp scram_attributes(message), do: SmolqueryPg.Scram.attributes(message)

  @doc """
  The `BackendKeyData` the server sent this socket at startup.
  """
  @spec backend_key(:gen_tcp.socket()) :: %{backend: {integer(), integer()}}
  def backend_key(socket), do: %{backend: Process.get({__MODULE__, socket})}

  @doc """
  Collects every backend message until `ReadyForQuery`.
  """
  @spec query_answer(:gen_tcp.socket()) :: map()
  def query_answer(socket),
    do: until_ready(socket, %{params: %{}, results: [], errors: [], notices: []})

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

    query_answer(socket)
  end

  @doc """
  One extended-protocol round: `Parse`, `Bind` with `params` as
  `{oid, format, value}` triples, `Describe` of the portal, `Execute` with
  `max_rows`, and `Sync`. `result_formats` is what `Bind` asks for.
  Answers the same map as `query/2`, plus `:parameters` (the
  `ParameterDescription` OIDs, when a statement was described), and
  `:suspended` when the portal was left open.
  """
  @spec extended(
          :gen_tcp.socket(),
          String.t(),
          [{pos_integer(), 0 | 1, binary() | nil}],
          keyword()
        ) ::
          map()
  def extended(socket, sql, params \\ [], opts \\ []) do
    result_formats = Keyword.get(opts, :result_formats, [])
    max_rows = Keyword.get(opts, :max_rows, 0)

    declared =
      Keyword.get(opts, :declared, Enum.map(params, fn {oid, _format, _value} -> oid end))

    messages = [
      parse("", sql, declared),
      if(Keyword.get(opts, :describe_statement, false), do: describe(?S, ""), else: []),
      bind("", "", params, result_formats),
      describe(?P, ""),
      execute("", max_rows),
      frame(?S, [])
    ]

    :ok = :gen_tcp.send(socket, messages)

    query_answer(socket)
  end

  @doc """
  A `Parse` message.
  """
  @spec parse(String.t(), String.t(), [non_neg_integer()]) :: iodata()
  def parse(name, sql, oids),
    do: frame(?P, [name, 0, sql, 0, <<length(oids)::16>>, Enum.map(oids, &<<&1::32>>)])

  @doc """
  A `Bind` message.
  """
  @spec bind(String.t(), String.t(), [{pos_integer(), 0 | 1, binary() | nil}], [0 | 1]) ::
          iodata()
  def bind(portal, statement, params, result_formats) do
    frame(?B, [
      portal,
      0,
      statement,
      0,
      <<length(params)::16>>,
      Enum.map(params, fn {_oid, format, _value} -> <<format::16>> end),
      <<length(params)::16>>,
      Enum.map(params, fn
        {_oid, _format, nil} -> <<-1::32-signed>>
        {_oid, _format, value} -> [<<byte_size(value)::32>>, value]
      end),
      <<length(result_formats)::16>>,
      Enum.map(result_formats, &<<&1::16>>)
    ])
  end

  @doc """
  A `Describe` (`?S` or `?P`) message.
  """
  @spec describe(byte(), String.t()) :: iodata()
  def describe(kind, name), do: frame(?D, [kind, name, 0])

  @doc """
  An `Execute` message.
  """
  @spec execute(String.t(), non_neg_integer()) :: iodata()
  def execute(portal, max_rows), do: frame(?E, [portal, 0, <<max_rows::32>>])

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

  defp absorb({?K, <<pid::32, key::32>>}, acc), do: Map.put(acc, :backend, {pid, key})
  defp absorb({tag, <<>>}, acc) when tag in [?1, ?2, ?3], do: acc
  defp absorb({?n, <<>>}, acc), do: acc
  defp absorb({?s, <<>>}, acc), do: Map.put(acc, :suspended, true)

  defp absorb({?t, <<count::16, rest::binary>>}, acc),
    do: Map.put(acc, :parameters, for(<<oid::32 <- binary_part(rest, 0, count * 4)>>, do: oid))

  defp absorb({?T, body}, acc),
    do: %{acc | results: [%{columns: columns(body), rows: []} | acc.results]}

  defp absorb({?D, body}, %{results: [current | rest]} = acc),
    do: %{acc | results: [%{current | rows: [row(body) | current.rows]} | rest]}

  defp absorb({?D, body}, %{results: []} = acc),
    do: %{acc | results: [%{columns: [], rows: [row(body)]}]}

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
