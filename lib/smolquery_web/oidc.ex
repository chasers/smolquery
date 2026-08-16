defmodule SmolqueryWeb.OIDC do
  @moduledoc """
  Authorization-code transactions for the browser OIDC flow.

  Each login uses an independently named, short-lived encrypted cookie. Concurrent
  login and callback responses therefore update different browser cookie slots
  instead of racing over one session value. A callback removes only its matching
  cookie before exchange, and the authorization code is single-use at the provider.
  Provider metadata and signing keys come only from the supervised provider cache;
  this module never runs discovery itself.
  """

  alias Assent.Strategy.OIDC
  alias Smolquery.Auth.OIDC.{Provider, Token}
  alias SmolqueryWeb.Runtime

  @transaction_ttl 300
  @max_transactions 4
  @transaction_cookie_prefix "_smolquery_oidc_"
  @transaction_cookie_salt "smolquery oidc transaction"
  @transaction_cookie_path "/auth"
  @transaction_cookie_options [
    http_only: true,
    same_site: "Lax",
    secure: Mix.env() == :prod,
    max_age: @transaction_ttl,
    path: @transaction_cookie_path
  ]
  @request_options [retry: false, redirect: false, raw: true, decode_body: false]

  @type transaction :: %{
          state: binary(),
          nonce: binary(),
          verifier: binary(),
          created_at: integer()
        }

  @doc "Stores a login transaction in its own encrypted callback cookie."
  def put_transaction(%Plug.Conn{} = conn, transaction) do
    with encoded when is_map(encoded) <- encode_transaction(transaction),
         {:ok, transaction} <- decode_transaction(encoded) do
      token =
        Phoenix.Token.encrypt(
          token_context(),
          @transaction_cookie_salt,
          encoded,
          max_age: @transaction_ttl
        )

      conn =
        conn
        |> Plug.Conn.fetch_cookies()
        |> prune_transaction_cookies(transaction.state)
        |> Plug.Conn.put_resp_cookie(
          transaction_cookie_name(transaction.state),
          token,
          @transaction_cookie_options
        )

      {:ok, conn}
    else
      _failure -> {:error, :invalid_transaction}
    end
  end

  def put_transaction(_conn, _transaction), do: {:error, :invalid_transaction}

  @doc "Removes and returns only the transaction cookie matching callback state."
  def take_transaction(%Plug.Conn{} = conn, provided)
      when is_binary(provided) and byte_size(provided) <= 256 do
    conn = Plug.Conn.fetch_cookies(conn)
    name = transaction_cookie_name(provided)
    token = Map.get(conn.req_cookies, name)
    conn = Plug.Conn.delete_resp_cookie(conn, name, path: @transaction_cookie_path)

    with {:ok, transaction} <- decrypt_transaction(token),
         {:ok, transaction} <- consume(transaction, provided) do
      {:ok, transaction, conn}
    else
      _failure -> {:error, :invalid_transaction, conn}
    end
  end

  def take_transaction(%Plug.Conn{} = conn, _provided),
    do: {:error, :invalid_transaction, conn}

  def take_transaction(conn, _provided), do: {:error, :invalid_transaction, conn}

  @doc "Expires every outstanding browser authorization transaction cookie."
  def clear_transactions(%Plug.Conn{} = conn) do
    conn = Plug.Conn.fetch_cookies(conn)

    Enum.reduce(conn.req_cookies, conn, fn {name, _token}, conn ->
      if String.starts_with?(name, @transaction_cookie_prefix),
        do: Plug.Conn.delete_resp_cookie(conn, name, path: @transaction_cookie_path),
        else: conn
    end)
  end

  def clear_transactions(conn), do: conn

  @doc "Serializes a bounded authorization transaction for an encrypted cookie."
  def encode_transaction(%{
        state: state,
        nonce: nonce,
        verifier: verifier,
        created_at: created_at
      })
      when is_binary(state) and is_binary(nonce) and is_binary(verifier) and
             is_integer(created_at) do
    %{
      "v" => 1,
      "state" => state,
      "nonce" => nonce,
      "verifier" => verifier,
      "created_at" => created_at
    }
  end

  def encode_transaction(_transaction), do: :error

  @doc "Reconstructs a bounded authorization transaction from the encrypted session."
  def decode_transaction(%{
        "v" => 1,
        "state" => state,
        "nonce" => nonce,
        "verifier" => verifier,
        "created_at" => created_at
      }) do
    with :ok <- bounded_binary(state, 1, 256),
         :ok <- bounded_binary(nonce, 1, 256),
         :ok <- bounded_binary(verifier, 43, 128),
         true <- is_integer(created_at) do
      {:ok, %{state: state, nonce: nonce, verifier: verifier, created_at: created_at}}
    else
      _failure -> {:error, :invalid_transaction}
    end
  end

  def decode_transaction(%{"v" => 2, "transactions" => [transaction]}),
    do: decode_transaction(transaction)

  def decode_transaction(_value), do: {:error, :invalid_transaction}

  @doc "Creates an explicit-state, nonce, and S256 authorization transaction."
  def begin(%Runtime{} = runtime, opts \\ []) do
    now = Keyword.get(opts, :now, System.system_time(:second))

    with {:ok, metadata} <- provider_metadata(runtime),
         {:ok, %{url: url, state: state, nonce: nonce, verifier: verifier}} <-
           authorization_url(runtime, metadata) do
      {:ok, url, %{state: state, nonce: nonce, verifier: verifier, created_at: now}}
    else
      _failure -> {:error, :login_unavailable}
    end
  end

  @doc "Validates and consumes the encrypted-session transaction before exchange."
  def consume(transaction, provided, opts \\ [])

  def consume(%{} = transaction, provided, opts)
      when is_binary(provided) and byte_size(provided) <= 256 do
    now = Keyword.get(opts, :now, System.system_time(:second))

    with {:ok, decoded} <- decode_transaction(encode_transaction(transaction)),
         age when age >= 0 and age <= @transaction_ttl <- now - decoded.created_at,
         true <- secure_equal?(decoded.state, provided) do
      {:ok, decoded}
    else
      _failure -> {:error, :invalid_transaction}
    end
  end

  def consume(_transaction, _provided, _opts), do: {:error, :invalid_transaction}

  @doc "Exchanges a code and strictly verifies the returned ID assertion."
  def authenticate(runtime, transaction, code, opts \\ [])

  def authenticate(%Runtime{} = runtime, %{} = transaction, code, opts)
      when is_binary(code) and byte_size(code) > 0 and byte_size(code) <= 4_096 do
    client = Keyword.get(opts, :http_client, &request/2)

    with {:ok, metadata} <- provider_metadata(runtime),
         {:ok, response} <- exchange(client, runtime, metadata, code, transaction.verifier),
         {:ok, context} <- verify_response(response, runtime, transaction, code) do
      {:ok, context}
    else
      _failure -> {:error, :authentication_failed}
    end
  end

  def authenticate(_runtime, _transaction, _code, _opts),
    do: {:error, :authentication_failed}

  defp provider_metadata(runtime) do
    Provider.metadata(provider(runtime))
  catch
    :exit, _reason -> {:error, :provider_unavailable}
  end

  defp authorization_url(runtime, metadata) do
    config = runtime.oidc
    nonce = random_value()

    with {:ok, %{url: url, session_params: params}} <-
           OIDC.authorize_url(
             base_url: config.issuer,
             client_id: config.web_client_id,
             redirect_uri: config.web_redirect_uri,
             openid_configuration: metadata,
             authorization_params: [scope: authorization_scopes(config.web_scopes)],
             state: random_value(),
             nonce: nonce,
             code_verifier: true
           ) do
      {:ok,
       %{
         url: url,
         state: Map.fetch!(params, :state),
         nonce: nonce,
         verifier: Map.fetch!(params, :code_verifier)
       }}
    end
  end

  defp exchange(client, runtime, metadata, code, verifier) do
    config = runtime.oidc
    token_url = Map.fetch!(metadata, "token_endpoint")

    body =
      URI.encode_query(
        code: code,
        redirect_uri: config.web_redirect_uri,
        grant_type: "authorization_code",
        client_id: config.web_client_id,
        code_verifier: verifier
      )

    headers =
      basic_auth(
        [
          {"content-type", "application/x-www-form-urlencoded"},
          {"accept", "application/json"}
        ],
        config
      )

    options =
      Keyword.merge(@request_options,
        method: :post,
        body: body,
        headers: headers,
        max_body_bytes: config.max_body_bytes,
        connect_options: [timeout: config.connect_timeout_ms],
        receive_timeout: config.receive_timeout_ms,
        request_timeout: config.request_timeout_ms
      )

    case client.(token_url, options) do
      {:ok, %Req.Response{status: 200, body: response} = http_response} ->
        case json_response?(http_response) do
          :ok -> decode_response(response, config.max_body_bytes)
          error -> error
        end

      _failure ->
        {:error, :token_exchange_failed}
    end
  end

  defp basic_auth(headers, %{web_client_auth_method: :none}), do: headers

  # RFC 6749 section 2.3.1 applies form encoding to both components before
  # constructing the HTTP Basic value.
  defp basic_auth(headers, %{web_client_auth_method: :client_secret_basic} = config) do
    client_id = URI.encode_www_form(config.web_client_id)
    client_secret = URI.encode_www_form(config.web_client_secret)
    value = Base.encode64(client_id <> ":" <> client_secret)
    [{"authorization", "Basic " <> value} | headers]
  end

  defp json_response?(response) do
    values = Req.Response.get_header(response, "content-type")

    if Enum.any?(values, fn value ->
         value
         |> String.split(";", parts: 2)
         |> hd()
         |> String.trim()
         |> String.downcase() == "application/json"
       end),
       do: :ok,
       else: {:error, :invalid_token_response}
  end

  defp verify_response(
         %{"id_token" => id_token, "access_token" => access_token},
         runtime,
         transaction,
         code
       )
       when is_binary(id_token) and is_binary(access_token) do
    Token.authenticate_web(id_token, runtime.oidc, provider(runtime), transaction.nonce,
      access_token: access_token,
      code: code
    )
  end

  defp verify_response(_response, _runtime, _transaction, _code),
    do: {:error, :invalid_token_response}

  defp decode_response(body, max) when is_binary(body) and byte_size(body) <= max do
    case JSON.decode(body) do
      {:ok, map} when is_map(map) -> {:ok, map}
      _failure -> {:error, :invalid_token_response}
    end
  end

  defp decode_response(_body, _max), do: {:error, :invalid_token_response}

  @doc false
  def request(url, options) do
    key = make_ref()
    max = Keyword.fetch!(options, :max_body_bytes)
    Process.put({__MODULE__, key}, {0, []})

    callback = fn {:data, data}, {request, response} ->
      {size, chunks} = Process.get({__MODULE__, key}, {0, []})

      if is_binary(data) and size + byte_size(data) <= max do
        Process.put({__MODULE__, key}, {size + byte_size(data), [data | chunks]})
        {:cont, {request, response}}
      else
        Process.put({__MODULE__, {key, :error}}, :response_too_large)
        {:halt, {request, response}}
      end
    end

    result =
      try do
        request_result =
          Req.request(
            Keyword.merge(Keyword.delete(options, :max_body_bytes), url: url, into: callback)
          )

        {request_result, Process.get({__MODULE__, key}, {0, []}),
         Process.get({__MODULE__, {key, :error}})}
      after
        Process.delete({__MODULE__, key})
        Process.delete({__MODULE__, {key, :error}})
      end

    case result do
      {{:ok, %Req.Response{} = response}, {_size, chunks}, nil} ->
        {:ok, %{response | body: chunks |> Enum.reverse() |> IO.iodata_to_binary()}}

      {{:error, reason}, _body, _error} ->
        {:error, reason}

      {_result, _body, reason} ->
        {:error, reason || :invalid_response}
    end
  end

  defp provider(%Runtime{name: name}), do: Module.concat(name, "OIDCProvider")

  defp prune_transaction_cookies(conn, new_state) do
    new_name = transaction_cookie_name(new_state)

    candidates =
      conn.req_cookies
      |> Enum.filter(fn {name, _token} ->
        String.starts_with?(name, @transaction_cookie_prefix) and name != new_name
      end)

    keep =
      candidates
      |> Enum.flat_map(fn {name, token} ->
        case decrypt_transaction(token) do
          {:ok, transaction} -> [{name, transaction.created_at}]
          {:error, :invalid_transaction} -> []
        end
      end)
      |> Enum.sort_by(&elem(&1, 1), :desc)
      |> Enum.take(@max_transactions - 1)
      |> MapSet.new(&elem(&1, 0))

    Enum.reduce(candidates, conn, fn {name, _token}, conn ->
      if MapSet.member?(keep, name),
        do: conn,
        else: Plug.Conn.delete_resp_cookie(conn, name, path: @transaction_cookie_path)
    end)
  end

  defp decrypt_transaction(nil), do: {:error, :invalid_transaction}

  defp decrypt_transaction(token) when is_binary(token) do
    with {:ok, encoded} <-
           Phoenix.Token.decrypt(
             token_context(),
             @transaction_cookie_salt,
             token,
             max_age: @transaction_ttl
           ),
         {:ok, transaction} <- decode_transaction(encoded),
         true <- active_transaction?(transaction, System.system_time(:second)) do
      {:ok, transaction}
    else
      _failure -> {:error, :invalid_transaction}
    end
  end

  defp decrypt_transaction(_token), do: {:error, :invalid_transaction}

  defp authorization_scopes(scopes) do
    scopes |> Enum.reject(&(&1 == "openid")) |> Enum.join(" ")
  end

  defp token_context do
    :smolquery
    |> Application.fetch_env!(SmolqueryWeb.Endpoint)
    |> Keyword.fetch!(:secret_key_base)
  end

  @doc false
  def decode_transaction_cookie(token), do: decrypt_transaction(token)

  @doc false
  def transaction_cookie_name(state) do
    digest = :crypto.hash(:sha256, state) |> Base.url_encode64(padding: false)
    @transaction_cookie_prefix <> digest
  end

  defp active_transaction?(transaction, now) do
    age = now - transaction.created_at
    age >= 0 and age <= @transaction_ttl
  end

  defp bounded_binary(value, min, max)
       when is_binary(value) and byte_size(value) >= min and byte_size(value) <= max,
       do: :ok

  defp bounded_binary(_value, _min, _max), do: :error

  defp random_value,
    do: 32 |> :crypto.strong_rand_bytes() |> Base.url_encode64(padding: false)

  defp secure_equal?(left, right) when is_binary(left) and is_binary(right),
    do: byte_size(left) == byte_size(right) and :crypto.hash_equals(left, right)

  defp secure_equal?(_left, _right), do: false
end
