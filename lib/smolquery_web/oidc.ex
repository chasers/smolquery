defmodule SmolqueryWeb.OIDC do
  @moduledoc """
  Bounded authorization-code transactions for the browser OIDC flow.

  The encrypted browser session carries a small set of short-lived transactions
  so concurrent tabs and callbacks may land on different web nodes. A callback
  removes only its matching state before exchange; unrelated transactions remain
  available and the authorization code is single-use at the provider. Provider
  metadata and signing keys come only from the supervised provider cache; this
  module never runs discovery itself.
  """

  alias Assent.Strategy.OIDC
  alias Smolquery.Auth.OIDC.{Provider, Token}
  alias SmolqueryWeb.Runtime

  @transaction_ttl 300
  @max_transactions 4
  @request_options [retry: false, redirect: false, raw: true, decode_body: false]
  @transaction_key "oidc_transaction"

  @type transaction :: %{
          state: binary(),
          nonce: binary(),
          verifier: binary(),
          created_at: integer()
        }

  @doc "Returns the encrypted-session key for authorization transactions."
  def transaction_key, do: @transaction_key

  @doc "Adds one transaction to the bounded encrypted-session value."
  def add_transaction(value, transaction, opts \\ []) do
    now = Keyword.get(opts, :now, System.system_time(:second))

    with encoded when is_map(encoded) <- encode_transaction(transaction),
         {:ok, transaction} <- decode_transaction(encoded) do
      transactions =
        value
        |> decode_or_empty()
        |> Enum.filter(&active_transaction?(&1, now))
        |> Enum.reject(&secure_equal?(&1.state, transaction.state))
        |> Kernel.++([transaction])
        |> Enum.take(-@max_transactions)

      {:ok, encode_transactions(transactions)}
    else
      _failure -> {:error, :invalid_transaction}
    end
  end

  @doc "Removes and returns only the active transaction matching callback state."
  def take_transaction(value, provided, opts \\ [])

  def take_transaction(value, provided, opts)
      when is_binary(provided) and byte_size(provided) <= 256 do
    now = Keyword.get(opts, :now, System.system_time(:second))

    case decode_transactions(value) do
      {:ok, transactions} ->
        transactions = Enum.filter(transactions, &active_transaction?(&1, now))

        case Enum.find_index(transactions, &secure_equal?(&1.state, provided)) do
          nil ->
            {:error, :invalid_transaction, encode_transactions(transactions)}

          index ->
            {transaction, remaining} = List.pop_at(transactions, index)
            {:ok, transaction, encode_transactions(remaining)}
        end

      {:error, :invalid_transaction} ->
        {:error, :invalid_transaction, nil}
    end
  end

  def take_transaction(_value, _provided, _opts),
    do: {:error, :invalid_transaction, nil}

  @doc "Serializes a bounded authorization transaction for the encrypted session."
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

  defp decode_or_empty(value) do
    case decode_transactions(value) do
      {:ok, transactions} -> transactions
      {:error, :invalid_transaction} -> []
    end
  end

  defp decode_transactions(nil), do: {:ok, []}

  defp decode_transactions(%{"v" => 2, "transactions" => values})
       when is_list(values) and length(values) <= @max_transactions do
    Enum.reduce_while(values, {:ok, []}, fn value, {:ok, transactions} ->
      case decode_transaction(value) do
        {:ok, transaction} -> {:cont, {:ok, [transaction | transactions]}}
        {:error, :invalid_transaction} -> {:halt, {:error, :invalid_transaction}}
      end
    end)
    |> case do
      {:ok, transactions} -> {:ok, Enum.reverse(transactions)}
      error -> error
    end
  end

  defp decode_transactions(value) do
    case decode_transaction(value) do
      {:ok, transaction} -> {:ok, [transaction]}
      {:error, :invalid_transaction} -> {:error, :invalid_transaction}
    end
  end

  defp encode_transactions([]), do: nil

  defp encode_transactions(transactions),
    do: %{"v" => 2, "transactions" => Enum.map(transactions, &encode_transaction/1)}

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
