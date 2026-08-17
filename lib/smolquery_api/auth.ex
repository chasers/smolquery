defmodule SmolqueryApi.Auth do
  @moduledoc """
  Bearer authentication for every `/v1` route (PL-8 D5).

  Static credentials use one key compared in constant time. OIDC credentials
  use strict access-token verification against the supervised provider cache.
  `GET /healthz` is exempt — a load
  balancer probing liveness holds no credentials — and `/metrics` answers to
  the *internal* secret instead of the API key: metrics are for operators,
  not tenants, and the scraper is the same class of caller as a hot-tier
  reader. The plug sits between `:match` and `:dispatch`, so an
  unauthenticated request learns nothing about which routes exist: it is a
  401 whether the path matches or not.

  The instance name arrives in `conn.private.smolquery_api`, placed there by
  `SmolqueryApi.Router` before the pipeline runs. No published runtime for
  that name means the API is not actually up here, and the answer is the same
  401 — never an open door.
  """

  @behaviour Plug

  import Plug.Conn

  alias Smolquery.Auth
  alias Smolquery.Auth.Context
  alias Smolquery.Auth.OIDC.Token
  alias Smolquery.InternalSecret
  alias SmolqueryApi.Errors
  alias SmolqueryApi.Runtime

  @impl Plug
  def init(opts), do: opts

  @impl Plug
  def call(%Plug.Conn{method: "GET", path_info: ["healthz"]} = conn, _opts), do: conn

  def call(%Plug.Conn{path_info: ["metrics"]} = conn, _opts) do
    if internal?(conn) do
      conn
    else
      conn
      |> Errors.send_error(401, "UNAUTHENTICATED", "missing or invalid internal secret")
      |> halt()
    end
  end

  def call(conn, _opts) do
    case authenticated_context(conn) do
      {:ok, context} ->
        Auth.assign_context(conn, context)

      :error ->
        conn
        |> Errors.send_error(401, "UNAUTHENTICATED", "missing or invalid API credential")
        |> halt()
    end
  end

  defp internal?(conn) do
    case get_req_header(conn, InternalSecret.header()) do
      [secret] -> Plug.Crypto.secure_compare(secret, InternalSecret.value())
      _missing -> false
    end
  end

  defp authenticated_context(conn) do
    with ["Bearer " <> token] <- get_req_header(conn, "authorization"),
         {:ok, runtime} <- Runtime.fetch(conn.private.smolquery_api) do
      authenticate(runtime, token)
    else
      _unauthenticated -> :error
    end
  end

  defp authenticate(%{auth_mode: :static, api_key: key, context: context}, token)
       when is_binary(key) do
    if Plug.Crypto.secure_compare(token, key), do: {:ok, context}, else: :error
  end

  defp authenticate(%{auth_mode: :oidc, oidc: config, name: name}, token) do
    provider = Module.concat(name, "OIDCProvider")

    case Token.authenticate(token, config, provider) do
      {:ok, context} ->
        if coarse_api_access?(context), do: {:ok, context}, else: :error

      :error ->
        :error
    end
  end

  defp authenticate(_runtime, _token), do: :error

  defp coarse_api_access?(context) do
    Enum.all?([:query, :ingest, :catalog_manage], &Context.granted?(context, &1))
  end
end
