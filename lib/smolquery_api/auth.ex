defmodule SmolqueryApi.Auth do
  @moduledoc """
  Bearer-key authentication for every `/v1` route (PL-8 D5).

  One static key, compared in constant time. `/healthz` is exempt — a load
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
  alias Smolquery.InternalSecret
  alias SmolqueryApi.Errors
  alias SmolqueryApi.Runtime

  @impl Plug
  def init(opts), do: opts

  @impl Plug
  def call(%Plug.Conn{path_info: ["healthz"]} = conn, _opts), do: conn

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
        |> Errors.send_error(401, "UNAUTHENTICATED", "missing or invalid API key")
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
    with ["Bearer " <> key] <- get_req_header(conn, "authorization"),
         {:ok, runtime} <- Runtime.fetch(conn.private.smolquery_api),
         :static <- runtime.auth_mode,
         true <- Plug.Crypto.secure_compare(key, runtime.api_key) do
      {:ok, runtime.context}
    else
      _unauthenticated -> :error
    end
  end
end
