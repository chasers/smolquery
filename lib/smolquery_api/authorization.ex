defmodule SmolqueryApi.Authorization do
  @moduledoc """
  Authorizes an authenticated API context for one closed route capability.

  Pipelines pass a compile-time capability atom to `init/1`; values outside the
  API capability contract fail application boot rather than becoming a client
  input. The plug runs before body parsers and controllers, so denied requests
  do not consume request bodies or reach a service or catalog client.
  """

  @behaviour Plug

  alias Smolquery.Auth
  alias Smolquery.Auth.Policy
  alias SmolqueryApi.Errors

  @api_capabilities [:query, :ingest, :catalog_manage]

  @impl Plug
  def init(opts) when is_list(opts) do
    capability = Keyword.fetch!(opts, :capability)

    if capability in @api_capabilities do
      capability
    else
      raise ArgumentError, "unsupported API capability"
    end
  end

  @impl Plug
  def call(conn, capability) when capability in @api_capabilities do
    case Auth.fetch_context(conn) do
      {:ok, context} -> authorize(conn, context, capability)
      :error -> unauthenticated(conn)
    end
  end

  defp authorize(conn, context, capability) do
    case Policy.authorize(context, capability) do
      :ok -> conn
      {:error, :forbidden} -> forbidden(conn)
      {:error, _reason} -> unauthenticated(conn)
    end
  end

  defp unauthenticated(conn) do
    conn
    |> Errors.send_error(401, "UNAUTHENTICATED", "missing or invalid API credential")
    |> Plug.Conn.halt()
  end

  defp forbidden(conn) do
    conn
    |> Errors.send_error(403, "PERMISSION_DENIED", "insufficient API capability")
    |> Plug.Conn.halt()
  end
end
