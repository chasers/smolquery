defmodule SmolqueryWeb.AuthController do
  @moduledoc "Fail-closed browser OIDC login, callback, and local logout endpoints."

  use SmolqueryWeb, :controller

  alias SmolqueryWeb.{Auth, OIDC, Runtime}

  def login(conn, _params) do
    case Runtime.fetch(SmolqueryWeb) do
      {:ok, %{auth_mode: :oidc} = runtime} ->
        with {:ok, url, transaction} <- OIDC.begin(runtime),
             {:ok, conn} <- OIDC.put_transaction(conn, transaction) do
          conn
          |> no_store()
          |> redirect(external: url)
        else
          _failure -> error(conn)
        end

      _ ->
        error(conn)
    end
  end

  def callback(conn, %{"state" => state, "code" => code}) do
    case OIDC.take_transaction(conn, state) do
      {:ok, transaction, conn} ->
        with {:ok, runtime} <- oidc_runtime(),
             {:ok, context} <-
               OIDC.authenticate(runtime, transaction, code, oidc_options(runtime)),
             true <- Auth.coarse_web_access?(context),
             {:ok, conn} <- Auth.assign_identity(conn, context) do
          conn |> no_store() |> redirect(to: "/")
        else
          _failure -> error(conn)
        end

      {:error, :invalid_transaction, conn} ->
        error(conn)
    end
  end

  def callback(conn, %{"state" => state}) do
    conn =
      case OIDC.take_transaction(conn, state) do
        {:ok, _transaction, conn} -> conn
        {:error, :invalid_transaction, conn} -> conn
      end

    error(conn)
  end

  def callback(conn, _params), do: error(conn)

  def logout(conn, _params) do
    conn
    |> no_store()
    |> OIDC.clear_transactions()
    |> configure_session(drop: true)
    |> redirect(to: "/")
  end

  defp oidc_options(%{oidc_http_client: client}) when is_function(client, 2),
    do: [http_client: client]

  defp oidc_options(_runtime), do: []

  defp oidc_runtime do
    case Runtime.fetch(SmolqueryWeb) do
      {:ok, %{auth_mode: :oidc} = runtime} -> {:ok, runtime}
      _ -> :error
    end
  end

  defp error(conn) do
    conn
    |> no_store()
    |> put_status(:bad_request)
    |> text("authentication failed")
  end

  defp no_store(conn), do: put_resp_header(conn, "cache-control", "no-store")
end
