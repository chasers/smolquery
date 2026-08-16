defmodule SmolqueryWeb.AuthController do
  @moduledoc "Fail-closed browser OIDC login, callback, and local logout endpoints."

  use SmolqueryWeb, :controller

  alias SmolqueryWeb.{Auth, OIDC, Runtime}

  def login(conn, _params) do
    case Runtime.fetch(SmolqueryWeb) do
      {:ok, %{auth_mode: :oidc} = runtime} ->
        with {:ok, url, transaction} <- OIDC.begin(runtime),
             {:ok, transactions} <-
               OIDC.add_transaction(
                 get_session(conn, OIDC.transaction_key()),
                 transaction
               ) do
          conn
          |> no_store()
          |> put_session(OIDC.transaction_key(), transactions)
          |> redirect(external: url)
        else
          _failure -> error(conn)
        end

      _ ->
        error(conn)
    end
  end

  def callback(conn, %{"state" => state, "code" => code}) do
    case OIDC.take_transaction(get_session(conn, OIDC.transaction_key()), state) do
      {:ok, transaction, remaining} ->
        conn = put_transactions(conn, remaining)

        with {:ok, runtime} <- oidc_runtime(),
             {:ok, context} <-
               OIDC.authenticate(runtime, transaction, code, oidc_options(runtime)),
             true <- Auth.coarse_web_access?(context),
             {:ok, conn} <- Auth.assign_identity(conn, context) do
          conn |> no_store() |> redirect(to: "/")
        else
          _failure -> error(conn)
        end

      {:error, :invalid_transaction, remaining} ->
        conn |> put_transactions(remaining) |> error()
    end
  end

  def callback(conn, %{"state" => state}) do
    remaining =
      case OIDC.take_transaction(get_session(conn, OIDC.transaction_key()), state) do
        {:ok, _transaction, remaining} -> remaining
        {:error, :invalid_transaction, remaining} -> remaining
      end

    conn |> put_transactions(remaining) |> error()
  end

  def callback(conn, _params), do: error(conn)

  def logout(conn, _params) do
    conn
    |> no_store()
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

  defp put_transactions(conn, nil), do: delete_session(conn, OIDC.transaction_key())

  defp put_transactions(conn, transactions),
    do: put_session(conn, OIDC.transaction_key(), transactions)

  defp error(conn) do
    conn
    |> no_store()
    |> put_status(:bad_request)
    |> text("authentication failed")
  end

  defp no_store(conn), do: put_resp_header(conn, "cache-control", "no-store")
end
