defmodule Smolquery.Api.Router do
  @moduledoc """
  The HTTP front door — one listener, every external surface (PL-8 D1).

  A peer of the services, not part of one: routes dispatch only to service
  client modules and `Smolquery.Catalog`, never into a service's internals, so
  the split-out rules hold by construction. Started by the `:api` role.

  The pipeline is telemetry → match → auth → dispatch:
  `Plug.Telemetry` emits `[:smolquery, :api, :start | :stop]` around every
  request, and `Smolquery.Api.Auth` runs after `:match` so an unauthenticated
  request is answered 401 before any route logic — including the 404 catch-all —
  can reveal what exists.

  v1 surface so far:

      GET /healthz    liveness, no auth

  Failures speak `Smolquery.Api.Errors`' envelope, and nothing else.
  """

  use Plug.Router

  alias Smolquery.Api.Errors
  alias Smolquery.Api.Runtime

  plug(Plug.Telemetry, event_prefix: [:smolquery, :api])
  plug(:match)
  plug(Smolquery.Api.Auth)
  plug(:dispatch)

  @impl Plug
  def init(name), do: name

  @impl Plug
  def call(conn, name) do
    conn
    |> put_private(:smolquery_api, name)
    |> super([])
  end

  get "/healthz" do
    conn
    |> put_resp_content_type("application/json")
    |> send_resp(200, JSON.encode!(%{"status" => "ok"}))
  end

  match _ do
    Errors.send_error(conn, 404, "NOT_FOUND", "no such route")
  end

  @doc """
  The pid of a running instance's HTTP listener.

  Not needed to serve requests — only for discovering the port Bandit bound
  when `port` is `0`, which is how tests run instances side by side.
  """
  @spec listener(atom()) :: pid() | nil
  def listener(name) do
    case Process.whereis(Runtime.supervisor(name)) do
      nil -> nil
      supervisor -> find_listener(supervisor, Runtime.listener(name))
    end
  end

  @doc """
  The base URL a running instance is listening on.
  """
  @spec base_url(atom()) :: String.t()
  def base_url(name) do
    {:ok, {_address, port}} = name |> listener() |> ThousandIsland.listener_info()

    "http://127.0.0.1:#{port}"
  end

  defp find_listener(supervisor, id) do
    supervisor
    |> Supervisor.which_children()
    |> Enum.find_value(fn
      {^id, pid, _type, _modules} -> pid
      _child -> nil
    end)
  end
end
