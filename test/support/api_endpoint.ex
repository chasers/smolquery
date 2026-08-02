defmodule Smolquery.Test.ApiEndpoint do
  @moduledoc """
  The one `SmolqueryApi.Endpoint` every in-process API test dispatches
  through.

  A Phoenix endpoint is a singleton, but the per-request instance seam is
  not: `SmolqueryApi.Router` fills `conn.private.smolquery_api` only when a
  caller has not already. So `test/test_helper.exs` starts this endpoint once
  (`server: false` — no listener), and each test publishes a uniquely-named
  `SmolqueryApi.Runtime`, injects that name with `request/2`, and keeps
  running `async: true` — the same isolation the Plug.Router's
  `plug: {Router, name}` seam gave.

  The wire tests (`SmolqueryApi.SupervisorTest`, the CRUD integration suite)
  are the exception: they boot the real `SmolqueryApi.Supervisor`, whose
  endpoint child collides with this shared one. They run `async: false`,
  `stop_shared!/0` first, and `start_shared!/0` back in `on_exit`.
  """

  import Plug.Conn, only: [put_private: 3]

  @supervisor __MODULE__.Supervisor

  @doc """
  Starts the shared endpoint under a named supervisor, idempotently.
  """
  @spec start_shared!() :: :ok
  def start_shared! do
    case Supervisor.start_link([SmolqueryApi.Endpoint],
           strategy: :one_for_one,
           name: @supervisor
         ) do
      {:ok, pid} ->
        Process.unlink(pid)

        :ok

      {:error, {:already_started, _pid}} ->
        :ok
    end
  end

  @doc """
  Stops the shared endpoint so a wire test can start the real supervisor.
  """
  @spec stop_shared!() :: :ok
  def stop_shared! do
    case Process.whereis(@supervisor) do
      nil -> :ok
      pid -> Supervisor.stop(pid)
    end
  end

  @doc """
  Dispatches `conn` through the endpoint as instance `name`.
  """
  @spec request(atom(), Plug.Conn.t()) :: Plug.Conn.t()
  def request(name, conn) do
    conn
    |> put_private(:smolquery_api, name)
    |> SmolqueryApi.Endpoint.call([])
  end
end
