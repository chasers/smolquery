defmodule Smolquery.Test.ManifestServer do
  @moduledoc """
  Serves a canned hot manifest over HTTP.

  Stands in for `BufferService.HotServer`'s manifest route in planner tests:
  claim shapes the buffer only produces mid-seal — a claim whose keys are
  already committed, one whose keys never will be — can be staged directly as
  entry maps instead of driving a real seal to the right instant.
  """

  @behaviour Plug

  import Plug.Conn

  @impl Plug
  def init(agent), do: agent

  @impl Plug
  def call(conn, agent) do
    conn
    |> put_resp_content_type("application/json")
    |> send_resp(200, agent |> Agent.get(& &1) |> JSON.encode!())
  end

  @doc """
  A Bandit child spec serving `agent`'s entries on an OS-assigned port.
  """
  @spec bandit_spec(pid()) :: {module(), keyword()}
  def bandit_spec(agent) do
    {Bandit, plug: {__MODULE__, agent}, port: 0, startup_log: false}
  end

  @doc """
  The base URL a running server is listening on.
  """
  @spec base_url(pid()) :: String.t()
  def base_url(server) do
    {:ok, {_address, port}} = ThousandIsland.listener_info(server)

    "http://127.0.0.1:#{port}"
  end
end
