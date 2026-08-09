defmodule Smolquery.Test.KindInsertServer do
  @moduledoc false

  @behaviour Plug

  import Plug.Conn

  @spec init(pid()) :: pid()
  def init(agent), do: agent

  @spec call(Plug.Conn.t(), pid()) :: Plug.Conn.t()
  def call(conn, agent) do
    conn = fetch_query_params(conn)
    {:ok, body, conn} = read_body(conn)

    request = %{
      body: body,
      content_type: get_req_header(conn, "content-type"),
      query_string: conn.query_string,
      query_params: conn.query_params
    }

    status =
      Agent.get_and_update(agent, fn
        %{statuses: [status | statuses], requests: requests} ->
          {status, %{statuses: statuses, requests: [request | requests]}}
      end)

    response =
      JSON.encode!(%{
        "insertedRows" => body |> String.split("\n", trim: true) |> length(),
        "insertErrors" => []
      })

    conn
    |> put_resp_content_type("application/json")
    |> send_resp(status, response)
  end

  @spec bandit_spec(pid()) :: {module(), keyword()}
  def bandit_spec(agent) do
    {Bandit, plug: {__MODULE__, agent}, port: 0, startup_log: false}
  end

  @spec base_url(pid()) :: String.t()
  def base_url(server) do
    {:ok, {_address, port}} = ThousandIsland.listener_info(server)
    "http://127.0.0.1:#{port}"
  end
end
