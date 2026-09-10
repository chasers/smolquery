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

  alias Smolquery.BufferService.HotClient

  @impl Plug
  def init(agent), do: agent

  @impl Plug
  def call(conn, agent) do
    conn = fetch_query_params(conn)
    params = conn.query_params
    {entries, next} = page(Agent.get(agent, & &1), params["newest"], params["before"])

    entries =
      if params["stats"] == "false",
        do: Enum.map(entries, &Map.delete(&1, "stats")),
        else: entries

    conn
    |> continue(next)
    |> put_resp_content_type("application/json")
    |> send_resp(200, JSON.encode!(entries))
  end

  # `?newest=N&before=ID` answers as `HotServer` does: the N newest entries
  # older than the cursor, newest first, naming the continuation of a full
  # page in `HotClient.next_header/0` (T-449).
  defp page(entries, nil, _before), do: {entries, nil}

  defp page(entries, newest, before) do
    limit = String.to_integer(newest)

    page =
      entries
      |> Enum.filter(&(is_nil(before) or &1["id"] < before))
      |> Enum.sort_by(& &1["id"], :desc)
      |> Enum.take(limit)

    {page, if(length(page) == limit, do: List.last(page)["id"])}
  end

  defp continue(conn, nil), do: conn

  defp continue(conn, next),
    do: put_resp_header(conn, HotClient.next_header(), next)

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
