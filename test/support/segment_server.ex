defmodule Smolquery.Test.SegmentServer do
  @moduledoc """
  Serves Parquet files from a directory over HTTP.

  Stands in for `BufferService.HotServer` in tests that need DuckDB to read hot
  micro-segments through `httpfs`, so the read path can be exercised before the
  buffer service exists.
  """

  @behaviour Plug

  import Plug.Conn

  @impl Plug
  def init(dir), do: dir

  @impl Plug
  def call(conn, dir) do
    path = Path.join(dir, Path.basename(conn.request_path))

    if File.exists?(path) do
      conn
      |> put_resp_content_type("application/octet-stream")
      |> send_file(200, path)
    else
      send_resp(conn, 404, "not found")
    end
  end

  @doc """
  A Bandit child spec serving `dir` on an OS-assigned port.
  """
  @spec bandit_spec(String.t()) :: Supervisor.child_spec() | {module(), keyword()}
  def bandit_spec(dir) do
    {Bandit, plug: {__MODULE__, dir}, port: 0, startup_log: false}
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
