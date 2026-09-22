defmodule SmolqueryVictoriaMetrics.Status do
  @moduledoc """
  The fixed answers a single-node VictoriaMetrics v1.152.0 gives where it
  has nothing of its own to say, so Grafana's datasource checks pass
  (PL-70, T-566). Each body is VictoriaMetrics' own, byte for byte
  (`app/vmselect/main.go`, `app/vmselect/prometheus/metadata_response.qtpl`):

  | path | body |
  |---|---|
  | `/api/v1/status/buildinfo` | `{"status":"success","data":{"version":"2.24.0"}}` |
  | `/api/v1/metadata` | `{"status":"success","data":{}}` |
  | `/api/v1/rules` | `{"status":"success","data":{"groups":[]}}` |
  | `/api/v1/alerts` | `{"status":"success","data":{"alerts":[]}}` |
  | `/api/v1/notifiers` | `{"status":"success","data":{"notifiers":[]}}` |
  | `/api/v1/query_exemplars` | `{"status":"success","data":[]}` |

  `buildinfo`'s `version` is a Prometheus version, not VictoriaMetrics':
  Grafana reads it to decide which APIs it uses to fetch label values
  (VictoriaMetrics issue 5370), so it is VictoriaMetrics' `2.24.0` here
  too. `metadata` is empty because the edge drops the metadata remote write
  carries (PL-70 D7). `rules`,
  `alerts` and `notifiers` are empty because there is no vmalert behind
  the edge, and `query_exemplars` because exemplars are dropped too.

  `/api/v1/status/tsdb` is not answered: VictoriaMetrics' answer counts the
  series of a day by metric name, label and label pair, which here is a
  scan of a day of samples, not a stub. It is a 404 until a series index
  can answer it cheaply (PL-70, T-569).
  """

  import Plug.Conn

  alias SmolqueryVictoriaMetrics.Response

  @typedoc "The routes answered here."
  @type route :: :buildinfo | :metadata | :rules | :alerts | :notifiers | :query_exemplars

  @routes %{
    ["status", "buildinfo"] => :buildinfo,
    ["metadata"] => :metadata,
    ["rules"] => :rules,
    ["alerts"] => :alerts,
    ["notifiers"] => :notifiers,
    ["query_exemplars"] => :query_exemplars
  }

  @doc """
  The route for a path under `/api/v1`, or `nil`.

      iex> SmolqueryVictoriaMetrics.Status.route(["status", "buildinfo"])
      :buildinfo
      iex> SmolqueryVictoriaMetrics.Status.route(["status", "tsdb"])
      nil
  """
  @spec route([String.t()]) :: route() | nil
  def route(path), do: Map.get(@routes, path)

  @doc """
  The body of `route`'s answer.

      iex> SmolqueryVictoriaMetrics.Status.body(:rules) |> IO.iodata_to_binary()
      ~s({"status":"success","data":{"groups":[]}})
  """
  @spec body(route()) :: iodata()
  def body(:buildinfo), do: Response.data(~s({"version":"2.24.0"}))
  def body(:metadata), do: Response.data("{}")
  def body(:rules), do: Response.data(~s({"groups":[]}))
  def body(:alerts), do: Response.data(~s({"alerts":[]}))
  def body(:notifiers), do: Response.data(~s({"notifiers":[]}))
  def body(:query_exemplars), do: Response.data("[]")

  @doc """
  Answers `route`.
  """
  @spec call(Plug.Conn.t(), route()) :: Plug.Conn.t()
  def call(conn, route) do
    conn
    |> put_resp_content_type("application/json")
    |> send_resp(200, body(route))
  end
end
