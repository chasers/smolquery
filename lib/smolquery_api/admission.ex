defmodule SmolqueryApi.Admission do
  @moduledoc """
  Bounds the bytes of ingest bodies in flight, before any body is read (T-245).

  The write path's 429s were correct and too late: `buffer_full` and
  `{:overloaded, _}` fire after the request body is in RAM, so nothing bounded
  how many bodies were resident at once, and memory grew with client
  concurrency until the kernel OOMKilled the pod — measured twice on the
  sandbox, at 128 VUs x 6.87 MiB from an in-region load generator. The right
  refusal costs a header read, not a body.

  `SmolqueryApi.Router` calls `admit_conn/1` between auth and parsing, so an
  ingest body is counted before the first byte is read and an unauthenticated
  request never reaches the counter. Only `POST .../insert` is counted —
  every other route carries small bodies the parsers already bound. The
  reservation is the request's `content-length`, capped at the route's own
  body limit; a request that does not declare a length reserves that limit
  outright, so a chunked body cannot slip under the counter.

  Admission over the limit answers 429 with `retry-after`, the same contract
  the buffer's own refusals speak. An idle counter always admits one request,
  whatever its size: the route's body cap decides what is too large (413),
  and a limit misconfigured below one body must not brick ingest.

  The counter releases when the response is sent, and a monitor on the
  request process releases on crash, so an abandoned request cannot leak its
  reservation. The server is one process per API instance; two calls per
  request is noise next to a multi-megabyte body.

  A request dispatched for an instance with no admission server passes
  uncounted. In production the server starts under `SmolqueryApi.Supervisor`
  ahead of the endpoint, so that window is a supervisor restart; in tests it
  is the default, and only admission's own tests start the server. The same
  contract covers a server that dies between the lookup and the call: the
  request passes uncounted rather than crash.
  """

  use GenServer

  require Logger

  alias SmolqueryApi.Errors
  alias SmolqueryApi.Runtime

  @doc """
  Starts the admission server for an API runtime.
  """
  @spec start_link(Runtime.t()) :: GenServer.on_start()
  def start_link(%Runtime{} = runtime) do
    GenServer.start_link(__MODULE__, runtime, name: server(runtime.name))
  end

  @doc """
  The registered name of an instance's admission server.
  """
  @spec server(atom()) :: atom()
  def server(name), do: Module.concat(name, "Admission")

  @doc """
  Admits or refuses `conn` — the plug seam `SmolqueryApi.Router` dispatches
  through.

  A refused conn is halted with a 429 and `retry-after`. An admitted ingest
  conn releases its reservation when the response is sent.
  """
  @spec admit_conn(Plug.Conn.t()) :: Plug.Conn.t()
  def admit_conn(
        %Plug.Conn{method: "POST", path_info: ["v1", "datasets", _, "tables", _, "insert"]} = conn
      ) do
    instance = conn.private.smolquery_api

    case Process.whereis(server(instance)) do
      nil -> conn
      server -> admit_conn(conn, server, reservation(conn, instance))
    end
  end

  def admit_conn(%Plug.Conn{} = conn), do: conn

  defp admit_conn(conn, server, bytes) do
    case admit(server, bytes) do
      :no_server ->
        conn

      {:ok, reservation} ->
        Plug.Conn.register_before_send(conn, fn conn ->
          release(server, reservation)
          conn
        end)

      {:error, :admission_full} ->
        conn
        |> Errors.send_resource_exhausted(1, "too many ingest bytes in flight, retry later")
        |> Plug.Conn.halt()
    end
  end

  defp admit(server, bytes) do
    GenServer.call(server, {:admit, bytes, self()})
  catch
    :exit, _reason -> :no_server
  end

  @doc """
  Releases an admitted reservation.
  """
  @spec release(GenServer.server(), reference()) :: :ok
  def release(server, reservation), do: GenServer.cast(server, {:release, reservation})

  @doc """
  The bytes currently admitted on an instance — what tests assert on.
  """
  @spec in_flight(atom()) :: non_neg_integer()
  def in_flight(instance), do: GenServer.call(server(instance), :in_flight)

  defp reservation(conn, instance) do
    ceiling = ceiling(instance)

    case Plug.Conn.get_req_header(conn, "content-length") do
      [length | _] ->
        case Integer.parse(length) do
          {bytes, ""} when bytes >= 0 -> min(bytes, ceiling)
          _not_a_length -> ceiling
        end

      [] ->
        ceiling
    end
  end

  defp ceiling(instance) do
    {:ok, runtime} = Runtime.fetch(instance)
    runtime.max_ndjson_bytes
  end

  @impl GenServer
  def init(%Runtime{} = runtime) do
    limit = Runtime.insert_max_in_flight_bytes(runtime)
    Logger.info("ingest admission limit=#{limit} bytes in flight")

    {:ok, %{limit: limit, in_flight: 0, reservations: %{}}}
  end

  @impl GenServer
  def handle_call({:admit, bytes, pid}, _from, state) do
    if state.in_flight > 0 and state.in_flight + bytes > state.limit do
      {:reply, {:error, :admission_full}, state}
    else
      reservation = Process.monitor(pid)

      {:reply, {:ok, reservation},
       %{
         state
         | in_flight: state.in_flight + bytes,
           reservations: Map.put(state.reservations, reservation, bytes)
       }}
    end
  end

  def handle_call(:in_flight, _from, state), do: {:reply, state.in_flight, state}

  @impl GenServer
  def handle_cast({:release, reservation}, state) do
    Process.demonitor(reservation, [:flush])
    {:noreply, drop(state, reservation)}
  end

  @impl GenServer
  def handle_info({:DOWN, reservation, :process, _pid, _reason}, state) do
    {:noreply, drop(state, reservation)}
  end

  def handle_info(_message, state), do: {:noreply, state}

  defp drop(state, reservation) do
    case Map.pop(state.reservations, reservation) do
      {nil, _reservations} ->
        state

      {bytes, reservations} ->
        %{state | in_flight: state.in_flight - bytes, reservations: reservations}
    end
  end
end
