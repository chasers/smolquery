defmodule SmolqueryWeb.QueryLive.Index do
  @moduledoc """
  The SQL editor — a query's whole job lifecycle, live.

  Run submits through `Smolquery.QueryService.Client` and polls the job until
  it lands somewhere terminal, so state, duration, and errors stream into the
  page as they happen. Results are one `Explorer.DataFrame` paged client-side;
  cancel really cancels the job, not just the page.
  """

  use SmolqueryWeb, :live_view

  alias Smolquery.QueryService
  alias SmolqueryWeb.{Authorization, DataTable, Runtime}

  @page_size 100
  @poll_ms 200

  @impl Phoenix.LiveView
  def mount(_params, _session, socket) do
    {:ok, runtime} = Runtime.fetch(SmolqueryWeb)

    socket =
      socket
      |> assign(:page_title, "Query")
      |> assign(:runtime, runtime)
      |> assign(:sql, "")
      |> assign(:job, nil)
      |> assign(:frame, nil)
      |> assign(:page, 0)
      |> assign(:columns, [])
      |> assign(:rows, [])
      |> assign(:run_error, nil)

    {:ok, socket}
  end

  @impl Phoenix.LiveView
  def handle_event("sql_changed", params, socket) do
    with :ok <- Authorization.event(socket, :query),
         %{"query" => %{"sql" => sql}} when is_binary(sql) <- params do
      {:noreply, assign(socket, :sql, sql)}
    else
      {:error, _reason, denied} -> {:noreply, denied}
      _ -> {:noreply, socket}
    end
  end

  def handle_event("run", params, socket) do
    with :ok <- Authorization.event(socket, :query),
         %{"query" => %{"sql" => sql}} when is_binary(sql) <- params do
      case String.trim(sql) do
        "" ->
          {:noreply, assign(socket, :run_error, "Write some SQL first")}

        _sql ->
          submit(assign(socket, :sql, sql))
      end
    else
      {:error, _reason, denied} -> {:noreply, denied}
      _ -> {:noreply, assign(socket, :run_error, "Write some SQL first")}
    end
  end

  def handle_event("cancel", _params, socket) do
    case Authorization.event(socket, :query) do
      {:error, _reason, denied} -> {:noreply, denied}
      :ok -> cancel(socket)
    end
  end

  def handle_event("page", params, socket) do
    with :ok <- Authorization.event(socket, :query),
         dir when dir in ["next", "prev"] <- Map.get(params, "dir") do
      page =
        case dir do
          "next" -> socket.assigns.page + 1
          "prev" -> max(socket.assigns.page - 1, 0)
        end

      {:noreply,
       socket |> assign(:page, min(page, last_page(socket.assigns.frame))) |> page_rows()}
    else
      {:error, _reason, denied} -> {:noreply, denied}
      _ -> {:noreply, socket}
    end
  end

  defp cancel(socket) do
    if socket.assigns.job do
      QueryService.Client.cancel(socket.assigns.runtime.query_name, socket.assigns.job.id)
    end

    {:noreply, socket}
  end

  @impl Phoenix.LiveView
  def handle_info({:poll, job_id}, socket) do
    with :ok <- Authorization.event(socket, :query),
         current when not is_nil(current) <- socket.assigns.job,
         true <- current.id == job_id do
      poll(socket, job_id)
    else
      {:error, _reason, denied} -> {:noreply, denied}
      _ -> {:noreply, socket}
    end
  end

  defp submit(socket) do
    case QueryService.Client.submit(socket.assigns.runtime.query_name, socket.assigns.sql) do
      {:ok, job} ->
        Process.send_after(self(), {:poll, job.id}, @poll_ms)

        {:noreply,
         socket
         |> assign(:job, job)
         |> assign(:frame, nil)
         |> assign(:page, 0)
         |> assign(:columns, [])
         |> assign(:rows, [])
         |> assign(:run_error, nil)}

      {:error, reason} ->
        {:noreply, assign(socket, :run_error, submit_error(reason))}
    end
  end

  defp poll(socket, job_id) do
    case QueryService.Client.fetch(socket.assigns.runtime.query_name, job_id) do
      {:ok, job, frame} ->
        if QueryService.Job.terminal?(job) do
          {:noreply,
           socket
           |> assign(:job, job)
           |> assign(:frame, frame)
           |> assign(:page, 0)
           |> page_rows()}
        else
          Process.send_after(self(), {:poll, job_id}, @poll_ms)
          {:noreply, assign(socket, :job, job)}
        end

      {:error, reason} ->
        {:noreply,
         socket
         |> assign(:job, nil)
         |> assign(:run_error, "Lost the job: #{inspect(reason)}")}
    end
  end

  defp page_rows(socket) do
    case socket.assigns.frame do
      nil ->
        socket |> assign(:columns, []) |> assign(:rows, [])

      frame ->
        {columns, rows} =
          DataTable.frame_page(frame, socket.assigns.page * @page_size, @page_size)

        socket |> assign(:columns, columns) |> assign(:rows, rows)
    end
  end

  defp last_page(nil), do: 0
  defp last_page(frame), do: div(max(Explorer.DataFrame.n_rows(frame) - 1, 0), @page_size)

  defp submit_error(:query_service_unavailable),
    do: "The query service is not running on this node."

  defp submit_error(:too_many_jobs),
    do: "The query service is at its concurrent job limit — try again in a moment."

  defp submit_error(reason), do: "Could not submit: #{inspect(reason)}"

  defp running?(nil), do: false
  defp running?(job), do: not QueryService.Job.terminal?(job)

  defp state_badge(:done), do: "badge-success"
  defp state_badge(:error), do: "badge-error"
  defp state_badge(:cancelled), do: "badge-warning"
  defp state_badge(_state), do: "badge-info"

  @impl Phoenix.LiveView
  def render(assigns) do
    ~H"""
    <Layouts.app flash={@flash}>
      <h1 class="text-xl font-semibold">Query</h1>

      <form id="editor" phx-change="sql_changed" phx-submit="run" class="space-y-2">
        <textarea
          name="query[sql]"
          rows="6"
          placeholder="SELECT ..."
          class="textarea textarea-bordered w-full font-mono"
        >{@sql}</textarea>
        <div class="flex gap-2 items-center">
          <button type="submit" class="btn btn-primary btn-sm" disabled={running?(@job)}>
            Run
          </button>
          <button
            :if={running?(@job)}
            type="button"
            phx-click="cancel"
            class="btn btn-warning btn-sm"
          >
            Cancel
          </button>
          <span :if={@job} class={["badge", state_badge(@job.state)]}>{@job.state}</span>
          <span :if={@job && @job.duration_ms} class="text-sm opacity-70">
            {@job.duration_ms} ms
          </span>
          <span :if={@job && @job.row_count} class="text-sm opacity-70">
            {@job.row_count} rows
          </span>
        </div>
      </form>

      <div :if={@run_error} class="alert alert-error text-sm">{@run_error}</div>

      <div :if={@job && @job.state == :error} class="alert alert-error text-sm font-mono">
        {inspect(@job.error)}
      </div>

      <div :if={@frame}>
        <DataTable.data_table id="results" columns={@columns} rows={@rows} />
        <div :if={last_page(@frame) > 0} class="flex gap-2 items-center mt-2">
          <button type="button" phx-click="page" phx-value-dir="prev" class="btn btn-sm">
            Prev
          </button>
          <span class="text-sm opacity-70">
            page {@page + 1} / {last_page(@frame) + 1}
          </span>
          <button type="button" phx-click="page" phx-value-dir="next" class="btn btn-sm">
            Next
          </button>
        </div>
      </div>
    </Layouts.app>
    """
  end
end
