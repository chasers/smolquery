defmodule SmolqueryWeb.QueryLive.Index do
  @moduledoc """
  The SQL editor — a query's whole job lifecycle, live.

  Run submits through `Smolquery.QueryService.Client` and polls the job until
  it lands somewhere terminal, so state, duration, and errors stream into the
  page as they happen. Results are one `Explorer.DataFrame` paged client-side;
  cancel really cancels the job, not just the page.

  A finished job's scan statistics render under the editor. Explain and
  Analyze submit the same SQL with the `explain` option and render the
  engine's plan text instead of rows. The Trace toggle submits with
  `trace: true` and renders the job's phase spans as a
  `SmolqueryWeb.Waterfall`; it starts enabled — the waterfall is what the
  page is for — while the API keeps tracing opt-in.
  """

  use SmolqueryWeb, :live_view

  alias Smolquery.QueryService
  alias Smolquery.QueryService.Statistics
  alias SmolqueryWeb.DataTable
  alias SmolqueryWeb.Runtime
  alias SmolqueryWeb.Waterfall

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
      |> assign(:trace, true)
      |> assign(:job, nil)
      |> assign(:frame, nil)
      |> assign(:page, 0)
      |> assign(:columns, [])
      |> assign(:rows, [])
      |> assign(:run_error, nil)

    {:ok, socket}
  end

  @impl Phoenix.LiveView
  def handle_event("sql_changed", %{"query" => query}, socket) do
    {:noreply, editor(socket, query)}
  end

  def handle_event("run", %{"query" => query}, socket) do
    socket |> editor(query) |> submit_or_warn([])
  end

  def handle_event("explain", %{"mode" => mode}, socket) do
    submit_or_warn(socket, explain: explain_mode(mode))
  end

  def handle_event("cancel", _params, socket) do
    if socket.assigns.job do
      QueryService.Client.cancel(socket.assigns.runtime.query_name, socket.assigns.job.id)
    end

    {:noreply, socket}
  end

  def handle_event("page", %{"dir" => dir}, socket) do
    page =
      case dir do
        "next" -> socket.assigns.page + 1
        "prev" -> max(socket.assigns.page - 1, 0)
      end

    {:noreply, socket |> assign(:page, min(page, last_page(socket.assigns.frame))) |> page_rows()}
  end

  @impl Phoenix.LiveView
  def handle_info({:poll, job_id}, socket) do
    current = socket.assigns.job

    if current && current.id == job_id do
      poll(socket, job_id)
    else
      {:noreply, socket}
    end
  end

  defp editor(socket, query) do
    socket
    |> assign(:sql, Map.get(query, "sql", socket.assigns.sql))
    |> assign(:trace, Map.get(query, "trace") == "true")
  end

  defp explain_mode("analyze"), do: :analyze
  defp explain_mode(_mode), do: :plan

  defp submit_or_warn(socket, opts) do
    cond do
      running?(socket.assigns.job) ->
        {:noreply, socket}

      String.trim(socket.assigns.sql) == "" ->
        {:noreply, assign(socket, :run_error, "Write some SQL first")}

      true ->
        submit(socket, opts)
    end
  end

  defp submit(socket, opts) do
    opts = if socket.assigns.trace, do: [{:trace, true} | opts], else: opts

    case QueryService.Client.submit(socket.assigns.runtime.query_name, socket.assigns.sql, opts) do
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

  defp statistics_line(statistics) do
    "scanned #{Statistics.mib_scanned(statistics)} MiB · " <>
      "#{Statistics.rows_scanned(statistics)} rows · " <>
      "#{Statistics.files_scanned(statistics)}/#{Statistics.files_total(statistics)} files " <>
      "(hot #{statistics.hot.files_scanned}/#{statistics.hot.files_total}, " <>
      "sealed #{statistics.sealed.files_scanned})"
  end

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
        <script :type={Phoenix.LiveView.ColocatedHook} name=".SubmitOnMetaEnter">
          export default {
            mounted() {
              this.el.addEventListener("keydown", (event) => {
                if ((event.metaKey || event.ctrlKey) && event.key === "Enter") {
                  event.preventDefault()
                  this.el.form.requestSubmit()
                }
              })
            }
          }
        </script>
        <textarea
          id="sql-editor"
          name="query[sql]"
          rows="6"
          placeholder="SELECT ..."
          phx-hook=".SubmitOnMetaEnter"
          class="textarea textarea-bordered w-full font-mono"
        >{@sql}</textarea>
        <div class="flex gap-2 items-center">
          <button type="submit" class="btn btn-primary btn-sm" disabled={running?(@job)}>
            Run ⌘⏎
          </button>
          <button
            :for={{label, mode} <- [{"Explain", "plan"}, {"Analyze", "analyze"}]}
            type="button"
            phx-click="explain"
            phx-value-mode={mode}
            class="btn btn-sm"
            disabled={running?(@job)}
          >
            {label}
          </button>
          <label class="label cursor-pointer gap-1 text-sm">
            <input
              type="checkbox"
              name="query[trace]"
              value="true"
              checked={@trace}
              class="checkbox checkbox-sm"
            /> Trace
          </label>
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

      <div :if={@job && @job.statistics} class="text-sm opacity-70">
        {statistics_line(@job.statistics)}
      </div>

      <pre
        :if={@job && @job.explain}
        class="bg-base-200 rounded p-3 text-xs font-mono overflow-x-auto"
      >{@job.explain}</pre>

      <div :if={@job && @job.trace} class="space-y-1">
        <h2 class="text-sm font-semibold">Trace</h2>
        <Waterfall.waterfall id="trace" spans={@job.trace} />
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
