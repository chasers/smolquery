defmodule SmolqueryWeb.TableLive.Show do
  @moduledoc """
  One table — its schema, its retention policy, and a peek at its rows.

  Retention is the catalog's one mutable table property today (PL-12 D5), so
  editing here means editing that. The row preview runs through
  `Smolquery.QueryService.Client` asynchronously; a node without the `:query`
  role renders everything else and says so.
  """

  use SmolqueryWeb, :live_view

  alias Smolquery.Catalog
  alias Smolquery.QueryService
  alias Smolquery.Schema
  alias SmolqueryWeb.DataTable
  alias SmolqueryWeb.Runtime

  @preview_rows 50

  @impl Phoenix.LiveView
  def mount(%{"dataset" => dataset, "table" => table}, _session, socket) do
    {:ok, runtime} = Runtime.fetch(SmolqueryWeb)

    case Catalog.table_schema(runtime.catalog, {dataset, table}) do
      {:ok, schema} ->
        socket =
          socket
          |> assign(:page_title, "#{dataset}.#{table}")
          |> assign(:runtime, runtime)
          |> assign(:dataset, dataset)
          |> assign(:table, table)
          |> assign(:schema, schema)
          |> assign(:preview, :loading)
          |> load_retention()
          |> start_preview()

        {:ok, socket}

      {:error, reason} ->
        {:ok,
         socket
         |> put_flash(:error, "Unknown table #{dataset}.#{table}: #{inspect(reason)}")
         |> push_navigate(to: ~p"/tables")}
    end
  end

  @impl Phoenix.LiveView
  def handle_event("save_retention", %{"retention" => params}, socket) do
    with {:ok, retention} <- parse_retention(params),
         :ok <- put_retention(socket, retention) do
      {:noreply,
       socket
       |> put_flash(:info, "Retention saved")
       |> load_retention()}
    else
      {:error, reason} ->
        {:noreply, put_flash(socket, :error, "Could not save retention: #{inspect(reason)}")}
    end
  end

  def handle_event("clear_retention", _params, socket) do
    case put_retention(socket, nil) do
      :ok ->
        {:noreply,
         socket
         |> put_flash(:info, "Retention cleared")
         |> load_retention()}

      {:error, reason} ->
        {:noreply, put_flash(socket, :error, "Could not clear retention: #{inspect(reason)}")}
    end
  end

  @impl Phoenix.LiveView
  def handle_async(:preview, {:ok, result}, socket) do
    {:noreply, assign(socket, :preview, result)}
  end

  def handle_async(:preview, {:exit, reason}, socket) do
    {:noreply, assign(socket, :preview, {:error, reason})}
  end

  defp start_preview(socket) do
    if connected?(socket) do
      runtime = socket.assigns.runtime
      sql = preview_sql(socket.assigns.dataset, socket.assigns.table)

      start_async(socket, :preview, fn -> fetch_preview(runtime, sql) end)
    else
      socket
    end
  end

  defp fetch_preview(runtime, sql) do
    case QueryService.Client.query(runtime.query_name, sql, timeout_ms: 15_000) do
      {:ok, %QueryService.Job{state: :done}, frame} ->
        {columns, rows} = DataTable.frame_page(frame, 0, @preview_rows)
        {:ok, columns, rows}

      {:ok, job, _frame} ->
        {:error, job.error || job.state}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp preview_sql(dataset, table) do
    ~s(SELECT * FROM #{quote_ident(dataset)}.#{quote_ident(table)} LIMIT #{@preview_rows})
  end

  defp quote_ident(name), do: ~s(") <> String.replace(name, ~s("), ~s("")) <> ~s(")

  defp load_retention(socket) do
    runtime = socket.assigns.runtime

    case Catalog.retention(runtime.catalog, {socket.assigns.dataset, socket.assigns.table}) do
      {:ok, retention} -> assign(socket, :retention, retention)
      {:error, _reason} -> assign(socket, :retention, nil)
    end
  end

  defp put_retention(socket, retention) do
    runtime = socket.assigns.runtime

    Catalog.put_retention(
      runtime.catalog,
      {socket.assigns.dataset, socket.assigns.table},
      retention
    )
  end

  defp parse_retention(params) do
    column = params |> Map.get("column", "") |> String.trim()
    ttl = params |> Map.get("ttl_ms", "") |> String.trim()

    with false <- column == "",
         {ttl_ms, ""} when ttl_ms > 0 <- Integer.parse(ttl) do
      {:ok, %{column: column, ttl_ms: ttl_ms}}
    else
      _invalid -> {:error, :invalid_retention}
    end
  end

  defp time_columns(schema) do
    for %Schema.Field{name: name, type: type} <- schema.fields,
        type in [:timestamp, :date],
        do: name
  end

  defp api_type!(type) do
    {:ok, name} = Schema.api_type(type)
    name
  end

  defp preview_error(:query_service_unavailable),
    do: "The query service is not running on this node — no preview."

  defp preview_error(reason), do: "Preview failed: #{inspect(reason)}"

  @impl Phoenix.LiveView
  def render(assigns) do
    ~H"""
    <Layouts.app flash={@flash}>
      <div class="flex items-center gap-2">
        <.link navigate={~p"/tables"} class="btn btn-ghost btn-sm">
          <.icon name="hero-arrow-left" class="size-4" />
        </.link>
        <h1 class="text-xl font-semibold font-mono">{@dataset}.{@table}</h1>
      </div>

      <div class="card bg-base-200 border border-base-300">
        <div class="card-body py-4">
          <h2 class="card-title text-base">Schema</h2>
          <table class="table table-sm">
            <thead>
              <tr>
                <th>field</th>
                <th>type</th>
                <th>nullable</th>
              </tr>
            </thead>
            <tbody>
              <tr :for={field <- @schema.fields}>
                <td class="font-mono">{field.name}</td>
                <td class="font-mono">{api_type!(field.type)}</td>
                <td class="font-mono">{field.nullable}</td>
              </tr>
            </tbody>
          </table>
        </div>
      </div>

      <div class="card bg-base-200 border border-base-300">
        <div class="card-body py-4">
          <h2 class="card-title text-base">Retention</h2>
          <div :if={@retention} class="text-sm font-mono">
            drop rows where <span class="font-semibold">{@retention.column}</span>
            is older than {@retention.ttl_ms} ms
          </div>
          <div :if={@retention == nil} class="text-sm opacity-70">No retention policy.</div>
          <form
            :if={time_columns(@schema) != []}
            id="retention"
            phx-submit="save_retention"
            class="flex gap-2 items-center"
          >
            <select name="retention[column]" class="select select-bordered select-sm font-mono">
              <option
                :for={column <- time_columns(@schema)}
                value={column}
                selected={@retention && @retention.column == column}
              >
                {column}
              </option>
            </select>
            <input
              type="text"
              name="retention[ttl_ms]"
              value={@retention && @retention.ttl_ms}
              placeholder="ttl ms"
              class="input input-bordered input-sm w-32 font-mono"
            />
            <button type="submit" class="btn btn-primary btn-sm">Save</button>
            <button :if={@retention} type="button" phx-click="clear_retention" class="btn btn-sm">
              Clear
            </button>
          </form>
          <div :if={time_columns(@schema) == []} class="text-sm opacity-70">
            Retention needs a TIMESTAMP or DATE column.
          </div>
        </div>
      </div>

      <div class="card bg-base-200 border border-base-300">
        <div class="card-body py-4">
          <h2 class="card-title text-base">Preview</h2>
          <%= case @preview do %>
            <% :loading -> %>
              <div class="text-sm opacity-70">Loading…</div>
            <% {:error, reason} -> %>
              <div class="text-sm opacity-70">{preview_error(reason)}</div>
            <% {:ok, columns, rows} -> %>
              <DataTable.data_table id="preview" columns={columns} rows={rows} />
          <% end %>
        </div>
      </div>
    </Layouts.app>
    """
  end
end
