defmodule SmolqueryWeb.TableLive.Show do
  @moduledoc """
  One table — its schema, its retention policy, its lifecycle, and a peek at
  its rows.

  Retention is the catalog's one mutable table property today (PL-12 D5), so
  editing here means editing that. The row preview runs through
  `Smolquery.QueryService.Client` asynchronously; a node without the `:query`
  role renders everything else and says so.

  The lifecycle card (T-295) shows the table's seal-and-compaction pipeline:
  per write partition, the hot tier's pending → claimed → sealed stages; the
  sealed tier's segments as a size strip with compaction candidates
  highlighted; and a live feed of commit, seal, and compaction events. It
  subscribes to `Smolquery.Lifecycle`, so a seal on a storage node or a
  commit on a buffer owner pushes to this page the moment it lands — an
  event updates the feed instantly and schedules one debounced refetch of
  the tier state, so an event burst costs one round of reads. The debounce
  guard holds until the fetch *completes*, not until its timer fires: the
  hot-manifest read blocks on the routed call's timeout when a buffer node
  is unreachable, and clearing the guard early let a steady commit stream
  stack unbounded concurrent blocked fetches against an already-degraded
  cluster. The hot tier
  reads through `Smolquery.BufferService.Client`, which routes to each
  partition's ring owner; a node the route cannot reach renders as
  unavailable rather than failing the page. Seal-failure streaks are
  derived from the event stream (a `:seal` error extends a partition's
  streak, `:ok` clears it), so the page needs nothing from the sealer's
  process state. The latest seal and compaction stay pinned above the feed:
  commits arrive every flush interval and would push the rare events out of
  a bounded feed within seconds — observed on the first live run of this
  page — while a seal or a compaction is exactly what a reader came to see.
  """

  use SmolqueryWeb, :live_view

  alias Smolquery.BufferService
  alias Smolquery.Catalog
  alias Smolquery.Lifecycle
  alias Smolquery.Partitions
  alias Smolquery.QueryService
  alias Smolquery.Schema
  alias SmolqueryWeb.DataTable
  alias SmolqueryWeb.Runtime
  alias SmolqueryWeb.Waterfall

  @preview_rows 50
  @lifecycle_debounce_ms 300
  @event_feed_limit 8

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
          |> assign(:lifecycle, :loading)
          |> assign(:events, [])
          |> assign(:last_by_kind, %{})
          |> assign(:seal_streaks, %{})
          |> assign(:lifecycle_refresh_queued, false)
          |> load_retention()
          |> start_preview()
          |> start_lifecycle()

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

  def handle_async(:lifecycle, {:ok, lifecycle}, socket) do
    {:noreply,
     socket
     |> assign(:lifecycle, lifecycle)
     |> assign(:lifecycle_refresh_queued, false)}
  end

  def handle_async(:lifecycle, {:exit, reason}, socket) do
    {:noreply,
     socket
     |> assign(:lifecycle, {:error, reason})
     |> assign(:lifecycle_refresh_queued, false)}
  end

  @impl Phoenix.LiveView
  def handle_info({:lifecycle, event}, socket) do
    {:noreply,
     socket
     |> update(:events, &Enum.take([event | &1], @event_feed_limit))
     |> update(:last_by_kind, &Map.put(&1, event.kind, event))
     |> track_streak(event)
     |> queue_lifecycle_refresh()}
  end

  def handle_info(:refresh_lifecycle, socket) do
    {:noreply, fetch_lifecycle(socket)}
  end

  defp start_lifecycle(socket) do
    if connected?(socket) do
      :ok = Lifecycle.subscribe({socket.assigns.dataset, socket.assigns.table})

      fetch_lifecycle(socket)
    else
      socket
    end
  end

  defp queue_lifecycle_refresh(%{assigns: %{lifecycle_refresh_queued: true}} = socket),
    do: socket

  defp queue_lifecycle_refresh(socket) do
    Process.send_after(self(), :refresh_lifecycle, @lifecycle_debounce_ms)

    assign(socket, :lifecycle_refresh_queued, true)
  end

  defp track_streak(socket, %{kind: :seal, table_ref: ref, result: result}) do
    streaks = socket.assigns.seal_streaks

    streaks =
      case result do
        :ok -> Map.delete(streaks, ref)
        _failure -> Map.update(streaks, ref, 1, &(&1 + 1))
      end

    assign(socket, :seal_streaks, streaks)
  end

  defp track_streak(socket, _event), do: socket

  defp fetch_lifecycle(socket) do
    runtime = socket.assigns.runtime
    table_ref = {socket.assigns.dataset, socket.assigns.table}

    start_async(socket, :lifecycle, fn -> load_lifecycle(runtime, table_ref) end)
  end

  defp load_lifecycle(runtime, table_ref) do
    %{
      hot: hot_partitions(runtime, table_ref),
      sealed: sealed_tier(runtime, table_ref),
      compact_below: compact_below_bytes()
    }
  end

  defp hot_partitions(runtime, table_ref) do
    for {_dataset, partition} = ref <- Partitions.refs(table_ref, write_partitions(runtime)) do
      %{ref: ref, label: partition, stages: hot_stages(runtime.buffer_name, ref)}
    end
  end

  defp hot_stages(buffer_name, ref) do
    case BufferService.Client.hot_manifest(buffer_name, ref) do
      {:ok, entries} ->
        {retired, unsealed} = Enum.split_with(entries, & &1.sealed_at)
        {claimed, pending} = Enum.split_with(unsealed, &(&1.claim_keys != []))

        %{pending: rollup(pending), claimed: rollup(claimed), retired: rollup(retired)}

      {:error, _reason} ->
        :unavailable
    end
  end

  defp rollup(entries) do
    %{
      count: length(entries),
      rows: Enum.sum_by(entries, & &1.row_count),
      bytes: Enum.sum_by(entries, & &1.byte_size)
    }
  end

  defp sealed_tier(runtime, table_ref) do
    with {:ok, snapshot} <- Catalog.current_snapshot(runtime.catalog),
         {:ok, stats} <- Catalog.segment_stats(runtime.catalog, table_ref, snapshot),
         {:ok, files} <- Catalog.segment_files(runtime.catalog, table_ref, snapshot) do
      %{stats: stats, files: files}
    else
      {:error, _reason} -> :unavailable
    end
  end

  defp write_partitions(runtime) do
    case QueryService.Runtime.fetch(runtime.query_name) do
      {:ok, query_runtime} ->
        query_runtime.write_partitions

      :error ->
        :smolquery
        |> Application.get_env(Smolquery.QueryService, [])
        |> Keyword.get(:write_partitions, 1)
    end
  end

  defp compact_below_bytes do
    :smolquery
    |> Application.get_env(Smolquery.StorageService, [])
    |> Keyword.get(:compact_below_bytes, 33_554_432)
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

  defp hot_total(stages),
    do: stages.pending.bytes + stages.claimed.bytes + stages.retired.bytes

  defp stage_width(stages, stage) do
    total = hot_total(stages)
    slice = Map.fetch!(stages, stage)

    "width: #{percent(slice.bytes, total)}%"
  end

  defp file_width(file, total_bytes), do: "width: #{percent(file.bytes, total_bytes)}%"

  defp percent(_part, 0), do: 0

  defp percent(part, total), do: Float.round(part / total * 100, 2)

  defp compactable(lifecycle),
    do: Enum.count(lifecycle.sealed.files, &(&1.bytes < lifecycle.compact_below))

  defp event_time(%{at: at}) do
    at
    |> DateTime.from_unix!(:millisecond)
    |> Calendar.strftime("%H:%M:%S")
  end

  defp event_line(%{kind: :commit} = event) do
    "commit #{event.result} · #{event.measurements.rows} rows · " <>
      "#{format_bytes(event.measurements.bytes)} (#{partition_of(event)})"
  end

  defp event_line(%{kind: :seal} = event) do
    "seal #{event.result} · #{event.measurements.segments} segments · " <>
      "#{Waterfall.duration_label(event.measurements.duration_us)} (#{partition_of(event)})"
  end

  defp event_line(%{kind: :compaction} = event) do
    "compaction #{event.result} · #{event.measurements.replaced} replaced · " <>
      "#{Waterfall.duration_label(event.measurements.duration_us)} (#{partition_of(event)})"
  end

  defp partition_of(%{table_ref: {_dataset, partition}}), do: partition

  defp format_bytes(bytes) when bytes >= 1_073_741_824,
    do: "#{Float.round(bytes / 1_073_741_824, 1)} GiB"

  defp format_bytes(bytes) when bytes >= 1_048_576, do: "#{Float.round(bytes / 1_048_576, 1)} MiB"
  defp format_bytes(bytes) when bytes >= 1_024, do: "#{Float.round(bytes / 1_024, 1)} KiB"
  defp format_bytes(bytes), do: "#{bytes} B"

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
          <h2 class="card-title text-base">Lifecycle</h2>
          <%= case @lifecycle do %>
            <% :loading -> %>
              <div class="text-sm opacity-70">Loading…</div>
            <% {:error, _reason} -> %>
              <div class="text-sm opacity-70">Lifecycle state is unavailable on this node.</div>
            <% lifecycle -> %>
              <div class="space-y-3">
                <div>
                  <div class="text-xs uppercase opacity-60 mb-1">
                    Hot tier — pending → claimed → sealed awaiting reap
                  </div>
                  <div :for={partition <- lifecycle.hot} class="mb-2">
                    <div class="flex items-center gap-2 text-sm font-mono">
                      <span class="w-56 truncate">{partition.label}</span>
                      <%= if partition.stages == :unavailable do %>
                        <span class="opacity-60">hot tier unreachable</span>
                      <% else %>
                        <span class="badge badge-info badge-sm">
                          pending {partition.stages.pending.count} · {format_bytes(
                            partition.stages.pending.bytes
                          )}
                        </span>
                        <span class="badge badge-warning badge-sm">
                          claimed {partition.stages.claimed.count}
                        </span>
                        <span class="badge badge-success badge-sm">
                          sealed {partition.stages.retired.count}
                        </span>
                      <% end %>
                      <span
                        :if={Map.get(@seal_streaks, partition.ref, 0) > 0}
                        class="badge badge-error badge-sm"
                      >
                        {Map.get(@seal_streaks, partition.ref)} failed seals
                      </span>
                    </div>
                    <div
                      :if={partition.stages != :unavailable and hot_total(partition.stages) > 0}
                      class="flex h-2 mt-1 rounded overflow-hidden bg-base-300"
                    >
                      <div class="bg-info" style={stage_width(partition.stages, :pending)}></div>
                      <div class="bg-warning" style={stage_width(partition.stages, :claimed)}></div>
                      <div class="bg-success" style={stage_width(partition.stages, :retired)}></div>
                    </div>
                  </div>
                </div>

                <div>
                  <div class="text-xs uppercase opacity-60 mb-1">Sealed tier</div>
                  <%= if lifecycle.sealed == :unavailable do %>
                    <div class="text-sm opacity-70">Catalog stats unavailable.</div>
                  <% else %>
                    <div class="text-sm font-mono">
                      {lifecycle.sealed.stats.files} segments · {lifecycle.sealed.stats.rows} rows · {format_bytes(
                        lifecycle.sealed.stats.bytes
                      )}
                      <span class="opacity-60">
                        ({compactable(lifecycle)} under {format_bytes(lifecycle.compact_below)} compact next)
                      </span>
                    </div>
                    <div
                      :if={lifecycle.sealed.files != []}
                      class="flex h-4 mt-1 gap-px rounded overflow-hidden"
                    >
                      <div
                        :for={file <- lifecycle.sealed.files}
                        class={[
                          "min-w-1",
                          file.bytes < lifecycle.compact_below && "bg-accent",
                          file.bytes >= lifecycle.compact_below && "bg-primary"
                        ]}
                        style={file_width(file, lifecycle.sealed.stats.bytes)}
                        title={"#{Path.basename(file.path)} · #{file.rows} rows · #{format_bytes(file.bytes)}"}
                      >
                      </div>
                    </div>
                  <% end %>
                </div>

                <div>
                  <div class="text-xs uppercase opacity-60 mb-1">Events</div>
                  <div
                    :for={kind <- [:seal, :compaction]}
                    :if={Map.has_key?(@last_by_kind, kind)}
                    class="text-sm font-mono flex gap-2"
                  >
                    <span class="badge badge-outline badge-sm">last</span>
                    <span class="opacity-60">{event_time(@last_by_kind[kind])}</span>
                    <span class={@last_by_kind[kind].result != :ok && "text-error"}>
                      {event_line(@last_by_kind[kind])}
                    </span>
                  </div>
                  <div :if={@events == []} class="text-sm opacity-70">
                    Waiting for commits, seals, and compactions…
                  </div>
                  <ul class="text-sm font-mono space-y-0.5">
                    <li :for={event <- @events} class="flex gap-2">
                      <span class="opacity-60">{event_time(event)}</span>
                      <span class={event.result != :ok && "text-error"}>{event_line(event)}</span>
                    </li>
                  </ul>
                </div>
              </div>
          <% end %>
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
