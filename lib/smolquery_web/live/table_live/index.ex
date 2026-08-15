defmodule SmolqueryWeb.TableLive.Index do
  @moduledoc """
  Datasets and tables — the catalog's CRUD surface, browsable.

  Lists every dataset with its tables, creates datasets, and creates tables
  from a schema built field by field over `Smolquery.Schema`'s logical types.
  Reads and writes go straight through `Smolquery.Catalog` — the same layering
  rule as `SmolqueryApi` (PL-12 D4).
  """

  use SmolqueryWeb, :live_view

  alias Smolquery.Catalog
  alias Smolquery.IngestService
  alias Smolquery.Schema
  alias SmolqueryWeb.{Authorization, Runtime}

  @types ["STRING", "INT64", "FLOAT64", "BOOL", "TIMESTAMP", "DATE", "NUMERIC(38,9)"]

  @impl Phoenix.LiveView
  def mount(_params, _session, socket) do
    {:ok, runtime} = Runtime.fetch(SmolqueryWeb)

    socket =
      socket
      |> assign(:page_title, "Tables")
      |> assign(:runtime, runtime)
      |> assign(:can_catalog_manage, can?(socket, :catalog_manage))
      |> assign(:types, @types)
      |> assign(:table_params, blank_table_params())
      |> load_listing()

    {:ok, socket}
  end

  @impl Phoenix.LiveView
  def handle_event("create_dataset", params, socket) do
    with :ok <- Authorization.event(socket, :catalog_manage),
         {:ok, name} <- fetch_dataset_name(params),
         dataset when dataset != "" <- String.trim(name) do
      case Catalog.create_dataset(socket.assigns.runtime.catalog, dataset) do
        :ok ->
          {:noreply,
           socket
           |> put_flash(:info, "Dataset #{dataset} created")
           |> load_listing()}

        {:error, reason} ->
          {:noreply, put_flash(socket, :error, "Could not create dataset: #{inspect(reason)}")}
      end
    else
      {:error, _reason, denied} -> {:noreply, denied}
      _ -> {:noreply, put_flash(socket, :error, "Dataset name is required")}
    end
  end

  def handle_event("table_changed", params, socket) do
    with :ok <- Authorization.event(socket, :catalog_manage),
         %{"table" => table_params} when is_map(table_params) <- params do
      {:noreply, assign(socket, :table_params, table_params)}
    else
      {:error, _reason, denied} -> {:noreply, denied}
      _ -> {:noreply, socket}
    end
  end

  def handle_event("add_field", _params, socket) do
    case Authorization.event(socket, :catalog_manage) do
      :ok ->
        params = socket.assigns.table_params
        fields = Map.get(params, "fields", %{})
        next = fields |> Map.keys() |> Enum.map(&String.to_integer/1) |> Enum.max(fn -> -1 end)

        fields = Map.put(fields, Integer.to_string(next + 1), blank_field())
        {:noreply, assign(socket, :table_params, Map.put(params, "fields", fields))}

      {:error, _reason, denied} ->
        {:noreply, denied}
    end
  end

  def handle_event("remove_field", params, socket) do
    with :ok <- Authorization.event(socket, :catalog_manage),
         index when is_binary(index) <- Map.get(params, "index") do
      fields = socket.assigns.table_params |> Map.get("fields", %{}) |> Map.delete(index)

      {:noreply,
       assign(socket, :table_params, Map.put(socket.assigns.table_params, "fields", fields))}
    else
      {:error, _reason, denied} -> {:noreply, denied}
      _ -> {:noreply, socket}
    end
  end

  def handle_event("create_table", params, socket) do
    with :ok <- Authorization.event(socket, :catalog_manage),
         %{"table" => table_params} when is_map(table_params) <- params,
         {:ok, dataset, table} <- table_ref(table_params),
         {:ok, schema} <- build_schema(table_params),
         :ok <- Catalog.create_table(socket.assigns.runtime.catalog, {dataset, table}, schema) do
      IngestService.Client.invalidate(socket.assigns.runtime.ingest_name, {dataset, table})

      {:noreply,
       socket
       |> put_flash(:info, "Table #{dataset}.#{table} created")
       |> push_navigate(to: ~p"/tables/#{dataset}/#{table}")}
    else
      {:error, _reason, denied} -> {:noreply, denied}
      {:error, reason} -> {:noreply, put_flash(socket, :error, create_table_error(reason))}
      _ -> {:noreply, put_flash(socket, :error, "Could not create table")}
    end
  end

  defp fetch_dataset_name(%{"dataset" => %{"name" => name}}) when is_binary(name),
    do: {:ok, name}

  defp fetch_dataset_name(_params), do: {:error, :invalid_dataset}

  defp can?(socket, capability),
    do: Authorization.authorize(socket, capability) == :ok

  defp load_listing(socket) do
    catalog = socket.assigns.runtime.catalog

    with :ok <- Authorization.authorize(socket, :query),
         {:ok, datasets} <- Catalog.list_datasets(catalog),
         {:ok, refs} <- Catalog.tables(catalog) do
      assign(socket, :listing, listing(datasets, refs))
    else
      {:error, reason} ->
        socket
        |> assign(:listing, [])
        |> put_flash(:error, "Could not read the catalog: #{inspect(reason)}")
    end
  end

  defp listing(datasets, refs) do
    grouped = Enum.group_by(refs, fn {dataset, _table} -> dataset end)

    datasets
    |> Enum.map(fn dataset ->
      tables = grouped |> Map.get(dataset, []) |> Enum.map(fn {_ds, table} -> table end)
      {dataset, Enum.sort(tables)}
    end)
    |> Enum.sort()
  end

  defp table_ref(params) do
    dataset = params |> Map.get("dataset", "") |> String.trim()
    table = params |> Map.get("name", "") |> String.trim()

    if dataset == "" or table == "" do
      {:error, :missing_name}
    else
      {:ok, dataset, table}
    end
  end

  defp build_schema(params) do
    specs =
      params
      |> Map.get("fields", %{})
      |> Enum.sort_by(fn {index, _field} -> String.to_integer(index) end)
      |> Enum.map(fn {_index, field} -> field end)
      |> Enum.reject(fn field -> String.trim(Map.get(field, "name", "")) == "" end)

    specs
    |> Enum.reduce_while({:ok, []}, fn field, {:ok, acc} ->
      case Schema.type_from_api(Map.get(field, "type", "")) do
        {:ok, type} ->
          spec = {String.trim(field["name"]), type, nullable: field["nullable"] == "true"}
          {:cont, {:ok, [spec | acc]}}

        {:error, reason} ->
          {:halt, {:error, reason}}
      end
    end)
    |> case do
      {:ok, specs} -> specs |> Enum.reverse() |> Schema.new()
      {:error, reason} -> {:error, reason}
    end
  end

  defp create_table_error(:missing_name), do: "Dataset and table name are both required"
  defp create_table_error(:empty_schema), do: "A table needs at least one field"

  defp create_table_error({:unsupported_type, type}),
    do: "Unsupported field type: #{inspect(type)}"

  defp create_table_error(reason), do: "Could not create table: #{inspect(reason)}"

  defp blank_table_params do
    %{"dataset" => "", "name" => "", "fields" => %{"0" => blank_field()}}
  end

  defp blank_field, do: %{"name" => "", "type" => "STRING", "nullable" => "true"}

  defp sorted_fields(params) do
    params
    |> Map.get("fields", %{})
    |> Enum.sort_by(fn {index, _field} -> String.to_integer(index) end)
  end

  @impl Phoenix.LiveView
  def render(assigns) do
    ~H"""
    <Layouts.app flash={@flash}>
      <div class="flex items-center justify-between">
        <h1 class="text-xl font-semibold">Tables</h1>
      </div>

      <div :if={@listing == []} class="text-sm opacity-70">
        No datasets yet — create one below.
      </div>

      <div :for={{dataset, tables} <- @listing} class="card bg-base-200 border border-base-300">
        <div class="card-body py-4">
          <h2 class="card-title text-base font-mono">{dataset}</h2>
          <div :if={tables == []} class="text-sm opacity-70">No tables in this dataset.</div>
          <ul class="space-y-1">
            <li :for={table <- tables}>
              <.link navigate={~p"/tables/#{dataset}/#{table}"} class="link link-hover font-mono">
                {table}
              </.link>
            </li>
          </ul>
        </div>
      </div>

      <div :if={@can_catalog_manage} class="grid gap-6 md:grid-cols-2">
        <div class="card bg-base-200 border border-base-300">
          <div class="card-body py-4">
            <h2 class="card-title text-base">New dataset</h2>
            <form id="create-dataset" phx-submit="create_dataset" class="flex gap-2">
              <input
                type="text"
                name="dataset[name]"
                placeholder="dataset name"
                class="input input-bordered input-sm flex-1 font-mono"
              />
              <button type="submit" class="btn btn-primary btn-sm">Create</button>
            </form>
          </div>
        </div>

        <div class="card bg-base-200 border border-base-300">
          <div class="card-body py-4">
            <h2 class="card-title text-base">New table</h2>
            <form
              id="create-table"
              phx-change="table_changed"
              phx-submit="create_table"
              class="space-y-2"
            >
              <div class="flex gap-2">
                <select name="table[dataset]" class="select select-bordered select-sm font-mono">
                  <option value="">dataset…</option>
                  <option
                    :for={{dataset, _tables} <- @listing}
                    value={dataset}
                    selected={@table_params["dataset"] == dataset}
                  >
                    {dataset}
                  </option>
                </select>
                <input
                  type="text"
                  name="table[name]"
                  value={@table_params["name"]}
                  placeholder="table name"
                  class="input input-bordered input-sm flex-1 font-mono"
                />
              </div>

              <div
                :for={{index, field} <- sorted_fields(@table_params)}
                class="flex gap-2 items-center"
              >
                <input
                  type="text"
                  name={"table[fields][#{index}][name]"}
                  value={field["name"]}
                  placeholder="field name"
                  class="input input-bordered input-sm flex-1 font-mono"
                />
                <select
                  name={"table[fields][#{index}][type]"}
                  class="select select-bordered select-sm font-mono"
                >
                  <option :for={type <- @types} value={type} selected={field["type"] == type}>
                    {type}
                  </option>
                </select>
                <label class="label cursor-pointer gap-1 text-xs">
                  <input type="hidden" name={"table[fields][#{index}][nullable]"} value="false" />
                  <input
                    type="checkbox"
                    name={"table[fields][#{index}][nullable]"}
                    value="true"
                    checked={field["nullable"] == "true"}
                    class="checkbox checkbox-sm"
                  /> null
                </label>
                <button
                  type="button"
                  phx-click="remove_field"
                  phx-value-index={index}
                  class="btn btn-ghost btn-xs"
                >
                  <.icon name="hero-x-mark" class="size-4" />
                </button>
              </div>

              <div class="flex gap-2">
                <button type="button" phx-click="add_field" class="btn btn-ghost btn-sm">
                  <.icon name="hero-plus" class="size-4" /> Field
                </button>
                <button type="submit" class="btn btn-primary btn-sm">Create table</button>
              </div>
            </form>
          </div>
        </div>
      </div>
    </Layouts.app>
    """
  end
end
