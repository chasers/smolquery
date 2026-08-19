defmodule SmolqueryWeb.ConnectionLive.Index do
  @moduledoc """
  Federated Postgres connections, managed (T-325).

  Lists the registered connections, registers and edits them, removes them,
  and tests one without leaving the page. Reads and writes go straight through
  `Smolquery.Catalog`, the same layering rule as `SmolqueryApi` (PL-12 D4).

  ## The password field is write-only, here as everywhere

  Nothing on this page renders a stored password, because nothing can read one
  — `Smolquery.Catalog.Connection.to_json/1` never returns it and the struct
  hides `:secret` from `Inspect`. Editing an existing connection leaves the
  password input empty, and an empty input is *absent*, not a clear: submitting
  the form without typing one keeps the stored credential. That is what lets an
  operator fix a port without knowing the password.

  ## Testing is the API's own path

  The Test button calls `Smolquery.Federation.probe/1`, which is what
  `POST /v1/connections/:name/test` calls. The button therefore exercises the
  statement a query will run, not an approximation of it, and its failure text
  is the scrubbed reason — never the connection string.
  """

  use SmolqueryWeb, :live_view

  alias Smolquery.Catalog
  alias Smolquery.Catalog.Connection
  alias Smolquery.Federation
  alias Smolquery.Secrets
  alias SmolqueryWeb.Runtime

  @impl Phoenix.LiveView
  def mount(_params, _session, socket) do
    {:ok, runtime} = Runtime.fetch(SmolqueryWeb)

    socket =
      socket
      |> assign(:page_title, "Connections")
      |> assign(:runtime, runtime)
      |> assign(:sslmodes, Connection.sslmodes())
      |> assign(:editing, nil)
      |> assign(:form, blank_form())
      |> assign(:probe, nil)
      |> load_connections()

    {:ok, socket}
  end

  @impl Phoenix.LiveView
  def handle_event("form_changed", %{"connection" => params}, socket) do
    {:noreply, assign(socket, :form, params)}
  end

  def handle_event("edit", %{"name" => name}, socket) do
    case Catalog.connection(socket.assigns.runtime.catalog, name) do
      {:ok, connection} ->
        {:noreply,
         socket
         |> assign(:editing, name)
         |> assign(:probe, nil)
         |> assign(:form, form_from(connection))}

      {:error, reason} ->
        {:noreply, put_flash(socket, :error, message(reason))}
    end
  end

  def handle_event("cancel", _params, socket) do
    {:noreply, socket |> assign(:editing, nil) |> assign(:form, blank_form())}
  end

  def handle_event("save", %{"connection" => params}, socket) do
    catalog = socket.assigns.runtime.catalog

    case save(catalog, socket.assigns.editing, params) do
      {:ok, name} ->
        {:noreply,
         socket
         |> put_flash(:info, "Connection #{name} saved")
         |> assign(:editing, nil)
         |> assign(:form, blank_form())
         |> assign(:probe, nil)
         |> load_connections()}

      {:error, reason} ->
        {:noreply, put_flash(socket, :error, message(reason))}
    end
  end

  def handle_event("delete", %{"name" => name}, socket) do
    case Catalog.delete_connection(socket.assigns.runtime.catalog, name) do
      :ok ->
        {:noreply,
         socket
         |> put_flash(:info, "Connection #{name} removed")
         |> assign(:probe, nil)
         |> load_connections()}

      {:error, reason} ->
        {:noreply, put_flash(socket, :error, message(reason))}
    end
  end

  def handle_event("test", %{"name" => name}, socket) do
    result =
      with {:ok, connection} <- Catalog.connection(socket.assigns.runtime.catalog, name) do
        Federation.probe(connection)
      end

    probe =
      case result do
        :ok -> {name, :ok}
        {:error, reason} -> {name, {:error, message(reason)}}
      end

    {:noreply, assign(socket, :probe, probe)}
  end

  defp save(catalog, nil, params) do
    with {:ok, params} <- normalize(params),
         {:ok, connection} <- Connection.new(params),
         :ok <- Catalog.put_connection(catalog, connection) do
      {:ok, connection.name}
    end
  end

  defp save(catalog, name, params) do
    with {:ok, params} <- normalize(params),
         {:ok, current} <- Catalog.connection(catalog, name),
         {:ok, updated} <- Connection.update(current, params),
         :ok <- Catalog.put_connection(catalog, updated) do
      {:ok, name}
    end
  end

  @doc """
  Turns one form submission into the shape `Smolquery.Catalog.Connection`
  takes.

  An HTML form carries every field as a string, and a JSON body carries `port`
  as a number, so the conversion belongs on this side rather than in the
  struct — leaving it there would make the API accept `"port": "5432"` and
  quietly widen its contract to serve the UI.

  A blank password is *dropped*, not passed through. On the edit form that is
  what keeps the stored credential; the field renders empty because nothing
  can read a password back, so an untouched form must mean "leave it alone".
  """
  @spec normalize(map()) :: {:ok, map()} | {:error, term()}
  def normalize(params) do
    with {:ok, params} <- port(params) do
      {:ok, strip_blank_password(params)}
    end
  end

  defp port(%{"port" => port} = params) when is_binary(port) do
    case Integer.parse(port) do
      {parsed, ""} -> {:ok, Map.put(params, "port", parsed)}
      _unparseable -> {:error, {:invalid_param, "port"}}
    end
  end

  defp port(params), do: {:ok, params}

  defp strip_blank_password(%{"password" => ""} = params), do: Map.delete(params, "password")
  defp strip_blank_password(params), do: params

  defp load_connections(socket) do
    case Catalog.list_connections(socket.assigns.runtime.catalog) do
      {:ok, connections} ->
        assign(socket, :connections, connections)

      {:error, reason} ->
        socket
        |> assign(:connections, [])
        |> put_flash(:error, message(reason))
    end
  end

  defp form_from(%Connection{} = connection) do
    %{
      "name" => connection.name,
      "host" => connection.host,
      "port" => Integer.to_string(connection.port),
      "database" => connection.database,
      "username" => connection.username,
      "password" => "",
      "sslmode" => connection.sslmode
    }
  end

  defp blank_form do
    %{
      "name" => "",
      "host" => "",
      "port" => Integer.to_string(Connection.default_port()),
      "database" => "",
      "username" => "",
      "password" => "",
      "sslmode" => "require"
    }
  end

  defp message(:no_credential_key),
    do: "This node has no SMOLQUERY_CREDENTIAL_KEY, so it cannot seal or open a password"

  defp message(:invalid_secret),
    do: "The stored password does not open with this node's credential key; re-enter it"

  defp message(:connections_unsupported),
    do: "This catalog does not store federated connections"

  defp message({:unknown_connection, name}), do: "Connection #{name} does not exist"
  defp message({:missing_field, field}), do: "#{field} is required"
  defp message({:invalid_param, param}), do: "#{param} is not valid"

  defp message({:invalid_identifier, name}),
    do: "#{inspect(name)} is not a valid name; it becomes the catalog a query qualifies with"

  defp message({:federation_error, name, _reason}), do: "Could not open connection #{name}"
  defp message(reason), do: "Failed: #{inspect(reason)}"

  @impl Phoenix.LiveView
  def render(assigns) do
    ~H"""
    <Layouts.app flash={@flash}>
      <div class="flex items-center justify-between">
        <h1 class="text-xl font-semibold">Connections</h1>
      </div>

      <div :if={!Secrets.configured?()} class="alert alert-warning">
        <span>
          This node has no <code>SMOLQUERY_CREDENTIAL_KEY</code>, so it cannot seal a new
          password or open a stored one. Set it and restart the node.
        </span>
      </div>

      <div :if={@connections == []} class="text-sm opacity-70">
        No connections yet — register one below to join a Postgres database in a query.
      </div>

      <div :if={@connections != []} class="card bg-base-200 border border-base-300">
        <div class="card-body py-4 overflow-x-auto">
          <table class="table table-sm">
            <thead>
              <tr>
                <th>Name</th>
                <th>Host</th>
                <th>Database</th>
                <th>User</th>
                <th>SSL</th>
                <th></th>
              </tr>
            </thead>
            <tbody>
              <tr :for={connection <- @connections}>
                <td class="font-mono">{connection.name}</td>
                <td class="font-mono">{connection.host}:{connection.port}</td>
                <td class="font-mono">{connection.database}</td>
                <td class="font-mono">{connection.username}</td>
                <td class="font-mono">{connection.sslmode}</td>
                <td class="flex gap-2 justify-end">
                  <button
                    type="button"
                    class="btn btn-ghost btn-xs"
                    phx-click="test"
                    phx-value-name={connection.name}
                  >
                    Test
                  </button>
                  <button
                    type="button"
                    class="btn btn-ghost btn-xs"
                    phx-click="edit"
                    phx-value-name={connection.name}
                  >
                    Edit
                  </button>
                  <button
                    type="button"
                    class="btn btn-ghost btn-xs text-error"
                    phx-click="delete"
                    phx-value-name={connection.name}
                    data-confirm={"Remove #{connection.name}? Queries naming it will fail."}
                  >
                    Remove
                  </button>
                </td>
              </tr>
            </tbody>
          </table>

          <div :if={@probe} class="text-sm">
            <span :if={match?({_name, :ok}, @probe)} class="text-success">
              {elem(@probe, 0)} answered.
            </span>
            <span :if={match?({_name, {:error, _reason}}, @probe)} class="text-error">
              {elem(elem(@probe, 1), 1)}
            </span>
          </div>
        </div>
      </div>

      <div class="card bg-base-200 border border-base-300">
        <div class="card-body py-4">
          <h2 class="card-title text-base">
            {if @editing, do: "Edit #{@editing}", else: "New connection"}
          </h2>

          <form id="connection-form" phx-change="form_changed" phx-submit="save" class="space-y-2">
            <div class="grid gap-2 md:grid-cols-2">
              <input
                type="text"
                name="connection[name]"
                value={@form["name"]}
                disabled={@editing != nil}
                placeholder="name — the catalog a query qualifies with"
                class="input input-bordered input-sm font-mono"
              />
              <input
                type="text"
                name="connection[host]"
                value={@form["host"]}
                placeholder="host"
                class="input input-bordered input-sm font-mono"
              />
              <input
                type="number"
                name="connection[port]"
                value={@form["port"]}
                placeholder="port"
                class="input input-bordered input-sm font-mono"
              />
              <input
                type="text"
                name="connection[database]"
                value={@form["database"]}
                placeholder="database"
                class="input input-bordered input-sm font-mono"
              />
              <input
                type="text"
                name="connection[username]"
                value={@form["username"]}
                placeholder="username"
                class="input input-bordered input-sm font-mono"
              />
              <input
                type="password"
                name="connection[password]"
                value={@form["password"]}
                placeholder={
                  if @editing, do: "password — blank keeps the stored one", else: "password"
                }
                class="input input-bordered input-sm font-mono"
              />
              <select name="connection[sslmode]" class="select select-bordered select-sm font-mono">
                <option :for={mode <- @sslmodes} value={mode} selected={@form["sslmode"] == mode}>
                  {mode}
                </option>
              </select>
            </div>

            <div class="flex gap-2">
              <button type="submit" class="btn btn-primary btn-sm">
                {if @editing, do: "Save", else: "Register"}
              </button>
              <button :if={@editing} type="button" class="btn btn-ghost btn-sm" phx-click="cancel">
                Cancel
              </button>
            </div>
          </form>
        </div>
      </div>
    </Layouts.app>
    """
  end
end
