defmodule SmolqueryWeb.ClusterLive.Index do
  @moduledoc """
  The fleet, live — every node's alive/ring/drain state, and three ways to
  disturb one on purpose.

  Reads through `Smolquery.Cluster.Topology`, the same layering rule as
  `TableLive`/`QueryLive` reading through `Smolquery.Catalog`. Push updates
  come from `Smolquery.Cluster.Membership.subscribe/1` (a node joining or
  leaving); the ring/epoch/drain fields it doesn't cover are refreshed on a
  timer instead, since nothing here broadcasts those.

  Three distinct actions per node, not one:

    * **Kill** — `Smolquery.Cluster.Pods.kill!/1`, an ungraceful pod
      force-delete. Simulates a crash; the fastest way to watch
      `Smolquery.BufferService.RingEpoch`'s fencing actually matter, since a
      force-delete is the one path where the old pod's process isn't
      confirmed stopped before its replacement can start.
    * **Restart** — `Smolquery.Cluster.Pods.restart!/1`, a plain pod delete.
      The controller reschedules it with its normal termination grace
      period; this is also what un-sticks a *drained* node, since
      `Smolquery.BufferService.Supervisor` refuses to rejoin the ring while
      the drain flag is up and only a fresh BEAM boot clears it.
    * **Drain** — `Smolquery.BufferService.Drain.drain/2`, invoked directly
      over distributed Erlang (`:rpc.call/5`) rather than through
      `Smolquery.Cluster.Pods` at all: a genuinely graceful ring exit,
      force-sealing every owned table before leaving. It can block for its
      full `timeout_ms`, so it runs under `start_async/3` instead of
      blocking `handle_event` and freezing the page. Buffer-only — storage
      has no drain analog — so the button only appears for buffer-member
      rows.

  Kill and restart both need `Smolquery.Cluster.Pods.available?/0`; drain
  needs only that the node is alive and a buffer member, since it goes
  straight over the cluster's own distribution rather than kubectl/the k8s
  API.

  Every action's target is validated against the current fleet snapshot
  before anything runs: a crafted event can't name an arbitrary pod in the
  namespace (the ServiceAccount token can delete more than smolquery pods)
  or mint an atom from a bogus node string. A kill/restart that fails —
  kubectl non-zero, an API-server 403 — flashes the error instead of
  crashing the LiveView. Setting `pod_actions: false` under this module's
  app env (as `config/test.exs` does) disables kill/restart wholesale, so
  `mix test` on a machine with a live `kind-smolquery` context can never
  reach a real cluster.
  """

  use SmolqueryWeb, :live_view

  alias Smolquery.BufferService
  alias Smolquery.BufferService.Drain
  alias Smolquery.Cluster
  alias Smolquery.Cluster.Membership
  alias Smolquery.Cluster.Pods
  alias Smolquery.Cluster.Topology

  @refresh_ms 2_000
  @drain_timeout_ms 30_000

  @impl Phoenix.LiveView
  def mount(_params, _session, socket) do
    if connected?(socket) do
      if Cluster.enabled?(), do: Membership.subscribe()
      Process.send_after(self(), :refresh, @refresh_ms)
    end

    socket =
      socket
      |> assign(:page_title, "Cluster")
      |> assign(:kill_available, pod_actions_enabled?() and Pods.available?())
      |> assign(:draining, MapSet.new())
      |> load_fleet()

    {:ok, socket}
  end

  @impl Phoenix.LiveView
  def handle_info({:cluster_membership, _members}, socket) do
    {:noreply, load_fleet(socket)}
  end

  def handle_info(:refresh, socket) do
    Process.send_after(self(), :refresh, @refresh_ms)

    {:noreply, load_fleet(socket)}
  end

  @impl Phoenix.LiveView
  def handle_async({:drain, node}, {:ok, result}, socket) do
    {:noreply,
     socket
     |> assign(:draining, MapSet.delete(socket.assigns.draining, node))
     |> put_flash(drain_flash_kind(result), drain_flash_message(node, result))
     |> load_fleet()}
  end

  def handle_async({:drain, node}, {:exit, reason}, socket) do
    {:noreply,
     socket
     |> assign(:draining, MapSet.delete(socket.assigns.draining, node))
     |> put_flash(:error, "Drain of #{node} crashed: #{inspect(reason)}")}
  end

  @impl Phoenix.LiveView
  def handle_event("kill", %{"pod" => pod}, socket) do
    pod_action(socket, "kill", pod, fn -> Pods.kill!(pod) end, "Killed #{pod}")
  end

  def handle_event("restart", %{"pod" => pod}, socket) do
    pod_action(socket, "restart", pod, fn -> Pods.restart!(pod) end, "Restarting #{pod}")
  end

  def handle_event("drain", %{"node" => node_str}, socket) do
    case Enum.find(socket.assigns.fleet, &(to_string(&1.node) == node_str)) do
      nil ->
        {:noreply, put_flash(socket, :error, "#{node_str} is not part of the fleet")}

      row ->
        {:noreply,
         socket
         |> assign(:draining, MapSet.put(socket.assigns.draining, row.node))
         |> start_async({:drain, row.node}, fn -> rpc_drain(row.node) end)}
    end
  end

  defp pod_action(socket, verb, pod, action, success_message) do
    cond do
      Enum.all?(socket.assigns.fleet, &(&1.pod != pod)) ->
        {:noreply, put_flash(socket, :error, "#{pod} is not part of the fleet")}

      !socket.assigns.kill_available ->
        {:noreply, put_flash(socket, :error, "No kind cluster detected — nothing to #{verb}")}

      true ->
        try do
          action.()

          {:noreply, put_flash(socket, :info, "#{success_message} — watching the ring recover…")}
        rescue
          error in [RuntimeError, Req.TransportError] ->
            {:noreply,
             put_flash(socket, :error, "#{verb} of #{pod} failed: #{Exception.message(error)}")}
        end
    end
  end

  defp pod_actions_enabled? do
    :smolquery
    |> Application.get_env(__MODULE__, [])
    |> Keyword.get(:pod_actions, true)
  end

  defp rpc_drain(node) do
    :rpc.call(
      node,
      Drain,
      :drain,
      [BufferService, [timeout_ms: @drain_timeout_ms]],
      @drain_timeout_ms + 5_000
    )
  end

  defp drain_flash_kind(:ok), do: :info
  defp drain_flash_kind(_error), do: :error

  defp drain_flash_message(node, :ok), do: "#{node} drained and left the ring"

  defp drain_flash_message(node, {:badrpc, reason}),
    do: "Drain of #{node} failed: #{inspect(reason)}"

  defp drain_flash_message(node, {:error, reason}),
    do: "Drain of #{node} failed: #{inspect(reason)}"

  defp load_fleet(socket), do: assign(socket, :fleet, Topology.fleet())

  defp draining?(draining, node), do: MapSet.member?(draining, node)

  @impl Phoenix.LiveView
  def render(assigns) do
    ~H"""
    <Layouts.app flash={@flash}>
      <div class="flex items-center justify-between">
        <h1 class="text-xl font-semibold">Cluster</h1>
      </div>

      <div :if={!@kill_available} class="alert alert-warning text-sm">
        No local kind cluster detected (<code>kubectl</code>
        context <code>kind-smolquery</code>) and no in-cluster ServiceAccount
        token found — the fleet below still reflects live cluster state, but
        killing/restarting a node is disabled. Bring one up with <code>scripts/kind-up.sh</code>, or deploy with
        <code>deploy/base/rbac.yaml</code>
        applied.
      </div>

      <div class="overflow-x-auto">
        <table class="table table-zebra table-sm">
          <thead>
            <tr>
              <th>Node</th>
              <th>Pod</th>
              <th>Alive</th>
              <th>Buffer</th>
              <th>Storage</th>
              <th>Expected</th>
              <th>Epoch</th>
              <th>Draining</th>
              <th></th>
            </tr>
          </thead>
          <tbody>
            <tr :for={row <- @fleet} class={[!row.alive && "opacity-60"]}>
              <td class="font-mono whitespace-nowrap">{row.node}</td>
              <td class="font-mono whitespace-nowrap">{row.pod}</td>
              <td>{status_badge(row.alive, "up", "down")}</td>
              <td>{boolean_badge(row.buffer_member)}</td>
              <td>{boolean_badge(row.storage_member)}</td>
              <td>{mismatch_badge(row.expected_buffer, row.buffer_member)}</td>
              <td class="font-mono">{row.buffer_epoch || "—"}</td>
              <td>{boolean_badge(row.draining or draining?(@draining, row.node))}</td>
              <td class="whitespace-nowrap">
                <button
                  :if={row.buffer_member}
                  type="button"
                  phx-click="drain"
                  phx-value-node={row.node}
                  data-confirm={"Gracefully drain #{row.node}?"}
                  disabled={!row.alive or draining?(@draining, row.node)}
                  class="btn btn-warning btn-xs"
                >
                  Drain
                </button>
                <button
                  type="button"
                  phx-click="restart"
                  phx-value-pod={row.pod}
                  data-confirm={"Restart #{row.pod}?"}
                  disabled={!@kill_available or !row.alive}
                  class="btn btn-ghost btn-xs"
                >
                  Restart
                </button>
                <button
                  type="button"
                  phx-click="kill"
                  phx-value-pod={row.pod}
                  data-confirm={"Force-kill #{row.pod}?"}
                  disabled={!@kill_available or !row.alive}
                  class="btn btn-error btn-xs"
                >
                  Kill
                </button>
              </td>
            </tr>
          </tbody>
        </table>
      </div>
    </Layouts.app>
    """
  end

  defp status_badge(true, up_label, _down_label) do
    assigns = %{label: up_label}

    ~H"""
    <span class="badge badge-success badge-sm">{@label}</span>
    """
  end

  defp status_badge(false, _up_label, down_label) do
    assigns = %{label: down_label}

    ~H"""
    <span class="badge badge-ghost badge-sm">{@label}</span>
    """
  end

  defp boolean_badge(true) do
    assigns = %{}

    ~H"""
    <span class="badge badge-success badge-sm">yes</span>
    """
  end

  defp boolean_badge(false) do
    assigns = %{}

    ~H"""
    <span class="badge badge-ghost badge-sm">no</span>
    """
  end

  defp mismatch_badge(expected, member) when expected == member, do: boolean_badge(expected)

  defp mismatch_badge(_expected, _member) do
    assigns = %{}

    ~H"""
    <span class="badge badge-warning badge-sm">mismatch</span>
    """
  end
end
