defmodule Smolquery.Cluster.Pods do
  @moduledoc """
  Force-kills, restarts, or otherwise reaches into a node's pod — from
  outside the BEAM, the only way to simulate an ungraceful crash or cycle a
  drained node back in.

  Two backends, chosen by whether this process is itself running inside a
  pod with a Kubernetes ServiceAccount token mounted:

    * **In-cluster** (`in_cluster?/0` — a real deployment, including one
      brought up by `scripts/kind-up.sh`): this pod's own ServiceAccount
      token authenticates straight to the Kubernetes API server
      (`KUBERNETES_SERVICE_HOST`), scoped to `POD_NAMESPACE` — the same
      downward-API env var every workload in `deploy/base/*.yaml` already
      gets. Needs the ServiceAccount bound to a Role granting `delete` (and
      `get`/`list`) on `pods` in its own namespace
      (`deploy/base/rbac.yaml`) — without it, a kill/restart attempt fails
      with the API server's own 403, surfaced to the caller rather than
      silently swallowed.
    * **Local dev, outside any pod** (`mix phx.server` or `mix test` run
      from the repo root): falls back to `kubectl`, pinned to the
      `kind-smolquery` context `scripts/kind-up.sh` sets up — the same
      pinning `Smolquery.Test.Kind`'s cluster test suite already relies on,
      so one wrapper can't point at the wrong cluster. `KUBECONFIG` is
      resolved relative to `File.cwd!/0` at call time, correct for that
      workflow specifically.

  `kill!/1` is ungraceful (`gracePeriodSeconds=0` / `--force
  --grace-period=0`) — a crash, not a shutdown. `restart!/1` is a plain
  delete with neither: the container gets its normal termination grace
  period, and the controller (StatefulSet) reschedules it. That plain delete
  is also what un-sticks a *drained* node — `Smolquery.BufferService.Supervisor`
  refuses to rejoin the ring while the drain flag is up, so only a fresh BEAM
  boot clears it.

  `pod_of_node/1` derives the pod name from the Erlang node atom every other
  cluster-facing module already works with.
  """

  @kind_context "kind-smolquery"
  @kind_namespace "smolquery"
  @sa_dir "/var/run/secrets/kubernetes.io/serviceaccount"

  @doc """
  The kube context every local-dev command here is pinned to. Only
  meaningful for the `kubectl` backend — in-cluster calls hit the API
  server directly and have no "context".
  """
  @spec context() :: String.t()
  def context, do: @kind_context

  @doc """
  Whether killing/restarting a node is possible right now — either this
  process is in-cluster with a ServiceAccount token (RBAC may still refuse
  the actual call), or a local `kind-smolquery` kubectl context is
  reachable.

  `false` wherever `kubectl` itself isn't installed, not just where the
  context is missing — this is called on every `/cluster` mount, not only
  from opt-in `:cluster`-tagged tests, so a dev machine or CI runner without
  `kubectl` must degrade rather than crash the page.
  """
  @spec available?() :: boolean()
  def available?, do: in_cluster?() or kind_context_available?()

  @doc """
  Runs `kubectl` against the local kind cluster, in the smolquery namespace
  — the local-dev backend's own primitive, also used by
  `Smolquery.Test.Kind` to `exec` into a pod's release (`rpc!/2`), which has
  nothing to do with kill/restart but must share the same pinned context.
  """
  @spec kubectl([String.t()]) :: {String.t(), non_neg_integer()}
  def kubectl(args) do
    System.cmd("kubectl", ["--context", @kind_context, "-n", @kind_namespace] ++ args,
      env: kubectl_env(),
      stderr_to_stdout: true
    )
  end

  @doc """
  `kubectl/1`, raising on a non-zero exit and returning trimmed output.
  """
  @spec kubectl!([String.t()]) :: String.t()
  def kubectl!(args) do
    case kubectl(args) do
      {output, 0} -> String.trim(output)
      {output, status} -> raise "kubectl #{Enum.join(args, " ")} exited #{status}:\n#{output}"
    end
  end

  @doc """
  The pod behind a node name — `smolquery@smolquery-buffer-1.<svc>…` is
  `smolquery-buffer-1`.
  """
  @spec pod_of_node(node() | String.t()) :: String.t()
  def pod_of_node(node) do
    host = node |> to_string() |> String.split("@", parts: 2) |> List.last()

    host |> String.split(".", parts: 2) |> hd()
  end

  @doc """
  Deletes `pod` without waiting for a graceful stop — an ungraceful
  departure, the way a crash leaves the cluster rather than the way a drain
  or a plain restart does.
  """
  @spec kill!(String.t()) :: :ok
  def kill!(pod), do: delete!(pod, force: true)

  @doc """
  Deletes `pod` with its normal termination grace period — the controller
  reschedules it, and a node whose drain flag was up comes back clean since
  that flag lives only in the BEAM the plain delete replaces.
  """
  @spec restart!(String.t()) :: :ok
  def restart!(pod), do: delete!(pod, force: false)

  defp delete!(pod, force: force?) do
    if in_cluster?() do
      api_delete!(pod, force?)
    else
      kubectl_delete!(pod, force?)
    end
  end

  defp in_cluster?, do: File.exists?(Path.join(@sa_dir, "token"))

  defp kind_context_available? do
    case System.cmd("kubectl", ["config", "get-contexts", "-o", "name"],
           env: kubectl_env(),
           stderr_to_stdout: true
         ) do
      {output, 0} -> @kind_context in String.split(output, "\n", trim: true)
      _other -> false
    end
  rescue
    ErlangError -> false
  end

  defp api_delete!(pod, force?) do
    namespace = System.fetch_env!("POD_NAMESPACE")
    host = System.fetch_env!("KUBERNETES_SERVICE_HOST")
    port = System.get_env("KUBERNETES_SERVICE_PORT", "443")
    token = File.read!(Path.join(@sa_dir, "token"))
    params = if force?, do: [gracePeriodSeconds: 0], else: []

    response =
      Req.delete!("https://#{host}:#{port}/api/v1/namespaces/#{namespace}/pods/#{pod}",
        auth: {:bearer, token},
        params: params,
        retry: false,
        connect_options: [transport_opts: [cacertfile: Path.join(@sa_dir, "ca.crt")]]
      )

    case response.status do
      status when status in 200..299 ->
        :ok

      status ->
        raise "kubernetes API delete of #{pod} failed: #{status} #{inspect(response.body)}"
    end
  end

  defp kubectl_delete!(pod, force?) do
    args =
      if force? do
        ["delete", "pod", pod, "--force", "--grace-period=0", "--wait=false"]
      else
        ["delete", "pod", pod, "--wait=false"]
      end

    kubectl!(args)

    :ok
  end

  defp kubectl_env, do: [{"KUBECONFIG", Path.join(File.cwd!(), ".kube/config")}]
end
