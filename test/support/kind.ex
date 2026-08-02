defmodule Smolquery.Test.Kind do
  @moduledoc """
  Drives the local kind cluster from ExUnit: `kubectl`, the release's `rpc`, and
  the API's HTTP surface.

  These are the moves the cluster tests are made of — resolve a table's owner,
  drain it, kill it, wait for the ring to come back, ask the API what it can
  see — kept here rather than in the tests so that each one is stated once and
  the destructive ones are hard to point at the wrong cluster.

  ## Every kubectl is pinned

  `kubectl/1` supplies `--context kind-smolquery` and a repo-scoped
  `KUBECONFIG` on every invocation, so no caller can inherit an ambient context.
  That is not stylistic: these tests delete pods, and a `kubectl` that silently
  followed `~/.kube/config` would delete them somewhere else. The shell scripts
  make the same guarantee by repeating the flag at each call site; a single
  wrapper cannot be forgotten.

  ## What replication will want from this

  Milestone 8 proved the cluster is where cross-host bugs actually surface —
  every one that mattered was found here and not by the `:peer` suite, which
  cannot give two nodes distinct hosts sharing a port. Buffer replication
  (PL-5 Stage 1) needs exactly the primitives below: kill a table's owner and
  assert a *complete* answer still comes back from a replica, and assert the
  merged manifest counts a replicated row once rather than once per copy.
  """

  alias Smolquery.Test.Eventually

  @context "kind-smolquery"
  @namespace "smolquery"
  @statefulset "statefulset/smolquery-buffer"
  @api_pod "smolquery-api-0"

  @doc """
  The kube context every command here is pinned to.
  """
  @spec context() :: String.t()
  def context, do: @context

  @doc """
  Whether the kind cluster's context exists at all.

  The cluster tests refuse to start without it, naming `scripts/kind-up.sh` in
  the failure — a missing cluster is a missing fixture, and saying so beats a
  pile of connection errors.
  """
  @spec available?() :: boolean()
  def available? do
    case System.cmd("kubectl", ["config", "get-contexts", "-o", "name"],
           env: env(),
           stderr_to_stdout: true
         ) do
      {output, 0} -> @context in String.split(output, "\n", trim: true)
      _other -> false
    end
  end

  @doc """
  Runs `kubectl` against the kind cluster, in the smolquery namespace.
  """
  @spec kubectl([String.t()]) :: {String.t(), non_neg_integer()}
  def kubectl(args) do
    System.cmd("kubectl", ["--context", @context, "-n", @namespace] ++ args,
      env: env(),
      stderr_to_stdout: true
    )
  end

  @doc """
  `kubectl/1`, raising on a non-zero exit and returning trimmed output.
  """
  @spec kubectl!([String.t()]) :: String.t()
  def kubectl!(args) do
    case kubectl(args) do
      {output, 0} ->
        String.trim(output)

      {output, status} ->
        raise "kubectl #{Enum.join(args, " ")} exited #{status}:\n#{output}"
    end
  end

  @doc """
  Evaluates `code` inside a running pod's release.
  """
  @spec rpc!(String.t(), String.t()) :: String.t()
  def rpc!(pod, code) do
    ["exec", pod, "-c", "smolquery", "--", "/app/bin/smolquery", "rpc", code]
    |> kubectl!()
    |> String.replace("\r", "")
    |> String.split("\n", trim: true)
    |> List.last()
    |> case do
      nil -> ""
      line -> line
    end
  end

  # ── the fleet ─────────────────────────────────────────────────────────

  @doc """
  The buffer nodes the ring currently names.
  """
  @spec ring_nodes() :: [String.t()]
  def ring_nodes do
    @api_pod
    |> rpc!(
      ~s|IO.puts(Enum.join(Smolquery.BufferService.Client.nodes(Smolquery.BufferService), " "))|
    )
    |> String.split(" ", trim: true)
  end

  @doc """
  The pod owning `table`, as the ring resolves it right now.
  """
  @spec owner_pod({String.t(), String.t()}) :: String.t()
  def owner_pod({dataset, table}) do
    @api_pod
    |> rpc!(
      ~s|IO.puts(Smolquery.BufferService.Client.owner(Smolquery.BufferService, {"#{dataset}", "#{table}"}))|
    )
    |> pod_of_node()
  end

  @doc """
  The pod behind a node name — `smolquery@smolquery-buffer-1.<svc>…` is
  `smolquery-buffer-1`.
  """
  @spec pod_of_node(String.t()) :: String.t()
  def pod_of_node(node) do
    node |> String.split("@", parts: 2) |> List.last() |> String.split(".") |> hd()
  end

  @doc """
  Two table names the ring places on two *different* buffer nodes.

  Walking the ring rather than naming tables up front is what keeps the fan-out
  assertions from passing vacuously: consistent hashing decides ownership, so
  which names land where changes with the member list.
  """
  @spec tables_on_distinct_owners(String.t()) :: {String.t(), String.t()}
  def tables_on_distinct_owners(dataset) do
    picked =
      rpc!(@api_pod, """
      owner = fn i ->
        Smolquery.BufferService.Client.owner(Smolquery.BufferService, {"#{dataset}", "t" <> Integer.to_string(i)})
      end

      first = owner.(1)

      case Enum.find(2..500, fn i -> owner.(i) != first end) do
        nil -> IO.puts("NONE")
        i -> IO.puts("t1 t" <> Integer.to_string(i))
      end
      """)

    case String.split(picked, " ", trim: true) do
      [first, second] ->
        {first, second}

      _other ->
        raise "every candidate table hashed to one buffer node — is the ring a single node?"
    end
  end

  @doc """
  Force-seals everything `pod` owns and takes it out of the ring.
  """
  @spec drain!(String.t()) :: String.t()
  def drain!(pod) do
    rpc!(
      pod,
      ":ok = Smolquery.BufferService.Drain.drain(Smolquery.BufferService, timeout_ms: 120_000)"
    )
  end

  @doc """
  Deletes `pod` without waiting for a graceful stop — an ungraceful departure,
  the way a crash leaves the ring rather than the way a drain does.
  """
  @spec kill!(String.t()) :: String.t()
  def kill!(pod) do
    kubectl!(["delete", "pod", pod, "--force", "--grace-period=0", "--wait=false"])
  end

  @doc """
  Restarts `pod`, then waits for the fleet.

  For a *drained* node, which is still running: it left the ring deliberately and
  `Smolquery.BufferService.Supervisor` refuses to rejoin while the drain flag is
  up, so only a fresh BEAM puts it back. A killed pod needs `await_fleet!/1`
  instead — the StatefulSet already replaced it, and deleting its replacement
  would just restart a healthy pod.
  """
  @spec restore!(String.t(), pos_integer()) :: :ok
  def restore!(pod, size \\ 3) do
    kubectl(["delete", "pod", pod, "--wait=false"])

    await_fleet!(size)
  end

  @doc """
  Waits until the buffer StatefulSet has rolled out and the ring names `size`
  nodes again.

  What every destructive test undoes itself with, so the clauses stay
  order-independent under ExUnit's shuffling rather than merely appearing to be.
  """
  @spec await_fleet!(pos_integer()) :: :ok
  def await_fleet!(size \\ 3) do
    kubectl!(["rollout", "status", @statefulset, "--timeout=180s"])

    Eventually.until(fn -> length(ring_nodes()) == size end, 60, 1_000) ||
      raise "ring did not return to #{size} members, saw #{inspect(ring_nodes())}"

    :ok
  end

  @doc """
  How many storage replicas are running — the ownership gate is only under test
  when there is more than one.
  """
  @spec running_storage_replicas() :: non_neg_integer()
  def running_storage_replicas do
    [
      "get",
      "pods",
      "-l",
      "app=smolquery-storage",
      "--field-selector",
      "status.phase=Running",
      "-o",
      "name"
    ]
    |> kubectl!()
    |> String.split("\n", trim: true)
    |> length()
  end

  @doc """
  Whether a sealed parquet object for `dataset` has appeared in MinIO.
  """
  @spec sealed_object(String.t()) :: String.t() | nil
  def sealed_object(dataset) do
    case kubectl([
           "exec",
           "deploy/minio",
           "--",
           "sh",
           "-c",
           "for f in /data/smolquery-sealed/#{dataset}/*/*.parquet; do [ -e \"$f\" ] && echo \"$f\" && break; done; :"
         ]) do
      {output, 0} -> output |> String.trim() |> presence()
      _other -> nil
    end
  end

  # ── the API ───────────────────────────────────────────────────────────

  @doc """
  Waits for the API edge to answer `/healthz`.
  """
  @spec await_api!() :: :ok
  def await_api! do
    Eventually.until(
      fn ->
        match?({:ok, %{status: 200}}, Req.get(base_url() <> "/healthz", retry: false))
      end,
      60,
      2_000
    ) || raise "the API never became healthy at #{base_url()}"

    :ok
  end

  @doc """
  Creates a dataset and its tables, each with an `id`/`v` schema.
  """
  @spec create_dataset!(String.t(), [String.t()]) :: :ok
  def create_dataset!(dataset, tables) do
    %{status: 200} = post!("/v1/datasets", %{"id" => dataset})

    for table <- tables do
      %{status: 200} =
        post!("/v1/datasets/#{dataset}/tables", %{
          "id" => table,
          "schema" => [
            %{"name" => "id", "type" => "INT64", "nullable" => false},
            %{"name" => "v", "type" => "STRING"}
          ]
        })
    end

    :ok
  end

  @doc """
  Inserts `count` rows with ids `from..from + count - 1`, so a table's ids stay
  unique across inserts and a duplicate can only come from a double-merge.
  """
  @spec insert!(String.t(), String.t(), pos_integer(), non_neg_integer()) :: :ok
  def insert!(dataset, table, count, from \\ 0) do
    rows = for i <- from..(from + count - 1), do: %{"id" => i, "v" => "r#{i}"}

    insert_with_retry(dataset, table, rows, 15)
  end

  defp insert_with_retry(dataset, table, rows, attempts) do
    case post!("/v1/datasets/#{dataset}/tables/#{table}/insert", %{"rows" => rows}) do
      %{status: 200, body: body} ->
        case Map.get(body, "insertErrors", []) do
          [] -> :ok
          errors -> raise "insert into #{dataset}.#{table} reported errors: #{inspect(errors)}"
        end

      %{status: _refused} when attempts > 1 ->
        Process.sleep(2_000)
        insert_with_retry(dataset, table, rows, attempts - 1)

      %{status: status} ->
        raise "insert into #{dataset}.#{table} kept failing with HTTP #{status}"
    end
  end

  @doc """
  Runs `sql`, returning the first row's `column` as an integer.

  `{:error, status}` rather than a raise when the query fails, because "did this
  fail cleanly?" is itself an assertion the cluster tests make.
  """
  @spec count(String.t(), String.t()) :: {:ok, integer()} | {:error, pos_integer()}
  def count(sql, column \\ "n") do
    case post!("/v1/queries", %{"query" => sql}) do
      %{status: 200, body: body} ->
        {:ok, body |> Map.fetch!("rows") |> hd() |> Map.fetch!(column) |> to_integer()}

      %{status: status} ->
        {:error, status}
    end
  end

  @doc """
  The total row count across `tables` in `dataset`.
  """
  @spec total_rows(String.t(), [String.t()]) :: {:ok, integer()} | {:error, pos_integer()}
  def total_rows(dataset, tables) do
    sums = Enum.map_join(tables, " + ", &"(SELECT count(*) FROM #{dataset}.#{&1})")

    count("SELECT #{sums} AS n")
  end

  @doc """
  How many rows in `dataset.table` share an id with another — non-zero only if a
  segment was merged into the catalog twice.
  """
  @spec duplicate_rows(String.t(), String.t()) :: {:ok, integer()} | {:error, pos_integer()}
  def duplicate_rows(dataset, table) do
    count("SELECT count(*) - count(DISTINCT id) AS n FROM #{dataset}.#{table}")
  end

  @doc """
  A dataset name no other run shares — buffers (PVCs), the catalog, and MinIO all
  outlive a redeploy, so a fixed name accumulates rows across runs.
  """
  @spec unique_dataset(String.t()) :: String.t()
  def unique_dataset(prefix) do
    "#{prefix}_#{System.system_time(:second)}_#{System.unique_integer([:positive])}"
  end

  defp post!(path, body) do
    Req.post!(base_url() <> path,
      json: body,
      headers: [{"authorization", "Bearer #{api_key()}"}],
      retry: false,
      receive_timeout: 60_000
    )
  end

  defp base_url, do: System.get_env("BASE", "http://localhost:8080")

  defp api_key, do: System.get_env("SMOLQUERY_API_KEY", "kind-only-api-key")

  defp env, do: [{"KUBECONFIG", Path.join(File.cwd!(), ".kube/config")}]

  defp presence(""), do: nil
  defp presence(value), do: value

  defp to_integer(value) when is_integer(value), do: value
  defp to_integer(value) when is_binary(value), do: String.to_integer(value)
end
