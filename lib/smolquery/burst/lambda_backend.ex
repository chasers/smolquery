defmodule Smolquery.Burst.LambdaBackend do
  @moduledoc """
  A `FLAME.Backend` whose runner is one AWS Lambda invocation (PL-77 L4).

  `remote_boot/1` invokes the function asynchronously with the runner's
  assignment in the payload: the encoded `FLAME.Parent`, the cluster cookie,
  the runner's node name, the VM flags, its lifetime, and how to reach this
  node. Lambda's own environment variables are fixed per function version
  and capped at 4 KB, so they cannot carry a per-boot parent; the function's
  bootstrap (`rel/lambda/bootstrap`) exports the payload into its
  environment and runs the release in the foreground until the BEAM exits.
  The runner's `FLAME.Terminator` then connects back to this node over
  distribution and announces itself; the boot blocks on that announcement
  for `boot_timeout`.

  ## Reaching this node

  The runner dials `node(parent.pid)`, this node's name, through this
  node's epmd and on its distribution port. On a pod that name is the
  headless-service DNS name (`smolquery@<pod>.smolquery-headless.<ns>.svc.cluster.local`),
  which the function's VPC cannot resolve. So the assignment carries an
  Erlang `inetrc` with one host entry, this node's name mapped to its IP
  (`POD_IP`, or the name resolved here), which the bootstrap writes to
  `/tmp/inetrc` and names in `ERL_INETRC`. The runner's resolver consults
  it before the system's. A node named by its IP needs no entry. The
  distribution port must be pinned (`inet_dist_listen_min`/`max`) and open,
  with epmd's 4369, from the function's security group.

  ## The runner

  It starts with `-hidden -dist_listen false`: it opens no distribution
  listener, registers with no epmd, and no other node can see or reach it.
  Its node name's host part is a label and nothing resolves it. It stops
  itself after `max_lifetime_ms` (`Smolquery.Burst.Lifetime`), under
  Lambda's 15-minute cap: at the cap the host kills the runner outright,
  the parent sees `:noconnection`, and the pool kills the shard's caller
  with `:kill`, which nothing catches (T-673). A shard on a runner that
  stopped itself fails as unreachable instead, which the scatter retries on
  a peer.

  ## The invoke

  The invoke closes its connection: the Lambda API drops an idle keep-alive
  after a few minutes, and a pooled connection that died that way fails the
  next invoke with `:closed` before the request is sent. Measured on the
  spike: 50 such connections from a 100-runner burst cost 52 s of
  one-second retries on the first lease after a quiet quarter hour. A
  `:closed` is the one transport error retried, since nothing was sent on
  it; any other failure is returned, never retried, because a retried
  `Event` invoke is a second runner. The function's own event-invoke config
  must set `MaximumRetryAttempts` to 0 for the same reason.

  The invoke is signed with the credential chain (`Smolquery.AwsCredentials`),
  so the node that leases runners needs `lambda:InvokeFunction` on the
  function through its pod identity, and nothing else of AWS.

  ## Configuration

      config :smolquery, Smolquery.Burst.LambdaBackend,
        function: "smolquery-burst-runner",
        region: "eu-central-1",
        boot_timeout: 120_000,
        max_lifetime_ms: 840_000,
        env: %{}

  `env` is extra environment for the runner, merged under the assignment.
  `config/runtime.exs` fills these from `SMOLQUERY_BURST_LAMBDA_FUNCTION`,
  `SMOLQUERY_BURST_LAMBDA_REGION`, `SMOLQUERY_BURST_BOOT_TIMEOUT_MS` and
  `SMOLQUERY_BURST_MAX_LIFETIME_MS` when `SMOLQUERY_BURST_BACKEND` is `lambda`.
  """

  @behaviour FLAME.Backend

  alias Smolquery.AwsCredentials

  require Logger

  defstruct function: nil,
            region: nil,
            boot_timeout: 120_000,
            max_lifetime_ms: 840_000,
            env: %{},
            parent_ref: nil,
            runner_node_base: nil,
            runner_node_name: nil,
            remote_terminator_pid: nil

  @type t :: %__MODULE__{}

  @valid_opts [:function, :region, :boot_timeout, :max_lifetime_ms, :env, :terminator_sup, :log]
  @invoke_path "/2015-03-31/functions/"
  @erl_options "-hidden -dist_listen false"

  @impl true
  def init(opts) do
    conf = Application.get_env(:smolquery, __MODULE__, [])

    provided =
      conf
      |> Keyword.merge(opts)
      |> Keyword.validate!(@valid_opts)
      |> Keyword.take([:function, :region, :boot_timeout, :max_lifetime_ms, :env])

    state = struct!(__MODULE__, provided)

    {:ok,
     %{
       state
       | parent_ref: make_ref(),
         runner_node_base: "smolquery-burst-" <> rand_id(12)
     }}
  end

  @impl true
  def remote_boot(%__MODULE__{parent_ref: parent_ref} = state) do
    for key <- [:function, :region], Map.get(state, key) == nil do
      raise ArgumentError, "missing :#{key} config for #{inspect(__MODULE__)}"
    end

    with {:ok, payload} <- payload(state),
         {invoke, invoke_ms} = timed(fn -> invoke(state, payload) end),
         :ok <- invoke do
      Logger.info("#{inspect(__MODULE__)} invoked #{state.function} in #{invoke_ms}ms")

      receive do
        {^parent_ref, {:remote_up, remote_terminator_pid}} ->
          {:ok, remote_terminator_pid,
           %{
             state
             | remote_terminator_pid: remote_terminator_pid,
               runner_node_name: node(remote_terminator_pid)
           }}
      after
        max(state.boot_timeout - invoke_ms, 0) ->
          Logger.error(
            "#{inspect(__MODULE__)} runner did not connect within #{state.boot_timeout}ms"
          )

          {:error, :boot_timeout}
      end
    end
  end

  @impl true
  def remote_spawn_monitor(%__MODULE__{runner_node_name: node} = _state, term) do
    case term do
      func when is_function(func, 0) ->
        {:ok, Node.spawn_monitor(node, func)}

      {mod, fun, args} when is_atom(mod) and is_atom(fun) and is_list(args) ->
        {:ok, Node.spawn_monitor(node, mod, fun, args)}

      other ->
        raise ArgumentError,
              "expected a null arity function or {mod, func, args}. Got: #{inspect(other)}"
    end
  end

  @impl true
  def system_shutdown, do: System.stop()

  @doc """
  The runner's assignment: the environment the bootstrap exports before it
  starts the release, as one `KEY='VALUE'` line per variable with the value
  single-quoted for `sh`, base64 encoded under `"env"` so the payload needs
  no JSON escaping on the shell side. `{:error, {:parent_unresolved, host}}`
  when this node's name resolves to no address, since a runner could never
  dial it.
  """
  @spec payload(t()) :: {:ok, %{String.t() => String.t()}} | {:error, term()}
  def payload(%__MODULE__{} = state) do
    with {:ok, inetrc} <- parent_inetrc() do
      encoded_parent =
        state.parent_ref
        |> FLAME.Parent.new(self(), __MODULE__, state.runner_node_base, "SMOLQUERY_BURST_HOST")
        |> FLAME.Parent.encode()

      env =
        state.env
        |> Map.merge(%{
          "FLAME_PARENT" => encoded_parent,
          "RELEASE_COOKIE" => Atom.to_string(Node.get_cookie()),
          "RELEASE_DISTRIBUTION" => "name",
          "RELEASE_NODE" => state.runner_node_base <> "@127.0.0.1",
          "SMOLQUERY_BURST_HOST" => "127.0.0.1",
          "SMOLQUERY_BURST_MAX_LIFETIME_MS" => Integer.to_string(state.max_lifetime_ms),
          "ELIXIR_ERL_OPTIONS" => @erl_options
        })
        |> Map.merge(inetrc)

      lines = Enum.map_join(env, "\n", fn {key, value} -> key <> "=" <> shell_quote(value) end)

      {:ok, %{"env" => Base.encode64(lines <> "\n")}}
    end
  end

  @doc """
  The `inetrc` a runner needs to dial this node, as `%{"SMOLQUERY_BURST_INETRC" => text}`:
  this node's host name mapped to `POD_IP` when set, else to the address it
  resolves to here. Empty when the host part of this node's name is already
  an address.
  """
  @spec parent_inetrc() :: {:ok, %{String.t() => String.t()}} | {:error, term()}
  def parent_inetrc do
    host = node() |> Atom.to_string() |> String.split("@") |> List.last()

    case :inet.parse_address(String.to_charlist(host)) do
      {:ok, _already_an_address} ->
        {:ok, %{}}

      {:error, :einval} ->
        with {:ok, address} <- parent_address(host) do
          entry = "{host, #{inspect(address)}, [#{inspect(host)}]}.\n{lookup, [file, native]}.\n"

          {:ok, %{"SMOLQUERY_BURST_INETRC" => entry}}
        end
    end
  end

  defp parent_address(host) do
    case System.get_env("POD_IP") do
      nil -> resolve(host)
      pod_ip -> pod_ip |> String.to_charlist() |> :inet.parse_address()
    end
  end

  defp resolve(host) do
    case :inet.getaddr(String.to_charlist(host), :inet) do
      {:ok, address} -> {:ok, address}
      {:error, _reason} -> {:error, {:parent_unresolved, host}}
    end
  end

  defp shell_quote(value), do: "'" <> String.replace(value, "'", "'\\''") <> "'"

  defp invoke(%__MODULE__{} = state, payload) do
    AwsCredentials.ensure_started()

    url =
      "https://lambda.#{state.region}.amazonaws.com" <>
        @invoke_path <> URI.encode(state.function) <> "/invocations"

    request =
      Req.new(
        url: url,
        headers: [{"x-amz-invocation-type", "Event"}, {"connection", "close"}],
        json: payload,
        aws_sigv4: [service: :lambda] ++ AwsCredentials.sigv4_options(state.region),
        retry: &stale_connection?/2,
        max_retries: 3,
        retry_delay: 0
      )

    case Req.post(request) do
      {:ok, %Req.Response{status: 202}} -> :ok
      {:ok, %Req.Response{status: status, body: body}} -> {:error, {:invoke, status, body}}
      {:error, reason} -> {:error, {:invoke, reason}}
    end
  end

  defp stale_connection?(_request, %Req.TransportError{reason: :closed}), do: true
  defp stale_connection?(_request, _response_or_error), do: false

  defp timed(fun) do
    {micro, result} = :timer.tc(fun)
    {result, div(micro, 1000)}
  end

  defp rand_id(len) do
    len
    |> :crypto.strong_rand_bytes()
    |> Base.encode32(case: :lower, padding: false)
    |> binary_part(0, len)
  end
end
