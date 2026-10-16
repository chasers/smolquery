defmodule Smolquery.Burst.LambdaBackend do
  @moduledoc """
  A `FLAME.Backend` whose runner is one AWS Lambda invocation (PL-77 L4).

  `remote_boot/1` invokes the function asynchronously with the runner's
  assignment in the payload: the encoded `FLAME.Parent`, the cluster cookie,
  the runner's node name, and the VM flags. Lambda's own environment
  variables are fixed per function version and capped at 4 KB, so they
  cannot carry a per-boot parent; the function's bootstrap
  (`rel/lambda/bootstrap`) exports the payload into its environment and runs
  the release in the foreground until the BEAM exits. The runner's
  `FLAME.Terminator` then connects back to this node over distribution and
  announces itself; the boot blocks on that announcement for `boot_timeout`.

  The runner starts with `-dist_listen false`: it opens no distribution
  listener, registers with no epmd, and is hidden, which is the shape of
  compute nothing can connect to. Its node name's host part is a label and
  nothing resolves it. This node must be reachable from the function on
  epmd (4369) and on its pinned distribution port.

  The invoke closes its connection: the Lambda API drops an idle keep-alive
  after a few minutes, and a pooled connection that died that way fails the
  next invoke with `:closed` before the request is sent. Measured on the
  spike: 50 such connections from a 100-runner burst cost 52 s of one-second
  retries on the first lease after a quiet quarter hour. A `:closed` is the
  one transport error retried, since nothing was sent on it; any other
  failure is returned, never retried, because a retried `Event` invoke is a
  second runner.

  The invoke is signed with the credential chain
  (`Smolquery.AwsCredentials`), so the node that leases runners needs
  `lambda:InvokeFunction` on the function through its pod identity, and
  nothing else of AWS.

  ## Configuration

      config :smolquery, Smolquery.Burst.LambdaBackend,
        function: "smolquery-burst-runner",
        region: "eu-central-1",
        boot_timeout: 120_000,
        env: %{}

  `env` is extra environment for the runner, merged under the assignment.
  """

  @behaviour FLAME.Backend

  alias Smolquery.AwsCredentials

  require Logger

  defstruct function: nil,
            region: nil,
            boot_timeout: 120_000,
            env: %{},
            parent_ref: nil,
            runner_node_base: nil,
            runner_node_name: nil,
            remote_terminator_pid: nil

  @type t :: %__MODULE__{}

  @valid_opts [:function, :region, :boot_timeout, :env, :terminator_sup, :log]
  @invoke_path "/2015-03-31/functions/"

  @impl true
  def init(opts) do
    conf = Application.get_env(:smolquery, __MODULE__, [])

    provided =
      conf
      |> Keyword.merge(opts)
      |> Keyword.validate!(@valid_opts)
      |> Keyword.take([:function, :region, :boot_timeout, :env])

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

    {invoke, invoke_ms} = timed(fn -> invoke(state, payload(state)) end)
    Logger.info("#{inspect(__MODULE__)} invoked #{state.function} in #{invoke_ms}ms")

    with :ok <- invoke do
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
  no JSON escaping on the shell side.
  """
  @spec payload(t()) :: %{String.t() => String.t()}
  def payload(%__MODULE__{} = state) do
    encoded_parent =
      state.parent_ref
      |> FLAME.Parent.new(self(), __MODULE__, state.runner_node_base, "SMOLQUERY_BURST_HOST")
      |> FLAME.Parent.encode()

    env =
      Map.merge(state.env, %{
        "FLAME_PARENT" => encoded_parent,
        "RELEASE_COOKIE" => Atom.to_string(Node.get_cookie()),
        "RELEASE_DISTRIBUTION" => "name",
        "RELEASE_NODE" => state.runner_node_base <> "@127.0.0.1",
        "SMOLQUERY_BURST_HOST" => "127.0.0.1",
        "ELIXIR_ERL_OPTIONS" => "-dist_listen false"
      })

    lines = Enum.map_join(env, "\n", fn {key, value} -> key <> "=" <> shell_quote(value) end)

    %{"env" => Base.encode64(lines <> "\n")}
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
