defmodule Smolquery.Api.Supervisor do
  @moduledoc """
  Top-level subtree for the `:api` role.

  Started only on nodes whose roles include `:api` (see `Smolquery.Roles`).
  Resolving the runtime happens in `start_link/1`, so a node missing its API
  key fails the boot right here — fail closed — rather than starting a
  listener that would wave requests through.
  """

  use Supervisor

  alias Smolquery.Api.Router
  alias Smolquery.Api.Runtime

  @doc """
  Starts the API.

  Takes any `Smolquery.Api.Runtime` option; application config supplies
  whatever is not passed.
  """
  @spec start_link(keyword()) :: Supervisor.on_start()
  def start_link(opts \\ []) do
    runtime = Runtime.new(opts)

    Supervisor.start_link(__MODULE__, runtime, name: Runtime.supervisor(runtime.name))
  end

  @impl Supervisor
  def init(%Runtime{} = runtime) do
    Runtime.put(runtime)

    children = [
      Supervisor.child_spec(
        {Bandit,
         plug: {Router, runtime.name}, ip: runtime.ip, port: runtime.port, startup_log: false},
        id: Runtime.listener(runtime.name)
      )
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end
end
