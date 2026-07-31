defmodule Smolquery.QueryService.Supervisor do
  @moduledoc """
  Top-level subtree for the `:query` role.

  Started only on nodes whose roles include `:query` (see `Smolquery.Roles`).
  Holds the read engine today; the job registry, planner, and HTTP surface join
  it as later milestones land.
  """

  use Supervisor

  alias Smolquery.Engine

  @spec start_link(keyword()) :: Supervisor.on_start()
  def start_link(opts \\ []) do
    Supervisor.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(_opts) do
    children = [{Engine, name: Engine}]

    Supervisor.init(children, strategy: :one_for_one)
  end
end
