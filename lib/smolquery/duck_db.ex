defmodule Smolquery.DuckDB do
  @moduledoc """
  The supervised DuckDB database process used by every database in smolquery.
  """

  @version Application.compile_env!(:smolquery, :duckdb_driver_version)

  @spec version() :: String.t()
  def version, do: @version

  defp database_options, do: [driver: :duckdb, version: @version]

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    Adbc.Database.start_link(database_options() ++ Keyword.drop(opts, [:driver, :version]))
  end

  @spec child_spec(keyword()) :: Supervisor.child_spec()
  def child_spec(opts) do
    %{id: __MODULE__, start: {__MODULE__, :start_link, [opts]}}
  end
end
