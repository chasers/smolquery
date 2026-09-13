defmodule Smolquery.QueryService.Lockdown do
  @moduledoc """
  The statements that confine an engine about to run SQL derived from a
  user's — a job engine (`Smolquery.QueryService.Runner`) or a partial
  worker (`Smolquery.QueryService.PartialWorker`) — in the one order that
  works.

  The lists came first: `allowed_directories` and `allowed_paths` say what
  the engine may still read once external access closes. External access
  then closes, unless the caller keeps it (a partial worker's `COPY ... TO`
  needs it, and the two lists bound what it can reach). Then `allowed_configs`
  names the options DuckLake sets on its metadata connection at every
  transaction (`Smolquery.Catalog.DuckLake.allowed_configs_statement/0`),
  and last `lock_configuration` seals everything, including that list. The
  order is the whole point: a `SET` after the lock is refused whatever its
  value, so a lockdown that locked before exempting DuckLake's options would
  fail every read of the lake with "Current transaction is aborted".
  """

  alias Smolquery.Catalog.DuckLake
  alias Smolquery.Identifier

  @doc """
  The lockdown statements for an engine allowed `directories` and `paths`.

  `external_access: true` keeps `enable_external_access` on; the default
  turns it off.
  """
  @spec statements([String.t()], [String.t()], external_access: boolean()) :: [String.t()]
  def statements(directories, paths, opts \\ []) do
    [
      "SET allowed_directories = #{Identifier.sql_list(directories)}",
      "SET allowed_paths = #{Identifier.sql_list(paths)}"
    ] ++
      external_access(Keyword.get(opts, :external_access, false)) ++
      [DuckLake.allowed_configs_statement(), "SET lock_configuration = true"]
  end

  defp external_access(true), do: []
  defp external_access(false), do: ["SET enable_external_access = false"]
end
