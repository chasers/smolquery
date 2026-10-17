defmodule Smolquery.Roles do
  @moduledoc """
  Which service subtrees this node runs.

  A role maps one-to-one onto a top-level supervision subtree — the four
  services, plus `:api` for the HTTP front door, `:web` for the LiveView
  UI, `:pg` for the Postgres wire edge, `:clickhouse` for the ClickHouse
  HTTP edge, and `:victoriametrics` for the VictoriaMetrics edge, which takes
  Prometheus remote write (PL-70). A node starts only the subtrees in its
  role set, so the same release deploys as a single-node dev
  instance (every role) or as a fleet of specialized nodes.

  Roles come from `config :smolquery, :roles`, which `config/runtime.exs`
  populates from the `SMOLQUERY_ROLES` environment variable — a comma-separated
  list of role names, `all`, or `none`:

      SMOLQUERY_ROLES=all
      SMOLQUERY_ROLES=ingest,query
      SMOLQUERY_ROLES=buffer
      SMOLQUERY_ROLES=none

  With the variable unset, a node runs every role. `none` runs no service
  subtree, for a node whose job is to lease burst runners (`Smolquery.Burst`).
  A burst runner itself has no roles whatever the variable says: it is not a
  node, and a cluster page that asks it over RPC must not see every service.
  """

  @type t ::
          :api
          | :ingest
          | :buffer
          | :storage
          | :query
          | :web
          | :pg
          | :clickhouse
          | :victoriametrics

  @all [:api, :ingest, :buffer, :storage, :query, :web, :pg, :clickhouse, :victoriametrics]
  @names Map.new(@all, &{Atom.to_string(&1), &1})

  @doc """
  Every known role, in pipeline order.
  """
  @spec all() :: [t()]
  def all, do: @all

  @doc """
  The roles enabled for this node; none on a burst runner.
  """
  @spec enabled() :: [t()]
  def enabled do
    if Smolquery.Burst.runner?(), do: [], else: configured()
  end

  defp configured do
    case Application.fetch_env(:smolquery, :roles) do
      {:ok, roles} -> roles
      :error -> @all
    end
  end

  @doc """
  Whether `role` is enabled on this node.
  """
  @spec enabled?(t()) :: boolean()
  def enabled?(role), do: role in enabled()

  @doc """
  Parses a `SMOLQUERY_ROLES` value into a role list.

  Accepts `all`, `none`, or a comma-separated list of role names. Unknown
  names are an error rather than a silent no-op, so a typo fails the boot
  instead of quietly starting nothing. `none` starts no service subtree at
  all: the node that leases burst runners (`Smolquery.Burst`) runs that way.
  """
  @spec parse(String.t()) :: {:ok, [t()]} | {:error, [String.t()]}
  def parse(value) when is_binary(value) do
    names =
      value
      |> String.split(",", trim: true)
      |> Enum.map(&String.trim/1)
      |> Enum.reject(&(&1 == ""))

    case names do
      ["all"] -> {:ok, @all}
      ["none"] -> {:ok, []}
      names -> resolve(names)
    end
  end

  @doc """
  Like `parse/1`, but raises on unknown role names.
  """
  @spec parse!(String.t()) :: [t()]
  def parse!(value) do
    case parse(value) do
      {:ok, roles} ->
        roles

      {:error, unknown} ->
        raise ArgumentError,
              IO.iodata_to_binary([
                "unknown SMOLQUERY_ROLES value(s): ",
                Enum.intersperse(unknown, ", "),
                ~s(. expected "all", "none", or a comma-separated subset of: ),
                @all |> Enum.map(&Atom.to_string/1) |> Enum.intersperse(", ")
              ])
    end
  end

  defp resolve(names) do
    {known, unknown} = Enum.split_with(names, &Map.has_key?(@names, &1))

    case unknown do
      [] -> {:ok, Enum.filter(@all, &(Atom.to_string(&1) in known))}
      unknown -> {:error, unknown}
    end
  end
end
