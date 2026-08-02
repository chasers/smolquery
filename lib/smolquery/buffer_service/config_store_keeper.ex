defmodule Smolquery.BufferService.ConfigStoreKeeper do
  @moduledoc """
  The store plumbing shared by the keepers that poll a
  `Smolquery.Cluster.ConfigStore` row: `Smolquery.BufferService.RingEpoch`
  (scope `"buffer:<name>"`, T-92) and
  `Smolquery.BufferService.ExpectedNodes` (scope `"expected:<name>"`,
  T-109). Both resolve the same store the same way and lazily set it up
  with the same retry-until-reachable shape; the rows and what each keeper
  publishes stay their own.
  """

  @doc """
  Resolves `{module, start_opts}` for a keeper: the `:store` option, the
  instance's `:epoch_store` configuration, or
  `Smolquery.Cluster.ConfigStore.Postgres` on `Smolquery.Cluster`'s
  `:postgres` config — one store, whichever keeper asks.
  """
  @spec store_spec(keyword()) :: {module(), term()}
  def store_spec(opts) do
    configured =
      Application.get_env(:smolquery, Smolquery.BufferService, [])
      |> Keyword.get(:epoch_store)

    Keyword.get(opts, :store) || configured || postgres_store()
  end

  @doc """
  Runs the store's idempotent `setup/1` once per keeper lifetime, retrying
  on every call until it succeeds — a keeper booting before its store is
  reachable keeps asking.
  """
  @spec setup(%{
          :setup_done => boolean(),
          :store_impl => module(),
          :store => term(),
          any() => any()
        }) ::
          {:ok, map()} | {:error, term()}
  def setup(%{setup_done: true} = state), do: {:ok, state}

  def setup(state) do
    case state.store_impl.setup(state.store) do
      :ok -> {:ok, %{state | setup_done: true}}
      {:error, reason} -> {:error, reason}
    end
  end

  defp postgres_store do
    postgres =
      Application.get_env(:smolquery, Smolquery.Cluster, [])
      |> Keyword.get(:postgres) ||
        raise ArgumentError,
              "a config-store keeper needs an :epoch_store or Smolquery.Cluster :postgres config"

    {Smolquery.Cluster.ConfigStore.Postgres, postgres}
  end
end
