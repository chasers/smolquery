defmodule Smolquery.Cluster.ConfigStore.Memory do
  @moduledoc """
  An in-memory `Smolquery.Cluster.ConfigStore`, for tests.

  One Agent plays the role Postgres plays in a deployment: several
  `Smolquery.BufferService.RingEpoch` instances started against the same
  store pid behave like several nodes sharing one configuration database,
  which is how a single-BEAM test exercises the compare-and-swap and
  settling paths.

  `age_ms` comes from the Agent's own monotonic clock — the same
  "store time, not node time" contract the Postgres implementation keeps.
  """

  @behaviour Smolquery.Cluster.ConfigStore

  use Agent

  @impl Smolquery.Cluster.ConfigStore
  def start_link(opts) do
    case Agent.start_link(fn -> %{} end, Keyword.take(opts, [:name])) do
      {:ok, pid} -> {:ok, pid}
      {:error, {:already_started, pid}} -> {:ok, pid}
    end
  end

  @impl Smolquery.Cluster.ConfigStore
  def setup(_store), do: :ok

  @impl Smolquery.Cluster.ConfigStore
  def fetch(store, scope) do
    Agent.get(store, fn state ->
      case Map.fetch(state, scope) do
        {:ok, row} -> {:ok, config(row)}
        :error -> :not_found
      end
    end)
  end

  @impl Smolquery.Cluster.ConfigStore
  def ensure(store, scope, members) do
    Agent.get_and_update(store, fn state ->
      row =
        Map.get(state, scope, %{
          epoch: 0,
          members: Enum.sort(members),
          prev_members: nil,
          changed_at: now()
        })

      {{:ok, config(row)}, Map.put(state, scope, row)}
    end)
  end

  @impl Smolquery.Cluster.ConfigStore
  def advance(store, scope, expected_epoch, members) do
    Agent.get_and_update(store, fn state ->
      case Map.fetch(state, scope) do
        {:ok, %{epoch: ^expected_epoch} = row} ->
          advanced = %{
            epoch: row.epoch + 1,
            members: Enum.sort(members),
            prev_members: row.members,
            changed_at: now()
          }

          {{:ok, config(advanced)}, Map.put(state, scope, advanced)}

        {:ok, _stale} ->
          {{:error, :conflict}, state}

        :error ->
          {{:error, :conflict}, state}
      end
    end)
  end

  @doc """
  Backdates `scope`'s last change, so a test can age a configuration past a
  lease without sleeping through one.
  """
  @spec age(Agent.agent(), String.t(), non_neg_integer()) :: :ok
  def age(store, scope, age_ms) do
    Agent.update(store, fn state ->
      Map.update!(state, scope, &%{&1 | changed_at: now() - age_ms})
    end)
  end

  defp config(row) do
    %{
      epoch: row.epoch,
      members: row.members,
      prev_members: row.prev_members,
      age_ms: max(now() - row.changed_at, 0)
    }
  end

  defp now, do: System.monotonic_time(:millisecond)
end
