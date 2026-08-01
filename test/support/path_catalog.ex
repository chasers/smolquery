defmodule Smolquery.Test.PathCatalog do
  @moduledoc """
  A `Smolquery.Catalog` that holds nothing but a set of paths, in an `Agent`.

  Garbage collection asks the catalog one question — has any snapshot ever
  referenced this path — and answering it for real needs DuckLake, a metadata
  database, and an extension download. This models the answer directly, so the
  sweep's own logic (grace periods, candidate tracking, what counts as garbage) is
  tested without any of that.

  It distinguishes the two lists the sweep must not confuse: `known_segments/1`
  spans every snapshot, `segments/3` only the current one. `drop_from_current/2`
  makes a path present in the first and absent from the second — the time-travel
  case that must survive a sweep.
  """

  @behaviour Smolquery.Catalog

  alias Smolquery.Catalog
  alias Smolquery.Schema

  @doc """
  A catalog handle over `agent`, which holds `{known, current, failure}`.
  """
  @spec new(pid()) :: Catalog.t()
  def new(agent), do: %Catalog{impl: __MODULE__, config: agent}

  @doc """
  Records `path` as registered — known to every snapshot, and to the current one.
  """
  @spec register(pid(), String.t()) :: :ok
  def register(agent, path), do: Agent.update(agent, &[{path, :current} | drop(&1, path)])

  @doc """
  Drops `path` from the current snapshot while leaving it known to older ones.
  """
  @spec drop_from_current(pid(), String.t()) :: :ok
  def drop_from_current(agent, path),
    do: Agent.update(agent, &[{path, :historical} | drop(&1, path)])

  @doc """
  Makes every read fail with `reason`.
  """
  @spec fail(pid(), term()) :: :ok
  def fail(agent, reason), do: Agent.update(agent, &[{:failure, reason} | &1])

  @doc """
  Registers `path` as a side effect of the next `known_segments/1` read, which
  itself still answers without it.

  This is how a test lands a commit *between* a sweep's decision snapshot and its
  deletes: the sweep's first read triggers the registration, and only a re-read
  sees it.
  """
  @spec register_on_next_read(pid(), String.t()) :: :ok
  def register_on_next_read(agent, path),
    do: Agent.update(agent, &[{:register_on_read, path} | &1])

  @impl Catalog
  def known_segments(agent) do
    Agent.get_and_update(agent, fn entries ->
      case failure(entries) do
        {:ok, reason} -> {{:error, reason}, entries}
        :error -> {{:ok, for({path, _state} <- paths(entries), do: path)}, absorb(entries)}
      end
    end)
  end

  @impl Catalog
  def segments(agent, _table, _snapshot) do
    Agent.get(agent, fn entries ->
      case failure(entries) do
        {:ok, reason} -> {:error, reason}
        :error -> {:ok, for({path, :current} <- paths(entries), do: path)}
      end
    end)
  end

  @impl Catalog
  def registered_through(agent, _table, _snapshot) do
    Agent.get(agent, fn entries ->
      case failure(entries) do
        {:ok, reason} -> {:error, reason}
        :error -> {:ok, for({path, _state} <- paths(entries), do: path)}
      end
    end)
  end

  @impl Catalog
  def register_segments(agent, _table, segments) do
    Enum.each(segments, &register(agent, &1.path))

    {:ok, 1}
  end

  @impl Catalog
  def drop_segments(agent, _table, paths) do
    Enum.each(paths, &drop_from_current(agent, &1))

    {:ok, 2}
  end

  @impl Catalog
  def replace_segments(agent, table, segments, paths) do
    {:ok, _registered} = register_segments(agent, table, segments)

    drop_segments(agent, table, paths)
  end

  @impl Catalog
  def put_retention(_agent, _table, _policy), do: :ok

  @impl Catalog
  def retention(_agent, _table), do: {:ok, nil}

  @impl Catalog
  def expire_snapshots(_agent, _older_than_ms), do: {:ok, 0}

  @impl Catalog
  def current_snapshot(_agent), do: {:ok, 1}

  @impl Catalog
  def create_dataset(_agent, _dataset), do: :ok

  @impl Catalog
  def list_datasets(_agent), do: {:ok, ["analytics"]}

  @impl Catalog
  def create_table(_agent, _table, _schema), do: :ok

  @impl Catalog
  def list_tables(_agent, _dataset), do: {:ok, ["events"]}

  @impl Catalog
  def table_schema(_agent, _table), do: {:ok, Schema.new!([{"id", :int64}])}

  defp paths(entries), do: for({path, state} <- entries, is_binary(path), do: {path, state})

  defp absorb(entries) do
    case for({:register_on_read, path} <- entries, do: path) do
      [] ->
        entries

      pending ->
        entries
        |> Enum.reject(&match?({:register_on_read, _path}, &1))
        |> then(
          &Enum.reduce(pending, &1, fn path, acc -> [{path, :current} | drop(acc, path)] end)
        )
    end
  end

  defp failure(entries) do
    case for({:failure, reason} <- entries, do: reason) do
      [reason | _rest] -> {:ok, reason}
      [] -> :error
    end
  end

  defp drop(entries, path), do: Enum.reject(entries, &match?({^path, _state}, &1))
end
