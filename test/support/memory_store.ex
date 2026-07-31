defmodule Smolquery.Test.MemoryStore do
  @moduledoc """
  `Smolquery.Segments.Store` holding segments in an Agent.

  Exists to keep `Smolquery.Segments.Store` an honest abstraction rather than a
  description of the filesystem. Every rule the hot manifest relies on — a put is
  durable when it returns, an unlogged object is an orphan, a deleted key is gone,
  a location may mean nothing to another node — is stated against the behaviour,
  and a second implementation is the only way to find out whether that is true.

  Locations are `memory://<key>`, which nothing can open. That is the point: a
  test that accidentally reaches past the store to a real file fails here.
  """

  @behaviour Smolquery.Segments.Store

  alias Smolquery.Segments.Store

  @doc """
  Starts a store, linked to the calling process.
  """
  @spec new() :: Store.t()
  def new do
    {:ok, agent} = Agent.start_link(fn -> %{} end)

    %Store{impl: __MODULE__, config: agent}
  end

  @impl Store
  def put(agent, key, encoder) do
    staged = Path.join(System.tmp_dir!(), "memory-store-#{:erlang.unique_integer([:positive])}")

    try do
      with :ok <- encoder.(staged),
           {:ok, contents} <- File.read(staged) do
        Agent.update(agent, &Map.put(&1, key, contents))

        {:ok, %{location: location(agent, key), byte_size: byte_size(contents)}}
      else
        {:error, reason} -> {:error, {:put_failed, key, reason}}
      end
    after
      File.rm(staged)
    end
  end

  @impl Store
  def location(_agent, key), do: "memory://" <> key

  @impl Store
  def list(agent, prefix) do
    keys =
      agent
      |> Agent.get(&Map.keys(&1))
      |> Enum.filter(&under?(&1, prefix))
      |> Enum.sort()

    {:ok, keys}
  end

  @impl Store
  def delete(agent, key) do
    Agent.update(agent, &Map.delete(&1, key))
  end

  @impl Store
  def shared?(_agent), do: true

  @doc """
  The bytes stored at `key`, for a test that wants to check what was written.
  """
  @spec fetch(Store.t(), Store.key()) :: {:ok, binary()} | :error
  def fetch(%Store{config: agent}, key), do: Agent.get(agent, &Map.fetch(&1, key))

  defp under?(_key, ""), do: true
  defp under?(key, prefix), do: String.starts_with?(key, prefix <> "/")
end
