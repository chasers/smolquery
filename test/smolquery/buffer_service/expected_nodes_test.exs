defmodule Smolquery.BufferService.ExpectedNodesTest do
  use ExUnit.Case, async: true

  alias Smolquery.BufferService.ExpectedNodes
  alias Smolquery.BufferService.Routing
  alias Smolquery.Cluster.ConfigStore.Memory

  @joiner :"buffer2@nonexistent.invalid"
  @third :"buffer3@nonexistent.invalid"

  defp unique_name(prefix), do: :"#{prefix}_#{:erlang.unique_integer([:positive])}"

  defp start_store do
    name = unique_name(:expected_store)
    start_supervised!({Memory, name: name}, id: name)

    name
  end

  defp start_keeper(name, store, static) do
    spec = {ExpectedNodes, name: name, store: {Memory, name: store}, static: static}

    start_supervised!(spec, id: {:expected, name})
    ExpectedNodes.refresh(name)

    name
  end

  defp scope(name), do: "expected:#{name}"

  test "a node with no keeper answers the static configuration" do
    assert ExpectedNodes.list(unique_name(:never_started)) == []
  end

  test "the first refresh seeds the row from the static configuration" do
    name = unique_name(:expected)
    store = start_store()
    start_keeper(name, store, [node(), @joiner])

    assert ExpectedNodes.list(name) == Enum.sort([node(), @joiner])

    assert ExpectedNodes.current(name) ==
             {:ok, %{epoch: 0, members: Enum.sort([node(), @joiner])}}

    assert {:ok, %{epoch: 0}} = Memory.fetch(store, scope(name))
  end

  test "an existing row wins over this node's static configuration" do
    name = unique_name(:expected)
    store = start_store()
    {:ok, _config} = Memory.ensure(store, scope(name), [@third])

    start_keeper(name, store, [node()])

    assert ExpectedNodes.list(name) == [@third]
  end

  test "resize is compare-and-swap: the named epoch wins, a stale one conflicts" do
    name = unique_name(:expected)
    store = start_store()
    start_keeper(name, store, [node()])

    assert {:ok, %{epoch: 1, members: members}} = ExpectedNodes.resize(name, 0, [node(), @joiner])
    assert members == Enum.sort([node(), @joiner])
    assert ExpectedNodes.list(name) == Enum.sort([node(), @joiner])

    assert ExpectedNodes.resize(name, 0, [node()]) == {:error, :conflict}
    assert ExpectedNodes.list(name) == Enum.sort([node(), @joiner])
  end

  test "another node's resize is visible after one refresh, stale until then" do
    name = unique_name(:expected)
    store = start_store()
    start_keeper(name, store, [node(), @joiner])

    {:ok, _config} = Memory.advance(store, scope(name), 0, [node()])

    assert ExpectedNodes.list(name) == Enum.sort([node(), @joiner])

    ExpectedNodes.refresh(name)

    assert ExpectedNodes.list(name) == [node()]
  end

  test "a second keeper for the same instance is ignored, not a clash" do
    name = unique_name(:expected)
    store = start_store()
    start_keeper(name, store, [node()])

    assert ExpectedNodes.start_link(name: name, store: {Memory, name: store}, static: []) ==
             :ignore
  end

  test "manifest_nodes reflects a resize without any configuration change" do
    name = unique_name(:expected)
    store = start_store()
    start_keeper(name, store, [node()])

    refute @joiner in Routing.manifest_nodes(name)

    assert {:ok, _config} = ExpectedNodes.resize(name, 0, [node(), @joiner])
    assert @joiner in Routing.manifest_nodes(name)

    assert {:ok, _config} = ExpectedNodes.resize(name, 1, [node()])
    refute @joiner in Routing.manifest_nodes(name)
  end
end
