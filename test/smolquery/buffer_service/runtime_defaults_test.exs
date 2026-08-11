defmodule Smolquery.BufferService.RuntimeDefaultsTest do
  use ExUnit.Case, async: false

  alias Smolquery.BufferService.Runtime

  @moduletag :tmp_dir

  test "an unconfigured node derives the write path from its own schedulers", %{tmp_dir: dir} do
    pinned = Application.fetch_env!(:smolquery, Smolquery.BufferService)

    Application.put_env(
      :smolquery,
      Smolquery.BufferService,
      Keyword.drop(pinned, [:write_pool_size, :encode_concurrency])
    )

    on_exit(fn -> Application.put_env(:smolquery, Smolquery.BufferService, pinned) end)

    name = :"runtime_defaults_#{:erlang.unique_integer([:positive])}"
    runtime = Runtime.new(name: name, dir: dir)

    assert runtime.write_pool_size == Runtime.default_write_pool_size()
    assert runtime.encode_concurrency == Runtime.default_encode_concurrency()
  end

  test "an unconfigured node adapts the group-commit wait at five siblings", %{tmp_dir: dir} do
    pinned = Application.fetch_env!(:smolquery, Smolquery.BufferService)

    Application.put_env(
      :smolquery,
      Smolquery.BufferService,
      Keyword.drop(pinned, [:commit_siblings, :flush_idle_interval_ms])
    )

    on_exit(fn -> Application.put_env(:smolquery, Smolquery.BufferService, pinned) end)

    name = :"runtime_defaults_#{:erlang.unique_integer([:positive])}"
    runtime = Runtime.new(name: name, dir: dir)

    assert runtime.commit_siblings == 5
    assert runtime.flush_idle_interval_ms == 5
  end
end
