defmodule Smolquery.DiskSpaceTest do
  use ExUnit.Case, async: true

  alias Smolquery.DiskSpace

  @moduletag :tmp_dir

  test "answers the free bytes of an existing directory's filesystem", %{tmp_dir: dir} do
    assert {:ok, bytes} = DiskSpace.free_bytes(dir)
    assert bytes > 0
  end

  test "a path not created yet answers for its nearest existing ancestor", %{tmp_dir: dir} do
    assert DiskSpace.free_bytes(Path.join([dir, "spill", "leaf"])) |> elem(0) == :ok
  end
end
