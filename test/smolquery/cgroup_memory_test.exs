defmodule Smolquery.CgroupMemoryTest do
  use ExUnit.Case, async: true

  alias Smolquery.CgroupMemory

  @moduletag :tmp_dir

  defp write_v2(root, contents), do: File.write!(Path.join(root, "memory.max"), contents)

  defp write_v1(root, contents) do
    dir = Path.join(root, "memory")
    File.mkdir_p!(dir)
    File.write!(Path.join(dir, "memory.limit_in_bytes"), contents)
  end

  test "reads a cgroup v2 limit", %{tmp_dir: root} do
    write_v2(root, "4294967296\n")

    assert CgroupMemory.limit_bytes(root) == {:ok, 4_294_967_296}
  end

  test "a v2 limit of max means none", %{tmp_dir: root} do
    write_v2(root, "max\n")

    assert CgroupMemory.limit_bytes(root) == :none
  end

  test "falls back to a cgroup v1 limit", %{tmp_dir: root} do
    write_v1(root, "2147483648\n")

    assert CgroupMemory.limit_bytes(root) == {:ok, 2_147_483_648}
  end

  test "v2 wins when both are present", %{tmp_dir: root} do
    write_v2(root, "4294967296\n")
    write_v1(root, "2147483648\n")

    assert CgroupMemory.limit_bytes(root) == {:ok, 4_294_967_296}
  end

  test "the v1 everything-fits sentinel means none", %{tmp_dir: root} do
    write_v1(root, "9223372036854771712\n")

    assert CgroupMemory.limit_bytes(root) == :none
  end

  test "a host without a cgroup filesystem has none", %{tmp_dir: root} do
    assert CgroupMemory.limit_bytes(root) == :none
  end

  test "unreadable contents mean none", %{tmp_dir: root} do
    write_v2(root, "not a number\n")

    assert CgroupMemory.limit_bytes(root) == :none
  end
end
