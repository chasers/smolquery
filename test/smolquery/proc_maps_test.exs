defmodule Smolquery.ProcMapsTest do
  use ExUnit.Case, async: true

  alias Smolquery.ProcMaps

  @moduletag :tmp_dir

  @mib 1_048_576

  defp mapping(size_bytes, rss_bytes, name \\ "") do
    """
    7f0000000000-#{Integer.to_string(0x7F0000000000 + size_bytes, 16)} rw-p 00000000 00:00 0 #{name}
    Size:           #{div(size_bytes, 1024)} kB
    Rss:            #{div(rss_bytes, 1024)} kB
    Private_Dirty:  #{div(rss_bytes, 1024)} kB
    VmFlags: rd wr mr mw me ac sd
    """
  end

  defp smaps(dir, mappings) do
    path = Path.join(dir, "smaps")
    File.write!(path, Enum.join(mappings))

    path
  end

  describe "breakdown/1" do
    test "splits the resident set by mapping class", %{tmp_dir: dir} do
      path =
        smaps(dir, [
          mapping(200 * @mib, 180 * @mib, "[heap]"),
          mapping(64 * @mib - 4096, 64 * @mib - 4096),
          mapping(64 * @mib - 8192, 64 * @mib - 8192),
          mapping(2048 * @mib, 2 * @mib),
          mapping(4 * @mib, 3 * @mib),
          mapping(10 * @mib, 8 * @mib, "/usr/lib/libfoo.so")
        ])

      assert {:ok, breakdown} = ProcMaps.breakdown(path)
      assert breakdown.heap == 180 * @mib
      assert breakdown.anon_arena == 128 * @mib - 12_288
      assert breakdown.anon_reserved == 2 * @mib
      assert breakdown.anon == 3 * @mib
      assert breakdown.file == 8 * @mib
      assert breakdown.anon_arena_count == 2
    end

    test "an arena sits just under HEAP_MAX_SIZE, a reservation above it", %{tmp_dir: dir} do
      max = ProcMaps.arena_max_bytes()

      arena = smaps(dir, [mapping(max - 4096, 10 * @mib)])
      assert {:ok, %{anon_arena: arena_rss, anon_arena_count: 1}} = ProcMaps.breakdown(arena)
      assert arena_rss == 10 * @mib

      exact = smaps(Path.join(dir, "exact") |> tap(&File.mkdir_p!/1), [mapping(max, 10 * @mib)])
      assert {:ok, %{anon_arena: exact_rss, anon_arena_count: 1}} = ProcMaps.breakdown(exact)
      assert exact_rss == 10 * @mib

      over =
        smaps(Path.join(dir, "over") |> tap(&File.mkdir_p!/1), [mapping(max + 4096, 9 * @mib)])

      assert {:ok, %{anon_reserved: 9_437_184, anon_arena_count: 0}} = ProcMaps.breakdown(over)
    end

    test "a mapping with no Rss line contributes nothing", %{tmp_dir: dir} do
      path = Path.join(dir, "smaps")
      File.write!(path, "7f0000000000-7f0004000000 rw-p 00000000 00:00 0 \nSize: 65536 kB\n")

      assert {:ok, breakdown} = ProcMaps.breakdown(path)
      assert breakdown.anon_arena == 0
      assert breakdown.anon_arena_count == 1
    end

    test "detail lines are never mistaken for mapping headers", %{tmp_dir: dir} do
      path = smaps(dir, [mapping(4 * @mib, 3 * @mib)])

      assert {:ok, %{anon: 3_145_728, file: 0, heap: 0}} = ProcMaps.breakdown(path)
    end

    test "answers :none without the file", %{tmp_dir: dir} do
      assert ProcMaps.breakdown(Path.join(dir, "absent")) == :none
    end
  end
end
