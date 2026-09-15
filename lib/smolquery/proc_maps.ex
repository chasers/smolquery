defmodule Smolquery.ProcMaps do
  @moduledoc """
  The process's resident set split by mapping class, read from `/proc`.

  `Smolquery.MemoryMetrics` publishes the total resident set and the cgroup's
  `anon`/`file`/`slab` split. Neither says *which* anonymous mappings hold the
  memory, and on a long-lived node that is the whole question: the allocator
  keeps pages the application has already freed, and the shape of the mapping
  they sit in names which allocator to reach for.

  A sandbox storage pod made the case. At 40 h it held 2547 MiB, of which
  1778 MiB sat in 41 arena-shaped mappings, while DuckDB's engine pools held
  2 MiB resident against 10,569 MiB of reserved address space. Capping glibc's
  arenas moved that memory into the main arena rather than releasing it —
  `[heap]` went 413 MiB at 10 h to 737 MiB at 36 h, which was the node's whole
  growth rate. None of it appears in any series the node exports; every
  reading came from a hand-run `smaps` inside the container and vanished with
  the shell.

  ## The classes, and why the sizes matter

  `[heap]` is glibc's main arena, grown through `brk` and returned only from
  its top, so fragmentation parks there and stays.

  `:anon_arena` is an anonymous mapping over `#{div(32 * 1024 * 1024, 1024 * 1024)} MiB`
  and at most `#{div(64 * 1024 * 1024, 1024 * 1024)} MiB`, glibc's
  `HEAP_MAX_SIZE` for a non-main arena. **They measure just under the round
  number** — 63.96 to 63.99 MiB on the pod above, because the arena's own
  header takes the first page — so a test for exactly 64 MiB finds none of
  them. These are the mappings that go fully resident and stay.

  `:anon_reserved` is an anonymous mapping larger than `HEAP_MAX_SIZE`. Those
  are reservations, not usage: the same pod mapped 10,165 MiB across nine of
  them and had 194 MiB resident. Counting them as memory is the mistake this
  split exists to prevent — a threshold of "64 MiB or more" captures exactly
  the reservations and misses every arena.

  `:anon` is every other anonymous mapping and `:file` is everything backed by
  a name.

  The split is by mapping shape rather than by guessed owner: the shape is a
  fact, and which allocator produced it is an inference the caller makes with
  the rest of what it knows.

  Linux only. `breakdown/1` answers `:none` wherever `/proc` does not carry
  the file, and the caller publishes nothing.
  """

  @arena_max_bytes 64 * 1024 * 1024
  @arena_min_bytes 32 * 1024 * 1024

  @typedoc """
  Resident bytes per mapping class, plus how many arena-shaped mappings the
  process holds.
  """
  @type breakdown :: %{
          heap: non_neg_integer(),
          anon_arena: non_neg_integer(),
          anon_reserved: non_neg_integer(),
          anon: non_neg_integer(),
          file: non_neg_integer(),
          anon_arena_count: non_neg_integer()
        }

  @empty %{
    heap: 0,
    anon_arena: 0,
    anon_reserved: 0,
    anon: 0,
    file: 0,
    anon_arena_count: 0
  }

  @doc """
  The resident set split by mapping class, or `:none` without `/proc`.
  """
  @spec breakdown(Path.t()) :: {:ok, breakdown()} | :none
  def breakdown(path \\ "/proc/self/smaps") do
    case File.read(path) do
      {:ok, contents} -> {:ok, contents |> String.split("\n") |> tally()}
      {:error, _reason} -> :none
    end
  end

  @doc """
  The largest an arena-shaped mapping can be — glibc's `HEAP_MAX_SIZE`.
  """
  @spec arena_max_bytes() :: pos_integer()
  def arena_max_bytes, do: @arena_max_bytes

  defp tally(lines) do
    {breakdown, _current} = Enum.reduce(lines, {@empty, nil}, &line/2)

    breakdown
  end

  defp line(line, {breakdown, current}) do
    case header(line) do
      {:ok, kind} -> {count(breakdown, kind), kind}
      :no -> {add_rss(breakdown, current, line), current}
    end
  end

  # A mapping header: "<lo>-<hi> <perms> <offset> <dev> <inode> [path]". The
  # path is absent for an anonymous mapping, which separates named from
  # anonymous; the size then separates the anonymous classes.
  defp header(line) do
    with [range, _perms, _offset, _dev, _inode | rest] <- String.split(line),
         [lo, hi] <- String.split(range, "-"),
         {low, ""} <- Integer.parse(lo, 16),
         {high, ""} <- Integer.parse(hi, 16) do
      {:ok, classify(rest, high - low)}
    else
      _other -> :no
    end
  end

  defp classify(["[heap]"], _size), do: :heap
  defp classify([], size) when size > @arena_max_bytes, do: :anon_reserved
  defp classify([], size) when size > @arena_min_bytes, do: :anon_arena
  defp classify([], _size), do: :anon
  defp classify(_named, _size), do: :file

  defp count(breakdown, :anon_arena), do: Map.update!(breakdown, :anon_arena_count, &(&1 + 1))
  defp count(breakdown, _kind), do: breakdown

  defp add_rss(breakdown, nil, _line), do: breakdown

  defp add_rss(breakdown, kind, line) do
    case String.split(line) do
      ["Rss:", kib, "kB"] -> Map.update!(breakdown, kind, &(&1 + String.to_integer(kib) * 1024))
      _other -> breakdown
    end
  end
end
