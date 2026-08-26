defmodule Smolquery.StorageService.Handoff.ReleasedClaimDoubleCountTest do
  @moduledoc """
  Regression test for TLA+ finding F-1 (`tla/FINDINGS.md`,
  `tla/ReleasedClaim.tla`).

  The T-294 fence must stop a *released* oversized claim's in-flight seal attempt
  from leaving its segment registered. The bug: `claim_live` and the retire
  key-fence skipped entries whose `sealed_at` was already set, treating them as
  reconciliation. So if the re-derived valve-sized claims sealed the released
  claim's micro-segments *before* the in-flight original attempt reached its
  `retire`, the original saw no stale entry and `retire` short-circuited on "no
  unsealed ids" → `:ok`, so `compensate_stale` never ran. The original's
  oversized segment was stranded and double-counted the rows against the
  re-derived segments forever.

  The fix (`claim_live` in `seal.ex`, `retire` in `hot_manifest.ex`) treats a
  `sealed_at` stamped under a *different* claim key as stale rather than
  reconciliation, so the original attempt's `retire` refuses with `stale_claim`
  and `compensate_stale` drops the orphan.

  This test drives the *real* concurrent seal flow and uses snabbkaffe
  `force_ordering` to make the race deterministic: the original attempt is parked
  right before its `retire` until the two re-derived seals have retired. It then
  asserts, through the same public surface the planner dedups on
  (`visible_ids/1`), that every row is counted exactly once — the orphan was
  compensated away.

  Against the pre-fix code this test FAILS: the original seal returns `:ok`, the
  catalog keeps 3 segments, and `visible_ids/1` reads every row twice.
  """

  use ExUnit.Case, async: false
  use Snabbkaffex

  alias Smolquery.BufferService
  alias Smolquery.BufferService.Client
  alias Smolquery.BufferService.HotManifest
  alias Smolquery.BufferService.HotServer
  alias Smolquery.Catalog
  alias Smolquery.Catalog.DuckLake
  alias Smolquery.Engine
  alias Smolquery.Schema
  alias Smolquery.Segments.Store
  alias Smolquery.StorageService.Handoff
  alias Smolquery.StorageService.Runtime

  @moduletag :integration
  @moduletag :tmp_dir

  @table {"analytics", "events"}

  @orig_ulid "01KYWPEEGAM8FQVQS5S2QF26S0"
  @re1_ulid "01KYWPEEGAM8FQVQS5S2QF26S1"
  @re2_ulid "01KYWPEEGAM8FQVQS5S2QF26S2"

  defp schema, do: Schema.new!([{"id", :int64}])
  defp batch(range), do: %{schema: schema(), rows: for(i <- range, do: %{"id" => i})}

  setup context do
    unique = :erlang.unique_integer([:positive])
    buffer = :"rc_buffer_#{unique}"
    storage = :"rc_storage_#{unique}"

    start_supervised!(
      {BufferService.Supervisor,
       name: buffer,
       dir: Path.join(context.tmp_dir, "buffer"),
       flush_interval_ms: 25,
       seal_max_files: 1_000_000,
       seal_max_bytes: 1_000_000_000,
       seal_max_age_ms: 600_000,
       retire_grace_ms: 600_000},
      id: buffer
    )

    on_exit(fn -> BufferService.Runtime.delete(buffer) end)

    start_supervised!(
      {DuckLake,
       name: Runtime.catalog_engine(storage),
       metadata: "sqlite:#{Path.join(context.tmp_dir, "catalog.sqlite")}",
       data_path: Path.join(context.tmp_dir, "ducklake")},
      id: Runtime.catalog_engine(storage)
    )

    catalog = DuckLake.new(engine: Runtime.catalog_engine(storage))
    :ok = Catalog.create_dataset(catalog, "analytics")
    :ok = Catalog.create_table(catalog, @table, schema())

    runtime =
      Runtime.new(
        name: storage,
        dir: Path.join(context.tmp_dir, "sealed"),
        buffer_name: buffer,
        buffer_base_url: HotServer.base_url(buffer),
        catalog: catalog
      )

    start_supervised!(
      {Engine,
       name: Runtime.engine(storage),
       extensions: [:httpfs],
       statements: [Smolquery.InternalSecret.create_secret_statement("http://")]}
    )

    {:ok, buffer_runtime} = BufferService.Runtime.fetch(buffer)

    %{buffer: buffer, buffer_runtime: buffer_runtime, runtime: runtime, catalog: catalog}
  end

  defp write(buffer, range) do
    {:ok, ack} = Client.write_batch(buffer, @table, batch(range))
    ack.segment_id
  end

  defp freeze_claim(context, ids, ulid) do
    {:ok, prefix} = Store.prefix(@table)
    {:ok, key} = Store.key(prefix, ulid)
    {:ok, claim} = HotManifest.claim(context.buffer_runtime.manifest, @table, ids, [key])

    claim
  end

  defp seal(context, claim),
    do: Handoff.seal(context.runtime.handoff, context.runtime, @table, claim)

  # The planner's dedup rule applied to the real catalog + hot manifest, then the
  # surviving rows actually read. A double-count shows up as duplicated ids.
  defp visible_ids(context) do
    {:ok, sealed} = Catalog.segments(context.catalog, @table, :current)
    {:ok, entries} = Client.hot_manifest(context.buffer, @table)

    registered = MapSet.new(sealed)

    hot =
      entries
      |> Enum.reject(&claim_committed?(&1, context.runtime, registered))
      |> Enum.map(&Store.location(context.buffer_runtime.store, &1.key))

    read_ids(context, sealed ++ hot)
  end

  defp claim_committed?(entry, runtime, registered) do
    entry.claim_keys != [] and
      Enum.all?(entry.claim_keys, &MapSet.member?(registered, Store.location(runtime.store, &1)))
  end

  defp read_ids(_context, []), do: []

  defp read_ids(context, paths) do
    placeholders = Enum.map_join(1..length(paths), ", ", &"$#{&1}")

    context.runtime.name
    |> Runtime.engine()
    |> Engine.query!(
      "SELECT id FROM read_parquet([#{placeholders}], union_by_name := true) ORDER BY id",
      paths
    )
    |> Map.fetch!(:rows)
    |> List.flatten()
  end

  defp sealed_count(context) do
    {:ok, sealed} = Catalog.segments(context.catalog, @table, :current)
    length(sealed)
  end

  test "F-1: a released claim's in-flight attempt is refused and its orphan compensated",
       context do
    m1 = write(context.buffer, 1..2)
    m2 = write(context.buffer, 3..4)

    assert visible_ids(context) == [1, 2, 3, 4]

    orig = freeze_claim(context, [m1, m2], @orig_ulid)
    orig_keys = orig.keys

    check_trace(
      fn ->
        # Park the original attempt right before its retire until BOTH re-derived
        # seals (which carry different keys, so the delay never parks them) have
        # retired.
        force_ordering(
          delay: %{:"$kind" => :"storage.seal.before_retire", keys: ^orig_keys},
          until: %{:"$kind" => :"storage.seal.retired", table_ref: @table},
          count: 2
        )

        task = Task.async(fn -> seal(context, orig) end)

        {:ok, _} = block_until(%{:"$kind" => :"storage.seal.registered", keys: ^orig_keys})

        :ok = HotManifest.release(context.buffer_runtime.manifest, @table, [m1, m2])

        r1 = freeze_claim(context, [m1], @re1_ulid)
        r2 = freeze_claim(context, [m2], @re2_ulid)

        assert seal(context, r1) == :ok
        assert seal(context, r2) == :ok

        Task.await(task, 15_000)
      end,
      fn result, _trace ->
        assert {:error, {:stale_claim, %{keys: ^orig_keys}}} = result
      end
    )

    assert visible_ids(context) == [1, 2, 3, 4]
    assert sealed_count(context) == 2
  end
end
