defmodule Smolquery.StorageService.RetentionTest do
  @moduledoc """
  Retention against a real DuckLake catalog and real Parquet footers.

  Tagged `:integration` because the two things that can actually be wrong are
  real-DuckDB things: whether footer stats answer "has every row aged out" the
  way the sweeper reads them, and whether the drop → snapshot-expiry →
  `known_segments/1` chain leaves GC able to reclaim what retention dropped.
  """

  use ExUnit.Case, async: false

  alias Smolquery.Catalog
  alias Smolquery.Catalog.DuckLake
  alias Smolquery.Engine
  alias Smolquery.Engine.Result
  alias Smolquery.Schema
  alias Smolquery.Segments.Id
  alias Smolquery.Segments.Store
  alias Smolquery.Segments.Writer
  alias Smolquery.StorageService.Retention
  alias Smolquery.StorageService.Runtime

  @moduletag :integration
  @moduletag :tmp_dir

  @table {"analytics", "events"}
  @day_ms 86_400_000

  defp schema, do: Schema.new!([{"id", :int64}, {"ts", :timestamp}])

  setup context do
    storage = :"retention_#{:erlang.unique_integer([:positive])}"

    start_supervised!(
      {DuckLake,
       name: Runtime.catalog_engine(storage),
       metadata: "sqlite:#{Path.join(context.tmp_dir, "catalog.sqlite")}",
       data_path: Path.join(context.tmp_dir, "ducklake")},
      id: Runtime.catalog_engine(storage)
    )

    start_supervised!({Engine, name: Runtime.engine(storage)}, id: Runtime.engine(storage))

    catalog = DuckLake.new(engine: Runtime.catalog_engine(storage))
    :ok = Catalog.create_dataset(catalog, "analytics")
    :ok = Catalog.create_table(catalog, @table, schema())

    %{storage: storage, catalog: catalog}
  end

  defp start_retention(context, opts) do
    runtime =
      Runtime.new(
        [
          name: context.storage,
          dir: Path.join(context.tmp_dir, "sealed"),
          catalog: context.catalog,
          engine_extensions: [],
          retention_interval_ms: 3_600_000
        ] ++ opts
      )

    start_supervised!({Retention, runtime}, id: {:retention, context.storage})

    runtime
  end

  defp seal(runtime, catalog, index, timestamps) do
    {:ok, prefix} = Store.prefix(@table)
    rows = timestamps |> Enum.with_index() |> Enum.map(fn {ts, i} -> %{"id" => i, "ts" => ts} end)

    {:ok, segment} =
      Writer.write(rows, schema(),
        store: runtime.store,
        prefix: prefix,
        id: Id.generate(index * 1_000)
      )

    {:ok, _snapshot} = Catalog.register_segments(catalog, @table, [segment])

    segment
  end

  defp lake_rows(storage) do
    Runtime.catalog_engine(storage)
    |> Engine.query!(~s|SELECT count(*) FROM lake."analytics"."events"|)
    |> Result.one!()
  end

  defp days_ago(days),
    do: NaiveDateTime.add(NaiveDateTime.utc_now(), -days * @day_ms, :millisecond)

  test "drops a segment only once every row in it has aged out", context do
    runtime = start_retention(context, [])
    aged = seal(runtime, context.catalog, 1, [days_ago(10), days_ago(9)])
    straddler = seal(runtime, context.catalog, 2, [days_ago(10), days_ago(0)])
    fresh = seal(runtime, context.catalog, 3, [days_ago(0)])

    :ok = Catalog.put_retention(context.catalog, @table, %{column: "ts", ttl_ms: @day_ms})

    assert {:ok, report} = Retention.sweep(context.storage)

    assert [%{table: @table, dropped: [dropped_path]}] = report.dropped
    assert dropped_path == aged.path
    assert report.failed == []

    assert {:ok, current} = Catalog.segments(context.catalog, @table, :current)
    assert Enum.sort(current) == Enum.sort([straddler.path, fresh.path])
    assert lake_rows(context.storage) == 3
    assert File.exists?(aged.path)
  end

  test "a table without a policy keeps everything", context do
    runtime = start_retention(context, [])
    seal(runtime, context.catalog, 1, [days_ago(1000)])

    assert {:ok, report} = Retention.sweep(context.storage)

    assert report.dropped == []
    assert {:ok, [_kept]} = Catalog.segments(context.catalog, @table, :current)
  end

  test "a dropped segment becomes reclaimable once its snapshots expire", context do
    runtime = start_retention(context, [])
    aged = seal(runtime, context.catalog, 1, [days_ago(10)])
    kept = seal(runtime, context.catalog, 2, [days_ago(0)])

    :ok = Catalog.put_retention(context.catalog, @table, %{column: "ts", ttl_ms: @day_ms})

    assert {:ok, %{dropped: [_drop], expired_snapshots: 0}} = Retention.sweep(context.storage)
    assert {:ok, known} = Catalog.known_segments(context.catalog)
    assert aged.path in known

    stop_supervised!({:retention, context.storage})
    start_retention(context, snapshot_keep_ms: 1_000)
    Process.sleep(1_100)

    assert {:ok, second} = Retention.sweep(context.storage)
    assert second.expired_snapshots > 0

    assert {:ok, known} = Catalog.known_segments(context.catalog)
    refute aged.path in known
    assert kept.path in known
    assert File.exists?(aged.path)
  end

  test "a sweep with nothing expired drops nothing and reports it", context do
    runtime = start_retention(context, [])
    seal(runtime, context.catalog, 1, [days_ago(0)])

    :ok = Catalog.put_retention(context.catalog, @table, %{column: "ts", ttl_ms: @day_ms})

    assert {:ok, report} = Retention.sweep(context.storage)
    assert report.dropped == []
    assert report.failed == []
  end
end
