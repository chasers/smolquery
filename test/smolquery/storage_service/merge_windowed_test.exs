defmodule Smolquery.StorageService.MergeWindowedTest do
  @moduledoc """
  `Merge.compact/5` window by window (T-607): a clustered group of more rows
  than one window holds merges through local runs, a partition by the leading
  clustering column, one in-memory sort per window, and an order-preserving
  concatenation. The output must be exactly the rows of the inputs, in
  clustering order, whatever the window count.
  """

  use ExUnit.Case, async: true

  alias Smolquery.Catalog
  alias Smolquery.Catalog.DuckLake
  alias Smolquery.Engine
  alias Smolquery.Schema
  alias Smolquery.Segments.Id
  alias Smolquery.Segments.Store
  alias Smolquery.StorageService.Merge
  alias Smolquery.StorageService.Runtime
  alias Smolquery.Test.SegmentFixture

  @moduletag :tmp_dir

  @table {"metrics", "samples"}
  @names [nil, "cpu", "disk", "mem", "net"]

  setup context do
    schema = Schema.new!([{"name", :string}, {"ts", :int64}, {"value", :float64}])
    storage = :"merge_windowed_#{System.unique_integer([:positive])}"

    start_supervised!(
      {DuckLake,
       name: Runtime.catalog_engine(storage),
       metadata: "sqlite:#{Path.join(context.tmp_dir, "catalog.sqlite")}",
       data_path: Path.join(context.tmp_dir, "ducklake")}
    )

    catalog = DuckLake.new(engine: Runtime.catalog_engine(storage))
    :ok = Catalog.create_dataset(catalog, "metrics")
    :ok = Catalog.create_table(catalog, @table, schema)
    :ok = Catalog.put_clustering(catalog, @table, ["name", "ts"])
    start_supervised!({Engine, name: Runtime.engine(storage)}, id: Runtime.engine(storage))

    runtime =
      Runtime.new(name: storage, dir: Path.join(context.tmp_dir, "sealed"), catalog: catalog)

    %{runtime: runtime, schema: %{schema | clustering: ["name", "ts"]}}
  end

  test "merges window by window into the rows of its inputs, in clustering order", context do
    {sealed, rows} = seal_overlapping(context, 6, 40)
    runtime = %{context.runtime | compact_window_decoded_bytes: 25 * 100}

    assert {:ok, merged} = compact(runtime, sealed, "01KYWPEEGAM8FQVQS5S2QF26W0", width: 100)

    assert merged.row_count == length(rows)
    assert read_in_file_order(runtime, merged) == Enum.sort_by(rows, &clustering_order/1)
    assert scratch(runtime) == []
  end

  test "merges the same rows in the same order in one window or in many", context do
    {sealed, _rows} = seal_overlapping(context, 4, 30)

    assert {:ok, one} = compact(context.runtime, sealed, "01KYWPEEGAM8FQVQS5S2QF26W1", [])

    many = %{context.runtime | compact_window_decoded_bytes: 7}

    assert {:ok, windowed} =
             compact(many, sealed, "01KYWPEEGAM8FQVQS5S2QF26W2", width: 1)

    assert read_in_file_order(many, windowed) == read_in_file_order(context.runtime, one)
  end

  test "a group within one window merges in one sort, as before", context do
    {sealed, rows} = seal_overlapping(context, 2, 5)

    assert {:ok, merged} =
             compact(context.runtime, sealed, "01KYWPEEGAM8FQVQS5S2QF26W3", width: 1)

    assert read_in_file_order(context.runtime, merged) ==
             Enum.sort_by(rows, &clustering_order/1)
  end

  test "sorted_rows/3 is one window's rows when the merge would window, else the group's",
       context do
    runtime = %{context.runtime | compact_window_decoded_bytes: 1_000}

    assert Merge.sorted_rows(runtime, 500, 10) == 100
    assert Merge.sorted_rows(runtime, 50, 10) == 50
    assert Merge.sorted_rows(runtime, 500, nil) == 500
  end

  test "clear_scratch/1 removes what a merge that died left behind", context do
    leftover =
      Path.expand(
        Path.join([Runtime.spill_root(), "merge_windows", "#{context.runtime.name}", "stale"])
      )

    File.mkdir_p!(Path.join(leftover, "windows"))

    assert Merge.clear_scratch(context.runtime) == :ok
    assert scratch(context.runtime) == []
  end

  defp compact(runtime, sealed, id, opts) do
    {:ok, prefix} = Store.prefix(@table)
    {:ok, key} = Store.key(prefix, id)

    Merge.compact(runtime, @table, key, Enum.map(sealed, & &1.path), opts)
  end

  defp seal_overlapping(context, files, per_file) do
    {:ok, prefix} = Store.prefix(@table)

    sealed =
      for file <- 1..files do
        rows =
          for n <- 1..per_file do
            %{
              "name" => Enum.at(@names, rem(n * 7 + file, length(@names))),
              "ts" => file * 10 + n,
              "value" => n / 1
            }
          end

        {:ok, segment} =
          SegmentFixture.write(rows, context.schema,
            store: context.runtime.store,
            prefix: prefix,
            id: Id.generate(file * 1_000)
          )

        {:ok, _snapshot} = Catalog.register_segments(context.runtime.catalog, @table, [segment])
        {segment, rows}
      end

    {Enum.map(sealed, &elem(&1, 0)),
     Enum.flat_map(sealed, fn {_segment, rows} -> Enum.map(rows, &{&1["name"], &1["ts"]}) end)}
  end

  defp clustering_order({name, ts}), do: {is_nil(name), name, ts}

  defp read_in_file_order(runtime, segment) do
    runtime.name
    |> Runtime.engine()
    |> Engine.query!("SELECT name, ts FROM read_parquet($1)", [
      Store.location(runtime.store, segment.key)
    ])
    |> Map.fetch!(:rows)
    |> Enum.map(&List.to_tuple/1)
  end

  defp scratch(runtime) do
    [Runtime.spill_root(), "merge_windows", "#{runtime.name}", "*"]
    |> Path.join()
    |> Path.expand()
    |> Path.wildcard()
  end
end
