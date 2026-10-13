defmodule Smolquery.Segments.WriterStatsTest do
  @moduledoc """
  The writer's manifest stats come from the `COPY`'s `RETURN_STATS`, not from a
  scan of the file it wrote. These tests hold them against that scan — the
  query the writer ran before, kept here as the reference — on the same file:

    * every schema column is present, under its own name, with the same
      `null_count`, and the same row count;
    * a bound is the scan's bound, of the same Elixir type, except a string
      bound DuckDB cut at 256 bytes (a prefix of the true min; a max raised
      past the true one) or dropped on both sides of a column holding a
      string it could not cut — and so never narrower than the data;
    * the entry built from them survives the hot manifest's log record and
      `HotServer` JSON exactly as the scan's does.

  The differential test draws its batches from a seeded generator, so a
  failure names the seed that reproduces it.
  """

  use ExUnit.Case, async: false

  alias Smolquery.BufferService.HotManifest.Entry
  alias Smolquery.Engine
  alias Smolquery.QueryService.Pruner
  alias Smolquery.Schema
  alias Smolquery.Schema.Field
  alias Smolquery.Schema.Materialized
  alias Smolquery.Segments.Store
  alias Smolquery.Segments.Writer

  @moduletag :tmp_dir
  @engine __MODULE__.Engine
  @epoch ~N[1970-01-01 00:00:00.000000]
  @cut_bytes 256
  @batches 60

  setup do
    start_supervised!({Engine, name: @engine, extensions: []})
    :ok
  end

  describe "against a full scan of the same file" do
    test "random batches: same columns, null counts and row count; bounds equal or wider",
         %{tmp_dir: dir} do
      seed = System.get_env("WRITER_STATS_SEED", "20261013") |> String.to_integer()
      :rand.seed(:exsss, {seed, seed, seed})

      tally =
        Enum.reduce(1..@batches, %{}, fn batch, tally ->
          schema = random_schema()
          paths = spool_batch(dir, batch, schema)

          {:ok, segment} =
            Writer.write({:ndjson, paths}, schema,
              store: Store.Local.new(dir: Path.join(dir, "seg#{batch}")),
              engine: @engine
            )

          context = "seed #{seed} batch #{batch}"
          reference = scanned_stats(segment.path, schema)

          assert segment.row_count == footer_rows(segment.path), context
          assert Map.keys(segment.stats) |> Enum.sort() == Enum.sort(Schema.names(schema))
          assert Map.keys(segment.stats) |> Enum.sort() == Map.keys(reference) |> Enum.sort()

          assert_round_trips(segment, reference, context)

          Enum.reduce(schema.fields, tally, fn %Field{} = field, tally ->
            new = segment.stats[field.name]
            old = reference[field.name]
            context = "#{context} column #{field.name} (#{inspect(field.type)})"

            assert new.null_count == old.null_count, context
            assert Map.keys(new) == [:max, :min, :null_count], context

            facts = %{longest: longest_bytes(segment.path, field), context: context}

            Enum.reduce([:min, :max], tally, fn side, tally ->
              kind = bound_kind(field.type, side, new, old, facts)
              Map.update(tally, {kind, side}, 1, &(&1 + 1))
            end)
          end)
        end)

      for kind <- [{:exact, :min}, {:cut, :min}, {:cut, :max}, {:dropped, :max}] do
        assert Map.get(tally, kind, 0) > 0, "seed #{seed}: no #{inspect(kind)} string bound"
      end

      for type <- [:int64, :float64, :timestamp, :timestamp_ns, :date, :numeric],
          side <- [:min, :max] do
        assert Map.get(tally, {{:bounded, type}, side}, 0) > 0,
               "seed #{seed}: no exact #{side} #{type} bound"
      end
    end

    test "every supported type at once, with the scan's Elixir types", %{tmp_dir: dir} do
      schema = full_schema(true)

      rows = [
        full_row(1, "b", 2.5, "2024-05-01 10:00:00.5", "2024-05-01 10:00:00.000000001"),
        full_row(2, "a", -1.0, "2023-01-01 00:00:00", "1969-12-31 23:59:59.999999999"),
        %{"id" => 3}
      ]

      {:ok, segment} = write(dir, schema, rows)
      reference = scanned_stats(segment.path, schema)

      assert segment.stats == reference
      assert segment.row_count == 3

      assert %{min: 1, max: 3, null_count: 0} = segment.stats["id"]
      assert %{min: -1.0, max: 2.5, null_count: 1} = segment.stats["f"]
      assert %{min: "a", max: "b", null_count: 1} = segment.stats["s"]
      assert segment.stats["ts"].min == ~N[2023-01-01 00:00:00.000000]
      assert segment.stats["ts"].max == ~N[2024-05-01 10:00:00.500000]
      assert segment.stats["tns"].min == ~N[1969-12-31 23:59:59.999999]
      assert segment.stats["tns"].max == ~N[2024-05-01 10:00:00.000001]
      assert segment.stats["d"] == %{min: ~D[2023-01-01], max: ~D[2024-05-01], null_count: 1}

      assert segment.stats["n"] == %{
               min: Decimal.new("-1.50"),
               max: Decimal.new("2.25"),
               null_count: 1
             }

      assert segment.stats["b"] == %{min: nil, max: nil, null_count: 1}
      assert segment.stats["m"] == %{min: nil, max: nil, null_count: 1}
      assert segment.stats["v"] == %{min: nil, max: nil, null_count: 1}
      assert segment.stats["mi"] == %{min: 0, max: 0, null_count: 1}

      assert_round_trips(segment, reference, "full schema")
    end
  end

  describe "strings" do
    test "a bound DuckDB cut at 256 bytes is kept: a prefix for the min, raised past for the max",
         %{tmp_dir: dir} do
      schema = Schema.new!([{"s", :string}])

      {:ok, segment} = write(dir, schema, [%{"s" => "a"}, %{"s" => String.duplicate("z", 300)}])

      assert segment.stats["s"] == %{
               min: "a",
               max: String.duplicate("z", 255) <> "{",
               null_count: 0
             }

      {:ok, segment} = write(dir, schema, [%{"s" => String.duplicate("b", 400)}, %{"s" => "c"}])
      assert segment.stats["s"] == %{min: String.duplicate("b", 256), max: "c", null_count: 0}

      {:ok, segment} = write(dir, schema, [%{"s" => String.duplicate("z", 257)}])

      assert segment.stats["s"] == %{
               min: String.duplicate("z", 256),
               max: String.duplicate("z", 255) <> "{",
               null_count: 0
             }

      {:ok, segment} = write(dir, schema, [%{"s" => "a"}, %{"s" => String.duplicate("~", 300)}])
      assert segment.stats["s"].max == String.duplicate("~", 255) <> <<0x7F>>
    end

    test "a bound up to 256 bytes is exact; past it, cut at a character, the max is raised",
         %{tmp_dir: dir} do
      schema = Schema.new!([{"s", :string}])

      for bytes <- [1, 100, 252, 253, 256] do
        max = String.duplicate("y", bytes)
        {:ok, segment} = write(dir, schema, [%{"s" => "a"}, %{"s" => max}])
        assert segment.stats["s"].max == max
      end

      {:ok, segment} = write(dir, schema, [%{"s" => "a"}, %{"s" => String.duplicate("y", 257)}])
      assert segment.stats["s"].max == String.duplicate("y", 255) <> "z"

      {:ok, segment} =
        write(dir, schema, [
          %{"s" => "a"},
          %{"s" => String.duplicate("z", 200) <> String.duplicate("é", 60)}
        ])

      assert segment.stats["s"] == %{
               min: "a",
               max: String.duplicate("z", 199) <> "{",
               null_count: 0
             }
    end

    test "a long string DuckDB cannot cut leaves the column with no bounds at all",
         %{tmp_dir: dir} do
      schema = Schema.new!([{"s", :string}])

      for value <- [
            String.duplicate("é", 150),
            String.duplicate("日", 150),
            String.duplicate("😀", 150),
            String.duplicate(<<0x7F>>, 300)
          ] do
        {:ok, segment} = write(dir, schema, [%{"s" => "a"}, %{"s" => value}, %{}])

        assert segment.stats["s"] == %{min: nil, max: nil, null_count: 1}
      end
    end

    test "a cut bound prunes what the true bound pruned, and keeps every match",
         %{tmp_dir: dir} do
      schema = Schema.new!([{"s", :string}])
      long_z = String.duplicate("z", 300)
      long_b = String.duplicate("b", 300)

      {:ok, segment} = write(dir, schema, [%{"s" => "a"}, %{"s" => long_z}])
      manifest = manifest(segment)

      assert %{min: "a", max: max} = segment.stats["s"]
      assert max > long_z
      refute Pruner.keep?(manifest, [{"s", :gt, "{"}])
      refute Pruner.keep?(manifest, [{"s", :gt, max}])
      assert Pruner.keep?(manifest, [{"s", :lt, "b"}])

      for op <- [:eq, :ge, :le] do
        assert Pruner.keep?(manifest, [{"s", op, long_z}])
        assert Pruner.keep?(manifest, [{"s", op, "a"}])
      end

      {:ok, segment} = write(dir, schema, [%{"s" => long_b}, %{"s" => "c"}])
      manifest = manifest(segment)

      assert %{min: min, max: "c"} = segment.stats["s"]
      assert String.starts_with?(long_b, min)
      refute Pruner.keep?(manifest, [{"s", :lt, "b"}])
      refute Pruner.keep?(manifest, [{"s", :le, "b"}])

      for op <- [:eq, :ge, :le] do
        assert Pruner.keep?(manifest, [{"s", op, long_b}])
      end
    end

    test "empty strings and unicode compare byte-wise, as DuckDB and the pruner do",
         %{tmp_dir: dir} do
      schema = Schema.new!([{"e", :string}, {"u", :string}])

      rows = [
        %{"e" => "", "u" => "é日本"},
        %{"e" => "", "u" => <<0, ?x>>},
        %{"e" => nil, "u" => "Z😀"}
      ]

      {:ok, segment} = write(dir, schema, rows)

      assert segment.stats["e"] == %{min: "", max: "", null_count: 1}
      assert segment.stats["u"] == %{min: <<0, ?x>>, max: "é日本", null_count: 0}
      assert segment.stats == scanned_stats(segment.path, schema)
    end
  end

  describe "timestamps and dates" do
    test "TIMESTAMP_NS bounds round outward to microseconds from the COPY's text (T-475)",
         %{tmp_dir: dir} do
      schema = Schema.new!([{"t", :timestamp_ns}])

      cases = [
        {["1969-12-31 23:59:59.999999999", "2024-01-01 00:00:00.000000001"],
         ~N[1969-12-31 23:59:59.999999], ~N[2024-01-01 00:00:00.000001]},
        {["2024-01-01 00:00:00.123456", "2024-01-01 00:00:00"], ~N[2024-01-01 00:00:00.000000],
         ~N[2024-01-01 00:00:00.123456]},
        {["2024-01-01 23:59:59.9999999", "1677-09-22 00:12:43.145224194"],
         ~N[1677-09-22 00:12:43.145224], ~N[2024-01-02 00:00:00.000000]}
      ]

      for {values, min, max} <- cases do
        {:ok, segment} = write(dir, schema, Enum.map(values, &%{"t" => &1}))
        assert segment.stats["t"] == %{min: min, max: max, null_count: 0}
        assert segment.stats == scanned_stats(segment.path, schema)
      end
    end

    test "TIMESTAMP and DATE are exact, at microsecond precision like the scan's", %{tmp_dir: dir} do
      schema = Schema.new!([{"t", :timestamp}, {"d", :date}])

      rows = [
        %{"t" => "0001-01-01 00:00:00", "d" => "0001-01-01"},
        %{"t" => "2024-02-29 12:34:56.000001", "d" => "9999-12-31"},
        %{"t" => "1960-06-01 00:00:00.5", "d" => "1960-06-01"}
      ]

      {:ok, segment} = write(dir, schema, rows)

      assert segment.stats["t"] == %{
               min: ~N[0001-01-01 00:00:00.000000],
               max: ~N[2024-02-29 12:34:56.000001],
               null_count: 0
             }

      assert segment.stats["d"] == %{min: ~D[0001-01-01], max: ~D[9999-12-31], null_count: 0}
      assert segment.stats == scanned_stats(segment.path, schema)
    end

    test "a pre-1970 TIMESTAMP with a fraction is a valid NaiveDateTime the manifest can encode",
         %{tmp_dir: dir} do
      schema = Schema.new!([{"t", :timestamp}])

      {:ok, segment} =
        write(dir, schema, [
          %{"t" => "1960-06-01 00:00:00.5"},
          %{"t" => "1969-12-31 23:59:59.999"}
        ])

      assert segment.stats["t"] == %{
               min: ~N[1960-06-01 00:00:00.500000],
               max: ~N[1969-12-31 23:59:59.999000],
               null_count: 0
             }

      {:ok, entry} =
        segment
        |> Entry.from_segment(0)
        |> Entry.to_record()
        |> JSON.encode!()
        |> JSON.decode!()
        |> Entry.from_record()

      assert entry.stats["t"] == segment.stats["t"]
    end

    test "years past 9999 are bounded, as the scan's values", %{tmp_dir: dir} do
      schema = Schema.new!([{"t", :timestamp}, {"d", :date}, {"n", :timestamp_ns}])

      wide = %{"t" => "10000-01-01 00:00:00", "d" => "10000-01-01", "n" => "2024-01-01 00:00:00"}
      {:ok, segment} = write(dir, schema, [wide])

      assert segment.stats["t"] == %{
               min: NaiveDateTime.new!(10_000, 1, 1, 0, 0, 0, {0, 6}),
               max: NaiveDateTime.new!(10_000, 1, 1, 0, 0, 0, {0, 6}),
               null_count: 0
             }

      assert segment.stats["d"] == %{
               min: Date.new!(10_000, 1, 1),
               max: Date.new!(10_000, 1, 1),
               null_count: 0
             }

      assert segment.stats == scanned_stats(segment.path, schema)
      assert_round_trips(segment, scanned_stats(segment.path, schema), "years past 9999")

      {:ok, segment} =
        write(dir, schema, [wide, %{"t" => "294247-01-10 04:00:54.775806", "d" => "2024-01-01"}])

      assert segment.stats["t"].max == NaiveDateTime.new!(294_247, 1, 10, 4, 0, 54, {775_806, 6})
      assert segment.stats["d"].min == ~D[2024-01-01]
      assert segment.stats == scanned_stats(segment.path, schema)
    end

    test "infinity and BC values have no Elixir form, so their bounds are unknown",
         %{tmp_dir: dir} do
      schema = Schema.new!([{"t", :timestamp}, {"d", :date}, {"n", :timestamp_ns}])

      rows = [
        %{"t" => "infinity", "d" => "-infinity", "n" => "infinity"},
        %{"t" => "-0044-03-15 10:00:00", "d" => "-0044-03-15", "n" => "2024-01-01 00:00:00"},
        %{"t" => "2024-01-01 00:00:00", "d" => "2024-01-01", "n" => "2024-01-02 00:00:00"}
      ]

      {:ok, segment} = write(dir, schema, rows)

      assert segment.scanned_columns == 0
      assert segment.stats["t"] == %{min: nil, max: nil, null_count: 0}
      assert segment.stats["d"] == %{min: nil, max: ~D[2024-01-01], null_count: 0}
      assert segment.stats["n"] == %{min: ~N[2024-01-01 00:00:00.000000], max: nil, null_count: 0}
    end
  end

  describe "decimals" do
    test "parse exactly at every precision, as the scan's Decimal", %{tmp_dir: dir} do
      schema =
        Schema.new!([
          {"p4", {:numeric, 4, 2}},
          {"p18", {:numeric, 18, 4}},
          {"p38", {:numeric, 38, 10}}
        ])

      rows = [
        %{
          "p4" => "-12.34",
          "p18" => "12345678901234.5678",
          "p38" => "-1234567890123456789012345678.0123456789"
        },
        %{"p4" => "0.5", "p18" => "-0.0001", "p38" => "99999999999999999999999999.9999999999"},
        %{"p4" => "0", "p18" => "0", "p38" => "0"}
      ]

      {:ok, segment} = write(dir, schema, rows)

      assert segment.stats["p4"] == %{
               min: Decimal.new("-12.34"),
               max: Decimal.new("0.50"),
               null_count: 0
             }

      assert segment.stats["p38"] == %{
               min: Decimal.new("-1234567890123456789012345678.0123456789", max_digits: 38),
               max: Decimal.new("99999999999999999999999999.9999999999", max_digits: 38),
               null_count: 0
             }

      assert segment.stats == scanned_stats(segment.path, schema)
    end
  end

  describe "floats" do
    test "NaN makes the max :nan, as DuckDB orders it above every number", %{tmp_dir: dir} do
      schema = Schema.new!([{"f", :float64}])

      {:ok, segment} = write(dir, schema, [%{"f" => 1.5}, %{"f" => "NaN"}, %{"f" => -2.0}])
      assert segment.stats["f"] == %{min: -2.0, max: :nan, null_count: 0}
      assert segment.stats == scanned_stats(segment.path, schema)

      {:ok, segment} = write(dir, schema, [%{"f" => "NaN"}, %{"f" => nil}])
      assert segment.stats["f"] == %{min: :nan, max: :nan, null_count: 1}
      assert segment.stats == scanned_stats(segment.path, schema)
    end

    test "infinities, signed zero and the extremes come back as the scan's values",
         %{tmp_dir: dir} do
      schema = Schema.new!([{"f", :float64}, {"z", :float64}, {"x", :float64}])

      rows = [
        %{"f" => "Infinity", "z" => -0.0, "x" => 5.0e-324},
        %{"f" => "-Infinity", "z" => -0.0, "x" => 1.797_693_134_862_315_7e308},
        %{"f" => 0.1, "z" => nil, "x" => 0.30000000000000004}
      ]

      {:ok, segment} = write(dir, schema, rows)

      assert segment.stats["f"] == %{min: :neg_infinity, max: :infinity, null_count: 0}
      assert segment.stats["z"] == %{min: -0.0, max: -0.0, null_count: 1}

      assert segment.stats["x"] == %{
               min: 5.0e-324,
               max: 1.797_693_134_862_315_7e308,
               null_count: 0
             }

      assert segment.stats == scanned_stats(segment.path, schema)
    end
  end

  describe "columns without bounds" do
    test "an all-NULL column of every type has no bounds and counts every row", %{tmp_dir: dir} do
      schema = full_schema(false)
      {:ok, segment} = write(dir, schema, [%{"id" => 1}, %{"id" => 2}])

      for %Field{name: name} <- schema.fields, name != "id" do
        assert segment.stats[name] == %{min: nil, max: nil, null_count: 2}, name
      end

      assert segment.stats == scanned_stats(segment.path, schema)
    end

    test "a MAP is counted by a scan, telling a null map from an empty one; a VARIANT by the COPY",
         %{tmp_dir: dir} do
      schema = Schema.new!([{"m", {:map, :string, :string}}, {"v", :variant}, {"b", :bool}])

      rows = [
        %{"m" => %{"a" => "1"}, "v" => %{"x" => 1}, "b" => true},
        %{"m" => %{}, "v" => nil, "b" => nil},
        %{"m" => nil, "v" => [1, 2], "b" => false},
        %{"v" => "s", "b" => false}
      ]

      {:ok, segment} = write(dir, schema, rows)

      assert segment.stats == %{
               "m" => %{min: nil, max: nil, null_count: 2},
               "v" => %{min: nil, max: nil, null_count: 1},
               "b" => %{min: nil, max: nil, null_count: 1}
             }
    end
  end

  describe "columns counted by a scan" do
    test "every type but MAP is described by the COPY; each MAP is scanned", %{tmp_dir: dir} do
      rows = [full_row(1, "b", 2.5, "2024-05-01 10:00:00.5", "2024-05-01 10:00:00"), %{"id" => 2}]
      schema = full_schema(true)

      without_maps = %{
        schema
        | fields: Enum.reject(schema.fields, &match?({:map, _, _}, &1.type))
      }

      two_maps =
        Schema.new!([{"m", {:map, :string, :string}}, {"n", {:map, :string, :string}}])

      {:ok, segment} = write(dir, without_maps, rows)
      assert segment.scanned_columns == 0
      assert Enum.any?(without_maps.fields, &(&1.type == :bool))
      assert Enum.any?(without_maps.fields, &(&1.type == :variant))

      {:ok, segment} = write(dir, schema, rows)
      assert segment.scanned_columns == 1

      {:ok, segment} = write(dir, two_maps, [%{"m" => %{"a" => "1"}}, %{"n" => %{}}])
      assert segment.scanned_columns == 2
    end

    test "a null_count that is not text sends the column to the scan, not the write down",
         %{tmp_dir: dir} do
      schema = Schema.new!([{"i", :int64}, {"s", :string}])
      rows = [%{"i" => 7, "s" => "a"}, %{"i" => -3}, %{"s" => "b"}]

      engine =
        rewriting_engine(fn stats ->
          Map.update!(stats, ~s("i"), &Map.put(&1, "null_count", 1))
        end)

      {:ok, segment} = write(dir, schema, rows, engine)

      assert segment.scanned_columns == 1
      assert segment.stats["i"] == %{min: -3, max: 7, null_count: 1}
      assert segment.stats["s"] == %{min: "a", max: "b", null_count: 1}
    end

    test "bound text that does not parse sends the column to the scan, not to nil",
         %{tmp_dir: dir} do
      schema = Schema.new!([{"t", :timestamp}, {"f", :float64}, {"g", :float64}])

      rows = [
        %{"t" => "2024-01-01 00:00:00", "f" => 1.5, "g" => "NaN"},
        %{"t" => "2024-01-02 00:00:00", "f" => -2.0, "g" => 0.25}
      ]

      engine =
        rewriting_engine(fn stats ->
          stats
          |> Map.update!(~s("t"), &Map.put(&1, "max", "2024-01-02 00:00:00+00"))
          |> Map.update!(~s("f"), &Map.delete(&1, "has_nan"))
          |> Map.update!(~s("g"), &Map.put(&1, "min", "0.25x"))
        end)

      {:ok, described} = write(dir, schema, rows)
      {:ok, segment} = write(dir, schema, rows, engine)

      assert described.scanned_columns == 0
      assert segment.scanned_columns == 3
      assert segment.stats == described.stats
      assert segment.stats["g"] == %{min: 0.25, max: :nan, null_count: 0}
    end

    test "an all-null column needs no bound text, so a missing has_nan is not scanned",
         %{tmp_dir: dir} do
      schema = Schema.new!([{"f", :float64}, {"s", :string}])
      rows = [%{"s" => "a"}, %{"s" => "b"}]

      engine =
        rewriting_engine(fn stats ->
          Map.update!(stats, ~s("f"), &Map.drop(&1, ["has_nan", "min", "max"]))
        end)

      {:ok, segment} = write(dir, schema, rows, engine)

      assert segment.scanned_columns == 0
      assert segment.stats["f"] == %{min: nil, max: nil, null_count: 2}
    end

    test "a scanned TIMESTAMP or DATE is read as text: pre-1970 fractions encode in the manifest",
         %{tmp_dir: dir} do
      schema = Schema.new!([{"t", :timestamp}, {"d", :date}, {"n", :timestamp_ns}])

      rows = [
        %{"t" => "1960-06-01 00:00:00.5", "d" => "1960-06-01", "n" => "1960-06-01 00:00:00.5"},
        %{"t" => "1969-12-31 23:59:59.999", "d" => "10000-01-01", "n" => nil}
      ]

      engine = rewriting_engine(fn stats -> Map.drop(stats, [~s("t"), ~s("d"), ~s("n")]) end)

      {:ok, described} = write(dir, schema, rows)
      {:ok, segment} = write(dir, schema, rows, engine)

      assert described.scanned_columns == 0
      assert segment.scanned_columns == 3
      assert segment.stats == described.stats

      assert segment.stats["t"] == %{
               min: ~N[1960-06-01 00:00:00.500000],
               max: ~N[1969-12-31 23:59:59.999000],
               null_count: 0
             }

      assert segment.stats["d"] == %{
               min: ~D[1960-06-01],
               max: Date.new!(10_000, 1, 1),
               null_count: 0
             }

      {:ok, entry} =
        segment
        |> Entry.from_segment(0)
        |> Entry.to_record()
        |> JSON.encode!()
        |> JSON.decode!()
        |> Entry.from_record()

      assert entry.stats == segment.stats
    end
  end

  describe "how the file is written" do
    test "materialized columns are bounded under their own names, with or without field ids",
         %{tmp_dir: dir} do
      for ids? <- [true, false] do
        schema = full_schema(ids?)
        rows = for i <- 1..20, do: %{"id" => i, "i" => i * 70, "s" => "s#{i}"}

        {:ok, segment} = write(dir, schema, rows)

        assert Schema.field_ids(schema) == nil == not ids?
        assert segment.stats["mi"] == %{min: 10, max: 200, null_count: 0}
        assert segment.stats["ms"].min == "S1"
        assert segment.stats == scanned_stats(segment.path, schema)
      end
    end

    test "the clustering sort changes the file, not its stats", %{tmp_dir: dir} do
      rows = for i <- 1..50, do: %{"id" => 50 - i, "i" => rem(i * 37, 11), "s" => "v#{rem(i, 7)}"}

      stats =
        for clustering <- [[], ["s"], ["i", "id"]] do
          schema = %{full_schema(true) | clustering: clustering}
          {:ok, segment} = write(dir, schema, rows)
          assert segment.stats == scanned_stats(segment.path, schema)
          segment.stats
        end

      assert stats |> Enum.uniq() |> length() == 1
    end

    @tag timeout: 120_000
    test "stats span every row group, NaN and long strings in a late one included",
         %{tmp_dir: dir} do
      schema = Schema.new!([{"i", :int64}, {"f", :float64}, {"s", :string}])

      rows =
        for(i <- 1..250_000, do: %{"i" => i, "f" => i / 4, "s" => "m#{rem(i, 997)}"}) ++
          [%{"i" => -7, "f" => "NaN", "s" => String.duplicate("z", 300)}]

      {:ok, segment} = write(dir, schema, rows)

      assert row_groups(segment.path) > 1
      assert segment.row_count == 250_001
      assert segment.stats["i"] == %{min: -7, max: 250_000, null_count: 0}
      assert segment.stats["f"] == %{min: 0.25, max: :nan, null_count: 0}

      assert segment.stats["s"] == %{
               min: "m0",
               max: String.duplicate("z", 255) <> "{",
               null_count: 0
             }
    end
  end

  defp write(dir, schema, rows, engine \\ @engine) do
    name = "w#{System.unique_integer([:positive])}"
    path = Path.join(dir, name <> ".ndjson")
    File.write!(path, Enum.map_join(rows, "", &(JSON.encode!(&1) <> "\n")))

    Writer.write({:ndjson, [path]}, schema,
      store: Store.Local.new(dir: Path.join(dir, name)),
      engine: engine
    )
  end

  defp rewriting_engine(rewrite) do
    name = Module.concat(@engine, "Rewriting#{System.unique_integer([:positive])}")
    relay = spawn_link(fn -> relay(Engine.connection_name(@engine), rewrite) end)
    Process.register(relay, Engine.connection_name(name))
    name
  end

  defp relay(connection, rewrite) do
    receive do
      {:"$gen_call", from, {:query, sql, _params} = call} ->
        reply = GenServer.call(connection, call)

        GenServer.reply(
          from,
          if(sql =~ "RETURN_STATS", do: rewritten(reply, rewrite), else: reply)
        )

        relay(connection, rewrite)
    end
  end

  defp rewritten({:ok, %{columns: columns, rows: [values]} = result}, rewrite) do
    values =
      Enum.zip_with(columns, values, fn
        "column_statistics", stats -> rewrite.(stats)
        _column, value -> value
      end)

    {:ok, %{result | rows: [values]}}
  end

  defp full_schema(ids?) do
    specs = [
      {"id", :int64},
      {"i", :int64},
      {"f", :float64},
      {"s", :string},
      {"ts", :timestamp},
      {"tns", :timestamp_ns},
      {"d", :date},
      {"n", {:numeric, 9, 2}},
      {"b", :bool},
      {"m", {:map, :string, :string}},
      {"v", :variant},
      {"mi", :int64, materialized: %Materialized{expression: "i // 7"}},
      {"ms", :string, materialized: %Materialized{expression: "upper(s)"}}
    ]

    specs
    |> Enum.with_index(1)
    |> Enum.map(fn {spec, index} ->
      {name, type, opts} = with({name, type} <- spec, do: {name, type, []})
      Field.new!(name, type, if(ids?, do: [{:id, index} | opts], else: opts))
    end)
    |> Schema.new!()
  end

  defp full_row(id, s, f, ts, tns) do
    %{
      "id" => id,
      "i" => 3,
      "f" => f,
      "s" => s,
      "ts" => ts,
      "tns" => tns,
      "d" => String.slice(ts, 0, 10),
      "n" => if(id == 1, do: "2.25", else: "-1.5"),
      "b" => true,
      "m" => %{"k" => s},
      "v" => %{"s" => s}
    }
  end

  defp random_schema do
    schema = full_schema(:rand.uniform() < 0.5)

    numeric = Enum.random([{:numeric, 4, 2}, {:numeric, 18, 4}, {:numeric, 38, 10}])

    fields =
      Enum.map(schema.fields, fn
        %Field{name: "n"} = field -> %{field | type: numeric}
        field -> field
      end)

    %{schema | fields: fields, clustering: Enum.random([[], ["ts"], ["s", "i"], ["f"], ["n"]])}
  end

  defp spool_batch(dir, batch, schema) do
    nulls = Map.new(Schema.regular_fields(schema), &{&1.name, Enum.random([0.0, 0.1, 0.5, 1.0])})
    strings = Enum.random([:short, :mixed, :long, :multibyte, :all_long, :all_cut])
    rows = for id <- 1..Enum.random(1..300), do: random_row(id, schema, nulls, strings)

    rows
    |> Enum.chunk_every(max(div(length(rows), Enum.random(1..3)), 1))
    |> Enum.with_index()
    |> Enum.map(fn {chunk, part} ->
      path = Path.join(dir, "b#{batch}-#{part}.ndjson")
      File.write!(path, Enum.map_join(chunk, "", &(JSON.encode!(&1) <> "\n")))
      path
    end)
  end

  defp random_row(id, schema, nulls, strings) do
    for %Field{name: name, type: type} <- Schema.regular_fields(schema),
        name == "id" or :rand.uniform() >= nulls[name],
        into: %{} do
      {name, if(name == "id", do: id, else: random_value(type, strings))}
    end
  end

  defp random_value(:int64, _strings),
    do:
      Enum.random([
        :rand.uniform(1_000) - 500,
        :rand.uniform(Integer.pow(2, 63)) - Integer.pow(2, 62),
        -Integer.pow(2, 63),
        Integer.pow(2, 63) - 1
      ])

  defp random_value(:float64, _strings) do
    case :rand.uniform(10) do
      1 -> Enum.random(["NaN", "Infinity", "-Infinity", -0.0, 0.0, 5.0e-324])
      2 -> random_float_bits()
      _other -> (:rand.uniform() - 0.5) * :math.pow(10, :rand.uniform(40) - 20)
    end
  end

  defp random_value(:string, strings), do: random_string(strings)
  defp random_value(:bool, _strings), do: :rand.uniform() < 0.5

  defp random_value(:timestamp, _strings) do
    if :rand.uniform(10) == 1 do
      100_000_000
      |> wide_date()
      |> NaiveDateTime.new!(Time.new!(0, 0, 0, {0, 6}))
      |> NaiveDateTime.add(:rand.uniform(86_400_000_000) - 1, :microsecond)
      |> NaiveDateTime.to_string()
    else
      micros = :rand.uniform(7_000_000_000_000_000) - 2_500_000_000_000_000
      @epoch |> NaiveDateTime.add(micros, :microsecond) |> NaiveDateTime.to_string()
    end
  end

  defp random_value(:timestamp_ns, _strings) do
    nanos = :rand.uniform(18_000_000_000_000_000_000) - 9_000_000_000_000_000_000
    second = NaiveDateTime.add(@epoch, Integer.floor_div(nanos, 1_000_000_000), :second)
    fraction = Integer.mod(nanos, 1_000_000_000)
    digits = Enum.random([0, 3, 6, 9])

    text = second |> NaiveDateTime.truncate(:second) |> NaiveDateTime.to_string()

    case digits do
      0 -> text
      digits -> text <> "." <> String.slice(String.pad_leading("#{fraction}", 9, "0"), 0, digits)
    end
  end

  defp random_value(:date, _strings) do
    if :rand.uniform(10) == 1,
      do: Date.to_string(wide_date(2_000_000_000)),
      else: Date.add(~D[1970-01-01], :rand.uniform(200_000) - 100_000) |> Date.to_string()
  end

  defp random_value({:numeric, precision, scale}, _strings) do
    coefficient = :rand.uniform(Integer.pow(10, precision)) - 1
    sign = Enum.random([1, -1])
    Decimal.new(sign, coefficient, -scale) |> Decimal.to_string(:normal)
  end

  defp random_value({:map, :string, :string}, strings) do
    Enum.random([%{}, %{"k" => random_string(strings)}, %{"a" => "1", "b" => "2"}])
  end

  defp random_value(:variant, strings),
    do: Enum.random([%{"x" => :rand.uniform(9)}, [1, "a"], random_string(strings), 1.5, true])

  defp wide_date(days), do: Date.add(Date.new!(10_000, 1, 1), :rand.uniform(days) - 1)

  defp random_float_bits do
    <<bits::64>> = :crypto.strong_rand_bytes(8)

    case <<bits::64>> do
      <<_sign::1, 0x7FF::11, _mantissa::52>> -> 1.0
      <<float::float-64>> -> float
    end
  end

  defp random_string(:short), do: random_chars(Enum.random(0..12), ["a", "b", "z", "A", "0", " "])

  defp random_string(:mixed),
    do: random_chars(Enum.random(0..20), ["a", "é", "日", "😀", <<0>>, "~", "Z", "\u{10FFFF}"])

  defp random_string(:long) do
    Enum.random([
      random_chars(Enum.random(240..270), ["y", "z"]),
      random_chars(Enum.random(0..8), ["a", "z"])
    ])
  end

  defp random_string(:all_cut), do: random_chars(Enum.random(257..300), ["a", "y", "z", "~"])

  defp random_string(:all_long), do: random_chars(Enum.random(130..200), ["y", "z", "é"])

  defp random_string(:multibyte) do
    Enum.random([
      random_chars(Enum.random(60..140), ["é", "日", "😀", "z"]),
      random_chars(Enum.random(60..140), ["é", "日", "😀"]),
      random_chars(Enum.random(0..8), ["a", "é"])
    ])
  end

  defp random_chars(count, alphabet),
    do: Enum.map_join(List.duplicate(nil, count), fn _ -> Enum.random(alphabet) end)

  defp bound_kind(:string, side, new, old, facts), do: string_bound_kind(side, new, old, facts)

  defp bound_kind(type, side, new, old, %{context: context}) do
    bound = Map.fetch!(new, side)
    truth = Map.fetch!(old, side)

    if bound != truth,
      do: flunk("#{context}: #{side} #{inspect(bound)} differs from the scan's #{inspect(truth)}")

    if is_nil(bound), do: :exact, else: {:bounded, type_tag(type)}
  end

  defp string_bound_kind(side, new, old, %{longest: longest, context: context}) do
    bound = Map.fetch!(new, side)
    truth = Map.fetch!(old, side)
    context = "#{context}: #{side} #{inspect(bound)} against the scan's #{inspect(truth)}"

    cond do
      bound == truth ->
        assert is_nil(truth) or byte_size(truth) <= @cut_bytes, context
        :exact

      is_nil(bound) ->
        assert is_integer(longest) and longest > @cut_bytes, context
        assert new.min == nil and new.max == nil, context
        :dropped

      true ->
        assert is_binary(bound) and is_binary(truth), context
        assert byte_size(bound) <= @cut_bytes, context
        assert String.valid?(bound), context
        assert_cut(side, bound, truth, context)
        :cut
    end
  end

  defp assert_cut(:min, bound, truth, context),
    do: assert(String.starts_with?(truth, bound), context)

  defp assert_cut(:max, bound, truth, context) do
    assert bound > truth, context
    refute String.starts_with?(truth, bound), context
  end

  defp type_tag({:numeric, _precision, _scale}), do: :numeric
  defp type_tag(type), do: type

  defp manifest(segment) do
    segment
    |> Entry.from_segment(0)
    |> Entry.to_manifest(stats: true)
    |> JSON.encode!()
    |> JSON.decode!()
  end

  defp assert_round_trips(segment, reference, context) do
    reference_segment = %{segment | stats: reference}

    for {encode, decode} <- [
          {&Entry.to_record/1, &record_stats/1},
          {&Entry.to_manifest(&1, stats: true), &manifest_stats/1}
        ] do
      new = segment |> Entry.from_segment(0) |> encode.() |> JSON.encode!() |> JSON.decode!()

      old =
        reference_segment
        |> Entry.from_segment(0)
        |> encode.()
        |> JSON.encode!()
        |> JSON.decode!()

      assert Map.keys(new) == Map.keys(old), context
      assert Map.keys(new["stats"]) == Map.keys(old["stats"]), context

      same = for {column, stats} <- segment.stats, stats == reference[column], do: column

      assert Enum.all?(Map.values(new["stats"]), &(Map.keys(&1) == ["max", "min", "null_count"]))
      assert Map.take(new["stats"], same) == Map.take(old["stats"], same), context
      assert Map.delete(new, "stats") == Map.delete(old, "stats"), context

      decoded = decode.(new)
      assert Map.keys(decoded) == Map.keys(segment.stats), context
      assert Map.take(decoded, same) == Map.take(decode.(old), same), context
      assert decoded == decode.(decoded |> encoded_again(segment, encode)), context
    end
  end

  defp encoded_again(stats, segment, encode) do
    %{segment | stats: stats}
    |> Entry.from_segment(0)
    |> encode.()
    |> JSON.encode!()
    |> JSON.decode!()
  end

  defp record_stats(record) do
    {:ok, entry} = Entry.from_record(record)
    entry.stats
  end

  defp manifest_stats(manifest), do: Entry.decode_stats(manifest["stats"])

  defp scanned_stats(path, %Schema{fields: fields}) do
    selects =
      Enum.map_join(fields, ", ", fn %Field{name: name, type: type} ->
        scan_select(type, ~s("#{name}"))
      end)

    %{rows: [values]} = Engine.query!(@engine, "SELECT #{selects} FROM read_parquet($1)", [path])

    fields
    |> Enum.zip(Enum.chunk_every(values, 3))
    |> Map.new(fn {%Field{name: name, type: type}, [min, max, nulls]} ->
      {name,
       %{min: scan_bound(type, min, :floor), max: scan_bound(type, max, :ceil), null_count: nulls}}
    end)
  end

  defp scan_select(:timestamp_ns, name),
    do: "epoch_ns(min(#{name})), epoch_ns(max(#{name})), count(*) - count(#{name})"

  defp scan_select(type, name) do
    if scan_bounded?(type),
      do: "min(#{name}), max(#{name}), count(*) - count(#{name})",
      else: "NULL, NULL, count(*) - count(#{name})"
  end

  defp scan_bounded?({:numeric, _precision, _scale}), do: true

  defp scan_bounded?(type),
    do: type in [:string, :int64, :float64, :timestamp, :timestamp_ns, :date]

  defp scan_bound(:timestamp_ns, ns, :floor) when is_integer(ns),
    do: NaiveDateTime.add(@epoch, Integer.floor_div(ns, 1_000), :microsecond)

  defp scan_bound(:timestamp_ns, ns, :ceil) when is_integer(ns),
    do: NaiveDateTime.add(@epoch, -Integer.floor_div(-ns, 1_000), :microsecond)

  defp scan_bound(:timestamp, %NaiveDateTime{microsecond: {micro, 6}} = datetime, _direction)
       when micro < 0,
       do: NaiveDateTime.add(%{datetime | microsecond: {0, 6}}, micro, :microsecond)

  defp scan_bound(_type, value, _direction), do: value

  defp footer_rows(path) do
    %{rows: [[rows]]} =
      Engine.query!(@engine, "SELECT num_rows FROM parquet_file_metadata($1)", [path])

    rows
  end

  defp row_groups(path) do
    %{rows: [[groups]]} =
      Engine.query!(@engine, "SELECT count(DISTINCT row_group_id) FROM parquet_metadata($1)", [
        path
      ])

    groups
  end

  defp longest_bytes(path, %Field{name: name, type: :string}) do
    %{rows: [[bytes]]} =
      Engine.query!(
        @engine,
        ~s{SELECT max(strlen("#{name}")) FROM read_parquet($1)},
        [path]
      )

    bytes
  end

  defp longest_bytes(_path, _field), do: nil
end
