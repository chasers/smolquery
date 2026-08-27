defmodule Smolquery.BufferService.NdjsonPassthroughTest do
  @moduledoc """
  The unparsed-body path, end to end through the buffer service.

  `writer_ndjson_test.exs` covers the DuckDB write on its own. This covers the
  seam that one does not: a batch carrying `:ndjson` reaching a real buffer,
  accumulating with others, and coming out of a group commit as one segment.
  Nothing tested that seam when it first shipped, and the missing
  `buffer_write/6` clause reached the rig as a 500 on every insert.
  """

  use ExUnit.Case, async: false

  alias Smolquery.BufferService
  alias Smolquery.BufferService.Client
  alias Smolquery.BufferService.HotManifest
  alias Smolquery.BufferService.Runtime
  alias Smolquery.Schema

  @moduletag :tmp_dir
  @table {"logs", "events"}

  defp schema do
    Schema.new!([{"id", :int64, nullable: false}, {"tenant", :string}])
  end

  defp ndjson_batch(range) do
    body =
      range
      |> Enum.map_join("\n", &JSON.encode!(%{"id" => &1, "tenant" => "t#{rem(&1, 3)}"}))
      |> Kernel.<>("\n")

    %{
      schema: schema(),
      ndjson: body,
      row_count: Enum.count(range),
      byte_size: byte_size(body)
    }
  end

  setup context do
    name = :"buffer_ndjson_#{:erlang.unique_integer([:positive])}"

    opts = [
      name: name,
      dir: Path.join(context.tmp_dir, "buffer"),
      flush_interval_ms: 25,
      write_pool_size: 1
    ]

    start_supervised!({BufferService.Supervisor, opts}, id: name)
    on_exit(fn -> Runtime.delete(name) end)

    %{name: name, runtime: Runtime.new(opts)}
  end

  test "acks an unparsed body once its segment is in the manifest", %{
    name: name,
    runtime: runtime
  } do
    assert {:ok, ack} = Client.write_batch(name, @table, ndjson_batch(1..3))

    assert ack.row_count == 3
    assert [entry] = HotManifest.entries(runtime.manifest, @table)
    assert entry.id == ack.segment_id
    assert entry.row_count == 3
  end

  test "merges several bodies into one segment", %{name: name, runtime: runtime} do
    tasks =
      for range <- [1..10, 11..20, 21..30] do
        Task.async(fn -> Client.write_batch(name, @table, ndjson_batch(range)) end)
      end

    acks = Task.await_many(tasks, 15_000)

    assert Enum.all?(acks, &match?({:ok, _ack}, &1))

    entries = HotManifest.entries(runtime.manifest, @table)

    assert Enum.sum(Enum.map(entries, & &1.row_count)) == 30
  end

  test "a rows batch and an unparsed body share a commit", context do
    name = :"buffer_shared_#{:erlang.unique_integer([:positive])}"

    start_supervised!(
      {BufferService.Supervisor,
       name: name,
       dir: Path.join(context.tmp_dir, "shared"),
       flush_interval_ms: 60_000,
       flush_max_rows: 5,
       write_pool_size: 1},
      id: name
    )

    on_exit(fn -> Runtime.delete(name) end)

    rows = %{
      schema: schema(),
      rows: [%{"id" => 1, "tenant" => "a"}, %{"id" => 2, "tenant" => "b"}]
    }

    tasks = [
      Task.async(fn -> Client.write_batch(name, @table, rows) end),
      Task.async(fn -> Client.write_batch(name, @table, ndjson_batch(3..5)) end)
    ]

    assert [{:ok, rows_ack}, {:ok, ndjson_ack}] = Task.await_many(tasks, 15_000)
    assert rows_ack.segment_id == ndjson_ack.segment_id
    assert rows_ack.row_count == 5
  end

  test "a row DuckDB refuses is named to its caller, and the body sharing the commit lands",
       context do
    name = :"buffer_refused_#{:erlang.unique_integer([:positive])}"

    start_supervised!(
      {BufferService.Supervisor,
       name: name,
       dir: Path.join(context.tmp_dir, "refused"),
       flush_interval_ms: 60_000,
       flush_max_rows: 5,
       write_pool_size: 1},
      id: name
    )

    on_exit(fn -> Runtime.delete(name) end)

    too_big = 9_223_372_036_854_775_808

    rows = %{
      schema: schema(),
      rows: [%{"id" => 1, "tenant" => "a"}, %{"id" => too_big, "tenant" => "b"}]
    }

    tasks = [
      Task.async(fn -> Client.write_batch(name, @table, rows) end),
      Task.async(fn -> Client.write_batch(name, @table, ndjson_batch(3..5)) end)
    ]

    assert [rows_reply, {:ok, ndjson_ack}] = Task.await_many(tasks, 15_000)
    assert {:ok, rows_ack, [%{index: 1, errors: [%{message: message}]}]} = rows_reply
    assert message =~ "the flush refused the row"
    assert rows_ack.segment_id == ndjson_ack.segment_id

    {:ok, runtime} = Runtime.fetch(name)
    assert [entry] = HotManifest.entries(runtime.manifest, @table)
    assert entry.row_count == 4
  end

  test "the row count comes from the Parquet footer, not the sender", %{
    name: name,
    runtime: runtime
  } do
    # A sender that miscounts must not scale the ack: the footer is authoritative.
    batch = %{ndjson_batch(1..5) | row_count: 99}

    assert {:ok, ack} = Client.write_batch(name, @table, batch)
    assert ack.row_count == 5

    assert [entry] = HotManifest.entries(runtime.manifest, @table)
    assert entry.row_count == 5
  end

  # A whitespace-only body is not invalid — there is no row to refuse — it is
  # empty. The commit must land nothing: no manifest entry (its bounds would all
  # be null, which no pruner can exclude), no segment file, and an ack that says
  # zero rather than echo the sender.
  test "a commit of only blank lines lands no segment and says so", %{
    name: name,
    runtime: runtime
  } do
    body = " \n  \n"

    assert {:ok, ack} =
             Client.write_batch(name, @table, %{
               schema: schema(),
               ndjson: body,
               row_count: 0,
               byte_size: byte_size(body)
             })

    assert ack == %{segment_id: nil, row_count: 0}
    assert HotManifest.entries(runtime.manifest, @table) == []

    assert runtime.store.config.dir
           |> Path.join("**")
           |> Path.wildcard()
           |> Enum.filter(&File.regular?/1) == []
  end

  defp raw_batch(body) do
    %{schema: schema(), ndjson: body, row_count: 1, byte_size: byte_size(body)}
  end

  describe "rows the schema refuses" do
    test "reports them at their index instead of failing the batch", %{name: name} do
      body =
        Enum.join(
          [
            ~s({"id": 1, "tenant": "a"}),
            ~s({"id": "not-an-integer", "tenant": "b"}),
            ~s({"id": 3, "tenant": "c"})
          ],
          "\n"
        ) <> "\n"

      assert {:ok, _ack, errors} =
               Client.write_batch(name, @table, %{raw_batch(body) | row_count: 3})

      assert [%{index: 1}] = errors
    end

    test "commits the valid rows of a body that had a bad one", %{name: name, runtime: runtime} do
      body =
        Enum.join(
          [
            ~s({"id": 1, "tenant": "a"}),
            ~s({"id": "nope", "tenant": "b"}),
            ~s({"id": 3, "tenant": "c"})
          ],
          "\n"
        ) <> "\n"

      assert {:ok, _ack, _errors} =
               Client.write_batch(name, @table, %{raw_batch(body) | row_count: 3})

      entries = HotManifest.entries(runtime.manifest, @table)

      assert Enum.sum(Enum.map(entries, & &1.row_count)) == 2
    end

    # The blast radius this path exists to contain: one caller's bad row must not
    # fail the other callers merged into the same group commit.
    test "does not fail the other callers in the same commit", %{name: name} do
      bad = ~s({"id": "nope", "tenant": "x"}\n)

      tasks = [
        Task.async(fn -> Client.write_batch(name, @table, ndjson_batch(1..50)) end),
        Task.async(fn -> Client.write_batch(name, @table, raw_batch(bad)) end),
        Task.async(fn -> Client.write_batch(name, @table, ndjson_batch(51..100)) end)
      ]

      [first, poisoned, last] = Task.await_many(tasks, 15_000)

      assert {:ok, ack_first} = first
      assert ack_first.row_count > 0
      assert {:ok, ack_last} = last
      assert ack_last.row_count > 0

      # A commit did happen, so the ack is real; this caller simply contributed
      # no rows to it. The ingest edge turns that into `insertedRows: 0` with the
      # errors listed, which is what the JSON route answers for the same body.
      assert {:ok, _ack, [%{index: 0}]} = poisoned
    end

    test "answers a wholly invalid body without acking anything", %{name: name} do
      assert {:invalid, errors} =
               Client.write_batch(name, @table, raw_batch(~s({"id": "nope"}\n)))

      assert [%{index: 0}] = errors
    end
  end
end
