defmodule Smolquery.BufferService.TableBufferTest do
  use ExUnit.Case, async: true

  alias Smolquery.BufferService
  alias Smolquery.BufferService.Client
  alias Smolquery.BufferService.HotManifest
  alias Smolquery.BufferService.Runtime
  alias Smolquery.Schema
  alias Smolquery.Segments.Store
  alias Smolquery.Test.Eventually

  defmodule FailingStore do
    @behaviour Smolquery.Segments.Store

    def new, do: %Store{impl: __MODULE__, config: nil}

    @impl Store
    def put(_config, key, _encoder), do: {:error, {:put_failed, key, :enospc}}

    @impl Store
    def location(_config, key), do: "failing://" <> key

    @impl Store
    def list(_config, _prefix), do: {:ok, []}

    @impl Store
    def delete(_config, _key), do: :ok

    @impl Store
    def shared?(_config), do: false

    @impl Store
    def sweep_staging(_config, _age_ms), do: {:ok, []}
  end

  @moduletag :tmp_dir

  @table {"analytics", "events"}

  defp schema do
    Schema.new!([{"id", :int64, nullable: false}, {"ts", :timestamp}])
  end

  defp rows(range) do
    for i <- range do
      %{"id" => i, "ts" => NaiveDateTime.add(~N[2026-07-31 12:00:00], i)}
    end
  end

  defp batch(range), do: %{schema: schema(), rows: rows(range)}

  defp start_buffer_service(context, opts) do
    opts =
      Keyword.merge(
        [
          name: :"buffer_#{:erlang.unique_integer([:positive])}",
          dir: Path.join(context.tmp_dir, "buffer"),
          flush_interval_ms: 25
        ],
        opts
      )

    name = Keyword.fetch!(opts, :name)
    pid = start_supervised!({BufferService.Supervisor, opts}, id: name)
    on_exit(fn -> Runtime.delete(name) end)

    %{name: name, supervisor: pid, runtime: Runtime.new(opts)}
  end

  setup context do
    start_buffer_service(context, [])
  end

  describe "group commit" do
    test "acks only after the rows are in the manifest", %{name: name, runtime: runtime} do
      assert {:ok, ack} = Client.write_batch(name, @table, batch(1..3))

      assert ack.row_count == 3
      assert [entry] = HotManifest.entries(runtime.manifest, @table)
      assert entry.id == ack.segment_id
      assert entry.row_count == 3
    end

    test "the segment exists in the store by the time the ack lands", context do
      {:ok, ack} = Client.write_batch(context.name, @table, batch(1..2))

      [entry] = HotManifest.entries(context.runtime.manifest, @table)

      assert entry.id == ack.segment_id
      assert File.exists?(Store.location(context.runtime.store, entry.key))
    end

    test "answers concurrent callers from one segment", %{name: name, runtime: runtime} do
      acks =
        1..8
        |> Task.async_stream(fn i -> Client.write_batch(name, @table, batch(i..i)) end,
          timeout: 5_000
        )
        |> Enum.map(fn {:ok, ack} -> ack end)

      assert Enum.all?(acks, &match?({:ok, _ack}, &1))

      entries = HotManifest.entries(runtime.manifest, @table)
      total = entries |> Enum.map(& &1.row_count) |> Enum.sum()

      assert total == 8
      assert Enum.count_until(entries, 8) < 8
    end

    test "flushes on the row threshold without waiting for the interval", context do
      %{name: name} = start_buffer_service(context, flush_max_rows: 5, flush_interval_ms: 60_000)

      assert {:ok, ack} = Client.write_batch(name, @table, batch(1..5))
      assert ack.row_count == 5
    end

    test "flushes on the byte threshold", context do
      %{name: name} = start_buffer_service(context, flush_max_bytes: 1, flush_interval_ms: 60_000)

      assert {:ok, ack} = Client.write_batch(name, @table, batch(1..1))
      assert ack.row_count == 1
    end

    test "refuses an empty batch", %{name: name} do
      assert Client.write_batch(name, @table, %{schema: schema(), rows: []}) ==
               {:error, :no_rows}
    end

    test "keeps tables in separate segments", %{name: name, runtime: runtime} do
      other = {"analytics", "clicks"}

      {:ok, _one} = Client.write_batch(name, @table, batch(1..1))
      {:ok, _two} = Client.write_batch(name, other, batch(1..1))

      assert match?([_entry], HotManifest.entries(runtime.manifest, @table))
      assert match?([_entry], HotManifest.entries(runtime.manifest, other))
    end
  end

  describe "schema changes" do
    test "start a new segment rather than failing", %{name: name, runtime: runtime} do
      wide = Schema.new!([{"id", :int64}, {"ts", :timestamp}, {"extra", :string}])

      {:ok, first} = Client.write_batch(name, @table, batch(1..1))

      {:ok, second} =
        Client.write_batch(name, @table, %{
          schema: wide,
          rows: [%{"id" => 2, "ts" => ~N[2026-07-31 12:00:00], "extra" => "new"}]
        })

      refute first.segment_id == second.segment_id
      assert match?([_first, _second], HotManifest.entries(runtime.manifest, @table))
    end
  end

  describe "backpressure" do
    test "sheds a batch that would exceed the row bound", context do
      %{name: name} =
        start_buffer_service(context,
          max_buffered_rows: 4,
          flush_max_rows: 1_000,
          flush_interval_ms: 60_000
        )

      task = Task.async(fn -> Client.write_batch(name, @table, batch(1..4)) end)

      assert Eventually.until(fn -> accumulated_rows(name) == 4 end)
      assert Client.write_batch(name, @table, batch(5..8)) == {:error, :buffer_full}

      :ok = Client.flush(name, @table)
      assert {:ok, ack} = Task.await(task)
      assert ack.row_count == 4
    end

    test "sheds a batch that would exceed the byte bound", context do
      %{name: name} =
        start_buffer_service(context,
          max_buffered_bytes: 1,
          flush_max_rows: 1_000,
          flush_interval_ms: 60_000
        )

      assert Client.write_batch(name, @table, batch(1..2)) == {:error, :buffer_full}
    end
  end

  describe "a flush that fails" do
    test "answers the caller with the error and keeps nothing", context do
      %{name: name, runtime: runtime} =
        start_buffer_service(context,
          store: FailingStore.new(),
          dir: Path.join(context.tmp_dir, "failing"),
          flush_max_rows: 1
        )

      assert {:error, {:put_failed, _key, :enospc}} =
               Client.write_batch(name, @table, batch(1..1))

      assert HotManifest.entries(runtime.manifest, @table) == []
      assert Client.flush(name, @table) == :ok
    end

    test "surfaces the commit result to the flusher", context do
      %{name: name} =
        start_buffer_service(context,
          store: FailingStore.new(),
          dir: Path.join(context.tmp_dir, "failing-flush"),
          flush_interval_ms: 60_000
        )

      writer = Task.async(fn -> Client.write_batch(name, @table, batch(1..2)) end)

      assert Eventually.until(fn ->
               match?({:error, {:put_failed, _key, :enospc}}, Client.flush(name, @table))
             end)

      assert {:error, {:put_failed, _key, :enospc}} = Task.await(writer)
    end

    test "refuses to start a buffer whose log cannot be opened", context do
      logs = Path.join(context.tmp_dir, "ro-logs")
      File.mkdir_p!(logs)
      File.chmod!(logs, 0o500)
      on_exit(fn -> File.chmod!(logs, 0o700) end)

      %{name: name, runtime: runtime} =
        start_buffer_service(context,
          log_dir: logs,
          dir: Path.join(context.tmp_dir, "ro-buffer"),
          flush_max_rows: 1
        )

      assert Client.write_batch(name, @table, batch(1..1)) ==
               {:error, {:log_open_failed, :eacces}}

      assert Store.list(runtime.store, "analytics/events") == {:ok, []}
    end

    test "deletes a stored segment when the manifest append fails", context do
      %{name: name, runtime: runtime} =
        start_buffer_service(context,
          dir: Path.join(context.tmp_dir, "sealed-log"),
          flush_max_rows: 1
        )

      {:ok, _ack} = Client.write_batch(name, @table, batch(1..1))

      [{pid, _value}] = Registry.lookup(Runtime.registry(name), @table)
      held = :sys.get_state(pid).log
      :sys.replace_state(pid, fn state -> %{state | log: nil} end)

      {:ok, path} = HotManifest.log_path(runtime.manifest, @table)
      File.chmod!(path, 0o400)
      on_exit(fn -> File.chmod!(path, 0o600) end)

      assert {:error, {:log_append_failed, :eacces}} =
               Client.write_batch(name, @table, batch(2..2))

      assert {:ok, [_first]} = Store.list(runtime.store, "analytics/events")

      :sys.replace_state(pid, fn state -> %{state | log: held} end)
    end
  end

  describe "a schema change against a near-full buffer" do
    test "flushes the accumulator rather than shedding the batch", context do
      %{name: name, runtime: runtime} =
        start_buffer_service(context,
          dir: Path.join(context.tmp_dir, "near-full"),
          max_buffered_rows: 4,
          flush_interval_ms: 60_000
        )

      first = Task.async(fn -> Client.write_batch(name, @table, batch(1..3)) end)
      assert Eventually.until(fn -> accumulated_rows(name) == 3 end)

      changed = Schema.new!([{"id", :int64, nullable: false}])
      rows = for i <- 1..3, do: %{"id" => i}

      second =
        Task.async(fn -> Client.write_batch(name, @table, %{schema: changed, rows: rows}) end)

      assert {:ok, %{row_count: 3}} = Task.await(first)
      assert Eventually.until(fn -> accumulated_rows(name) == 3 end)
      assert Client.flush(name, @table) == :ok
      assert {:ok, %{row_count: 3}} = Task.await(second)

      assert match?([_first, _second], HotManifest.entries(runtime.manifest, @table))
    end
  end

  defp accumulated_rows(name) do
    case Registry.lookup(Runtime.registry(name), @table) do
      [{pid, _value}] -> :sys.get_state(pid).row_count
      [] -> 0
    end
  end

  defp accumulating?(name) do
    case Registry.lookup(Runtime.registry(name), @table) do
      [{pid, _value}] -> :sys.get_state(pid).pending != []
      [] -> false
    end
  end

  describe "crash safety" do
    test "acked rows survive the buffer being killed", %{name: name, runtime: runtime} do
      {:ok, ack} = Client.write_batch(name, @table, batch(1..3))

      [{pid, _value}] = Registry.lookup(Runtime.registry(name), @table)
      Process.exit(pid, :kill)

      assert Eventually.until(fn ->
               match?({:ok, [_entry]}, Client.hot_manifest(name, @table))
             end)

      assert {:ok, [entry]} = Client.hot_manifest(name, @table)
      assert entry.id == ack.segment_id

      assert {:ok, second} = Client.write_batch(name, @table, batch(4..5))
      assert second.row_count == 2

      ids = runtime.manifest |> HotManifest.entries(@table) |> Enum.map(& &1.id)
      assert ack.segment_id in ids
      assert second.segment_id in ids
    end

    test "acked rows survive the whole subtree restarting", context do
      %{name: name, runtime: runtime} = context

      {:ok, ack} = Client.write_batch(name, @table, batch(1..3))

      stop_supervised!(name)

      %{name: restarted} =
        start_buffer_service(context, name: name, dir: Path.join(context.tmp_dir, "buffer"))

      assert {:ok, [entry]} = Client.hot_manifest(restarted, @table)
      assert entry.id == ack.segment_id

      {:ok, _ack} = Client.write_batch(restarted, @table, batch(4..4))

      recovered = HotManifest.entries(runtime.manifest, @table)

      assert ack.segment_id in Enum.map(recovered, & &1.id)
      assert match?([_first, _second], recovered)
    end

    test "a graceful shutdown flushes the accumulated tail", context do
      %{name: name, runtime: runtime} = start_buffer_service(context, flush_interval_ms: 60_000)

      task = Task.async(fn -> Client.write_batch(name, @table, batch(1..3)) end)

      assert Eventually.until(fn -> accumulated_rows(name) == 3 end)

      [{pid, _value}] = Registry.lookup(Runtime.registry(name), @table)
      GenServer.stop(pid, :normal)

      assert {:ok, ack} = Task.await(task)
      assert [entry] = HotManifest.entries(runtime.manifest, @table)
      assert entry.id == ack.segment_id
    end

    test "a killed buffer loses only rows nobody was acked for", context do
      %{name: name, runtime: runtime} = start_buffer_service(context, flush_interval_ms: 60_000)

      test_pid = self()

      {writer, ref} =
        spawn_monitor(fn ->
          send(test_pid, {:acked, Client.write_batch(name, @table, batch(1..3))})
        end)

      assert Eventually.until(fn -> accumulating?(name) end)

      [{pid, _value}] = Registry.lookup(Runtime.registry(name), @table)
      Process.exit(pid, :kill)

      assert_receive {:DOWN, ^ref, :process, ^writer, :normal}
      assert_received {:acked, {:error, {:badrpc, {:EXIT, {:killed, _call}}}}}
      assert HotManifest.entries(runtime.manifest, @table) == []
      assert {:ok, []} = Store.list(runtime.store, "analytics/events")
    end
  end
end
