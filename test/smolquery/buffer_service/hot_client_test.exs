defmodule Smolquery.BufferService.HotClientTest do
  use ExUnit.Case, async: false

  alias Smolquery.BufferService
  alias Smolquery.BufferService.Client
  alias Smolquery.BufferService.HotClient
  alias Smolquery.BufferService.HotServer
  alias Smolquery.Schema

  @moduletag :tmp_dir

  @table {"analytics", "events"}

  defp batch(range) do
    %{schema: Schema.new!([{"id", :int64}]), rows: for(i <- range, do: %{"id" => i})}
  end

  defp start_buffer(context, opts \\ []) do
    name = :"hot_client_buffer_#{:erlang.unique_integer([:positive])}"

    opts =
      Keyword.merge(
        [name: name, dir: Path.join(context.tmp_dir, "buffer"), flush_interval_ms: 25],
        opts
      )

    start_supervised!({BufferService.Supervisor, opts}, id: name)
    on_exit(fn -> BufferService.Runtime.delete(name) end)

    name
  end

  describe "manifest/3" do
    test "reads the owning node's entries over HTTP", context do
      buffer = start_buffer(context)
      {:ok, ack} = Client.write_batch(buffer, @table, batch(1..3))

      assert {:ok, [entry]} = HotClient.manifest(HotServer.base_url(buffer), @table)
      assert entry["id"] == ack.segment_id
      assert entry["row_count"] == 3
      assert entry["claim_keys"] == []
      assert String.starts_with?(entry["url"], HotServer.base_url(buffer))
    end

    test "an untouched table is an empty manifest, not an error", context do
      buffer = start_buffer(context)

      assert HotClient.manifest(HotServer.base_url(buffer), {"analytics", "absent"}) ==
               {:ok, []}
    end

    test "tolerates a base url with a trailing slash", context do
      buffer = start_buffer(context)
      {:ok, _ack} = Client.write_batch(buffer, @table, batch(1..1))

      assert {:ok, [_entry]} = HotClient.manifest(HotServer.base_url(buffer) <> "/", @table)
    end

    test "reports an unreachable buffer node rather than raising" do
      assert {:error, {:manifest_unreachable, _reason}} =
               HotClient.manifest("http://127.0.0.1:1", @table)
    end

    test "reports a non-200 as a status error", context do
      buffer = start_buffer(context)
      base_url = HotServer.base_url(buffer)

      stop_supervised!(buffer)

      assert match?({:error, {:manifest_status, 503}}, HotClient.manifest(base_url, @table)) or
               match?(
                 {:error, {:manifest_unreachable, _reason}},
                 HotClient.manifest(base_url, @table)
               )
    end

    test "refuses a table name that is not an identifier", context do
      buffer = start_buffer(context)

      assert HotClient.manifest(HotServer.base_url(buffer), {"analytics", "../etc"}) ==
               {:error, {:invalid_identifier, "../etc"}}
    end
  end
end
