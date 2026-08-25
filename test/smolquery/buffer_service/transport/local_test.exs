defmodule Smolquery.BufferService.Transport.LocalTest do
  use ExUnit.Case, async: true

  alias Smolquery.BufferService.Runtime
  alias Smolquery.BufferService.Transport
  alias Smolquery.Schema

  @moduletag :tmp_dir

  @table {"analytics", "events"}

  defp batch, do: %{schema: Schema.new!([{"id", :int64}]), rows: [%{"id" => 1}]}

  describe "invoke/5" do
    test "hands the call straight to the endpoint" do
      assert Transport.Local.invoke(node(), :control, :flush, [:never_started, @table], 5_000) ==
               {:error, :buffer_service_unavailable}
    end

    test "maps a timed-out call to the shape a remote timeout has", context do
      name = :"local_transport_#{:erlang.unique_integer([:positive])}"

      start_supervised!(
        {Smolquery.BufferService.Supervisor,
         name: name,
         dir: Path.join(context.tmp_dir, "buffer"),
         flush_interval_ms: 60_000,
         write_timeout_ms: 1}
      )

      on_exit(fn -> Runtime.delete(name) end)

      assert Transport.Local.invoke(
               node(),
               {:bulk, @table},
               :write_batch,
               [name, @table, batch()],
               1
             ) ==
               {:error, {:badrpc, :timeout}}
    end
  end
end
