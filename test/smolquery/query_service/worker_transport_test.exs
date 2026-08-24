defmodule Smolquery.QueryService.WorkerTransportTest do
  use ExUnit.Case, async: true

  alias Smolquery.QueryService.WorkerTransport

  @peer :"query1@elsewhere.invalid"
  @request %{statements: [], partial_sql: "SELECT 1", allowed_paths: []}

  describe "call/5" do
    test "runs a local shard through erpc against this node's runtime" do
      assert WorkerTransport.call(node(), :never_started, @request, "job", 5_000) ==
               {:error, :query_service_unavailable}
    end

    test "refuses a peer outside the cluster before any connect" do
      assert WorkerTransport.call(@peer, :never_started, @request, "job", 5_000) ==
               {:error, {:worker_unreachable, @peer, :nodedown}}
    end
  end

  describe "destination/2" do
    test "a job always lands on the same scatter slot" do
      assert {@peer, {:scatter, slot}} = WorkerTransport.destination(@peer, "job-1")
      assert slot in 1..WorkerTransport.channels()
      assert WorkerTransport.destination(@peer, "job-1") == {@peer, {:scatter, slot}}
    end

    test "distinct jobs spread over the pool" do
      slots =
        for i <- 1..64, uniq: true do
          {@peer, {:scatter, slot}} = WorkerTransport.destination(@peer, "job-#{i}")
          slot
        end

      assert [_, _ | _] = slots
    end
  end
end
