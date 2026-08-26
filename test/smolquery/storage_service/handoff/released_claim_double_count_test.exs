defmodule Smolquery.StorageService.Handoff.ReleasedClaimDoubleCountTest do
  @moduledoc """
  Regression test for TLA+ finding F-1 (`tla/FINDINGS.md`,
  `tla/ReleasedClaim.tla`), driven end to end through production paths only.

  The T-294 fence must stop a *released* oversized claim's in-flight seal
  attempt from leaving its segment registered. The bug: `claim_live` and the
  retire key-fence skipped entries whose `sealed_at` was already set, so if
  the re-derived valve-sized claims sealed the released claim's
  micro-segments before the original attempt's retire, every gate passed and
  the original's oversized segment double-counted the rows forever. The
  T-385 fix treats a `sealed_at` stamped under a different claim key as
  stale, so the original attempt's retire refuses and `compensate_stale`
  drops the orphan.

  Everything runs as deployed (`Smolquery.Test.FullNode`): the valves freeze
  the claim, a real seal signal starts the real attempt, a buffer restart
  under shrunk valves releases and re-derives it, and the reads go through
  `QueryService.Client.query/3`. The only staging is snabbkaffe
  `force_ordering` over the production tracepoints: the two-id original
  attempt is parked right before its retire until both one-id re-derived
  seals have retired — the exact schedule TLC found.
  """

  use ExUnit.Case, async: false
  use Snabbkaffex

  import ExUnit.CaptureLog

  alias Smolquery.BufferService
  alias Smolquery.BufferService.Client
  alias Smolquery.Test.Eventually
  alias Smolquery.Test.FullNode

  @moduletag :integration
  @moduletag :tmp_dir

  @table {"analytics", "events"}

  @frozen_valves [
    seal_max_files: 2,
    seal_max_bytes: 1_000_000_000,
    seal_max_age_ms: 600_000,
    retire_grace_ms: 600_000,
    seal_retry_ms: 600_000
  ]

  @shrunk_valves [
    seal_max_files: 1,
    seal_max_bytes: 1_000_000_000,
    seal_max_age_ms: 600_000,
    claim_valve_factor: 1,
    retire_grace_ms: 600_000,
    seal_retry_ms: 25
  ]

  defp batch(range), do: %{schema: FullNode.schema(), rows: for(i <- range, do: %{"id" => i})}

  defp write(buffer, range) do
    {:ok, ack} = Client.write_batch(buffer, @table, batch(range))
    ack.segment_id
  end

  # Gated on buffer-side ETS state first: polling the catalog or spawning a
  # query job every tick fights the seal commits for the DuckLake sqlite
  # lock, which is the very work the poll is waiting on. The tombstone
  # clears last, so the catalog and the query are read after the dust
  # settles.
  defp settled?(node) do
    tombstones(node) == [] and FullNode.sealed_count(node) == 2 and
      FullNode.query_ids(node) == [1, 2, 3, 4]
  end

  defp tombstones(node) do
    {:ok, runtime} = BufferService.Runtime.fetch(node.buffer)

    BufferService.HotManifest.tombstones(runtime.manifest, @table)
  end

  test "F-1: a released claim's in-flight attempt is refused and its orphan compensated",
       context do
    node = FullNode.start(context, @frozen_valves)

    log =
      capture_log(fn ->
        check_trace(
          fn ->
            force_ordering(
              delay: %{:"$kind" => :"storage.seal.before_retire", ids: [_, _]},
              until: %{:"$kind" => :"storage.seal.retired", ids: [_]},
              count: 2
            )

            write(node.buffer, 1..2)
            write(node.buffer, 3..4)

            {:ok, _} = block_until(%{:"$kind" => :"storage.seal.registered", ids: [_, _]}, 15_000)

            assert FullNode.query_ids(node) == [1, 2, 3, 4]

            FullNode.restart_buffer(context, node, @shrunk_valves)

            assert Eventually.until(fn -> settled?(node) end, 200, 100)
          end,
          fn _result, trace ->
            assert [%{result: {:error, {:stale_claim, _diff}}}] =
                     trace
                     |> of_kind(:"storage.seal.retired")
                     |> Enum.filter(&match?(%{ids: [_, _]}, &1))
          end
        )
      end)

    assert log =~ "released oversized claim"
    assert log =~ "dropped 1 registered segment(s) of a released claim"
    assert FullNode.query_ids(node) == [1, 2, 3, 4]
  end
end
