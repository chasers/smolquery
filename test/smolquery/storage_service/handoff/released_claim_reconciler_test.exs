defmodule Smolquery.StorageService.Handoff.ReleasedClaimReconcilerTest do
  @moduledoc """
  The durable reconciler for the F-1 residuals (T-386, `tla/FINDINGS.md`,
  `tla/ReleasedClaim.tla`), driven end to end through production paths only.

  Everything runs as deployed (`Smolquery.Test.FullNode`): writes enter
  through `BufferService.Client.write_batch/3`, the valves freeze and release
  claims, real seal signals reach the real `Sealer` running the real
  `Handoff.Seal`, the reconcile signal fires from the owner's own maintenance
  tick, and every read goes through `QueryService.Client.query/3` — the
  planner, a job, a private engine. The only staging is snabbkaffe nemesis
  over the production tracepoints: a crash or a delay injected at
  `storage.seal.before_retire`, matched structurally to the two-id original
  claim so the one-id re-derived claims run free.

  The two residual schedules the T-385 gate fix cannot reach:

    * **crash-after-register** — the attempt dies between register and
      retire; a released claim is never re-signalled, so nothing retries.
    * **reap-before-retire** — the attempt's retire lands after the grace
      reaper deleted the entries; the fence has no evidence and returns
      `:ok`.

  Both strand a registered orphan double-counting the rows until the
  reconciler drops it: the release records a tombstone, the owner re-signals
  `reconcile_released` once the re-derived claims sealed every released id,
  and the storage side drops the orphan and acks the tombstone away.
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

  defp tombstones(node) do
    {:ok, runtime} = BufferService.Runtime.fetch(node.buffer)

    BufferService.HotManifest.tombstones(runtime.manifest, @table)
  end

  # Tombstones first: buffer-side ETS, no catalog read and no query job
  # until reconciliation actually finished — polling the catalog while the
  # seal commits write the same sqlite metadata is lock churn against the
  # very work being awaited.
  defp reconciled?(node) do
    tombstones(node) == [] and FullNode.sealed_count(node) == 2 and
      FullNode.query_ids(node) == [1, 2, 3, 4]
  end

  test "crash-after-register: the reconciler drops the orphan the dead attempt stranded",
       context do
    node = FullNode.start(context, @frozen_valves)

    log =
      capture_log(fn ->
        check_trace(
          fn ->
            inject_crash(%{:"$kind" => :"storage.seal.before_retire", ids: [_, _]})

            write(node.buffer, 1..2)
            write(node.buffer, 3..4)

            {:ok, _} = block_until(%{:"$kind" => :"storage.seal.registered", ids: [_, _]}, 15_000)

            assert FullNode.query_ids(node) == [1, 2, 3, 4]

            FullNode.restart_buffer(context, node, @shrunk_valves)

            assert Eventually.until(fn -> reconciled?(node) end, 200, 100)
          end,
          fn _result, trace ->
            assert [_re1, _re2] =
                     trace
                     |> of_kind(:"storage.seal.retired")
                     |> Enum.filter(&match?(%{ids: [_], result: :ok}, &1))
          end
        )
      end)

    assert log =~ "released oversized claim"
    assert log =~ "dropped 1 registered segment(s) of a released claim"
    assert FullNode.query_ids(node) == [1, 2, 3, 4]
  end

  test "reap-before-retire: the fence has no evidence, and the reconciler still converges",
       context do
    node = FullNode.start(context, @frozen_valves)

    log =
      capture_log(fn ->
        check_trace(
          fn ->
            force_ordering(
              delay: %{:"$kind" => :"storage.seal.before_retire", ids: [_, _]},
              until: %{:"$kind" => :test_reaped}
            )

            m1 = write(node.buffer, 1..2)
            m2 = write(node.buffer, 3..4)

            {:ok, _} = block_until(%{:"$kind" => :"storage.seal.registered", ids: [_, _]}, 15_000)

            FullNode.restart_buffer(
              context,
              node,
              Keyword.put(@shrunk_valves, :retire_grace_ms, 0)
            )

            assert Eventually.until(
                     fn ->
                       {:ok, entries} = Client.hot_manifest(node.buffer, @table)

                       not Enum.any?(entries, &(&1.id in [m1, m2]))
                     end,
                     400,
                     25
                   )

            tp(:test_reaped, %{})

            assert Eventually.until(fn -> reconciled?(node) end, 200, 100)
          end,
          fn _result, trace ->
            assert [%{result: :ok}] =
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
