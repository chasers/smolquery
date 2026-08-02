defmodule Smolquery.Cluster.KindClusterTest do
  @moduledoc """
  Milestone 8's exit criterion (PL-11), asserted against the running kind
  cluster — the paths that need real distinct hosts and therefore have no
  `:peer` equivalent.

  Every M8 bug that mattered was found here and nowhere else: L7's four on first
  boot (the `ERL_MAX_PORTS` OOM, the Dockerfile never copying `rel/`, k8s service
  links shadowing `SMOLQUERY_API_PORT`, two S3 seams no unit test reached), and
  T-94, which survived a fully green suite because `:peer` cannot give two nodes
  distinct hosts sharing one port. These were a shell script until now, run when
  someone remembered; as tests they are gated, diffable, and runnable one at a
  time.

  Not run by `mix test`: `:cluster` is excluded in `test_helper.exs`. Bring the
  cluster up with `scripts/kind-up.sh`, then `mix test --only cluster`.

  ## These are stateful, and undo themselves

  ExUnit shuffles tests within a module, and these drain a node and kill a pod.
  So every destructive test restores the fleet in `on_exit` and waits for the
  ring to come back — order-independence is arranged rather than assumed. Each
  test also gets its own dataset, because the buffer PVCs, the Postgres catalog,
  and MinIO all outlive a redeploy and a fixed name silently accumulates rows
  across runs.

  The sealing clauses share one `setup_all` dataset: a seal is bounded by
  `seal_max_age_ms` (60 s in the kind overlay), and that wait is worth paying
  once rather than per test.
  """

  use ExUnit.Case, async: false

  alias Smolquery.Test.Eventually
  alias Smolquery.Test.Kind

  @moduletag :cluster
  @moduletag timeout: 900_000

  setup_all do
    if Kind.available?() do
      Kind.await_api!()

      dataset = Kind.unique_dataset("sealed")
      {first, second} = Kind.tables_on_distinct_owners(dataset)

      Kind.create_dataset!(dataset, [first, second])
      Kind.insert!(dataset, first, 200)
      Kind.insert!(dataset, second, 200)

      %{dataset: dataset, tables: [first, second], first: first, second: second}
    else
      raise """
      kube context #{Kind.context()} not found — run scripts/kind-up.sh first.
      """
    end
  end

  describe "the fleet" do
    test "answers a query whose tables two different buffer nodes own", context do
      %{dataset: dataset, first: first, second: second, tables: tables} = context

      owner = Kind.owner_pod({dataset, first})
      other = Kind.owner_pod({dataset, second})

      refute owner == other, "#{first} and #{second} both landed on #{owner}"

      assert Kind.total_rows(dataset, tables) == {:ok, 400}
    end

    test "seals to MinIO through the Postgres catalog and reads back over s3://", context do
      %{dataset: dataset, tables: tables} = context

      assert Eventually.until(fn -> Kind.sealed_object(dataset) != nil end, 60, 3_000),
             "no sealed segment appeared in MinIO within 180s"

      assert Kind.total_rows(dataset, tables) == {:ok, 400}
    end

    test "never double-merges a table, with two storage replicas holding the ring", context do
      %{dataset: dataset, tables: tables} = context

      assert Kind.running_storage_replicas() >= 2,
             "the seal-ownership gate is only under test with more than one storage replica"

      assert Eventually.until(fn -> Kind.sealed_object(dataset) != nil end, 60, 3_000)

      for table <- tables do
        assert Kind.duplicate_rows(dataset, table) == {:ok, 0},
               "#{dataset}.#{table} has rows sharing an id — a segment was merged twice"
      end
    end
  end

  describe "a buffer node leaving" do
    setup do
      dataset = Kind.unique_dataset("drain")
      {first, second} = Kind.tables_on_distinct_owners(dataset)

      Kind.create_dataset!(dataset, [first, second])
      Kind.insert!(dataset, first, 200)
      Kind.insert!(dataset, second, 200)

      %{dataset: dataset, tables: [first, second], first: first}
    end

    test "loses nothing when drained, and new writes route to the remaining owners", context do
      %{dataset: dataset, tables: tables, first: first} = context

      owner = Kind.owner_pod({dataset, first})
      on_exit(fn -> Kind.restore!(owner) end)

      Kind.drain!(owner)

      assert Eventually.until(fn -> match?([_, _], Kind.ring_nodes()) end, 60, 1_000),
             "the drained node never left the ring"

      Kind.insert!(dataset, first, 100, 200)

      assert Kind.total_rows(dataset, tables) == {:ok, 500}
    end

    test "fails queries cleanly when killed, and never answers short", context do
      %{dataset: dataset, tables: tables, first: first} = context

      victim = Kind.owner_pod({dataset, first})
      on_exit(fn -> Kind.await_fleet!() end)

      Kind.kill!(victim)

      for _attempt <- 1..10 do
        outcome = Kind.count("SELECT count(*) AS n FROM #{dataset}.#{first}")

        assert outcome in [{:ok, 200}, {:error, 503}],
               "while #{victim} was down a query answered #{inspect(outcome)} — " <>
                 "acked rows go missing silently rather than failing (T-94)"

        Process.sleep(2_000)
      end

      Kind.await_fleet!()

      assert Eventually.until(
               fn -> Kind.total_rows(dataset, tables) == {:ok, 400} end,
               30,
               3_000
             ),
             "rows did not come back after #{victim} restarted and adopted its tail"
    end
  end
end
