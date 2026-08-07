defmodule Smolquery.Cluster.ConfigStore.PostgresTest do
  @moduledoc """
  The Postgres configuration store against a real Postgres (T-92) — the same
  `TEST_POSTGRES_*` environment the DuckLake Postgres suite uses.

  Scopes must be unique across BEAM runs, not merely within one VM:
  `smolquery_ring_config` lives in the shared test Postgres and
  `ensure/3` is insert-or-fetch (`ON CONFLICT DO NOTHING`). Scopes built
  from `:erlang.unique_integer/1` collide across mix invocations —
  counters restart, leftover rows from a prior advance make the next
  `ensure` return epoch > 0 and the following `advance(..., 0, _)` look
  like a CAS conflict. Random scope keys keep tests from inheriting that
  durable state; the table itself is still created by `setup/1`.
  """

  use ExUnit.Case, async: false

  alias Smolquery.Cluster.ConfigStore.Postgres

  @moduletag :integration

  @node_a :"buffer1@pod.invalid"
  @node_b :"buffer2@pod.invalid"

  setup_all do
    {:ok, conn} = Postgres.start_link(connection_opts())
    :ok = Postgres.setup(conn)

    %{conn: conn}
  end

  setup %{conn: conn} do
    %{conn: conn, scope: unique_scope()}
  end

  test "ensure creates at epoch 0 and a racing ensure returns the winner", ctx do
    assert {:ok, config} = Postgres.ensure(ctx.conn, ctx.scope, [@node_a])
    assert config.epoch == 0
    assert config.members == [@node_a]
    assert config.prev_members == nil

    assert {:ok, again} = Postgres.ensure(ctx.conn, ctx.scope, [@node_b])
    assert again.members == [@node_a]
  end

  test "advance is compare-and-swap and remembers the displaced members", ctx do
    {:ok, _config} = Postgres.ensure(ctx.conn, ctx.scope, [@node_a])

    assert {:ok, advanced} = Postgres.advance(ctx.conn, ctx.scope, 0, [@node_a, @node_b])
    assert advanced.epoch == 1
    assert advanced.members == [@node_a, @node_b]
    assert advanced.prev_members == [@node_a]

    assert Postgres.advance(ctx.conn, ctx.scope, 0, [@node_b]) == {:error, :conflict}
  end

  test "fetch reports an age computed by the database", ctx do
    {:ok, _config} = Postgres.ensure(ctx.conn, ctx.scope, [@node_a])

    assert {:ok, config} = Postgres.fetch(ctx.conn, ctx.scope)
    assert config.age_ms >= 0
    assert config.age_ms < 60_000
  end

  test "fetch of an unknown scope is :not_found", ctx do
    assert Postgres.fetch(ctx.conn, unique_scope()) == :not_found
  end

  test "an empty member list round-trips as empty, not as one blank node", ctx do
    {:ok, _config} = Postgres.ensure(ctx.conn, ctx.scope, [@node_a])
    {:ok, _config} = Postgres.advance(ctx.conn, ctx.scope, 0, [])

    assert {:ok, config} = Postgres.fetch(ctx.conn, ctx.scope)
    assert config.members == []
  end

  defp unique_scope do
    "test:#{Base.encode16(:crypto.strong_rand_bytes(8), case: :lower)}"
  end

  defp connection_opts do
    [
      hostname: System.get_env("TEST_POSTGRES_HOST", "localhost"),
      port: String.to_integer(System.get_env("TEST_POSTGRES_PORT", "5432")),
      username: System.get_env("TEST_POSTGRES_USER", "postgres"),
      password: System.get_env("TEST_POSTGRES_PASSWORD", "postgres"),
      database: System.get_env("TEST_POSTGRES_DATABASE", "postgres")
    ]
  end
end
