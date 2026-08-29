Code.require_file("support.exs", __DIR__)
Code.require_file("../test/support/pg_client.ex", __DIR__)

defmodule Bench.PgWire do
  @moduledoc """
  What the Postgres wire edge's *parsing* layer costs, in isolation
  (PL-58).

  No sockets and no query service: this measures the pure functions a
  query passes through before any job runs — the binary message decode
  (`SmolqueryPg.Protocol`), the statement splitter and keyword classifier
  (`SmolqueryPg.Statements` / `SmolqueryPg.Sql`), and the per-`Bind`
  parameter work (`SmolqueryPg.Params`). The whole-pipeline bench (this
  script's previous life, `git log bench/pg_wire.exs`) showed the job
  machinery dominates end-to-end latency; this answers the narrower
  question of how much headroom the parser itself has.

  Each row is one operation timed over `REPS` iterations: operations per
  second, microseconds per operation, and — where the input is bytes —
  megabytes per second.

      mix run bench/pg_wire.exs
      REPS=1000000 mix run bench/pg_wire.exs
  """

  import Bench.Support

  alias Smolquery.Test.PgClient
  alias SmolqueryPg.Params
  alias SmolqueryPg.Protocol
  alias SmolqueryPg.Sql
  alias SmolqueryPg.Statements

  @small_sql "SELECT $1::bigint + $2::bigint AS sum"
  @multi_sql "BEGIN; SELECT id, ts FROM analytics.events WHERE name = 'a;b' -- trailing; comment\n; COMMIT"

  def main do
    reps = env("REPS", 200_000)

    big_sql = big_sql()
    small_batch = extended_batch(@small_sql)
    big_batch = extended_batch(big_sql)
    two_params = [{20, 1, <<40::64-signed>>}, {20, 1, <<2::64-signed>>}]
    small_oids = Params.oids(@small_sql, [0, 0])

    heading(
      "Wire-edge parser, no pipeline — #{reps} reps per row, " <>
        "small query #{byte_size(@small_sql)} B, big query #{byte_size(big_sql)} B"
    )

    IO.puts(
      label("operation", 42) <>
        pad("ops/s", 12) <> pad("us/op", 9) <> pad("MB/s", 9)
    )

    row("decode extended batch, small (5 msgs)", reps, byte_size(small_batch), fn ->
      decode_all(small_batch)
    end)

    row("decode extended batch, big (5 msgs)", reps, byte_size(big_batch), fn ->
      decode_all(big_batch)
    end)

    row("decode simple Query message", reps, byte_size(query_message(@small_sql)), fn ->
      {:ok, _message, _rest} = Protocol.decode(query_message(@small_sql))
    end)

    row("split multi-statement Query (3 stmts)", reps, byte_size(@multi_sql), fn ->
      [_a, _b, _c] = Statements.split(@multi_sql)
    end)

    row("split big single statement", reps, byte_size(big_sql), fn ->
      [_one] = Statements.split(big_sql)
    end)

    row("leading_keyword", reps, byte_size(@small_sql), fn ->
      "select" = Sql.leading_keyword(@small_sql)
    end)

    row("params: oids (2 casts)", reps, nil, fn ->
      [20, 20] = Params.oids(@small_sql, [0, 0])
    end)

    row("params: substitute 2 binary params", reps, nil, fn ->
      {:ok, _sql} = Params.substitute(@small_sql, typed(small_oids, two_params))
    end)

    IO.puts("")
  end

  defp typed(oids, values) do
    Enum.zip_with(oids, values, fn oid, {_oid, format, bytes} -> {oid, format, bytes} end)
  end

  defp row(name, reps, bytes, fun) do
    fun.()
    {us, :ok} = :timer.tc(fn -> loop(reps, fun) end)
    ops = Float.round(reps * 1_000_000 / us, 0)
    per_op = Float.round(us / reps, 2)

    throughput =
      if bytes, do: Float.round(bytes * reps / us, 1), else: ""

    IO.puts(label(name, 42) <> pad(ops, 12) <> pad(per_op, 9) <> pad(throughput, 9))
  end

  defp loop(0, _fun), do: :ok

  defp loop(n, fun) do
    fun.()
    loop(n - 1, fun)
  end

  defp decode_all(<<>>), do: :ok

  defp decode_all(buffer) do
    {:ok, _message, rest} = Protocol.decode(buffer)

    decode_all(rest)
  end

  defp query_message(sql), do: IO.iodata_to_binary(PgClient.frame(?Q, [sql, 0]))

  defp extended_batch(sql) do
    IO.iodata_to_binary([
      PgClient.parse("", sql, [20, 20]),
      PgClient.bind("", "", [{20, 1, <<40::64-signed>>}, {20, 1, <<2::64-signed>>}], [1]),
      PgClient.describe(?P, ""),
      PgClient.execute("", 0),
      PgClient.frame(?S, [])
    ])
  end

  defp big_sql do
    predicates =
      Enum.map_join(1..40, " AND ", fn n ->
        "(events.col_#{n} > #{n} OR events.name_#{n} = 'value with a fairly long string #{n}')"
      end)

    """
    SELECT events.id, events.ts, events.name, sum(events.amount) AS total,
           count(*) AS n, avg(events.duration_ms) AS avg_ms
    FROM analytics.events AS events
    JOIN analytics.clicks AS clicks ON clicks.id = events.id
    WHERE events.ts > TIMESTAMP '2026-01-01 00:00:00' AND #{predicates}
    GROUP BY events.id, events.ts, events.name
    ORDER BY total DESC
    LIMIT 100
    """
  end
end

Bench.PgWire.main()
