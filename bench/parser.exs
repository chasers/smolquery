Code.require_file("support.exs", __DIR__)

defmodule Bench.Parser do
  @moduledoc """
  What DuckDB's SQL parser costs on the query path, per statement (T-635).

  Every query is parsed by DuckDB before it runs: `Smolquery.QueryService.Planner`
  sends one statement that calls `json_serialize_sql` three times and
  `json_deserialize_sql` once, for the AST and the canonical text, and the
  Postgres edge's catalog classifier does the same. DuckDB 2.0 replaces the
  parser with a PEG one, reported as "10x slower"; this script prices it on the
  statements smolquery actually sends, so the driver bump can be decided on
  numbers. Run it once per pinned driver and compare.

  Three statements per corpus entry, each a full round trip through
  `Smolquery.Engine.query/2` on a bare engine:

    * **literal** — `SELECT length('<sql>')`: the round trip and the outer
      parse of the statement as a string literal, and no parse of it.
    * **serialize** — `SELECT json_serialize_sql('<sql>')`: one parse and one
      AST serialization. **parse** is serialize less literal.
    * **planner** — the planner's own parse statement, verbatim.

  The corpus is built from the code that produces it, not pasted: HyperDX's
  search session from the ClickStack fixture through the ClickHouse edge's
  quoting, parameter substitution and rewrite; MetricsQL aggregates through
  `SmolqueryVictoriaMetrics.Pushdown.sql/2`; a pgjdbc catalog probe through
  `SmolqueryPg.PgCatalog.Rewrite.pre/2`; and BigQuery-style SELECTs, with
  `bench/pg_wire.exs`'s 3.6 KB statement as the large one. `ok` is whether
  DuckDB parsed the statement: `y`, `n` for a refusal, or `bad` for an AST
  that is not valid JSON, which the planner cannot read either.

      mix run bench/parser.exs
      REPS=2000 mix run bench/parser.exs

  """

  import Bench.Support

  alias Smolquery.Engine
  alias Smolquery.Identifier
  alias SmolqueryClickHouse.Params
  alias SmolqueryClickHouse.Rewrite
  alias SmolqueryClickHouse.Statement
  alias SmolqueryPg.PgCatalog
  alias SmolqueryVictoriaMetrics.Eval.Constants
  alias SmolqueryVictoriaMetrics.MetricsQL
  alias SmolqueryVictoriaMetrics.Pushdown
  alias SmolqueryVictoriaMetrics.Runtime

  @engine __MODULE__.Engine
  @hyperdx "test/support/fixtures/clickstack/hyperdx_search.json"
  @hyperdx_steps ~w(rows histogram rows_term count_field_filters key_values rows_underscore_term)

  @metricsql [
    ~s|sum by (job) (rate(http_requests_total{job="api"}[5m]))|,
    ~s|quantile(0.9, max_over_time(latency_seconds{env="prod", region=~"us-.*"}[10m]))|,
    ~s|avg without (instance) (last_over_time(node_load1[1m]))|
  ]

  @context %{
    start_ms: 1_789_812_000_000,
    end_ms: 1_789_815_600_000,
    step_ms: 60_000,
    lookback_ms: 300_000,
    timestamps: Enum.to_list(1_789_812_000_000..1_789_815_600_000//60_000)
  }

  def main do
    reps = env("REPS", 500)
    {:ok, _pid} = Engine.start_link(name: @engine, extensions: [])
    %{rows: [[version]]} = Engine.query!(@engine, "SELECT version()")

    heading("DuckDB #{version} — parse cost per statement, median of #{reps} (us)")

    IO.puts(
      label("statement", 34) <>
        pad("bytes", 7) <>
        pad("ok", 5) <>
        pad("literal", 9) <>
        pad("serialize", 11) <> pad("parse", 8) <> pad("planner", 9)
    )

    totals = Enum.map(corpus(), &measure(&1, reps))

    IO.puts(
      "\n" <>
        label("sum", 34) <>
        pad("", 12) <>
        pad(sum(totals, :literal), 9) <>
        pad(sum(totals, :serialize), 11) <>
        pad(sum(totals, :parse), 8) <> pad(sum(totals, :planner), 9)
    )
  end

  defp measure({name, sql}, reps) do
    quoted = Identifier.sql_string(sql)
    serialize = "SELECT json_serialize_sql(#{quoted})"

    planner =
      "SELECT json_serialize_sql(#{quoted}), " <>
        "CASE WHEN json_extract_string(json_serialize_sql(#{quoted}), '$.error') = 'false' " <>
        "THEN json_deserialize_sql(json_serialize_sql(#{quoted})) END"

    %{rows: [[ast]]} = Engine.query!(@engine, serialize)

    literal_us = median_us("SELECT length(#{quoted})", reps)
    serialize_us = median_us(serialize, reps)
    planner_us = median_us(planner, reps)
    parse_us = serialize_us - literal_us

    IO.puts(
      label(name, 34) <>
        pad(byte_size(sql), 7) <>
        pad(parsed(ast), 5) <>
        pad(literal_us, 9) <>
        pad(serialize_us, 11) <> pad(parse_us, 8) <> pad(planner_us, 9)
    )

    %{literal: literal_us, serialize: serialize_us, parse: parse_us, planner: planner_us}
  end

  defp parsed(ast) do
    case JSON.decode(ast) do
      {:ok, %{"error" => false}} -> "y"
      {:ok, _refused} -> "n"
      {:error, _invalid} -> "bad"
    end
  end

  defp median_us(sql, reps), do: timed(fn -> Engine.query!(@engine, sql) end, reps).median

  defp sum(totals, key), do: totals |> Enum.map(& &1[key]) |> Enum.sum()

  defp corpus do
    [
      {"select 1", "SELECT 1"},
      {"bq point lookup", "SELECT id, ts, name FROM analytics.events WHERE id = 42"},
      {"bq time-range aggregate",
       "SELECT date_trunc('hour', ts) AS hour, name, count(*) AS n, sum(amount) AS total " <>
         "FROM analytics.events WHERE ts >= TIMESTAMP '2026-01-01' " <>
         "AND ts < TIMESTAMP '2026-01-02' GROUP BY ALL ORDER BY hour, total DESC LIMIT 100"}
    ] ++ hyperdx() ++ metricsql() ++ [{"pgjdbc getColumns", pgjdbc()}, {"wide 3.6 KB", wide()}]
  end

  defp hyperdx do
    %{"steps" => steps} = @hyperdx |> File.read!() |> JSON.decode!()

    for %{"step" => step, "sql" => sql, "params" => params} <- steps, step in @hyperdx_steps do
      url_params = Map.new(params, fn {key, value} -> {"param_" <> key, value} end)
      {:ok, statement} = sql |> Statement.standard_quoting() |> Params.substitute(url_params)
      {"hyperdx #{step}", Rewrite.call(statement)}
    end
  end

  defp metricsql do
    runtime = Runtime.new(name: __MODULE__.VictoriaMetrics, password: "bench")

    for query <- @metricsql do
      {:ok, expr} = MetricsQL.parse(query)
      {:ok, plan} = Pushdown.plan(Constants.prepare(expr), @context)
      {:ok, sql, _params} = Pushdown.sql(plan, runtime)
      {"metricsql #{plan.aggregate}(#{plan.rollup})", sql}
    end
  end

  defp pgjdbc do
    PgCatalog.Rewrite.pre(
      """
      SELECT * FROM (SELECT n.nspname,c.relname,a.attname,a.atttypid,
        a.attnotnull OR (t.typtype = 'd' AND t.typnotnull) AS attnotnull,a.atttypmod,a.attlen,
        t.typtypmod,row_number() OVER (PARTITION BY a.attrelid ORDER BY a.attnum) AS attnum,
        nullif(a.attidentity, '') as attidentity,nullif(a.attgenerated, '') as attgenerated,
        pg_catalog.pg_get_expr(def.adbin, def.adrelid) AS adsrc,dsc.description,t.typbasetype,t.typtype
      FROM pg_catalog.pg_namespace n
        JOIN pg_catalog.pg_class c ON (c.relnamespace = n.oid)
        JOIN pg_catalog.pg_attribute a ON (a.attrelid=c.oid)
        JOIN pg_catalog.pg_type t ON (a.atttypid = t.oid)
        LEFT JOIN pg_catalog.pg_attrdef def ON (a.attrelid=def.adrelid AND a.attnum = def.adnum)
        LEFT JOIN pg_catalog.pg_description dsc ON (c.oid=dsc.objoid AND a.attnum = dsc.objsubid)
        LEFT JOIN pg_catalog.pg_class dc ON (dc.oid=dsc.classoid AND dc.relname='pg_class')
        LEFT JOIN pg_catalog.pg_namespace dn ON (dc.relnamespace=dn.oid AND dn.nspname='pg_catalog')
      WHERE c.relkind in ('r','p','v','f','m') and a.attnum > 0 AND NOT a.attisdropped
        AND n.nspname LIKE 'analytics' AND c.relname LIKE 'events') c
      WHERE true ORDER BY nspname,c.relname,attnum
      """,
      %{}
    )
  end

  defp wide do
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

Bench.Parser.main()
