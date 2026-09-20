defmodule Smolquery.QueryService.ClickHouseFunctions do
  @moduledoc """
  ClickHouse's function names, as DuckDB macros every job engine defines
  (T-485, PL-66).

  A ClickHouse client writes `toStartOfInterval(toDateTime(Timestamp),
  INTERVAL 1 minute)`, and the engine knows `time_bucket`. The names do not
  collide — ClickHouse's are camelCase compounds DuckDB has none of — so a
  macro per name lets the client's SQL run as written, with no rewrite and
  no cost to a statement that calls none. The macros live in the query
  service, not in the ClickHouse edge, because a job engine may run on a
  node the edge is not on (`Smolquery.QueryService.JobEngine.options/1`);
  `Runtime.clickhouse_functions` switches them off.

  Only what DuckDB's parser accepts as an ordinary call is here. Syntax it
  refuses — `quantile(0.95)(x)`, `CAST(x, 'Float64')` — is rewritten on the
  edge (`SmolqueryClickHouse.Rewrite`), since a macro cannot reach it.

  ## What differs from ClickHouse

  - Timestamps are UTC and `toDateTime` answers a `TIMESTAMP` truncated to
    the second, where ClickHouse answers a `DateTime`. A number is not taken
    as seconds since the epoch.
  - `toStartOfInterval` and the `toStartOf*` family bucket from
    1970-01-01, as ClickHouse does, so a 7-day bucket starts on a Thursday
    in both.
  - `hasToken` splits on every character that is not an ASCII letter or
    digit, as ClickHouse's tokenizer does, but scans the value: there is no
    token index behind it.
  - `uniq` is exact (`count(DISTINCT x)`); ClickHouse's is approximate.
  - `notEmpty` and `empty` answer `1` or `0`, since clients compare them to
    a number (`notEmpty(x) = 1`). `mapContains` answers a boolean.
  - `indexHint(x)` is `TRUE`: it only ever told ClickHouse which index to
    read, and the predicate beside it still filters.
  - The `-OrZero`, `-OrNull` and `-OrDefault` casts answer `0`, `NULL` and
    `0` for a value that does not parse, as ClickHouse's do.
  """

  @epoch "TIMESTAMP '1970-01-01'"

  @buckets [
    {"toStartOfSecond", "date_trunc('second', CAST(x AS TIMESTAMP))"},
    {"toStartOfMinute", "date_trunc('minute', CAST(x AS TIMESTAMP))"},
    {"toStartOfFiveMinutes", "time_bucket(INTERVAL 5 MINUTE, CAST(x AS TIMESTAMP), #{@epoch})"},
    {"toStartOfTenMinutes", "time_bucket(INTERVAL 10 MINUTE, CAST(x AS TIMESTAMP), #{@epoch})"},
    {"toStartOfFifteenMinutes",
     "time_bucket(INTERVAL 15 MINUTE, CAST(x AS TIMESTAMP), #{@epoch})"},
    {"toStartOfHour", "date_trunc('hour', CAST(x AS TIMESTAMP))"},
    {"toStartOfDay", "date_trunc('day', CAST(x AS TIMESTAMP))"},
    {"toStartOfMonth", "CAST(date_trunc('month', CAST(x AS TIMESTAMP)) AS DATE)"},
    {"toStartOfYear", "CAST(date_trunc('year', CAST(x AS TIMESTAMP)) AS DATE)"}
  ]

  @casts [
    {"toInt64", "BIGINT"},
    {"toInt32", "INTEGER"},
    {"toUInt64", "UBIGINT"},
    {"toUInt32", "UINTEGER"},
    {"toUInt8", "UTINYINT"},
    {"toFloat64", "DOUBLE"},
    {"toFloat32", "FLOAT"}
  ]

  @macros [
            {"toDateTime(x)", "date_trunc('second', CAST(x AS TIMESTAMP))"},
            {"toDateTime64(x, p)", "CAST(x AS TIMESTAMP)"},
            {"toDate(x)", "CAST(x AS DATE)"},
            {"toStartOfInterval(x, i)", "time_bucket(i, CAST(x AS TIMESTAMP), #{@epoch})"},
            {"fromUnixTimestamp(x)", "make_timestamp(CAST(x AS BIGINT) * 1000000)"},
            {"fromUnixTimestamp64Milli(x)", "make_timestamp(CAST(x AS BIGINT) * 1000)"},
            {"fromUnixTimestamp64Micro(x)", "make_timestamp(CAST(x AS BIGINT))"},
            {"fromUnixTimestamp64Nano(x)", "make_timestamp_ns(CAST(x AS BIGINT))"},
            {"toUnixTimestamp(x)", "epoch_ms(CAST(x AS TIMESTAMP)) // 1000"},
            {"toUnixTimestamp64Milli(x)", "epoch_ms(x)"},
            {"toUnixTimestamp64Micro(x)", "epoch_us(x)"},
            {"toUnixTimestamp64Nano(x)", "epoch_ns(x)"},
            {"now64()", "CAST(now() AS TIMESTAMP), (p) AS CAST(now() AS TIMESTAMP)"},
            {"parseDateTime64BestEffort(s)",
             "CAST(s AS TIMESTAMP_NS), (s, p) AS CAST(s AS TIMESTAMP_NS)"},
            {"parseDateTimeBestEffort(s)", "CAST(s AS TIMESTAMP)"},
            {"hasToken(h, n)", "list_contains(regexp_split_to_array(h, '[^a-zA-Z0-9]+'), n)"},
            {"notEmpty(x)", "CAST(COALESCE(length(x) > 0, FALSE) AS UTINYINT)"},
            {"empty(x)", "CAST(COALESCE(length(x) = 0, TRUE) AS UTINYINT)"},
            {"mapContains(m, k)", "map_contains(m, k)"},
            {"mapKeys(m)", "map_keys(m)"},
            {"mapValues(m)", "map_values(m)"},
            {"indexHint(x)", "TRUE"},
            {"has(a, x)", "list_contains(a, x)"},
            {"match(s, p)", "regexp_matches(s, p)"},
            {"startsWith(s, p)", "starts_with(s, p)"},
            {"endsWith(s, p)", "ends_with(s, p)"},
            {"toString(x)", "CAST(x AS VARCHAR)"},
            {"toFloat64OrDefault(x)", "COALESCE(TRY_CAST(x AS DOUBLE), 0)"},
            {"sumIf(x, c)", "sum(x) FILTER (WHERE c)"},
            {"avgIf(x, c)", "avg(x) FILTER (WHERE c)"},
            {"minIf(x, c)", "min(x) FILTER (WHERE c)"},
            {"maxIf(x, c)", "max(x) FILTER (WHERE c)"},
            {"uniq(x)", "count(DISTINCT x)"},
            {"uniqExact(x)", "count(DISTINCT x)"}
          ] ++
            Enum.map(@buckets, fn {name, body} -> {name <> "(x)", body} end) ++
            Enum.flat_map(@casts, fn {name, type} ->
              [
                {name <> "(x)", "CAST(x AS #{type})"},
                {name <> "OrZero(x)", "COALESCE(TRY_CAST(x AS #{type}), 0)"},
                {name <> "OrNull(x)", "TRY_CAST(x AS #{type})"}
              ]
            end)

  @doc """
  The `CREATE OR REPLACE MACRO` statements, one per function.
  """
  @spec statements() :: [String.t()]
  def statements do
    Enum.map(@macros, fn {signature, body} ->
      "CREATE OR REPLACE MACRO #{signature} AS #{body}"
    end)
  end

  @doc """
  The names defined, as ClickHouse spells them.
  """
  @spec names() :: [String.t()]
  def names do
    Enum.map(@macros, fn {signature, _body} ->
      signature |> String.split("(", parts: 2) |> hd()
    end)
  end
end
