defmodule Smolquery.QueryService.ClickHouseFunctions do
  @moduledoc """
  ClickHouse's function names, as DuckDB macros every job engine defines
  (T-485, PL-66).

  A ClickHouse client writes `toStartOfInterval(toDateTime(Timestamp),
  INTERVAL 1 minute)`, and the engine knows `time_bucket`. The names do not
  collide — ClickHouse's are camelCase compounds DuckDB has none of — so a
  macro per name lets the client's SQL run as written, with no rewrite.

  ## Only the statement that names one pays for it

  Defining all of them costs a fresh engine 26 ms (74 statements, measured
  2026-09-20), which every query would pay, from every edge. So none is
  defined at engine start. `statements_for/1` reads the SQL a job is about
  to run and answers the definitions of the functions it names — three for
  HyperDX's histogram, none for a statement written in the engine's own
  dialect, which pays one scan of its text. The runner defines them on the
  job's engine before it plans, since planning may run part of the statement,
  and a scatter worker does the same for its partial SQL, so the
  macros are wherever the statement runs, on whatever node. They live in the
  query service, not in the ClickHouse edge, for that reason.
  `Runtime.clickhouse_functions` switches them off.

  ## Every macro is stable

  The Top-N planner (`Smolquery.QueryService.TopN`) probes a statement only
  when every function it names gives the same answer twice, and the engine's
  catalog reports no stability for a macro. `stable?/1` answers for these:
  each body calls only functions the catalog reports `CONSISTENT` or
  `CONSISTENT_WITHIN_QUERY` (`now64` reads the clock, as `now()` does, which
  the planner accepts). That is a rule for whoever adds a macro, and a test
  asks the engine about every body, so a macro over `random()` fails the
  suite rather than giving a last-N query the wrong rows (T-504).

  ## The ones that are not

  ClickHouse's `rand()` cannot be stable, and HyperDX calls it: it orders
  the rows Event Patterns and Event Deltas sample by it, and thins a large
  table's sidebar values with `cityHash64(ts, rand()) % n = 0` (T-526). So
  `rand`, `rand32`, `rand64` and `randCanonical` are macros over `random()`
  that `stable?/1` does not vouch for. The planner then asks the catalog
  about the name, as it does for any function it does not know, finds a
  macro with no stability, and leaves the statement unprobed, which is the
  right answer for a statement ordered by a random number. They are listed
  apart (`volatile/0`) so that the rule above still reads as a rule.

  A name is matched without regard to case, as the engine resolves it, and
  only before a `(`. A column or a table of the same name defines a macro
  nobody calls, which costs a fraction of a millisecond and changes nothing.

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
  - `groupUniqArray`, `groupUniqArrayArray`, `groupArray` and their `-If`
    forms take their size as a last argument: the edge writes ClickHouse's
    `groupUniqArray(20)(x)` as `groupUniqArray(x, 20)`. Which values a
    capped one keeps is not ClickHouse's choice, in either engine.
  - `getSubcolumn(map, 'keys')` and `'values'` are a map's two halves; no
    other subcolumn exists here.
  - `clickhouse_isNull` and `clickhouse_isNotNull` stand in for `isNull`
    and `isNotNull`, which the parser keeps as words of its own.
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
            {"quantileIf(x, c, p)", "quantile_cont(x, p) FILTER (WHERE c)"},
            {"groupArray(x)", "list(x), (x, n) AS list_slice(list(x), 1, n)"},
            {"groupArrayIf(x, c)",
             "list(x) FILTER (WHERE c), (x, c, n) AS list_slice(list(x) FILTER (WHERE c), 1, n)"},
            {"groupUniqArray(x)",
             "list(DISTINCT x), (x, n) AS list_slice(list(DISTINCT x), 1, n)"},
            {"groupUniqArrayIf(x, c)",
             "list(DISTINCT x) FILTER (WHERE c), " <>
               "(x, c, n) AS list_slice(list(DISTINCT x) FILTER (WHERE c), 1, n)"},
            {"groupUniqArrayArray(x)",
             "list_distinct(flatten(list(x))), " <>
               "(x, n) AS list_slice(list_distinct(flatten(list(x))), 1, n)"},
            {"getSubcolumn(m, part)",
             "CASE part WHEN 'keys' THEN map_keys(m) WHEN 'values' THEN map_values(m) END"},
            {"lowCardinalityKeys(x)", "x"},
            {"toJSONString(x)", "CAST(to_json(x) AS VARCHAR)"},
            {"cityHash64(a)",
             "hash(a), (a, b) AS hash(a, b), (a, b, c) AS hash(a, b, c), (a, b, c, d) AS hash(a, b, c, d)"},
            {"JSONDynamicPathsWithTypes(x)",
             "(SELECT map_from_entries(list((substr(node.fullkey, 3), CASE node.\"type\" WHEN 'VARCHAR' THEN 'String' WHEN 'DOUBLE' THEN 'Float64' WHEN 'BOOLEAN' THEN 'Bool' WHEN 'ARRAY' THEN 'Array(Nullable(String))' ELSE 'Int64' END) ORDER BY node.id)) FROM json_tree(CAST(x AS JSON)) AS node WHERE node.\"type\" NOT IN ('OBJECT', 'NULL') AND node.fullkey <> '$' AND NOT regexp_matches(node.path, '\\[[0-9]+\\]') AND NOT EXISTS (SELECT 1 FROM json_tree(CAST(x AS JSON)) AS holder WHERE holder.id = node.parent AND holder.\"type\" = 'ARRAY'))"},
            {"groupUniqArrayMap(m)",
             "coalesce((SELECT map_from_entries(list((path, kinds) ORDER BY path)) FROM (SELECT entry.key AS path, list_sort(list(DISTINCT entry.value)) AS kinds FROM (SELECT unnest(flatten(list(map_entries(m)) FILTER (WHERE m IS NOT NULL))) AS entry) GROUP BY entry.key)), map_from_entries(CAST([] AS STRUCT(k VARCHAR, v VARCHAR[])[])))"},
            {"dynamicType(x)",
             "CASE WHEN x IS NULL THEN 'None' ELSE CASE regexp_extract(variant_typeof(x), '^[A-Z]+') " <>
               "WHEN 'VARCHAR' THEN 'String' WHEN 'BOOL' THEN 'Bool' " <>
               "WHEN 'DOUBLE' THEN 'Float64' WHEN 'FLOAT' THEN 'Float64' WHEN 'DECIMAL' THEN 'Float64' " <>
               "WHEN 'INT' THEN 'Int64' WHEN 'UINT' THEN 'Int64' WHEN 'TINYINT' THEN 'Int64' " <>
               "WHEN 'SMALLINT' THEN 'Int64' WHEN 'INTEGER' THEN 'Int64' WHEN 'BIGINT' THEN 'Int64' " <>
               "WHEN 'UTINYINT' THEN 'Int64' WHEN 'USMALLINT' THEN 'Int64' WHEN 'UINTEGER' THEN 'Int64' " <>
               "WHEN 'UBIGINT' THEN 'Int64' WHEN 'ARRAY' THEN 'Array(Nullable(String))' " <>
               "WHEN 'OBJECT' THEN 'JSON' WHEN 'VARIANT' THEN 'None' WHEN 'TIMESTAMP' THEN 'DateTime64(6)' " <>
               "WHEN 'DATE' THEN 'Date' ELSE 'String' END END"},
            {"leftUTF8(s, n)", "left(s, n)"},
            {"clickhouse_MD5(x)", "unhex(md5(x))"},
            {"clickhouse_isNull(x)", "x IS NULL"},
            {"clickhouse_isNotNull(x)", "x IS NOT NULL"},
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

  @uint32_draw "CAST(floor(random() * 4294967296) AS UINTEGER)"

  @volatile [
    {"rand()", @uint32_draw},
    {"rand32()", @uint32_draw},
    {"rand64()", "(CAST(#{@uint32_draw} AS UBIGINT) << 32) | CAST(#{@uint32_draw} AS UBIGINT)"},
    {"randCanonical()", "random()"}
  ]

  name_of = fn {signature, _body} -> signature |> String.split("(", parts: 2) |> hd() end

  @names Enum.map(@macros, name_of)
  @volatile_names Enum.map(@volatile, name_of)
  @stable_names MapSet.new(@names, &String.downcase/1)
  @aggregate_names ~w(sumif avgif minif maxif quantileif grouparray grouparrayif groupuniqarray
                      groupuniqarrayif groupuniqarrayarray groupuniqarraymap uniq uniqexact)

  @definitions Map.new(@macros ++ @volatile, fn {signature, body} = macro ->
                 {String.downcase(name_of.(macro)),
                  "CREATE OR REPLACE MACRO #{signature} AS #{body}"}
               end)

  @called Regex.compile!(
            "(?<![\\w.\"])(" <>
              Enum.map_join(Map.keys(@definitions), "|", &Regex.escape/1) <> ")\\s*\\(",
            "i"
          )

  @doc """
  The `CREATE OR REPLACE MACRO` statements, one per function.
  """
  @spec statements() :: [String.t()]
  def statements, do: @definitions |> Map.values() |> Enum.sort()

  @doc """
  The definitions of the functions `sql` names, and no others.
  """
  @spec statements_for(String.t()) :: [String.t()]
  def statements_for(sql) when is_binary(sql) do
    @called
    |> Regex.scan(sql, capture: :all_but_first)
    |> Enum.map(fn [name] -> String.downcase(name) end)
    |> Enum.uniq()
    |> Enum.sort()
    |> Enum.map(&Map.fetch!(@definitions, &1))
  end

  @doc """
  Whether `name`, in any case, is one of these macros, all of which are
  stable (see the moduledoc).
  """
  @spec stable?(String.t()) :: boolean()
  def stable?(name) when is_binary(name), do: MapSet.member?(@stable_names, String.downcase(name))

  @doc """
  The names of the macros that are not stable, as ClickHouse spells them:
  defined like the rest, and never vouched for by `stable?/1`.
  """
  @spec volatile() :: [String.t()]
  def volatile, do: @volatile_names

  @doc """
  Whether `name`, in any case, is one of these macros and an aggregate: its
  body aggregates the rows it is called over. The engine's catalog calls
  every macro a macro, so a reader that must know an aggregate when it sees
  one (`Smolquery.QueryService.Decomposer`) asks here.
  """
  @spec aggregate?(String.t()) :: boolean()
  def aggregate?(name) when is_binary(name), do: String.downcase(name) in @aggregate_names

  @doc """
  The names defined, as ClickHouse spells them.
  """
  @spec names() :: [String.t()]
  def names, do: @names
end
