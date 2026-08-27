defmodule Smolquery.QueryService.TopNTest do
  use ExUnit.Case, async: false

  alias Smolquery.Engine
  alias Smolquery.Engine.Connection
  alias Smolquery.Engine.Result
  alias Smolquery.Identifier
  alias Smolquery.QueryService.TopN

  @engine __MODULE__.Parser
  @conn Engine.connection_name(@engine)

  @events {"analytics", "events"}
  @users {"analytics", "users"}

  setup_all do
    start_supervised!({Engine, name: @engine, extensions: []})
    :ok
  end

  defp statement(sql) do
    {:ok, result} =
      Connection.query(@conn, "SELECT json_serialize_sql(#{Identifier.sql_string(sql)})")

    %{"statements" => [statement]} = result |> Result.one!() |> JSON.decode!()

    statement
  end

  defp spec(sql, refs \\ [@events]), do: TopN.spec(statement(sql), refs)

  defp entry(id, rows, stats) do
    %{"id" => id, "row_count" => rows, "stats" => stats, "url" => "http://hot.test/#{id}"}
  end

  defp ts(min, max) do
    %{
      "ts" => %{
        "min" => %{"type" => "naive_datetime", "value" => min},
        "max" => %{"type" => "naive_datetime", "value" => max},
        "null_count" => 0
      }
    }
  end

  defp ids(entries), do: Enum.map(entries, & &1["id"])

  describe "spec/2" do
    test "a last-N query over one table qualifies" do
      assert spec("SELECT * FROM analytics.events WHERE project = 'x' ORDER BY ts DESC LIMIT 100") ==
               %{ref: @events, column: "ts", direction: :desc, limit: 100}
    end

    test "an unqualified ORDER BY is ascending, as DuckDB defaults it" do
      assert %{direction: :asc} = spec("SELECT id FROM analytics.events ORDER BY ts LIMIT 5")
    end

    test "the offset rows are read before they are skipped, so they count" do
      assert %{limit: 7} =
               spec("SELECT id FROM analytics.events ORDER BY ts DESC LIMIT 5 OFFSET 2")
    end

    test "the ordering column may be qualified by the table's alias or name" do
      assert %{column: "ts"} =
               spec("SELECT id FROM analytics.events e ORDER BY e.ts DESC LIMIT 5")

      assert %{column: "ts"} =
               spec("SELECT id FROM analytics.events ORDER BY events.ts DESC LIMIT 5")

      assert is_nil(spec("SELECT id FROM analytics.events e ORDER BY events.ts DESC LIMIT 5"))
    end

    test "later ORDER keys do not disqualify: the first key bounds the answer" do
      assert %{column: "ts"} =
               spec("SELECT id FROM analytics.events ORDER BY ts DESC, id ASC LIMIT 5")
    end

    test "a WHERE the pruner cannot read is fine: the probe runs it verbatim" do
      assert %{column: "ts"} =
               spec(
                 "SELECT id FROM analytics.events WHERE level IN (1, 2) AND msg LIKE '%x%' ORDER BY ts DESC LIMIT 5"
               )
    end

    test "no LIMIT, no ORDER BY, or a LIMIT without ORDER BY does not qualify" do
      assert is_nil(spec("SELECT id FROM analytics.events ORDER BY ts DESC"))
      assert is_nil(spec("SELECT id FROM analytics.events LIMIT 5"))
      assert is_nil(spec("SELECT id FROM analytics.events"))
    end

    test "a LIMIT that is not a constant, or a percentage, does not qualify" do
      assert is_nil(spec("SELECT id FROM analytics.events ORDER BY ts DESC LIMIT 1 + 1"))
      assert is_nil(spec("SELECT id FROM analytics.events ORDER BY ts DESC LIMIT 10%"))
      assert is_nil(spec("SELECT id FROM analytics.events ORDER BY ts DESC LIMIT 0"))
    end

    test "an ORDER BY that is not a plain column does not qualify" do
      assert is_nil(spec("SELECT id FROM analytics.events ORDER BY 1 DESC LIMIT 5"))
      assert is_nil(spec("SELECT id FROM analytics.events ORDER BY lower(msg) DESC LIMIT 5"))
      assert is_nil(spec("SELECT id FROM analytics.events ORDER BY ts + INTERVAL 1 DAY LIMIT 5"))
    end

    test "NULLS FIRST does not qualify: a null lives in any entry" do
      assert is_nil(spec("SELECT id FROM analytics.events ORDER BY ts DESC NULLS FIRST LIMIT 5"))

      assert %{column: "ts"} =
               spec("SELECT id FROM analytics.events ORDER BY ts DESC NULLS LAST LIMIT 5")
    end

    test "a select-list alias shadowing the ordering column does not qualify" do
      assert is_nil(spec("SELECT -ts AS ts FROM analytics.events ORDER BY ts DESC LIMIT 5"))

      assert is_nil(
               spec("SELECT * REPLACE (-ts AS ts) FROM analytics.events ORDER BY ts DESC LIMIT 5")
             )

      assert %{column: "ts"} =
               spec("SELECT ts AS t FROM analytics.events ORDER BY ts DESC LIMIT 5")
    end

    test "a column rename on the FROM does not qualify" do
      assert is_nil(spec("SELECT a FROM analytics.events AS e(a, b) ORDER BY a DESC LIMIT 5"))
    end

    test "DISTINCT, GROUP BY, HAVING, QUALIFY, and SAMPLE do not qualify" do
      assert is_nil(spec("SELECT DISTINCT id FROM analytics.events ORDER BY ts DESC LIMIT 5"))

      assert is_nil(
               spec(
                 "SELECT ts, count(*) FROM analytics.events GROUP BY ts ORDER BY ts DESC LIMIT 5"
               )
             )

      assert is_nil(spec("SELECT ts FROM analytics.events GROUP BY ALL ORDER BY ts DESC LIMIT 5"))

      assert is_nil(
               spec(
                 "SELECT ts FROM analytics.events GROUP BY ts HAVING count(*) > 1 ORDER BY ts DESC LIMIT 5"
               )
             )

      assert is_nil(
               spec(
                 "SELECT id FROM analytics.events QUALIFY row_number() OVER () = 1 ORDER BY ts DESC LIMIT 5"
               )
             )

      assert is_nil(
               spec("SELECT id FROM analytics.events USING SAMPLE 10% ORDER BY ts DESC LIMIT 5")
             )
    end

    test "a second reference to any table does not qualify: the view is shared" do
      assert is_nil(
               spec(
                 "SELECT e.id FROM analytics.events e JOIN analytics.users u ON u.id = e.user_id ORDER BY e.ts DESC LIMIT 5",
                 [@events, @users]
               )
             )

      assert is_nil(
               spec(
                 "SELECT id FROM analytics.events WHERE id IN (SELECT id FROM analytics.events) ORDER BY ts DESC LIMIT 5"
               )
             )

      assert is_nil(
               spec(
                 "SELECT id, (SELECT count(*) FROM analytics.users) FROM analytics.events ORDER BY ts DESC LIMIT 5",
                 [@events, @users]
               )
             )

      assert is_nil(
               spec(
                 "WITH recent AS (SELECT * FROM analytics.events) SELECT id FROM recent ORDER BY ts DESC LIMIT 5"
               )
             )
    end

    test "a window expression does not qualify: it is computed over every row" do
      assert is_nil(
               spec(
                 "SELECT id, row_number() OVER () FROM analytics.events ORDER BY ts DESC LIMIT 5"
               )
             )
    end

    test "a FROM that is a subquery or an unknown table does not qualify" do
      assert is_nil(
               spec("SELECT id FROM (SELECT * FROM analytics.events) ORDER BY ts DESC LIMIT 5")
             )

      assert is_nil(spec("SELECT id FROM analytics.events ORDER BY ts DESC LIMIT 5", [@users]))
    end
  end

  describe "probe_ast/2" do
    defp probe(sql) do
      statement = statement(sql)
      spec = TopN.spec(statement, [@events])
      json = statement |> TopN.probe_ast(spec) |> JSON.encode!()

      {:ok, result} =
        Connection.query(@conn, "SELECT json_deserialize_sql(#{Identifier.sql_string(json)})")

      Result.one!(result)
    end

    test "selects the ordering column, keeps the FROM and WHERE, limits to n" do
      assert probe(
               "SELECT id, msg FROM analytics.events e WHERE e.project = 'x' AND level IN (1, 2) ORDER BY ts DESC, id LIMIT 100 OFFSET 5"
             ) ==
               ~s|SELECT ts FROM analytics.events AS e WHERE ((e.project = 'x') AND ("level" IN (1, 2))) ORDER BY ts DESC LIMIT 105|
    end

    test "a query without a WHERE probes the whole candidate set" do
      assert probe("SELECT * FROM analytics.events ORDER BY ts LIMIT 3") ==
               "SELECT ts FROM analytics.events ORDER BY ts LIMIT 3"
    end
  end

  describe "candidates/3" do
    @desc %{ref: @events, column: "ts", direction: :desc, limit: 10}
    @asc %{ref: @events, column: "ts", direction: :asc, limit: 10}

    test "DESC takes the entries with the latest max, until the rows cover the budget" do
      entries = [
        entry("01A", 100, ts("2026-08-27T10:00:00", "2026-08-27T10:00:09")),
        entry("01B", 100, ts("2026-08-27T10:00:30", "2026-08-27T10:00:39")),
        entry("01C", 100, ts("2026-08-27T10:00:10", "2026-08-27T10:00:29"))
      ]

      assert ids(TopN.candidates(entries, @desc, 1)) == ["01B"]
      assert ids(TopN.candidates(entries, @desc, 100)) == ["01B"]
      assert ids(TopN.candidates(entries, @desc, 101)) == ["01B", "01C"]
      assert ids(TopN.candidates(entries, @desc, 1_000)) == ["01B", "01C", "01A"]
    end

    test "ASC takes the entries with the earliest min" do
      entries = [
        entry("01A", 100, ts("2026-08-27T10:00:00", "2026-08-27T10:00:59")),
        entry("01B", 100, ts("2026-08-27T10:00:30", "2026-08-27T10:00:39")),
        entry("01C", 100, ts("2026-08-27T10:00:10", "2026-08-27T10:00:29"))
      ]

      assert ids(TopN.candidates(entries, @asc, 150)) == ["01A", "01C"]
    end

    test "an entry without stats for the column is never a candidate" do
      entries = [
        entry("01A", 100, %{}),
        entry("01B", 100, ts("2026-08-27T10:00:30", "2026-08-27T10:00:39")),
        entry("01C", 100, %{"ts" => %{"min" => nil, "max" => nil, "null_count" => 100}})
      ]

      assert ids(TopN.candidates(entries, @desc, 1_000)) == ["01B"]
      assert TopN.candidates([entry("01A", 100, %{})], @desc, 1) == []
    end

    test "edges of mixed types rank without raising: the choice is only tightness" do
      entries = [
        entry("01A", 1, ts("2026-08-27T10:00:00", "2026-08-27T10:00:09")),
        entry("01B", 1, %{"ts" => %{"min" => 1, "max" => 9, "null_count" => 0}}),
        entry("01C", 1, ts("2026-08-27T10:00:30", "2026-08-27T10:00:39"))
      ]

      assert [_first, _second, _third] = TopN.candidates(entries, @desc, 100)
      assert hd(ids(TopN.candidates(entries, @desc, 1))) in ["01C", "01B"]
    end

    test "numeric stats sort as numbers" do
      spec = %{@desc | column: "id"}

      entries = [
        entry("01A", 1, %{"id" => %{"min" => 1, "max" => 9, "null_count" => 0}}),
        entry("01B", 1, %{"id" => %{"min" => 10, "max" => 100, "null_count" => 0}})
      ]

      assert ids(TopN.candidates(entries, spec, 1)) == ["01B"]
    end
  end

  describe "prune/3" do
    test "DESC drops the entries whose max falls short of the bound" do
      entries = [
        entry("01A", 100, ts("2026-08-27T10:00:00", "2026-08-27T10:00:09")),
        entry("01B", 100, ts("2026-08-27T10:00:30", "2026-08-27T10:00:39")),
        entry("01C", 100, ts("2026-08-27T10:00:10", "2026-08-27T10:00:29"))
      ]

      spec = %{ref: @events, column: "ts", direction: :desc, limit: 10}

      assert ids(TopN.prune(entries, spec, ~N[2026-08-27 10:00:29])) == ["01B", "01C"]
      assert ids(TopN.prune(entries, spec, ~N[2026-08-27 10:00:29.000001])) == ["01B"]
    end

    test "ASC drops the entries whose min lies past the bound" do
      entries = [
        entry("01A", 100, ts("2026-08-27T10:00:00", "2026-08-27T10:00:09")),
        entry("01B", 100, ts("2026-08-27T10:00:30", "2026-08-27T10:00:39"))
      ]

      spec = %{ref: @events, column: "ts", direction: :asc, limit: 10}

      assert ids(TopN.prune(entries, spec, ~N[2026-08-27 10:00:05])) == ["01A"]
    end

    test "an entry without stats, or with stats of another type, is kept" do
      entries = [
        entry("01A", 100, %{}),
        entry("01B", 100, %{"ts" => %{"min" => 1, "max" => 2, "null_count" => 0}}),
        entry("01C", 100, ts("2026-08-27T10:00:00", "2026-08-27T10:00:09"))
      ]

      spec = %{ref: @events, column: "ts", direction: :desc, limit: 10}

      assert ids(TopN.prune(entries, spec, ~N[2026-08-27 10:00:30])) == ["01A", "01B"]
    end
  end
end
