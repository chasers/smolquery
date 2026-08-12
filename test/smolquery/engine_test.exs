defmodule Smolquery.EngineTest do
  use ExUnit.Case, async: false

  alias Explorer.DataFrame
  alias Smolquery.Engine
  alias Smolquery.Engine.Result
  alias Smolquery.Engine.ResultTooLarge
  alias Smolquery.Test.Eventually

  @engine __MODULE__.Instance

  setup do
    start_supervised!({Engine, name: @engine})
    :ok
  end

  describe "process naming" do
    test "derives child names from the engine name" do
      assert Engine.database_name(MyEngine) == MyEngine.Database
      assert Engine.connection_name(MyEngine) == MyEngine.Connection
      assert Engine.supervisor_name(MyEngine) == MyEngine.Supervisor
    end

    test "registers the whole subtree" do
      assert is_pid(Process.whereis(Engine.supervisor_name(@engine)))
      assert is_pid(Process.whereis(Engine.database_name(@engine)))
      assert is_pid(Process.whereis(Engine.connection_name(@engine)))
    end
  end

  describe "version/1" do
    test "reports the linked DuckDB version" do
      assert Engine.version(@engine) == "v1.5.3"
    end
  end

  describe "query/3" do
    test "returns the neutral result shape" do
      assert {:ok, %Result{columns: ["n"], rows: [[1]], num_rows: 1}} =
               Engine.query(@engine, "SELECT 1 AS n")
    end

    test "binds positional parameters" do
      assert {:ok, result} =
               Engine.query(@engine, "SELECT $1::int + 1 AS n, $2 AS who", [41, "smol"])

      assert result.columns == ["n", "who"]
      assert result.rows == [[42, "smol"]]
    end

    test "reuses a parameter placeholder" do
      assert {:ok, result} = Engine.query(@engine, "SELECT $1::int AS a, $1::int AS b", [7])

      assert result.rows == [[7, 7]]
    end

    test "returns multiple ordered rows" do
      assert {:ok, result} =
               Engine.query(@engine, "SELECT i FROM range(3) AS t(i) ORDER BY i")

      assert result.rows == [[0], [1], [2]]
      assert result.num_rows == 3
    end

    test "binds a timestamp as TIMESTAMP, the type the columns declare" do
      assert Engine.query!(@engine, "SELECT typeof($1)", [~N[2026-07-31 12:00:00]])
             |> Result.one!() == "TIMESTAMP"
    end

    test "round-trips a timestamp parameter at microsecond precision" do
      ts = ~N[2026-07-31 12:00:00.123456]

      assert Engine.query!(@engine, "SELECT $1", [ts]) |> Result.one!() == ts
    end

    test "accepts a DateTime, which ADBC cannot infer a type for" do
      utc = DateTime.new!(~D[2026-07-31], ~T[12:00:00.123456], "Etc/UTC")

      assert Engine.query!(@engine, "SELECT $1", [utc]) |> Result.one!() ==
               ~N[2026-07-31 12:00:00.123456]
    end

    test "returns an error tuple for invalid SQL rather than crashing" do
      assert {:error, %Adbc.Error{}} = Engine.query(@engine, "SELECT * FROM no_such_table")

      assert {:ok, _} = Engine.query(@engine, "SELECT 1")
    end
  end

  describe "query!/3" do
    test "unwraps a successful result" do
      assert %Result{rows: [[2]]} = Engine.query!(@engine, "SELECT 1 + 1")
    end

    test "raises on error" do
      assert_raise Adbc.Error, fn -> Engine.query!(@engine, "NOT SQL") end
    end
  end

  describe "transaction/2" do
    test "commits every statement together" do
      assert Engine.transaction(@engine, [
               "CREATE TABLE txn_commit (n INTEGER)",
               "INSERT INTO txn_commit VALUES (1), (2)"
             ]) == :ok

      assert Engine.query!(@engine, "SELECT count(*) FROM txn_commit") |> Result.one!() == 2
    end

    test "a failing statement rolls back the ones already applied" do
      assert {:error, %Adbc.Error{}} =
               Engine.transaction(@engine, [
                 "CREATE TABLE txn_rollback (n INTEGER)",
                 "INSERT INTO txn_rollback VALUES (1)",
                 "SELECT * FROM no_such_table"
               ])

      assert {:error, error} = Engine.query(@engine, "SELECT count(*) FROM txn_rollback")
      assert Exception.message(error) =~ "txn_rollback"
    end

    test "returns the failing statement's error, not the rollback's" do
      assert {:error, error} = Engine.transaction(@engine, ["SELECT * FROM no_such_table"])
      assert Exception.message(error) =~ "no_such_table"
    end

    test "the connection is usable after a rollback" do
      {:error, _error} = Engine.transaction(@engine, ["NOT SQL"])

      assert Engine.query!(@engine, "SELECT 1") |> Result.one!() == 1
      assert Engine.transaction(@engine, ["CREATE TABLE txn_after (n INTEGER)"]) == :ok
    end

    test "an empty statement list commits nothing and succeeds" do
      assert Engine.transaction(@engine, []) == :ok
    end
  end

  describe "frame/3" do
    test "returns an Explorer.DataFrame" do
      assert {:ok, frame} = Engine.frame(@engine, "SELECT i FROM range(3) t(i) ORDER BY i")

      assert DataFrame.to_columns(frame, atom_keys: true) == %{i: [0, 1, 2]}
    end

    test "binds positional parameters, timestamps included" do
      assert {:ok, frame} =
               Engine.frame(@engine, "SELECT $1::int AS n, $2 AS ts", [
                 7,
                 ~N[2026-07-31 12:00:00]
               ])

      assert DataFrame.to_columns(frame, atom_keys: true) == %{
               n: [7],
               ts: [~N[2026-07-31 12:00:00.000000]]
             }
    end

    test "is not subject to max_result_rows" do
      start_supervised!({Engine, name: __MODULE__.Framed, max_result_rows: 10}, id: :framed)

      assert {:error, %ResultTooLarge{}} =
               Engine.query(__MODULE__.Framed, "SELECT i FROM range(500) t(i)")

      assert {:ok, frame} = Engine.frame(__MODULE__.Framed, "SELECT i FROM range(500) t(i)")
      assert DataFrame.n_rows(frame) == 500
    end

    test "returns an error tuple for invalid SQL rather than crashing" do
      assert {:error, %Adbc.Error{}} = Engine.frame(@engine, "SELECT * FROM no_such_table")

      assert {:ok, _frame} = Engine.frame(@engine, "SELECT 1")
    end

    test "serializes to Parquet without building Elixir terms" do
      assert {:ok, frame} = Engine.frame(@engine, "SELECT i AS id FROM range(100) t(i)")
      assert {:ok, parquet} = DataFrame.dump_parquet(frame)
      assert is_binary(parquet)

      assert Engine.query!(@engine, "SELECT count(*) FROM read_parquet($1)", [
               write_temp(parquet)
             ])
             |> Result.one!() == 100
    end

    test "frame!/3 raises on error" do
      assert_raise Adbc.Error, fn -> Engine.frame!(@engine, "NOT SQL") end
    end
  end

  describe "max_result_rows" do
    test "refuses to convert a result over the ceiling" do
      start_supervised!({Engine, name: __MODULE__.Capped, max_result_rows: 100}, id: :capped)

      assert {:error, %ResultTooLarge{max: 100}} =
               Engine.query(__MODULE__.Capped, "SELECT i FROM range(101) t(i)")
    end

    test "allows a result exactly at the ceiling" do
      start_supervised!({Engine, name: __MODULE__.AtLimit, max_result_rows: 100}, id: :at_limit)

      assert {:ok, %Result{num_rows: 100}} =
               Engine.query(__MODULE__.AtLimit, "SELECT i FROM range(100) t(i)")
    end

    test "says what to do instead" do
      start_supervised!({Engine, name: __MODULE__.Message, max_result_rows: 1}, id: :message)

      assert {:error, error} = Engine.query(__MODULE__.Message, "SELECT i FROM range(2) t(i)")
      assert Exception.message(error) =~ "Smolquery.Engine.frame/3"
    end

    test "query!/3 raises it" do
      start_supervised!({Engine, name: __MODULE__.Raising, max_result_rows: 1}, id: :raising)

      assert_raise ResultTooLarge, fn ->
        Engine.query!(__MODULE__.Raising, "SELECT i FROM range(2) t(i)")
      end
    end

    test ":infinity lifts the ceiling for a caller that means it" do
      start_supervised!(
        {Engine, name: __MODULE__.Unbounded, max_result_rows: :infinity},
        id: :unbounded
      )

      assert {:ok, %Result{num_rows: 50_000}} =
               Engine.query(__MODULE__.Unbounded, "SELECT i FROM range(50000) t(i)")
    end

    test "leaves the connection usable after refusing" do
      start_supervised!({Engine, name: __MODULE__.Survives, max_result_rows: 10}, id: :survives)

      assert {:error, %ResultTooLarge{}} =
               Engine.query(__MODULE__.Survives, "SELECT i FROM range(11) t(i)")

      assert Engine.query!(__MODULE__.Survives, "SELECT 1") |> Result.one!() == 1
    end
  end

  describe "session settings" do
    test "applies the configured thread count" do
      assert Engine.query!(@engine, "SELECT current_setting('threads')") |> Result.one!() == 2
    end

    test "applies the configured memory limit" do
      limit = Engine.query!(@engine, "SELECT current_setting('memory_limit')") |> Result.one!()

      assert limit =~ "MiB"
    end

    test "per-instance options override application config" do
      start_supervised!({Engine, name: __MODULE__.Override, threads: 1}, id: :override)

      assert Engine.query!(__MODULE__.Override, "SELECT current_setting('threads')")
             |> Result.one!() == 1
    end
  end

  describe "spill isolation (T-201)" do
    test "two defaulted instances use distinct spill directories" do
      start_supervised!({Engine, name: __MODULE__.SpillA}, id: :spill_a)
      start_supervised!({Engine, name: __MODULE__.SpillB}, id: :spill_b)

      assert temp_directory(__MODULE__.SpillA) != temp_directory(__MODULE__.SpillB)
    end

    test "filesystem-safe tokens do not collapse distinct registered names" do
      name_a = :"spill/a"
      name_b = :"spill?a"

      start_supervised!({Engine, name: name_a}, id: :spill_encoded_a)
      start_supervised!({Engine, name: name_b}, id: :spill_encoded_b)

      assert temp_directory(name_a) != temp_directory(name_b)
    end

    test "the directory is named for the instance, so a restart reuses its own" do
      directory = temp_directory(@engine)

      assert Path.basename(directory) =~ "EngineTest.Instance"

      stop_supervised!(Engine)
      start_supervised!({Engine, name: @engine})

      assert temp_directory(@engine) == directory
    end

    test "an explicit directory wins over the derived one" do
      directory =
        Path.join(System.tmp_dir!(), "smolquery-spill-#{System.unique_integer([:positive])}")

      start_supervised!({Engine, name: __MODULE__.SpillStated, temp_directory: directory},
        id: :spill_stated
      )

      assert temp_directory(__MODULE__.SpillStated) == directory
    end

    test "a stated ceiling reaches DuckDB" do
      start_supervised!(
        {Engine, name: __MODULE__.SpillBounded, max_temp_directory_size: "1GiB"},
        id: :spill_bounded
      )

      assert Engine.query!(
               __MODULE__.SpillBounded,
               "SELECT current_setting('max_temp_directory_size')"
             )
             |> Result.one!() == "1.0 GiB"
    end

    test "concurrent spilling instances do not corrupt each other's temp files" do
      # The low memory limit forces DuckDB to spill.
      engines =
        for i <- 1..4 do
          name = Module.concat(__MODULE__, "SpillRace#{i}")

          start_supervised!(
            {Engine, name: name, memory_limit: "32MB", threads: 1, max_result_rows: :infinity},
            id: {:spill_race, i}
          )

          name
        end

      results =
        engines
        |> Task.async_stream(
          fn engine ->
            Engine.query(
              engine,
              "SELECT count(*) AS n FROM (SELECT i, hash(i) AS h FROM range(2000000) t(i) ORDER BY h)"
            )
          end,
          max_concurrency: length(engines),
          timeout: 120_000
        )
        |> Enum.map(fn {:ok, result} -> result end)

      for result <- results do
        assert {:ok, %Result{rows: [[2_000_000]]}} = result
      end
    end
  end

  defp temp_directory(engine) do
    Engine.query!(engine, "SELECT current_setting('temp_directory')") |> Result.one!()
  end

  describe "bootstrap statements" do
    test "re-run on every connection, so session state survives a restart" do
      start_supervised!(
        {Engine,
         name: __MODULE__.Bootstrapped, statements: ["CREATE TABLE marker AS SELECT 42 AS n"]},
        id: :bootstrapped
      )

      assert Engine.query!(__MODULE__.Bootstrapped, "SELECT n FROM marker") |> Result.one!() == 42

      conn = Process.whereis(Engine.connection_name(__MODULE__.Bootstrapped))
      kill_and_await(conn)
      assert await_registered(Engine.connection_name(__MODULE__.Bootstrapped), conn) != conn

      assert Eventually.until(fn -> rebootstrapped?(__MODULE__.Bootstrapped) end)
    end
  end

  describe "supervision" do
    test "a connection crash yields a fresh bootstrapped connection" do
      conn = Process.whereis(Engine.connection_name(@engine))
      database = Process.whereis(Engine.database_name(@engine))

      kill_and_await(conn)

      assert new_conn = await_registered(Engine.connection_name(@engine), conn)
      assert new_conn != conn
      assert Process.whereis(Engine.database_name(@engine)) == database
      assert {:ok, _} = Engine.query(@engine, "SELECT 1")
    end

    test "a database crash rebuilds the connections after it" do
      conn = Process.whereis(Engine.connection_name(@engine))
      database = Process.whereis(Engine.database_name(@engine))

      kill_and_await(database)

      assert await_registered(Engine.database_name(@engine), database)
      assert new_conn = await_registered(Engine.connection_name(@engine), conn)
      assert new_conn != conn
      assert {:ok, _} = Engine.query(@engine, "SELECT 1")
    end
  end

  describe "parquet round-trip" do
    @tag :tmp_dir
    test "reads a segment written by Explorer with types intact", %{tmp_dir: tmp_dir} do
      path = Path.join(tmp_dir, "segment.parquet")

      Explorer.DataFrame.new(
        id: [1, 2],
        ts: [~N[2026-07-31 10:00:00.000000], ~N[2026-07-31 10:00:01.000000]],
        amount: [Decimal.new("1.25"), Decimal.new("2.50")],
        ratio: [1.5, 2.5],
        label: ["a", "b"]
      )
      |> Explorer.DataFrame.to_parquet!(path)

      result = Engine.query!(@engine, "SELECT * FROM read_parquet($1) ORDER BY id", [path])

      assert result.columns == ["id", "ts", "amount", "ratio", "label"]

      assert result.rows == [
               [1, ~N[2026-07-31 10:00:00.000000], Decimal.new("1.25"), 1.5, "a"],
               [2, ~N[2026-07-31 10:00:01.000000], Decimal.new("2.50"), 2.5, "b"]
             ]
    end

    @tag :tmp_dir
    test "Explorer segments carry the min-max stats pruning needs", %{tmp_dir: tmp_dir} do
      path = Path.join(tmp_dir, "segment.parquet")

      Explorer.DataFrame.new(id: [5, 1, 9], label: ["b", "a", "c"])
      |> Explorer.DataFrame.to_parquet!(path)

      stats =
        Engine.query!(
          @engine,
          "SELECT path_in_schema, stats_min_value, stats_max_value FROM parquet_metadata($1)",
          [path]
        )

      assert stats.rows == [["id", "1", "9"], ["label", "a", "c"]]
    end

    @tag :tmp_dir
    test "unions segments across tiers in one plan", %{tmp_dir: tmp_dir} do
      sealed = Path.join(tmp_dir, "sealed.parquet")
      hot = Path.join(tmp_dir, "hot.parquet")

      Explorer.DataFrame.new(id: [1, 2]) |> Explorer.DataFrame.to_parquet!(sealed)
      Explorer.DataFrame.new(id: [3]) |> Explorer.DataFrame.to_parquet!(hot)

      result =
        Engine.query!(
          @engine,
          """
          SELECT count(*) AS n, max(id) AS newest
            FROM read_parquet([$1, $2])
          """,
          [sealed, hot]
        )

      assert result.rows == [[3, 3]]
    end
  end

  defp write_temp(contents) do
    path = Path.join(System.tmp_dir!(), "frame-#{System.unique_integer([:positive])}.parquet")
    File.write!(path, contents)
    on_exit(fn -> File.rm_rf!(path) end)

    path
  end

  defp rebootstrapped?(engine) do
    match?({:ok, %Result{rows: [[42]]}}, Engine.query(engine, "SELECT n FROM marker"))
  catch
    :exit, _mid_restart -> false
  end

  defp kill_and_await(pid) do
    ref = Process.monitor(pid)
    Process.exit(pid, :kill)
    assert_receive {:DOWN, ^ref, :process, ^pid, :killed}, 1_000
  end

  defp await_registered(name, previous, attempts \\ 100) do
    case Process.whereis(name) do
      nil when attempts > 0 ->
        Process.sleep(10)
        await_registered(name, previous, attempts - 1)

      ^previous when attempts > 0 ->
        Process.sleep(10)
        await_registered(name, previous, attempts - 1)

      pid ->
        pid
    end
  end
end
