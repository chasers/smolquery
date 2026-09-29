defmodule Smolquery.Engine.LogTest do
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog

  alias Smolquery.Engine
  alias Smolquery.Engine.Log
  alias Smolquery.Engine.Result

  doctest Log

  @http ~s|{'request': {'type': GET, 'url': 'https://bucket.s3.amazonaws.com/sealed/a.parquet?X-Amz-Signature=abc', | <>
          ~s|'headers': {User-Agent='duckdb/v1.5.3', Authorization='AWS4-HMAC-SHA256 Credential=AKIAEXAMPLE/20260929//s3/aws4_request, | <>
          ~s|Signature=c622', x-amz-security-token=tok-abc, Range='bytes=100-940', Host='bucket'}, | <>
          ~s|'start_time': '2026-09-29 14:56:17.602757', 'duration_ms': 12}, | <>
          ~s|'response': {'status': PartialContent_206, 'reason': Partial Content, 'headers': {Content-Length=840}}}|

  describe "parse/1" do
    test "groups types by role, and answers nothing for nothing" do
      assert Log.parse("compact:QueryLog, catalog:QueryLog,catalog:QueryLog") ==
               [catalog: ["QueryLog"], compact: ["QueryLog"]]

      assert Log.parse(nil) == []
      assert Log.parse("") == []
    end

    test "refuses an unknown role or a type that is not a word" do
      assert_raise ArgumentError, ~r/SMOLQUERY_DUCKDB_LOG/, fn -> Log.parse("query:QueryLog") end

      assert_raise ArgumentError, ~r/SMOLQUERY_DUCKDB_LOG/, fn ->
        Log.parse("catalog:Query'); --")
      end

      assert_raise ArgumentError, ~r/SMOLQUERY_DUCKDB_LOG/, fn -> Log.parse("catalog") end
    end
  end

  describe "redact/2" do
    test "an HTTP row keeps method, URL, range, status and duration, and no header" do
      redacted = Log.redact("HTTP", @http)

      assert redacted ==
               "method=GET url=https://bucket.s3.amazonaws.com/sealed/a.parquet " <>
                 "range=bytes=100-940 status=PartialContent_206 duration_ms=12"

      for secret <- ["AKIAEXAMPLE", "c622", "tok-abc", "X-Amz-Signature", "abc"] do
        refute redacted =~ secret
      end
    end

    test "a CREATE SECRET is replaced whole, and other statements pass as they are" do
      assert Log.redact("QueryLog", "CREATE OR REPLACE SECRET s (TYPE s3, SECRET 'x')") ==
               "CREATE SECRET <redacted>"

      assert Log.redact("QueryLog", "  create temporary secret h (TYPE http)") ==
               "CREATE SECRET <redacted>"

      assert Log.redact("QueryLog", "SELECT 1") == "SELECT 1"
    end

    test "a catalog ATTACH, a URL and an S3 setting lose their credentials, and keep the rest" do
      attach =
        "ATTACH IF NOT EXISTS 'ducklake:postgres:dbname=smolquery host=catalog.internal " <>
          "user=app password=hunter2 sslmode=require' AS \"lake\" (DATA_PATH 's3://b/lake')"

      redacted = Log.redact("QueryLog", attach)
      refute redacted =~ "hunter2"
      assert redacted =~ "password=<redacted> sslmode=require"
      assert redacted =~ "host=catalog.internal"

      refute Log.redact("QueryLog", "ATTACH 'postgres://app:hunter2@db:5432/x'") =~ "hunter2"
      refute Log.redact("QueryLog", "SET s3_secret_access_key='abc'") =~ "abc"
      refute Log.redact("QueryLog", "SET s3_session_token = zzsecret") =~ "zzsecret"
    end
  end

  test "boot_engines/2 starts a drain per configured role that has an engine" do
    assert Log.boot_engines(%{catalog: :cat, compact: :comp},
             catalog: ["QueryLog"],
             merge: ["HTTP"]
           ) ==
             [{Log, engine: :cat, types: ["QueryLog"]}]
  end

  describe "a drain" do
    setup do
      engine = __MODULE__.Lake
      start_supervised!({Engine, name: engine})
      Logger.put_module_level(Log, :info)
      on_exit(fn -> Logger.delete_module_level(Log) end)

      %{engine: engine}
    end

    test "writes the engine's statements to the log, secrets redacted, then truncates", %{
      engine: engine
    } do
      log =
        capture_log([level: :info], fn ->
          {:ok, _drain} = Log.start(engine, ["QueryLog"], interval_ms: 50)
          Engine.query!(engine, "SELECT 4242 AS marker")
          Engine.query!(engine, "CREATE SECRET leak (TYPE s3, KEY_ID 'AKIAX', SECRET 'sekret')")
          Process.sleep(300)
          :ok = Log.stop(engine)
        end)

      assert log =~ "SELECT 4242 AS marker"
      assert log =~ "CREATE SECRET <redacted>"
      refute log =~ "sekret"
      refute log =~ "duckdb_logs"

      assert %Result{rows: [[0]]} = Engine.query!(engine, "SELECT count(*) FROM duckdb_logs")
    end

    test "loses no row written between drains, however often they run", %{engine: engine} do
      log =
        capture_log([level: :info], fn ->
          {:ok, _drain} = Log.start(engine, ["QueryLog"], interval_ms: 10)
          for n <- 1..200, do: Engine.query!(engine, "SELECT #{n} AS mark_#{n}")
          :ok = Log.stop(engine)
        end)

      for n <- 1..200, do: assert(log =~ "AS mark_#{n}\n")
    end

    test "caps the rows one drain writes, and says how many it dropped", %{engine: engine} do
      log =
        capture_log([level: :info], fn ->
          {:ok, _drain} = Log.start(engine, ["QueryLog"], interval_ms: 200, max_rows: 1)
          for n <- 1..5, do: Engine.query!(engine, "SELECT #{n}")
          Process.sleep(400)
          :ok = Log.stop(engine)
        end)

      assert log =~ ~r/dropped \d+ row\(s\) over its cap/
    end

    test "turns logging off when its time is up", %{engine: engine} do
      {:ok, drain} = Log.start(engine, ["QueryLog"], interval_ms: 50, for_ms: 100)
      ref = Process.monitor(drain)

      assert_receive {:DOWN, ^ref, :process, ^drain, :normal}, 2_000

      assert %Result{rows: [[off]]} =
               Engine.query!(engine, "SELECT current_setting('enable_logging')")

      assert off in [false, 0]
    end

    test "Engine.log/3 takes minutes, and a second call retargets the same drain", %{
      engine: engine
    } do
      {:ok, first} = Engine.log(engine, ["QueryLog"], for: 1)
      {:ok, second} = Engine.log(engine, ["QueryLog", "HTTP"], for: 1)

      assert first == second
      assert Process.alive?(first)
      :ok = Log.stop(engine)
    end

    test "a runtime call on a boot drain retargets it, and it outlives the call's time", %{
      engine: engine
    } do
      boot = start_supervised!({Log, engine: engine, types: ["QueryLog"], interval_ms: 50})

      assert {:ok, ^boot} = Log.start(engine, ["QueryLog", "HTTP"], for_ms: 100)
      Process.sleep(300)

      assert Process.alive?(boot)

      assert %Result{rows: [[on]]} =
               Engine.query!(engine, "SELECT current_setting('enable_logging')")

      assert on in [true, 1]
    end

    test "drains on a connection of its own, so a busy engine connection does not hold it", %{
      engine: engine
    } do
      busy = Process.whereis(Engine.connection_name(engine))

      log =
        capture_log([level: :info], fn ->
          {:ok, _drain} = Log.start(engine, ["QueryLog"], interval_ms: 50)
          Engine.query!(engine, "SELECT 777 AS marker")
          :ok = :sys.suspend(busy)
          Process.sleep(300)
          :ok = :sys.resume(busy)
          :ok = Log.stop(engine)
        end)

      assert log =~ "SELECT 777 AS marker"
    end
  end
end
