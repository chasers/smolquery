defmodule Smolquery.QueryService.ClientTest do
  use ExUnit.Case, async: false

  alias Explorer.DataFrame
  alias Smolquery.QueryService
  alias Smolquery.QueryService.Client
  alias Smolquery.QueryService.Runtime
  alias Smolquery.Test.Eventually
  alias Smolquery.Test.FixedCatalog

  doctest Client, only: [server_failure_message?: 1]

  @slow "SELECT max(a.range * b.range) FROM range(100000) a, range(100000) b"

  defp start_service(opts \\ []) do
    name = :"query_client_#{:erlang.unique_integer([:positive])}"

    opts =
      Keyword.merge(
        [
          name: name,
          catalog: FixedCatalog.new(%{snapshot: 1, schemas: %{}, segments: %{}})
        ],
        opts
      )

    start_supervised!({QueryService.Supervisor, opts}, id: name)
    on_exit(fn -> Runtime.delete(name) end)

    name
  end

  describe "query/3 (sync)" do
    test "a job's engine is in UTC, as every engine is (T-543)" do
      name = start_service()

      assert {:ok, %{state: :done}, frame} =
               Client.query(name, "SELECT current_setting('TimeZone') AS zone")

      assert DataFrame.to_columns(frame) == %{"zone" => ["UTC"]}
    end

    test "runs a query and returns the finished job with its frame" do
      name = start_service()

      assert {:ok, job, frame} = Client.query(name, "SELECT 1 + 1 AS n")

      assert job.state == :done
      assert job.row_count == 1
      assert is_integer(job.duration_ms)
      assert DataFrame.to_columns(frame)["n"] == [2]
    end

    test "a result with no rows keeps its columns, bounded or not, however the statement ends (T-509)" do
      for opts <- [[], [result_max_rows: :infinity]],
          sql <- [
            "SELECT 1::BIGINT AS n, 'a' AS s WHERE false",
            "SELECT 1::BIGINT AS n, 'a' AS s WHERE false; -- none"
          ] do
        name = start_service(opts)

        assert {:ok, %{state: :done, row_count: 0}, frame} = Client.query(name, sql)
        assert DataFrame.names(frame) == ["n", "s"]
        assert DataFrame.dtypes(frame) == %{"n" => {:s, 64}, "s" => :string}
      end
    end

    test "only the statement's own result says which columns are never NULL: a DESCRIBE's frame is another shape (review of T-510)" do
      name = start_service()
      sql = "SELECT 1 AS a, 2 AS b, 3 AS c, 4 AS d, 5 AS e, 6 AS f"

      assert {:ok, %{non_null_columns: [true, true, true, true, true, true]}, _frame} =
               Client.query(name, sql)

      assert {:ok, %{non_null_columns: :unknown}, described} =
               Client.query(name, sql, explain: :describe)

      assert DataFrame.n_columns(described) == 6

      assert {:ok, %{non_null_columns: :unknown}, nil} = Client.query(name, sql, explain: :plan)
    end

    test "result_max_rows: replaces the runtime's result budget for one job (T-564)" do
      name = start_service(result_max_rows: 5)
      sql = "SELECT range AS n FROM range(10)"

      assert {:ok, %{state: :error, error: {:result_too_large, 5}}, nil} = Client.query(name, sql)

      assert {:ok, %{state: :done, row_count: 10}, _frame} =
               Client.query(name, sql, result_max_rows: 10)

      assert {:ok, %{state: :error, error: {:result_too_large, 3}}, nil} =
               Client.query(name, sql, result_max_rows: 3)
    end

    test "read_engine_threads sets the job engine's DuckDB threads (T-279)" do
      name = start_service(read_engine_threads: 2)

      assert {:ok, job, frame} =
               Client.query(name, "SELECT current_setting('threads') AS threads")

      assert job.state == :done
      assert DataFrame.to_columns(frame)["threads"] == [2]
    end

    test "lockdown denies user SQL the filesystem (PL-8 D7)" do
      name = start_service()

      {:ok, job, nil} = Client.query(name, "SELECT * FROM read_csv('/etc/passwd')")

      assert job.state == :error
      assert job.error == {:unsupported_table_function, "read_csv"}
    end

    test "lockdown denies the readers that engine settings never covered (T-321)" do
      name = start_service()

      for {sql, function} <- [
            {"SELECT path FROM duckdb_databases()", "duckdb_databases"},
            {"SELECT * FROM postgres_scan('host=10.0.0.1', 'public', 't')", "postgres_scan"}
          ] do
        {:ok, job, nil} = Client.query(name, sql)

        assert job.state == :error
        assert job.error == {:unsupported_table_function, function}
      end
    end

    test "lockdown: false restores the trusted posture" do
      name = start_service(lockdown: false, allowed_directories: [])

      tmp = Path.join(System.tmp_dir!(), "smolquery-trusted-#{System.unique_integer()}.csv")
      File.write!(tmp, "a\n1\n")
      on_exit(fn -> File.rm(tmp) end)

      {:ok, job, frame} = Client.query(name, "SELECT * FROM read_csv('#{tmp}')")

      assert job.state == :done
      assert DataFrame.to_columns(frame)["a"] == [1]
    end

    test "a query that cannot plan finishes as an error, which is still an answer" do
      name = start_service()

      assert {:ok, job, nil} = Client.query(name, "DROP TABLE analytics.events")

      assert job.state == :error
      assert {:invalid_query, _message} = job.error
    end

    test "a node not running the query service says so" do
      assert Client.query(:no_such_instance, "SELECT 1") ==
               {:error, :query_service_unavailable}
    end
  end

  describe "submit/3, await/3 and fetch/2 (async)" do
    test "a submitted job is pending, runs, and holds its result" do
      name = start_service()

      assert {:ok, job} = Client.submit(name, "SELECT 40 + 2 AS n")
      assert job.state == :pending

      assert Eventually.until(fn ->
               match?({:ok, %{state: :done}, _frame}, Client.fetch(name, job.id))
             end)

      assert {:ok, done, frame} = Client.fetch(name, job.id)
      assert done.id == job.id
      assert done.row_count == 1
      assert DataFrame.to_columns(frame)["n"] == [42]
    end

    test "await blocks until the job finishes" do
      name = start_service()

      {:ok, job} = Client.submit(name, "SELECT 1")

      assert {:ok, %{state: :done}, _frame} = Client.await(name, job.id, 5_000)
    end

    test "waiting out an await cancels the job rather than orphaning it" do
      name = start_service()

      {:ok, job} = Client.submit(name, @slow)

      assert Client.await(name, job.id, 150) == {:error, :timeout}

      assert Eventually.until(fn ->
               match?({:ok, %{state: :cancelled}, nil}, Client.fetch(name, job.id))
             end)
    end

    test "an unknown job is not found" do
      name = start_service()

      assert Client.fetch(name, "01UNKNOWN") == {:error, :not_found}
    end

    test "a finished job's result expires with its TTL" do
      name = start_service(result_ttl_ms: 50)

      {:ok, job, _frame} = Client.query(name, "SELECT 1")

      assert Eventually.until(fn -> Client.fetch(name, job.id) == {:error, :not_found} end)
    end
  end

  describe "cancel/2" do
    test "cancels a running job" do
      name = start_service()

      {:ok, job} = Client.submit(name, @slow)

      assert Client.cancel(name, job.id) == :ok
      assert {:ok, %{state: :cancelled, error: :cancelled}, nil} = Client.fetch(name, job.id)
    end

    test "cancelling a finished or unknown job is ok" do
      name = start_service()

      {:ok, job, _frame} = Client.query(name, "SELECT 1")

      assert Client.cancel(name, job.id) == :ok
      assert Client.cancel(name, "01UNKNOWN") == :ok
    end
  end

  describe "release/2" do
    test "drops a finished job's frame now, not at its result TTL" do
      name = start_service(result_ttl_ms: 600_000)

      {:ok, job, frame} = Client.query(name, "SELECT 42 AS answer")
      assert {:ok, %{state: :done}, ^frame} = Client.fetch(name, job.id)

      assert Client.release(name, job.id) == :ok
      assert Client.fetch(name, job.id) == {:error, :not_found}
      assert DataFrame.to_columns(frame) == %{"answer" => [42]}
    end

    test "cancels a running job, and releasing an unknown one is ok" do
      name = start_service(max_concurrent_jobs: 1)

      {:ok, job} = Client.submit(name, @slow)

      assert Client.release(name, job.id) == :ok
      assert Client.fetch(name, job.id) == {:error, :not_found}
      assert {:ok, _job} = Client.submit(name, "SELECT 1")
      assert Client.release(name, "01UNKNOWN") == :ok
    end
  end

  describe "service_failure?/1" do
    test "an engine, a worker or the hot tier failing is the service's" do
      for error <- [
            {:engine_failed, :eaddrinuse},
            {:engine_exit, :killed},
            {:query_crashed, :boom},
            {:worker_unreachable, :nodedown},
            {:statement_failed, "ATTACH ...", :locked},
            {:statement_failed, :locked},
            {:hot_tier_unavailable, :econnrefused},
            {:hot_tier_unavailable, {"m", "t"}, :econnrefused},
            {:pinned_hot_retired, {"m", "t"}, ["a"]},
            {:pinned_hot_expired, 10, 5},
            {:invalid_query, "IO Error: Could not read file"},
            %Adbc.Error{message: "Out of Memory Error: failed to allocate block"},
            %Adbc.Error{message: "INTERNAL Error: Attempted to access index 3"}
          ] do
        assert Client.service_failure?(error), inspect(error)
      end
    end

    test "a statement the engine refused is the statement's" do
      for error <- [
            {:invalid_query, "Parser Error: syntax error at or near \"(\""},
            {:invalid_query, "Binder Error: column x not found"},
            %Adbc.Error{message: "Invalid Input Error: invalid perl operator: (?="},
            {:result_too_large, 10},
            :cancelled
          ] do
        refute Client.service_failure?(error), inspect(error)
      end
    end
  end

  describe "deadlines and admission" do
    test "a job past its own deadline cancels itself" do
      name = start_service()

      {:ok, job} = Client.submit(name, @slow, timeout_ms: 150)

      assert Eventually.until(fn ->
               match?(
                 {:ok, %{state: :cancelled, error: :timeout}, nil},
                 Client.fetch(name, job.id)
               )
             end)
    end

    test "a node at max_concurrent_jobs refuses, and frees a slot when a job settles" do
      name = start_service(max_concurrent_jobs: 1)

      {:ok, first} = Client.submit(name, @slow)

      assert Client.submit(name, "SELECT 1") == {:error, :too_many_jobs}

      :ok = Client.cancel(name, first.id)

      assert Eventually.until(fn ->
               match?({:ok, _job}, Client.submit(name, "SELECT 1"))
             end)
    end
  end
end
