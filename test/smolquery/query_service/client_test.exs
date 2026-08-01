defmodule Smolquery.QueryService.ClientTest do
  use ExUnit.Case, async: false

  alias Explorer.DataFrame
  alias Smolquery.QueryService
  alias Smolquery.QueryService.Client
  alias Smolquery.QueryService.Runtime
  alias Smolquery.Test.Eventually
  alias Smolquery.Test.FixedCatalog

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
    test "runs a query and returns the finished job with its frame" do
      name = start_service()

      assert {:ok, job, frame} = Client.query(name, "SELECT 1 + 1 AS n")

      assert job.state == :done
      assert job.row_count == 1
      assert is_integer(job.duration_ms)
      assert DataFrame.to_columns(frame)["n"] == [2]
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
