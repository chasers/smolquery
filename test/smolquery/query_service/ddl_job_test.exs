defmodule Smolquery.QueryService.DdlJobTest do
  @moduledoc """
  An `ALTER TABLE` submitted as a query job (PL-61 L3): the runner consults
  `Smolquery.Ddl` before the planner, runs the catalog call without an
  engine, and settles with the outcome on `job.ddl`.
  """

  use ExUnit.Case, async: false

  alias Smolquery.Catalog
  alias Smolquery.QueryService
  alias Smolquery.QueryService.Client
  alias Smolquery.QueryService.Runtime
  alias Smolquery.Schema
  alias Smolquery.Test.MapCatalog

  @table {"ds", "t"}

  setup do
    catalog = MapCatalog.new()
    :ok = Catalog.create_dataset(catalog, "ds")

    :ok =
      Catalog.create_table(
        catalog,
        @table,
        Schema.new!([{"id", :int64, nullable: false}, {"ts", :timestamp}])
      )

    name = :"query_ddl_#{:erlang.unique_integer([:positive])}"
    start_supervised!({QueryService.Supervisor, name: name, catalog: catalog}, id: name)
    on_exit(fn -> Runtime.delete(name) end)

    %{name: name, catalog: catalog}
  end

  test "adds a column and settles with the outcome, no frame", %{name: name, catalog: catalog} do
    assert {:ok, job, nil} = Client.query(name, "ALTER TABLE ds.t ADD COLUMN label STRING")

    assert job.state == :done
    assert job.row_count == nil
    assert job.snapshot == nil
    assert is_integer(job.duration_ms)

    assert job.ddl == %{
             operation: :add_column,
             table: @table,
             column: "label",
             performed: true
           }

    {:ok, schema} = Catalog.table_schema(catalog, @table)
    assert Schema.names(schema) == ["id", "ts", "label"]
  end

  test "drops a column; the guards report performed: false", %{name: name} do
    assert {:ok, %{ddl: %{operation: :drop_column, column: "ts", performed: true}}, nil} =
             Client.query(name, "ALTER TABLE ds.t DROP COLUMN ts")

    assert {:ok, %{state: :done, ddl: %{performed: false}}, nil} =
             Client.query(name, "ALTER TABLE ds.t DROP COLUMN IF EXISTS ts")

    assert {:ok, %{state: :done, ddl: %{performed: false}}, nil} =
             Client.query(name, "ALTER TABLE ds.t ADD COLUMN IF NOT EXISTS id INT64")
  end

  test "the catalog's refusals fail the job with their reason", %{name: name} do
    assert {:ok, %{state: :error, error: {:duplicate_columns, ["id"]}}, nil} =
             Client.query(name, "ALTER TABLE ds.t ADD COLUMN id INT64")

    assert {:ok, %{state: :error, error: {:unknown_column, "nope"}}, nil} =
             Client.query(name, "ALTER TABLE ds.t DROP COLUMN nope")

    assert {:ok, %{state: :error, error: {:unknown_table, {"ds", "nope"}}}, nil} =
             Client.query(name, "ALTER TABLE ds.nope DROP COLUMN id")
  end

  test "a statement that begins with ALTER but is not accepted fails with the parser's reason",
       %{name: name} do
    assert {:ok, %{state: :error, error: {:unsupported_ddl, "RENAME"}}, nil} =
             Client.query(name, "ALTER TABLE ds.t RENAME COLUMN ts TO when")

    assert {:ok, %{state: :error, error: {:unsupported_ddl, "NOT NULL"}}, nil} =
             Client.query(name, "ALTER TABLE ds.t ADD COLUMN n BIGINT NOT NULL")
  end

  test "explain, describe and bind parameters are refused before the catalog is touched",
       %{name: name, catalog: catalog} do
    sql = "ALTER TABLE ds.t ADD COLUMN label STRING"

    assert {:ok, %{state: :error, error: :ddl_not_explainable}, nil} =
             Client.query(name, sql, explain: :plan)

    assert {:ok, %{state: :error, error: :ddl_not_explainable}, nil} =
             Client.query(name, sql, describe: true)

    assert {:ok, %{state: :error, error: :ddl_takes_no_params}, nil} =
             Client.query(name, sql, params: [1])

    {:ok, schema} = Catalog.table_schema(catalog, @table)
    assert Schema.names(schema) == ["id", "ts"]
  end

  test "a traced DDL job carries its one phase", %{name: name} do
    assert {:ok, %{state: :done, trace: spans}, nil} =
             Client.query(name, "ALTER TABLE ds.t ADD COLUMN label STRING", trace: true)

    assert Enum.map(spans, & &1.name) == [:ddl]
  end

  test "a DDL job is not cancelled and has no deadline: it settles with the catalog's answer",
       %{name: name, catalog: catalog} do
    {:ok, job} = Client.submit(name, "ALTER TABLE ds.t ADD COLUMN label STRING", timeout_ms: 1)
    :ok = Client.cancel(name, job.id)

    assert {:ok, %{state: :done, ddl: %{performed: true}}, nil} =
             Client.await(name, job.id, 5_000)

    {:ok, schema} = Catalog.table_schema(catalog, @table)
    assert "label" in Schema.names(schema)
  end
end
