defmodule SmolqueryPg.PostgrexTest do
  @moduledoc """
  Postgrex — a real Postgres driver — against the wire edge (PL-58 layer 3).

  This is the full driver path: the `pg_type` bootstrap at connect (simple
  protocol, real OIDs), then Parse/Bind/Describe/Execute with binary
  results for every query.
  """

  use ExUnit.Case, async: false

  alias Smolquery.Catalog
  alias Smolquery.QueryService
  alias Smolquery.Schema
  alias Smolquery.Test.FixedCatalog
  alias Smolquery.Test.MapCatalog
  alias SmolqueryPg.Runtime

  @moduletag :integration
  @password "postgrex-test-password"

  setup do
    unique = :erlang.unique_integer([:positive])
    query = :"pg_postgrex_query_#{unique}"
    pg = :"pg_postgrex_edge_#{unique}"

    catalog = MapCatalog.new()
    :ok = Catalog.create_dataset(catalog, "analytics")
    :ok = Catalog.create_table(catalog, {"analytics", "events"}, Schema.new!([{"id", :int64}]))

    start_supervised!(
      {QueryService.Supervisor,
       name: query, catalog: FixedCatalog.new(%{snapshot: 1, schemas: %{}, segments: %{}})},
      id: query
    )

    on_exit(fn -> QueryService.Runtime.delete(query) end)

    start_supervised!(
      {SmolqueryPg.Supervisor,
       name: pg, password: @password, query_name: query, port: 0, catalog: catalog},
      id: pg
    )

    on_exit(fn -> Runtime.delete(pg) end)

    {:ok, {_ip, port}} = SmolqueryPg.Supervisor.bound(pg)

    {:ok, conn} =
      Postgrex.start_link(
        hostname: "127.0.0.1",
        port: port,
        username: "smolquery",
        password: @password,
        database: "smolquery",
        show_sensitive_data_on_connection_error: true
      )

    %{conn: conn}
  end

  test "connects (the pg_type bootstrap) and runs typed queries", %{conn: conn} do
    result =
      Postgrex.query!(
        conn,
        "SELECT 42::BIGINT AS i, 1.5::DOUBLE AS f, 'x' AS s, true AS b, " <>
          "DATE '2026-08-01' AS d, TIMESTAMP '2026-08-01 10:00:00' AS ts, " <>
          "12.50::DECIMAL(38,2) AS n, NULL::VARCHAR AS z",
        []
      )

    assert result.columns == ~w(i f s b d ts n z)

    assert [[42, 1.5, "x", true, ~D[2026-08-01], ~N[2026-08-01 10:00:00.000000], numeric, nil]] =
             result.rows

    assert Decimal.equal?(numeric, Decimal.new("12.50"))
  end

  test "binds parameters through the extended protocol", %{conn: conn} do
    result =
      Postgrex.query!(conn, "SELECT $1::bigint + $2::bigint AS sum, $3::text AS s", [
        40,
        2,
        "it's"
      ])

    assert result.rows == [[42, "it's"]]
  end

  test "reads the emulated catalog", %{conn: conn} do
    result =
      Postgrex.query!(
        conn,
        "SELECT table_schema, table_name FROM information_schema.tables WHERE table_schema = 'analytics'",
        []
      )

    assert result.rows == [["analytics", "events"]]
  end

  test "a query error carries its SQLSTATE", %{conn: conn} do
    assert {:error, %Postgrex.Error{postgres: %{code: :undefined_table}}} =
             Postgrex.query(conn, "SELECT * FROM nope.missing", [])
  end
end
