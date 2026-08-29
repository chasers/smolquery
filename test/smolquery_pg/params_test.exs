defmodule SmolqueryPg.ParamsTest do
  use ExUnit.Case, async: true

  alias SmolqueryPg.Params

  describe "oids/2" do
    test "keeps declared OIDs, reads cast hints, and defaults to text" do
      assert Params.oids("SELECT $1, $2::bigint, $3", [0, 0, 0]) == [25, 20, 25]
      assert Params.oids("SELECT $1", [701]) == [701]
      assert Params.oids("SELECT $2::date, $1", []) == [25, 1082]
      assert Params.oids("SELECT 1", []) == []
      assert Params.oids("SELECT $1::timestamp with time zone", [0]) == [1184]
    end
  end

  describe "substitute/2" do
    test "renders each type as a literal the engine reads" do
      sql = "SELECT $1, $2, $3, $4, $5, $6, $7, $8, $9"

      values = [
        {20, 0, "42"},
        {701, 0, "1.5"},
        {16, 0, "t"},
        {25, 0, "it's"},
        {1114, 0, "2026-08-01 10:00:00"},
        {1082, 0, "2026-08-01"},
        {1700, 0, "12.50"},
        {3802, 0, ~s|{"a":1}|},
        {25, 0, nil}
      ]

      assert Params.substitute(sql, values) ==
               {:ok,
                "SELECT 42, 1.5, TRUE, 'it''s', TIMESTAMP '2026-08-01 10:00:00', " <>
                  "DATE '2026-08-01', 12.50, '{\"a\":1}'::JSON, NULL"}
    end

    test "binary parameters decode by OID" do
      values = [
        {20, 1, <<-7::64-signed>>},
        {701, 1, <<2.5::float-64>>},
        {16, 1, <<1>>},
        {1082, 1, <<1::32-signed>>},
        {1114, 1, <<0::64-signed>>},
        {25, 1, "text"}
      ]

      assert Params.substitute("SELECT $1, $2, $3, $4, $5, $6", values) ==
               {:ok,
                "SELECT -7, 2.5, TRUE, DATE '2000-01-02', TIMESTAMP '2000-01-01 00:00:00.000000', 'text'"}
    end

    test "a numeric in binary decodes to its decimal text" do
      digits = <<3::16, 1::16-signed, 0::16, 2::16, 12::16, 3456::16, 7800::16>>

      assert Params.substitute("SELECT $1", [{1700, 1, digits}]) == {:ok, "SELECT 123456.78"}
    end

    test "a value that is not what its OID says is refused, naming the parameter" do
      assert {:error, {:invalid_parameter, 2, {:invalid_integer, "x"}}} =
               Params.substitute("SELECT $1, $2", [{25, 0, "a"}, {20, 0, "x"}])
    end

    test "a placeholder inside a string or a comment stays" do
      assert Params.substitute("SELECT $1, '$1' /* $1 */", [{20, 0, "1"}]) ==
               {:ok, "SELECT 1, '$1' /* $1 */"}
    end

    test "text never reaches the SQL unquoted" do
      injection = "'; DROP TABLE x; --"

      assert Params.substitute("SELECT $1", [{25, 0, injection}]) ==
               {:ok, "SELECT '''; DROP TABLE x; --'"}

      assert {:error, _reason} = Params.substitute("SELECT $1", [{1700, 0, "1; DROP"}])
    end
  end

  test "with_typed_nulls/2 casts each placeholder to its declared type" do
    assert Params.with_typed_nulls("SELECT $1, $2 FROM t WHERE a = $1", [20, 25]) ==
             "SELECT NULL::BIGINT, NULL::VARCHAR FROM t WHERE a = NULL::BIGINT"
  end
end
