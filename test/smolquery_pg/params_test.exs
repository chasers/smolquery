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

  describe "values/1" do
    test "decodes each type to the term the engine binds" do
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

      assert Params.values(values) ==
               {:ok,
                [
                  42,
                  1.5,
                  true,
                  "it's",
                  ~N[2026-08-01 10:00:00],
                  ~D[2026-08-01],
                  Decimal.new("12.50"),
                  ~s|{"a":1}|,
                  nil
                ]}
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

      assert Params.values(values) ==
               {:ok, [-7, 2.5, true, ~D[2000-01-02], ~N[2000-01-01 00:00:00.000000], "text"]}
    end

    test "a numeric in binary decodes to a Decimal" do
      digits = <<3::16, 1::16-signed, 0::16, 2::16, 12::16, 3456::16, 7800::16>>

      assert {:ok, [decimal]} = Params.values([{1700, 1, digits}])
      assert Decimal.equal?(decimal, Decimal.new("123456.78"))
    end

    test "a NULL binds typed by its declared OID; an undeclared one stays untyped (T-426)" do
      assert {:ok, [%Adbc.Column{field: %Adbc.Field{type: :s64}}, nil]} =
               Params.values([{20, 0, nil}, {25, 0, nil}])
    end

    test "a timestamp infinity binds as the engine's sentinel, never as text (T-426)" do
      assert {:ok, [%NaiveDateTime{year: 294_247}, %NaiveDateTime{year: -290_308}]} =
               Params.values([{1114, 0, "infinity"}, {1114, 0, "-infinity"}])
    end

    test "a bytea binds as a blob column, never as text" do
      assert {:ok, [%Adbc.Column{field: %Adbc.Field{type: :binary}}]} =
               Params.values([{17, 1, <<0, 255>>}])
    end

    test "a value that is not what its OID says is refused, naming the parameter" do
      assert {:error, {:invalid_parameter, 2, {:invalid_integer, "x"}}} =
               Params.values([{25, 0, "a"}, {20, 0, "x"}])
    end

    test "text is a value, never SQL: an injection binds as the string it is" do
      injection = "'; DROP TABLE x; --"

      assert Params.values([{25, 0, injection}]) == {:ok, [injection]}
      assert {:error, _reason} = Params.values([{1700, 0, "1; DROP"}])
    end
  end

  test "with_typed_nulls/2 casts each placeholder to its declared type" do
    assert Params.with_typed_nulls("SELECT $1, $2 FROM t WHERE a = $1", [20, 25]) ==
             "SELECT NULL::BIGINT, NULL::VARCHAR FROM t WHERE a = NULL::BIGINT"
  end
end
