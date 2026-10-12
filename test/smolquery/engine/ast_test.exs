defmodule Smolquery.Engine.AstTest do
  use ExUnit.Case, async: true

  alias Smolquery.Engine.Ast

  test "decode/1 reads the tree, and refuses by name the bare Infinity DuckDB 2.0 writes (T-653)" do
    assert Ast.decode(~s({"error": false, "statements": []})) ==
             {:ok, %{"error" => false, "statements" => []}}

    assert {:error, {:invalid_query, message}} =
             Ast.decode(~s({"class": "CONSTANT", "value": Infinity}))

    assert message =~ "not valid JSON"
    assert message =~ "CAST('1e308' AS DOUBLE)"
    assert message =~ "the JSON breaks at byte 31"
  end

  test "shape/1 is an expression without where it was written or what it was called" do
    written = %{
      "class" => "FUNCTION",
      "function_name" => "count_star",
      "alias" => "n",
      "query_location" => 7,
      "children" => [
        %{"class" => "CONSTANT", "alias" => "", "query_location" => 19, "value" => 1}
      ]
    }

    again = %{
      "class" => "FUNCTION",
      "function_name" => "count_star",
      "alias" => "",
      "query_location" => 88,
      "children" => [
        %{"class" => "CONSTANT", "alias" => "x", "query_location" => 3, "value" => 1}
      ]
    }

    assert Ast.shape(written) == Ast.shape(again)

    assert Ast.shape(written) == %{
             "class" => "FUNCTION",
             "function_name" => "count_star",
             "children" => [%{"class" => "CONSTANT", "value" => 1}]
           }

    refute Ast.shape(written) == Ast.shape(put_in(again, ["function_name"], "count"))

    assert Ast.shape(Map.put(written, "query_location_length", 9)) == Ast.shape(written)
  end

  test "portable/1 writes every FUNCTION's arguments as children, deep, and drops qualified names (T-656)" do
    column = %{"class" => "COLUMN_REF", "column_names" => ["x"]}

    inner = %{
      "class" => "FUNCTION",
      "function_name" => "abs",
      "qualified_name" => %{"path" => ["abs"]},
      "arguments" => [%{"name" => "", "expression" => column}]
    }

    outer = %{
      "class" => "FUNCTION",
      "function_name" => "sum",
      "qualified_name" => %{"path" => ["sum"]},
      "arguments" => [%{"name" => "", "expression" => inner}],
      "filter" => %{"class" => "CONJUNCTION", "children" => [column]}
    }

    assert Ast.portable(%{"select_list" => [outer]}) == %{
             "select_list" => [
               %{
                 "class" => "FUNCTION",
                 "function_name" => "sum",
                 "children" => [
                   %{"class" => "FUNCTION", "function_name" => "abs", "children" => [column]}
                 ],
                 "filter" => %{"class" => "CONJUNCTION", "children" => [column]}
               }
             ]
           }

    one_five = %{"class" => "FUNCTION", "function_name" => "abs", "children" => [column]}
    assert Ast.portable(one_five) == one_five

    table = %{"type" => "BASE_TABLE", "table_name" => "events", "schema_name" => "analytics"}

    assert Ast.portable(Map.put(table, "qualified_name", %{"path" => ["analytics", "events"]})) ==
             table
  end

  test "cte_node/1 and set_operands/1 read either spelling (T-656)" do
    select = %{"type" => "SELECT_NODE"}

    assert Ast.cte_node(%{"query" => %{"named_param_map" => [], "node" => select}}) == select
    assert Ast.cte_node(%{"query_node" => select}) == select
    assert Ast.cte_node(%{"aliases" => []}) == nil

    assert Ast.set_operands(%{"left" => select, "right" => select}) == [select, select]

    assert Ast.set_operands(%{"children" => [select, select], "left" => nil, "right" => nil}) ==
             [select, select]

    assert Ast.set_operands(select) == []
  end

  test "collect/2 visits every map in document order, and keeps only what the reader answers" do
    tree = %{
      "class" => "FUNCTION",
      "children" => [
        %{"class" => "COLUMN_REF", "column_names" => ["a"]},
        %{"class" => "CAST", "child" => %{"class" => "COLUMN_REF", "column_names" => ["b"]}}
      ],
      "filter" => nil
    }

    assert Ast.collect(tree, &List.wrap(&1["class"])) == [
             "FUNCTION",
             "COLUMN_REF",
             "CAST",
             "COLUMN_REF"
           ]

    assert Ast.collect(tree, fn
             %{"class" => "COLUMN_REF", "column_names" => names} -> names
             _node -> []
           end) == ["a", "b"]

    assert Ast.collect([1, "leaf", nil], fn _node -> [:never] end) == []
  end

  describe "cast_type/1" do
    test "reads a resolved 1.5 cast, an unbound 2.0 cast, and a type_expr cast alike" do
      assert Ast.cast_type(%{"class" => "CAST", "cast_type" => %{"id" => "TIMESTAMP"}}) ==
               "TIMESTAMP"

      unbound = %{
        "class" => "CAST",
        "cast_type" => %{
          "id" => "UNBOUND",
          "type_info" => %{"expr" => %{"class" => "TYPE", "type_name" => "timestamp"}}
        }
      }

      assert Ast.cast_type(unbound) == "TIMESTAMP"

      assert Ast.cast_type(%{
               "class" => "CAST",
               "type_expr" => %{"class" => "TYPE", "type_name" => "DATE"}
             }) == "DATE"
    end

    test "expands the zoned aliases to the names 1.5 resolved, and answers nil off a cast" do
      assert Ast.cast_type(%{"type_expr" => %{"class" => "TYPE", "type_name" => "TIMESTAMPTZ"}}) ==
               "TIMESTAMP WITH TIME ZONE"

      assert Ast.cast_type(%{"type_expr" => %{"class" => "TYPE", "type_name" => "timetz"}}) ==
               "TIME WITH TIME ZONE"

      assert Ast.cast_type(%{"class" => "COLUMN_REF"}) == nil
      assert Ast.cast_type(%{"class" => "CAST", "cast_type" => %{"id" => "UNBOUND"}}) == nil

      assert Ast.type_name(%{"class" => "TYPE", "type_name" => "timestamptz"}) ==
               "TIMESTAMP WITH TIME ZONE"

      assert Ast.type_name(%{"class" => "CAST"}) == nil
    end
  end

  describe "cast_types/1" do
    test "names a 1.5 composite target's nested types, and a 2.0 cast's target alone" do
      struct = %{
        "class" => "CAST",
        "cast_type" => %{
          "id" => "STRUCT",
          "type_info" => %{
            "child_types" => [
              %{"first" => "a", "second" => %{"id" => "TIMESTAMP WITH TIME ZONE"}}
            ]
          }
        }
      }

      assert Ast.cast_types(struct) == ["STRUCT", "TIMESTAMP WITH TIME ZONE"]

      assert Ast.cast_types(%{
               "class" => "CAST",
               "type_expr" => %{"class" => "TYPE", "type_name" => "STRUCT"}
             }) == ["STRUCT"]

      assert Ast.cast_types(%{"class" => "COLUMN_REF"}) == []
    end
  end

  describe "constant/1" do
    test "reads a typed value, a NULL, and a literal of each kind" do
      typed = %{
        "class" => "CONSTANT",
        "value" => %{"type" => %{"id" => "INTEGER"}, "is_null" => false, "value" => 5}
      }

      assert Ast.constant(typed) == {:ok, {"INTEGER", 5}}

      assert Ast.constant(%{"class" => "CONSTANT", "value" => %{"is_null" => true}}) ==
               {:ok, :null}

      assert Ast.constant(literal("INTEGER", "12345678901234")) ==
               {:ok, {"BIGINT", 12_345_678_901_234}}

      assert Ast.constant(literal("NUMERIC", "-1.5")) == {:ok, {"DECIMAL", -1.5}}
      assert Ast.constant(literal("STRING", "x")) == {:ok, {"VARCHAR", "x"}}
      assert Ast.constant(literal("BOOLEAN", "true")) == {:ok, {"BOOLEAN", true}}
      assert Ast.constant(literal("NULL_LITERAL", "")) == {:ok, :null}
    end

    test "is an error for an unreadable literal or another node" do
      assert Ast.constant(literal("INTEGER", "ten")) == :error
      assert Ast.constant(literal("INTERVAL", "1 day")) == :error
      assert Ast.constant(%{"class" => "COLUMN_REF", "column_names" => ["a"]}) == :error
    end
  end

  describe "integer/1" do
    test "reads an integer-typed constant, and refuses a DECIMAL, a string and a NULL" do
      typed = fn type, value ->
        %{
          "class" => "CONSTANT",
          "value" => %{"type" => %{"id" => type}, "is_null" => false, "value" => value}
        }
      end

      assert Ast.integer(typed.("INTEGER", 5)) == {:ok, 5}
      assert Ast.integer(typed.("UBIGINT", 7)) == {:ok, 7}
      assert Ast.integer(literal("INTEGER", "12")) == {:ok, 12}
      assert Ast.integer(typed.("DECIMAL", 15)) == :error
      assert Ast.integer(typed.("VARCHAR", "5")) == :error
      assert Ast.integer(%{"class" => "CONSTANT", "value" => %{"is_null" => true}}) == :error
    end
  end

  describe "against the driver's parser" do
    test "reads a cast, a constant, a function's arguments and a percent LIMIT as DuckDB serializes them" do
      start_supervised!({Smolquery.Engine, name: __MODULE__.Engine, extensions: [:json]})

      sql =
        "SELECT CAST(a AS TIMESTAMPTZ), lower(b, 2), 5 FROM t WHERE ts > TIMESTAMP '2026-01-01' LIMIT 10%"

      %{rows: [[json]]} =
        Smolquery.Engine.query!(__MODULE__.Engine, "SELECT json_serialize_sql($1::VARCHAR)", [sql])

      %{"statements" => [%{"node" => node}]} = JSON.decode!(json)
      [cast, lower, five] = node["select_list"]

      assert Ast.cast_type(cast) == "TIMESTAMP WITH TIME ZONE"
      assert {:ok, {integer, 5}} = Ast.constant(five)
      assert integer in ["INTEGER", "BIGINT"]
      assert Ast.integer(five) == {:ok, 5}

      assert [%{"class" => "COLUMN_REF", "column_names" => ["b"]}, two] = Ast.arguments(lower)
      assert Ast.integer(two) == {:ok, 2}

      %{"right" => bound} = node["where_clause"]
      assert Ast.cast_type(bound) == "TIMESTAMP"
      assert Ast.constant(bound["child"]) == {:ok, {"VARCHAR", "2026-01-01"}}

      assert [percent] = node["modifiers"]
      assert percent["type"] == "LIMIT_PERCENT_MODIFIER" or percent["limit_type"] == "PERCENTAGE"
    end

    test "a function and a table renamed in a portable tree render by their new names (T-656)" do
      start_supervised!({Smolquery.Engine, name: __MODULE__.Render, extensions: [:json]})
      sql = "SELECT count(x) FROM analytics.events"

      %{rows: [[json]]} =
        Smolquery.Engine.query!(__MODULE__.Render, "SELECT json_serialize_sql($1::VARCHAR)", [sql])

      %{"statements" => [%{"node" => node} = statement]} = ast = JSON.decode!(json)
      %{"select_list" => [count], "from_table" => from} = node = Ast.portable(node)

      renamed = %{
        node
        | "select_list" => [%{count | "function_name" => "sum"}],
          "from_table" => %{from | "schema_name" => "", "table_name" => "rows"}
      }

      json = JSON.encode!(%{ast | "statements" => [%{statement | "node" => renamed}]})

      %{rows: [[rendered]]} =
        Smolquery.Engine.query!(__MODULE__.Render, "SELECT json_deserialize_sql($1::JSON)", [json])

      assert rendered =~ "sum(x)"
      assert rendered =~ ~r/FROM "?rows"?\z/
    end
  end

  describe "arguments/1" do
    test "reads children or named arguments, and answers none for a node without either" do
      column = %{"class" => "COLUMN_REF", "column_names" => ["a"]}

      assert Ast.arguments(%{"class" => "FUNCTION", "children" => [column]}) == [column]

      assert Ast.arguments(%{
               "class" => "FUNCTION",
               "arguments" => [%{"name" => "", "expression" => column}]
             }) == [column]

      assert Ast.arguments(%{"class" => "FUNCTION"}) == []
    end
  end

  defp literal(kind, text),
    do: %{"class" => "CONSTANT", "literal" => %{"kind" => kind, "text" => text}}
end
