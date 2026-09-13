defmodule Smolquery.Engine.AstTest do
  use ExUnit.Case, async: true

  alias Smolquery.Engine.Ast

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

  describe "against the pinned parser" do
    test "reads a cast, a constant, a function's arguments and a percent LIMIT as DuckDB serializes them" do
      start_supervised!({Smolquery.Engine, name: __MODULE__.Engine, extensions: [:json]})

      sql =
        "SELECT CAST(a AS TIMESTAMPTZ), lower(b, 2), 5 FROM t WHERE ts > TIMESTAMP '2026-01-01' LIMIT 10%"

      %{rows: [[json]]} =
        Smolquery.Engine.query!(__MODULE__.Engine, "SELECT json_serialize_sql($1::VARCHAR)", [sql])

      %{"statements" => [%{"node" => node}]} = JSON.decode!(json)
      [cast, lower, five] = node["select_list"]

      assert Ast.cast_type(cast) == "TIMESTAMP WITH TIME ZONE"
      assert Ast.constant(five) == {:ok, {"INTEGER", 5}}
      assert Ast.integer(five) == {:ok, 5}

      assert [%{"class" => "COLUMN_REF", "column_names" => ["b"]}, two] = Ast.arguments(lower)
      assert Ast.integer(two) == {:ok, 2}

      %{"right" => bound} = node["where_clause"]
      assert Ast.cast_type(bound) == "TIMESTAMP"
      assert Ast.constant(bound["child"]) == {:ok, {"VARCHAR", "2026-01-01"}}

      assert [%{"type" => "LIMIT_MODIFIER", "limit_type" => "PERCENTAGE"}] = node["modifiers"]
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
