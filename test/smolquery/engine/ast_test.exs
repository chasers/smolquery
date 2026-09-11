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
