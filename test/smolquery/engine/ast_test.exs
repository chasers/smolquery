defmodule Smolquery.Engine.AstTest do
  use ExUnit.Case, async: true

  alias Smolquery.Engine.Ast

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
end
