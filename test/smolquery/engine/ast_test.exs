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
end
