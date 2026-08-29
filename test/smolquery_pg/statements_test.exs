defmodule SmolqueryPg.StatementsTest do
  use ExUnit.Case, async: true

  alias SmolqueryPg.Statements

  test "splits on semicolons and drops empty statements" do
    assert Statements.split("BEGIN; SELECT 1;; COMMIT ;") == ["BEGIN", "SELECT 1", "COMMIT"]
    assert Statements.split("   ") == []
    assert Statements.split("SELECT 1") == ["SELECT 1"]
  end

  test "a semicolon inside a string, an identifier, or a comment does not split" do
    assert Statements.split("SELECT ';' AS a; SELECT 'it''s;' AS b") ==
             ["SELECT ';' AS a", "SELECT 'it''s;' AS b"]

    assert Statements.split(~s|SELECT 1 AS "a;b"; SELECT 2|) == [
             ~s|SELECT 1 AS "a;b"|,
             "SELECT 2"
           ]

    assert Statements.split("SELECT 1 -- one; two\n; SELECT 2") ==
             ["SELECT 1 -- one; two", "SELECT 2"]

    assert Statements.split("SELECT /* a; /* nested; */ b */ 1; SELECT 2") ==
             ["SELECT /* a; /* nested; */ b */ 1", "SELECT 2"]
  end

  test "a dollar-quoted body keeps its semicolons" do
    assert Statements.split("SELECT $$a;b$$; SELECT $tag$c;d$tag$ ; SELECT $1") ==
             ["SELECT $$a;b$$", "SELECT $tag$c;d$tag$", "SELECT $1"]
  end
end
