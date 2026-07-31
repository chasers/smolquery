defmodule Smolquery.Engine.ResultTest do
  use ExUnit.Case, async: true

  alias Smolquery.Engine.Result

  defp batch do
    [
      Adbc.Column.s64([1, 2], name: "id"),
      Adbc.Column.string(["a", "b"], name: "label")
    ]
  end

  describe "from_adbc/1" do
    test "preserves column order and transposes to rows" do
      result = Result.from_adbc(%Adbc.Result{data: [batch()], num_rows: 2})

      assert result.columns == ["id", "label"]
      assert result.rows == [[1, "a"], [2, "b"]]
      assert result.num_rows == 2
    end

    test "flattens multiple record batches into one row list" do
      result = Result.from_adbc(%Adbc.Result{data: [batch(), batch()], num_rows: 4})

      assert result.columns == ["id", "label"]
      assert result.rows == [[1, "a"], [2, "b"], [1, "a"], [2, "b"]]
      assert result.num_rows == 4
    end

    test "counts rows from the data rather than trusting num_rows" do
      result = Result.from_adbc(%Adbc.Result{data: [batch()], num_rows: nil})

      assert result.num_rows == 2
    end

    test "handles a statement that returned no data" do
      result = Result.from_adbc(%Adbc.Result{data: nil, num_rows: 7})

      assert result.columns == []
      assert result.rows == []
      assert result.num_rows == 7
    end

    test "handles a nil row count with no data" do
      assert Result.from_adbc(%Adbc.Result{data: nil, num_rows: nil}).num_rows == 0
    end

    test "handles an empty batch list" do
      result = Result.from_adbc(%Adbc.Result{data: [], num_rows: 0})

      assert result.columns == []
      assert result.rows == []
    end
  end

  describe "to_maps/1" do
    test "keys each row by column name" do
      result = Result.from_adbc(%Adbc.Result{data: [batch()], num_rows: 2})

      assert Result.to_maps(result) == [
               %{"id" => 1, "label" => "a"},
               %{"id" => 2, "label" => "b"}
             ]
    end

    test "is empty for an empty result" do
      assert Result.to_maps(%Result{}) == []
    end
  end

  describe "one!/1" do
    test "unwraps a scalar result" do
      assert Result.one!(%Result{columns: ["n"], rows: [[42]], num_rows: 1}) == 42
    end

    test "raises when the shape is not one row by one column" do
      assert_raise ArgumentError, ~r/expected exactly one row and one column/, fn ->
        Result.one!(%Result{columns: ["a", "b"], rows: [[1, 2]], num_rows: 1})
      end
    end

    test "raises on an empty result" do
      assert_raise ArgumentError, fn -> Result.one!(%Result{}) end
    end
  end
end
