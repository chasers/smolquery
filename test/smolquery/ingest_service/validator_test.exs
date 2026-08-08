defmodule Smolquery.IngestService.ValidatorTest do
  use ExUnit.Case, async: true

  alias Smolquery.IngestService.Validator
  alias Smolquery.Schema

  @schema Schema.new!([
            {"id", :int64, nullable: false},
            {"ts", :timestamp},
            {"amount", {:numeric, 38, 2}},
            {"name", :string}
          ])

  describe "validate/2" do
    test "coerces valid rows into one list per schema field, in field order" do
      {batch, errors} =
        Validator.validate(@schema, [
          %{"id" => 1, "ts" => "2026-08-01T10:00:00", "amount" => "12.50", "name" => "a"}
        ])

      assert errors == []
      assert batch.row_count == 1

      assert batch.columns == [
               [1],
               [~N[2026-08-01 10:00:00]],
               [Decimal.new("12.50")],
               ["a"]
             ]
    end

    # The rule column-major cannot get wrong: a column the row does not carry
    # still advances by one, or every column after it shifts against the others.
    test "a missing nullable column becomes an explicit nil, keeping columns aligned" do
      {batch, []} = Validator.validate(@schema, [%{"id" => 1}])

      assert batch.columns == [[1], [nil], [nil], [nil]]
      assert Enum.all?(batch.columns, &(Enum.count(&1) == batch.row_count))
    end

    test "columns stay the same length when only some rows carry a column" do
      {batch, []} =
        Validator.validate(@schema, [
          %{"id" => 1, "name" => "a"},
          %{"id" => 2},
          %{"id" => 3, "name" => "c"}
        ])

      assert batch.row_count == 3
      assert batch.columns == [[1, 2, 3], [nil, nil, nil], [nil, nil, nil], ["a", nil, "c"]]
    end

    test "valid rows proceed even when neighbors fail, and indexes are original" do
      {batch, errors} =
        Validator.validate(@schema, [
          %{"id" => 1},
          %{"id" => "not a number"},
          %{"id" => 3}
        ])

      assert batch.row_count == 2
      assert [[1, 3] | _rest] = batch.columns
      assert [%{index: 1, errors: [%{message: message}]}] = errors
      assert message =~ "id (INT64)"
    end

    # A rejected row is coerced before it is known to be rejected, so its values
    # must leave nothing behind in any column.
    test "a rejected row leaves no trace in any column" do
      {batch, [_rejection]} =
        Validator.validate(@schema, [%{"id" => 1, "name" => "keep"}, %{"name" => "drop"}])

      assert batch.row_count == 1
      assert Enum.all?(batch.columns, &match?([_only], &1))
      refute Enum.any?(batch.columns, &("drop" in &1))
    end

    test "a rejected row reports every problem it has" do
      {%{row_count: 0}, [%{index: 0, errors: errors}]} =
        Validator.validate(@schema, [
          %{"ts" => "not a time", "extra" => 1, "another" => 2}
        ])

      messages = Enum.map(errors, & &1.message)

      assert Enum.any?(messages, &(&1 =~ ~s(unknown column: "another")))
      assert Enum.any?(messages, &(&1 =~ ~s(unknown column: "extra")))
      assert Enum.any?(messages, &(&1 =~ "id must not be null"))
      assert Enum.any?(messages, &(&1 =~ "ts (TIMESTAMP)"))
    end

    test "an explicit null in a non-nullable column is rejected" do
      {%{row_count: 0}, [%{index: 0, errors: [%{message: message}]}]} =
        Validator.validate(@schema, [%{"id" => nil}])

      assert message =~ "must not be null"
    end

    test "an explicit null in a nullable column is fine" do
      {batch, []} = Validator.validate(@schema, [%{"id" => 1, "name" => nil}])

      assert batch.columns == [[1], [nil], [nil], [nil]]
    end

    test "a row that is not an object is rejected" do
      {%{row_count: 0}, [%{index: 0, errors: [%{message: message}]}]} =
        Validator.validate(@schema, [[1, 2]])

      assert message =~ "must be a JSON object"
    end
  end

  # The columnar body is a wire format, not a second semantics. Every test here
  # asserts against `validate/2` on the same data rather than against a literal,
  # so the two can never drift apart unnoticed.
  describe "validate_columns/3" do
    defp both(rows) do
      columns =
        Map.new(Schema.names(@schema), fn name ->
          {name, Enum.map(rows, &Map.get(&1, name))}
        end)

      {Validator.validate(@schema, rows),
       Validator.validate_columns(@schema, columns, length(rows))}
    end

    test "agrees with validate/2 on a clean batch" do
      {row_major, columnar} =
        both([
          %{"id" => 1, "ts" => "2026-08-01T10:00:00", "amount" => "12.50", "name" => "a"},
          %{"id" => 2, "ts" => "2026-08-01T10:00:01", "amount" => "0.01", "name" => "b"}
        ])

      assert columnar == row_major
    end

    test "agrees on a missing column, which becomes nulls" do
      assert {row_major, columnar} = both([%{"id" => 1}, %{"id" => 2}])
      assert columnar == row_major
    end

    test "a rejected position is dropped from every column, keeping them aligned" do
      {batch, errors} =
        Validator.validate_columns(
          @schema,
          %{"id" => [1, "not a number", 3], "name" => ["a", "b", "c"]},
          3
        )

      assert batch.row_count == 2
      assert batch.columns == [[1, 3], [nil, nil], [nil, nil], ["a", "c"]]
      assert [%{index: 1, errors: [%{message: message}]}] = errors
      assert message =~ "id"
    end

    test "a column the schema does not declare rejects the whole batch, per row" do
      {batch, errors} =
        Validator.validate_columns(@schema, %{"id" => [1, 2], "nope" => ["x", "y"]}, 2)

      assert batch.row_count == 0
      assert [%{index: 0, errors: [%{message: message}]}, %{index: 1}] = errors
      assert message =~ ~s(unknown column: "nope")
    end

    test "a short column is padded, and its nulls are checked like any other" do
      {batch, errors} = Validator.validate_columns(@schema, %{"id" => [1]}, 3)

      assert batch.row_count == 1
      assert batch.columns == [[1], [nil], [nil], [nil]]
      assert [%{index: 1}, %{index: 2}] = errors
    end

    test "the byte estimate bounds the columns it measured" do
      {batch, []} =
        Validator.validate_columns(
          @schema,
          %{"id" => Enum.to_list(1..200), "name" => Enum.map(1..200, &"row-#{&1}")},
          200
        )

      assert batch.byte_size >= :erlang.external_size(batch.columns)
    end
  end

  describe "validate/2 byte estimate" do
    @wide Schema.new!([
            {"id", :int64},
            {"big", :int64},
            {"huge", :int64},
            {"ratio", :float64},
            {"ok", :bool},
            {"ts", :timestamp},
            {"day", :date},
            {"amount", {:numeric, 38, 2}},
            {"name", :string}
          ])

    test "never comes out below the external size of the columns it measured" do
      for rows <- byte_estimate_cases() do
        {batch, _errors} = Validator.validate(@wide, rows)
        exact = :erlang.external_size(batch.columns)

        assert batch.byte_size >= exact,
               "under-counted #{inspect(rows)}: #{batch.byte_size} < #{exact}"
      end
    end

    test "stays within a tenth of the external size it bounds" do
      {batch, []} = Validator.validate(@wide, Enum.map(1..200, &wide_row/1))

      assert batch.byte_size <= :erlang.external_size(batch.columns) * 1.1
    end

    test "counts only the rows that survived validation" do
      {batch, [_rejection]} =
        Validator.validate(@wide, [wide_row(1), %{"id" => "not a number"}])

      exact = :erlang.external_size(batch.columns)

      assert batch.byte_size >= exact
      assert batch.byte_size <= exact * 1.1
    end

    test "an empty batch still covers its own framing" do
      assert {batch, []} = Validator.validate(@wide, [])
      assert batch.byte_size >= :erlang.external_size(batch.columns)
    end

    defp byte_estimate_cases do
      [
        [],
        [%{}],
        [wide_row(1)],
        Enum.map(1..50, &wide_row/1),
        [%{"id" => 0, "name" => ""}],
        [%{"id" => 255, "big" => 256, "huge" => 9_223_372_036_854_775_807}],
        [%{"id" => -1, "big" => -2_147_483_648, "huge" => -9_223_372_036_854_775_808}],
        [%{"huge" => 170_141_183_460_469_231_731_687_303_715_884_105_727}],
        [%{"ratio" => 1.0e308, "ok" => false}],
        [%{"ok" => true}],
        [%{"ts" => "9999-12-31T23:59:59.999999Z", "day" => "9999-12-31"}],
        [%{"ts" => "0001-01-01T00:00:00Z", "day" => "0001-01-01"}],
        [%{"amount" => "0"}],
        [%{"amount" => "123456789012345678901234567890.12"}],
        [%{"name" => String.duplicate("é", 5_000)}],
        [%{"name" => "a"}, %{"name" => String.duplicate("b", 300)}]
      ]
    end

    defp wide_row(index) do
      %{
        "id" => index,
        "big" => index * 1_000_000_000,
        "huge" => index * 1_000_000_000_000_000_000,
        "ratio" => index / 7,
        "ok" => rem(index, 2) == 0,
        "ts" => "2026-08-01T10:00:00.123456Z",
        "day" => "2026-08-01",
        "amount" => "12.50",
        "name" => "row-#{index}"
      }
    end
  end
end
