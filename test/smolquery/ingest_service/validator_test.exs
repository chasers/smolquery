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
    test "coerces valid rows and keeps their column set to the schema" do
      {valid, errors} =
        Validator.validate(@schema, [
          %{"id" => 1, "ts" => "2026-08-01T10:00:00", "amount" => "12.50", "name" => "a"}
        ])

      assert errors == []

      assert valid == [
               %{
                 "id" => 1,
                 "ts" => ~N[2026-08-01 10:00:00],
                 "amount" => Decimal.new("12.50"),
                 "name" => "a"
               }
             ]
    end

    test "a missing nullable column is simply absent" do
      {[row], []} = Validator.validate(@schema, [%{"id" => 1}])

      assert row == %{"id" => 1}
    end

    test "valid rows proceed even when neighbors fail, and indexes are original" do
      {valid, errors} =
        Validator.validate(@schema, [
          %{"id" => 1},
          %{"id" => "not a number"},
          %{"id" => 3}
        ])

      assert [%{"id" => 1}, %{"id" => 3}] = valid
      assert [%{index: 1, errors: [%{message: message}]}] = errors
      assert message =~ "id (INT64)"
    end

    test "a rejected row reports every problem it has" do
      {[], [%{index: 0, errors: errors}]} =
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
      {[], [%{index: 0, errors: [%{message: message}]}]} =
        Validator.validate(@schema, [%{"id" => nil}])

      assert message =~ "must not be null"
    end

    test "an explicit null in a nullable column is fine" do
      {[row], []} = Validator.validate(@schema, [%{"id" => 1, "name" => nil}])

      assert row == %{"id" => 1}
    end

    test "a row that is not an object is rejected" do
      {[], [%{index: 0, errors: [%{message: message}]}]} = Validator.validate(@schema, [[1, 2]])

      assert message =~ "must be a JSON object"
    end
  end
end
