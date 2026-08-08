defmodule Smolquery.IngestService.ColumnarValidatorTest do
  use ExUnit.Case, async: true

  alias Explorer.DataFrame
  alias Explorer.Series
  alias Smolquery.IngestService.ColumnarValidator
  alias Smolquery.IngestService.Validator
  alias Smolquery.Schema

  defp schema do
    Schema.new!([
      {"id", :int64, nullable: false},
      {"ts", :timestamp},
      {"amount", :float64},
      {"name", :string},
      {"ok", :bool},
      {"day", :date}
    ])
  end

  defp ndjson(rows), do: Enum.map_join(rows, "\n", &JSON.encode!/1) <> "\n"

  test "a well-typed batch comes out as a schema-shaped frame" do
    body =
      ndjson([
        %{
          "id" => 1,
          "ts" => "2026-08-07T12:00:01.123456Z",
          "amount" => 1.5,
          "name" => "a",
          "ok" => true,
          "day" => "2026-08-07"
        },
        %{"id" => 2, "ts" => "2026-08-07T12:00:02Z", "amount" => 2, "name" => "b", "ok" => false}
      ])

    assert {:ok, frame} = ColumnarValidator.validate(schema(), body)
    assert DataFrame.names(frame) == ["id", "ts", "amount", "name", "ok", "day"]
    assert DataFrame.n_rows(frame) == 2

    assert Series.to_list(frame["id"]) == [1, 2]
    assert Series.to_list(frame["amount"]) == [1.5, 2.0]

    assert [~N[2026-08-07 12:00:01.123456], ~N[2026-08-07 12:00:02.000000]] =
             Series.to_list(frame["ts"])

    assert Series.to_list(frame["day"]) == [~D[2026-08-07], nil]
  end

  test "the frame matches what the per-row validator coerces" do
    rows = [
      %{"id" => 1, "ts" => "2026-08-07T12:00:01.123456Z", "amount" => 3, "name" => "x"},
      %{"id" => 2, "ok" => true}
    ]

    {:ok, frame} = ColumnarValidator.validate(schema(), ndjson(rows))
    {valid, []} = Validator.validate(schema(), rows)

    for row <- valid, {name, value} <- row do
      index = Enum.find_index(valid, &(&1 == row))
      assert Series.to_list(frame[name]) |> Enum.at(index) == value
    end
  end

  test "numbers arriving as strings coerce, like value_from_json" do
    body = ndjson([%{"id" => "41", "amount" => "1.25"}])

    assert {:ok, frame} = ColumnarValidator.validate(schema(), body)
    assert Series.to_list(frame["id"]) == [41]
    assert Series.to_list(frame["amount"]) == [1.25]
  end

  test "a column the schema does not have falls back" do
    assert ColumnarValidator.validate(schema(), ndjson([%{"id" => 1, "extra" => 1}])) == :fallback
  end

  test "a value that does not coerce falls back rather than nulling" do
    assert ColumnarValidator.validate(schema(), ndjson([%{"id" => 1, "ts" => "not a time"}])) ==
             :fallback

    assert ColumnarValidator.validate(schema(), ndjson([%{"id" => "junk"}])) == :fallback
  end

  test "a JSON float in an int column falls back rather than truncating" do
    assert ColumnarValidator.validate(schema(), ndjson([%{"id" => 1.5}])) == :fallback
  end

  test "a null in a non-nullable column falls back" do
    assert ColumnarValidator.validate(schema(), ndjson([%{"id" => nil, "name" => "x"}])) ==
             :fallback

    assert ColumnarValidator.validate(schema(), ndjson([%{"name" => "x"}])) == :fallback
  end

  test "a wrong scalar type falls back" do
    assert ColumnarValidator.validate(schema(), ndjson([%{"id" => 1, "ok" => "true"}])) ==
             :fallback

    assert ColumnarValidator.validate(schema(), ndjson([%{"id" => 1, "name" => 7}])) == :fallback
  end

  test "an unparseable body falls back" do
    assert ColumnarValidator.validate(schema(), "not json at all\n") == :fallback
    assert ColumnarValidator.validate(schema(), "") == :fallback
  end

  test "an all-null nullable column keeps its dtype" do
    body = ndjson([%{"id" => 1, "ts" => nil}, %{"id" => 2, "ts" => nil}])

    assert {:ok, frame} = ColumnarValidator.validate(schema(), body)
    assert Series.dtype(frame["ts"]) == {:naive_datetime, :microsecond}
    assert Series.nil_count(frame["ts"]) == 2
  end
end
