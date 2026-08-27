defmodule Smolquery.Engine.FrameTest do
  use ExUnit.Case, async: false

  alias Smolquery.Engine
  alias Smolquery.Engine.Frame

  @engine __MODULE__.Instance

  setup do
    start_supervised!({Engine, name: @engine})
    :ok
  end

  test "folds a MAP column's entries back into a map, and leaves other lists alone" do
    {:ok, frame} =
      Engine.frame(
        @engine,
        "SELECT 1::BIGINT AS id, MAP {'host': 'h1', 'pod': 'api-7'} AS attrs, ['a', 'b'] AS tags " <>
          "UNION ALL SELECT 2, MAP {}, []"
      )

    assert Frame.map_columns(frame) == ["attrs"]

    assert Frame.to_rows(frame) == [
             %{"id" => 1, "attrs" => %{"host" => "h1", "pod" => "api-7"}, "tags" => ["a", "b"]},
             %{"id" => 2, "attrs" => %{}, "tags" => []}
           ]
  end

  test "a frame without a map column is plain to_rows" do
    {:ok, frame} = Engine.frame(@engine, "SELECT 1::BIGINT AS id, 'x' AS name")

    assert Frame.map_columns(frame) == []
    assert Frame.to_rows(frame) == [%{"id" => 1, "name" => "x"}]
  end

  test "decodes the columns the job says crossed as JSON text" do
    {:ok, frame} =
      Engine.frame(
        @engine,
        ~s|SELECT 1::BIGINT AS id, '{"a":[1,{"b":null}]}' AS doc, 'kept' AS text UNION ALL SELECT 2, NULL, NULL|
      )

    assert Frame.to_rows(frame, json_columns: ["doc"]) == [
             %{"id" => 1, "doc" => %{"a" => [1, %{"b" => nil}]}, "text" => "kept"},
             %{"id" => 2, "doc" => nil, "text" => nil}
           ]
  end
end
