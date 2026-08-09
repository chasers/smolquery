defmodule Smolquery.Test.KindTest do
  use ExUnit.Case, async: false

  alias Smolquery.Test.Kind
  alias Smolquery.Test.KindInsertServer

  test "insert! sends NDJSON and retries with the same insertId" do
    state = start_supervised!({Agent, fn -> %{statuses: [503, 200], requests: []} end})
    server = start_supervised!(KindInsertServer.bandit_spec(state))
    previous_base = System.get_env("BASE")
    System.put_env("BASE", KindInsertServer.base_url(server))

    on_exit(fn ->
      case previous_base do
        nil -> System.delete_env("BASE")
        value -> System.put_env("BASE", value)
      end
    end)

    assert :ok = Kind.insert!("dataset", "table", 2)

    requests = Agent.get(state, &Enum.reverse(&1.requests))
    assert [%{body: body, content_type: ["application/x-ndjson"]} = first, second] = requests
    assert body != ""
    assert String.ends_with?(body, "\n")
    refute body =~ "\\n"

    assert body
           |> String.split("\n", trim: true)
           |> Enum.map(&JSON.decode!/1) == [%{"id" => 0, "v" => "r0"}, %{"id" => 1, "v" => "r1"}]

    assert second.body == body
    assert second.content_type == ["application/x-ndjson"]
    assert first.query_params["insertId"] != ""
    assert byte_size(first.query_params["insertId"]) <= 128
    assert second.query_params["insertId"] == first.query_params["insertId"]

    assert first.query_string ==
             "insertId=" <> URI.encode_www_form(first.query_params["insertId"])
  end
end
