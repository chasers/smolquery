defmodule SmolqueryApi.ErrorsTest do
  use ExUnit.Case, async: true

  import Plug.Test

  alias SmolqueryApi.Errors

  test "sends the envelope as json with the code as the http status" do
    response = Errors.send_error(conn(:get, "/"), 429, "BUFFER_FULL", "try again")

    assert response.status == 429

    assert response.resp_body |> JSON.decode!() == %{
             "error" => %{"code" => 429, "status" => "BUFFER_FULL", "message" => "try again"}
           }

    assert {"content-type", "application/json; charset=utf-8"} in response.resp_headers
  end

  test "maps a catalog commit conflict to a retryable 409, not a 500" do
    response = Errors.from_reason(conn(:delete, "/"), :commit_conflict)

    assert response.status == 409
    assert %{"error" => %{"status" => "ABORTED"}} = JSON.decode!(response.resp_body)
  end

  test "sends the shed-load refusal with its retry-after" do
    response = Errors.send_resource_exhausted(conn(:post, "/"), 3, "buffer full, retry later")

    assert response.status == 429
    assert {"retry-after", "3"} in response.resp_headers

    assert response.resp_body |> JSON.decode!() == %{
             "error" => %{
               "code" => 429,
               "status" => "RESOURCE_EXHAUSTED",
               "message" => "buffer full, retry later"
             }
           }
  end
end
