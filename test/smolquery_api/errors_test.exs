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

  test "maps the dataset conflicts to 409 and an immutable field to 400" do
    for reason <- [{:dataset_exists, "analytics"}, {:catalog_in_use, "analytics"}] do
      response = Errors.from_reason(conn(:post, "/"), reason)

      assert response.status == 409
      assert %{"error" => %{"status" => "ALREADY_EXISTS"}} = JSON.decode!(response.resp_body)
    end

    response = Errors.from_reason(conn(:patch, "/"), {:immutable_field, "storage.bucket"})

    assert response.status == 400

    assert %{"error" => %{"message" => "storage.bucket cannot change after creation"}} =
             JSON.decode!(response.resp_body)

    assert Errors.from_reason(conn(:get, "/"), :datasets_unsupported).status == 501
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
