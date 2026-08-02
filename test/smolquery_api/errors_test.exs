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
end
