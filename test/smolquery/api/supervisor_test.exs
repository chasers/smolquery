defmodule Smolquery.Api.SupervisorTest do
  use ExUnit.Case, async: true

  alias Smolquery.Api
  alias Smolquery.Api.Router
  alias Smolquery.Api.Runtime
  alias Smolquery.Test.MapCatalog

  @key "supervisor-test-key"

  defp start_api(opts \\ []) do
    opts =
      Keyword.merge(
        [
          name: :"api_#{:erlang.unique_integer([:positive])}",
          api_key: @key,
          port: 0,
          catalog: MapCatalog.new()
        ],
        opts
      )

    name = Keyword.fetch!(opts, :name)
    start_supervised!({Api.Supervisor, opts}, id: name)
    on_exit(fn -> Runtime.delete(name) end)

    name
  end

  test "serves the api over a real listener" do
    name = start_api()

    response = Req.get!(Router.base_url(name) <> "/healthz", retry: false)

    assert response.status == 200
    assert response.body == %{"status" => "ok"}
  end

  test "enforces auth over the wire" do
    name = start_api()
    base = Router.base_url(name)

    assert Req.get!(base <> "/v1/datasets", retry: false).status == 401
    assert Req.get!(base <> "/v1/datasets", auth: {:bearer, @key}, retry: false).status == 200

    response = Req.get!(base <> "/v1/no/such/route", auth: {:bearer, @key}, retry: false)

    assert response.status == 404
    assert %{"error" => %{"status" => "NOT_FOUND"}} = response.body
  end

  test "refuses to boot without an api key" do
    assert_raise ArgumentError, ~r/refuses to boot/, fn ->
      Api.Supervisor.start_link(name: :api_no_key, port: 0, catalog: MapCatalog.new())
    end
  end
end
