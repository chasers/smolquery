defmodule SmolqueryWeb.SupervisorTest do
  use ExUnit.Case, async: false

  alias Smolquery.Test.MapCatalog

  setup do
    original = Application.get_env(:smolquery, SmolqueryWeb.Endpoint, [])
    on_exit(fn -> Application.put_env(:smolquery, SmolqueryWeb.Endpoint, original) end)

    {:ok, endpoint_config: original}
  end

  defp put_secret(config, secret) do
    Application.put_env(
      :smolquery,
      SmolqueryWeb.Endpoint,
      Keyword.put(config, :secret_key_base, secret)
    )
  end

  test "refuses to boot without a session secret", %{endpoint_config: config} do
    put_secret(config, nil)

    assert_raise ArgumentError, ~r/at least 64 bytes \(got 0\)/, fn ->
      SmolqueryWeb.Supervisor.start_link(catalog: MapCatalog.new())
    end
  end

  test "refuses a session secret the cookie store would reject", %{endpoint_config: config} do
    put_secret(config, String.duplicate("a", 63))

    assert_raise ArgumentError, ~r/at least 64 bytes \(got 63\)/, fn ->
      SmolqueryWeb.Supervisor.start_link(catalog: MapCatalog.new())
    end
  end
end
