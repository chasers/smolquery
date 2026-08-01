defmodule Smolquery.InternalSecretTest do
  use ExUnit.Case, async: false

  alias Smolquery.InternalSecret

  test "generates once and then agrees with itself" do
    value = InternalSecret.value()

    assert is_binary(value)
    assert byte_size(value) >= 32
    assert InternalSecret.value() == value
    assert InternalSecret.ensure() == value
  end

  test "a configured secret wins" do
    previous = Application.fetch_env(:smolquery, :internal_secret)
    Application.put_env(:smolquery, :internal_secret, "configured")
    on_exit(fn -> restore(previous) end)

    assert InternalSecret.value() == "configured"
  end

  test "the create-secret statement carries the header, the value, and the scope" do
    statement = InternalSecret.create_secret_statement("http://127.0.0.1:4001")

    assert statement =~ "TYPE http"
    assert statement =~ InternalSecret.header()
    assert statement =~ InternalSecret.value()
    assert statement =~ "SCOPE 'http://127.0.0.1:4001'"
  end

  defp restore({:ok, value}), do: Application.put_env(:smolquery, :internal_secret, value)
  defp restore(:error), do: Application.delete_env(:smolquery, :internal_secret)
end
