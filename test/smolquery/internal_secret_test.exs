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

  test "clustered nodes reject a missing secret" do
    previous_secret = Application.fetch_env(:smolquery, :internal_secret)
    previous_cluster = Application.fetch_env(:smolquery, Smolquery.Cluster)
    Application.delete_env(:smolquery, :internal_secret)
    Application.put_env(:smolquery, Smolquery.Cluster, enabled: true)

    on_exit(fn ->
      restore(previous_secret)
      restore_cluster(previous_cluster)
    end)

    assert_raise ArgumentError, ~r/SMOLQUERY_INTERNAL_SECRET.*non-empty shared value/, fn ->
      InternalSecret.ensure()
    end
  end

  test "clustered nodes reject an empty secret" do
    previous_secret = Application.fetch_env(:smolquery, :internal_secret)
    previous_cluster = Application.fetch_env(:smolquery, Smolquery.Cluster)
    Application.put_env(:smolquery, :internal_secret, "")
    Application.put_env(:smolquery, Smolquery.Cluster, enabled: true)

    on_exit(fn ->
      restore(previous_secret)
      restore_cluster(previous_cluster)
    end)

    assert_raise ArgumentError, ~r/SMOLQUERY_INTERNAL_SECRET.*non-empty shared value/, fn ->
      InternalSecret.ensure()
    end
  end

  test "clustered nodes use the configured secret" do
    previous_secret = Application.fetch_env(:smolquery, :internal_secret)
    previous_cluster = Application.fetch_env(:smolquery, Smolquery.Cluster)
    Application.put_env(:smolquery, :internal_secret, "cluster-secret")
    Application.put_env(:smolquery, Smolquery.Cluster, enabled: true)

    on_exit(fn ->
      restore(previous_secret)
      restore_cluster(previous_cluster)
    end)

    assert InternalSecret.ensure() == "cluster-secret"
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

  defp restore_cluster({:ok, value}),
    do: Application.put_env(:smolquery, Smolquery.Cluster, value)

  defp restore_cluster(:error), do: Application.delete_env(:smolquery, Smolquery.Cluster)
end
