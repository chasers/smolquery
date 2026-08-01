defmodule Smolquery.InternalSecret do
  @moduledoc """
  The shared secret internal HTTP proves itself with (PL-8 D6, closing PL-3
  D12).

  `BufferService.HotServer` requires it on every request as the
  `x-smolquery-internal` header; every reader attaches it — `HotClient` on
  manifest fetches, and the DuckDB engines that read micro-segment bytes over
  `httpfs`, via an http `CREATE SECRET` scoped to the hot tier's base URL.

  The value comes from `config :smolquery, :internal_secret`
  (`SMOLQUERY_INTERNAL_SECRET`). A single node that has not set one gets a
  fresh secret generated at boot — every role in the BEAM shares application
  env, so the node agrees with itself. A cluster must set it explicitly:
  two nodes that each generated their own cannot read each other's hot tier,
  and the failure is an honest 401, not an open door.
  """

  @header "x-smolquery-internal"

  @doc """
  The header name the secret travels in.
  """
  @spec header() :: String.t()
  def header, do: @header

  @doc """
  This node's secret.

  `Smolquery.Application` calls `ensure/0` before any subtree starts, so by
  the time a server or reader asks, the answer exists.
  """
  @spec value() :: String.t()
  def value do
    case Application.fetch_env(:smolquery, :internal_secret) do
      {:ok, secret} when is_binary(secret) and secret != "" -> secret
      _absent -> ensure()
    end
  end

  @doc """
  Puts a generated secret in application env when none is configured, and
  returns the one in force.
  """
  @spec ensure() :: String.t()
  def ensure do
    case Application.fetch_env(:smolquery, :internal_secret) do
      {:ok, secret} when is_binary(secret) and secret != "" ->
        secret

      _absent ->
        secret = 32 |> :crypto.strong_rand_bytes() |> Base.url_encode64(padding: false)
        Application.put_env(:smolquery, :internal_secret, secret)

        secret
    end
  end

  @doc """
  The `CREATE SECRET` statement that makes a DuckDB engine attach the header
  to every `httpfs` request under `scope`.
  """
  @spec create_secret_statement(String.t()) :: String.t()
  def create_secret_statement(scope) do
    "CREATE SECRET IF NOT EXISTS smolquery_hot_tier (TYPE http, " <>
      "EXTRA_HTTP_HEADERS MAP {'#{@header}': #{Smolquery.Identifier.sql_string(value())}}, " <>
      "SCOPE #{Smolquery.Identifier.sql_string(scope)})"
  end
end
