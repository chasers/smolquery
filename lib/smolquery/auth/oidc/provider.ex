defmodule Smolquery.Auth.OIDC.Provider do
  @moduledoc """
  Supervised, bounded-freshness OIDC metadata and JWKS cache.

  Discovery and JWKS are loaded synchronously during `init/1`, so a role cannot
  start its listener without a validated provider key set. Metadata refreshes
  use monotonic time and atomically refresh the key set before publishing new
  metadata. Failed refreshes retain the prior state but return an error; keys
  are never returned as a successful stale fallback.
  """

  use GenServer

  alias Smolquery.Auth.OIDC.{Config, Discovery}

  @operation_timeout_ms 301_000

  @type t :: %{
          config: Config.t(),
          metadata: map(),
          metadata_at: integer(),
          jwks: map(),
          jwks_at: integer(),
          http_client: Discovery.http_client()
        }

  @doc "Starts the provider cache and validates discovery and JWKS before returning."
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) do
    name = Keyword.fetch!(opts, :name)
    GenServer.start_link(__MODULE__, opts, name: name, timeout: @operation_timeout_ms)
  end

  @doc "Returns validated discovery metadata while it remains within policy."
  @spec metadata(pid() | atom()) :: {:ok, map()} | {:error, term()}
  def metadata(server), do: GenServer.call(server, :metadata, @operation_timeout_ms)

  @doc "Returns cached JWKS, refreshing discovery first when metadata expires."
  @spec jwks(pid() | atom()) :: {:ok, map()} | {:error, term()}
  def jwks(server), do: GenServer.call(server, :jwks, @operation_timeout_ms)

  @doc "Forces one bounded JWKS refresh, used later for an unknown key id."
  @spec refresh_jwks(pid() | atom()) :: {:ok, map()} | {:error, term()}
  def refresh_jwks(server), do: GenServer.call(server, :refresh_jwks, @operation_timeout_ms)

  @impl GenServer
  def init(opts) do
    config = Keyword.fetch!(opts, :config)
    client = Keyword.get(opts, :http_client, &Discovery.fetch_request/2)

    with {:ok, metadata} <- Discovery.fetch(config, client),
         {:ok, jwks} <- Discovery.fetch_jwks(config, metadata, client) do
      now = now_ms()

      {:ok,
       %{
         config: config,
         metadata: metadata,
         metadata_at: now,
         jwks: jwks,
         jwks_at: now,
         http_client: client
       }}
    else
      {:error, reason} -> {:stop, {:oidc_provider_failed, reason}}
    end
  end

  @impl GenServer
  def handle_call(:metadata, _from, state) do
    case ensure_metadata(state) do
      {:ok, state} -> {:reply, {:ok, state.metadata}, state}
      {:error, reason, state} -> {:reply, {:error, {:discovery_refresh_failed, reason}}, state}
    end
  end

  @impl GenServer
  def handle_call(operation, _from, state) when operation in [:jwks, :refresh_jwks] do
    case ensure_metadata_if_expired(state) do
      {:ok, state} ->
        case fetch_jwks_if_needed(operation, state) do
          {:ok, reply, state} -> {:reply, reply, state}
          {:error, reason, state} -> {:reply, {:error, reason}, state}
        end

      {:error, reason, state} ->
        {:reply, {:error, {:discovery_refresh_failed, reason}}, state}
    end
  end

  defp ensure_metadata_if_expired(state) do
    if fresh?(state.metadata_at, state.config.discovery_max_age_ms),
      do: {:ok, state},
      else: refresh_provider(state)
  end

  defp refresh_provider(state) do
    with {:ok, metadata} <- Discovery.fetch(state.config, state.http_client),
         {:ok, jwks} <- refresh_jwks_for_metadata(state, metadata) do
      now = now_ms()
      {:ok, %{state | metadata: metadata, metadata_at: now, jwks: jwks, jwks_at: now}}
    else
      {:error, reason} -> {:error, reason, state}
    end
  end

  defp refresh_jwks_for_metadata(state, metadata) do
    case Discovery.fetch_jwks(state.config, metadata, state.http_client) do
      {:ok, jwks} -> {:ok, jwks}
      {:error, reason} -> {:error, {:jwks_refresh_failed, reason}}
    end
  end

  defp ensure_metadata(state), do: ensure_metadata_if_expired(state)

  defp fetch_jwks_if_needed(:jwks, state) do
    if fresh?(state.jwks_at, state.config.jwks_max_age_ms) do
      {:ok, {:ok, state.jwks}, state}
    else
      fetch_jwks(state)
    end
  end

  defp fetch_jwks_if_needed(:refresh_jwks, state), do: fetch_jwks(state)

  defp fetch_jwks(state) do
    case Discovery.fetch_jwks(state.config, state.metadata, state.http_client) do
      {:ok, jwks} -> {:ok, {:ok, jwks}, %{state | jwks: jwks, jwks_at: now_ms()}}
      {:error, reason} -> {:error, {:jwks_unavailable, reason}, state}
    end
  end

  defp fresh?(_fetched_at, 0), do: false
  defp fresh?(fetched_at, max_age), do: now_ms() - fetched_at <= max_age
  defp now_ms, do: System.monotonic_time(:millisecond)
end
