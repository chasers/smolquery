defmodule Smolquery.Auth.OIDC.Provider do
  @moduledoc """
  Supervised, bounded-freshness OIDC metadata and JWKS cache.

  Discovery and JWKS are loaded synchronously during `init/1`, so a role cannot
  start its listener without a compatible provider signing key. Refresh I/O
  runs outside the GenServer so fresh cache reads remain available while an
  unknown key triggers a fetch. Metadata refreshes atomically replace the key
  set before publishing new metadata. Failed refreshes retain the prior state,
  fail closed, and suppress outbound retries for the configured backoff.
  """

  use GenServer

  alias Smolquery.Auth.OIDC.{Config, Discovery}

  @operation_timeout_ms 301_000

  @type refresh :: %{
          monitor_ref: reference(),
          result_ref: reference(),
          from: GenServer.from(),
          kind: :provider | :jwks,
          operation: :metadata | :jwks | :refresh_jwks
        }
  @type t :: %{
          config: Config.t(),
          metadata: map(),
          metadata_at: integer(),
          jwks: map(),
          jwks_at: integer(),
          refresh: refresh() | nil,
          refresh_error: term() | nil,
          refresh_failed_at: integer() | nil,
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
         refresh: nil,
         refresh_error: nil,
         refresh_failed_at: nil,
         http_client: client
       }}
    else
      {:error, reason} -> {:stop, {:oidc_provider_failed, reason}}
    end
  end

  @impl GenServer
  def handle_call(:metadata, from, state) do
    if fresh?(state.metadata_at, state.config.discovery_max_age_ms),
      do: {:reply, {:ok, state.metadata}, state},
      else: start_refresh(:provider, :metadata, from, state)
  end

  @impl GenServer
  def handle_call(:jwks, from, state) do
    cond do
      not fresh?(state.metadata_at, state.config.discovery_max_age_ms) ->
        start_refresh(:provider, :jwks, from, state)

      fresh?(state.jwks_at, state.config.jwks_max_age_ms) ->
        {:reply, {:ok, state.jwks}, state}

      true ->
        start_refresh(:jwks, :jwks, from, state)
    end
  end

  @impl GenServer
  def handle_call(:refresh_jwks, from, state) do
    if fresh?(state.metadata_at, state.config.discovery_max_age_ms),
      do: start_refresh(:jwks, :refresh_jwks, from, state),
      else: start_refresh(:provider, :refresh_jwks, from, state)
  end

  @impl GenServer
  def handle_info(
        {result_ref, result},
        %{refresh: %{result_ref: result_ref} = refresh} = state
      ) do
    Process.demonitor(refresh.monitor_ref, [:flush])
    state = %{state | refresh: nil}
    {reply, state} = complete_refresh(refresh, result, state)
    GenServer.reply(refresh.from, reply)
    {:noreply, state}
  end

  def handle_info(
        {:DOWN, monitor_ref, :process, _pid, _reason},
        %{refresh: %{monitor_ref: monitor_ref} = refresh} = state
      ) do
    reason = :provider_refresh_failed
    GenServer.reply(refresh.from, {:error, reason})

    {:noreply,
     %{
       state
       | refresh: nil,
         refresh_error: reason,
         refresh_failed_at: now_ms()
     }}
  end

  def handle_info(_message, state), do: {:noreply, state}

  defp start_refresh(kind, operation, from, state) do
    cond do
      state.refresh != nil ->
        {:reply, {:error, :provider_refresh_in_progress}, state}

      not retry_allowed?(state) ->
        {:reply, {:error, state.refresh_error}, state}

      true ->
        parent = self()
        result_ref = make_ref()

        {_pid, monitor_ref} =
          spawn_monitor(fn -> send(parent, {result_ref, perform_refresh(kind, state)}) end)

        refresh = %{
          monitor_ref: monitor_ref,
          result_ref: result_ref,
          from: from,
          kind: kind,
          operation: operation
        }

        {:noreply, %{state | refresh: refresh}}
    end
  end

  defp perform_refresh(:provider, state) do
    with {:ok, metadata} <- Discovery.fetch(state.config, state.http_client),
         {:ok, jwks} <- refresh_jwks_for_metadata(state, metadata) do
      {:ok, %{metadata: metadata, jwks: jwks}}
    else
      {:error, reason} -> {:error, {:discovery_refresh_failed, reason}}
    end
  end

  defp perform_refresh(:jwks, state) do
    case Discovery.fetch_jwks(state.config, state.metadata, state.http_client) do
      {:ok, jwks} -> {:ok, %{jwks: jwks}}
      {:error, reason} -> {:error, {:jwks_unavailable, reason}}
    end
  end

  defp refresh_jwks_for_metadata(state, metadata) do
    case Discovery.fetch_jwks(state.config, metadata, state.http_client) do
      {:ok, jwks} -> {:ok, jwks}
      {:error, reason} -> {:error, {:jwks_refresh_failed, reason}}
    end
  end

  defp complete_refresh(refresh, {:ok, values}, state) do
    now = now_ms()
    state = publish_refresh(refresh.kind, values, now, state)
    {success_reply(refresh.operation, state), clear_failure(state)}
  end

  defp complete_refresh(_refresh, {:error, reason}, state) do
    {{:error, reason}, %{state | refresh_error: reason, refresh_failed_at: now_ms()}}
  end

  defp publish_refresh(:provider, %{metadata: metadata, jwks: jwks}, now, state),
    do: %{state | metadata: metadata, metadata_at: now, jwks: jwks, jwks_at: now}

  defp publish_refresh(:jwks, %{jwks: jwks}, now, state),
    do: %{state | jwks: jwks, jwks_at: now}

  defp success_reply(:metadata, state), do: {:ok, state.metadata}

  defp success_reply(operation, state) when operation in [:jwks, :refresh_jwks],
    do: {:ok, state.jwks}

  defp clear_failure(state), do: %{state | refresh_error: nil, refresh_failed_at: nil}

  defp retry_allowed?(%{refresh_failed_at: nil}), do: true

  defp retry_allowed?(state),
    do: now_ms() - state.refresh_failed_at >= state.config.refresh_failure_backoff_ms

  defp fresh?(_fetched_at, 0), do: false
  defp fresh?(fetched_at, max_age), do: now_ms() - fetched_at <= max_age
  defp now_ms, do: System.monotonic_time(:millisecond)
end
