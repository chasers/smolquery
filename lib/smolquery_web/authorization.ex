defmodule SmolqueryWeb.Authorization do
  @moduledoc """
  Closed capability authorization for LiveView mounts and socket lifecycle.

  Capabilities are compile-time route or handler requirements. Authorization
  reads only the normalized context assigned by `Smolquery.Auth` and delegates
  expiry and capability semantics to `Smolquery.Auth.Policy`.
  """

  alias Phoenix.LiveView
  alias Smolquery.Auth
  alias Smolquery.Auth.Policy
  alias SmolqueryWeb.Runtime

  @capabilities [:web_access, :query, :catalog_manage, :platform_operate]
  @max_expiry_timer_ms 86_400_000
  @static_check_ms 1_000

  @doc "Validates a compile-time LiveView capability requirement."
  def init(capability) when capability in @capabilities, do: capability

  def init(capability),
    do: raise(ArgumentError, "unsupported web capability: #{inspect(capability)}")

  @doc "Requires a capability during LiveView initial mount or reconnect."
  def on_mount(capability, _params, _session, socket) do
    case authorize(socket, capability) do
      :ok -> {:cont, attach(socket, capability)}
      {:error, :unauthenticated} -> {:halt, redirect(socket, :unauthenticated)}
      {:error, :forbidden} -> {:halt, redirect(socket, :forbidden)}
    end
  end

  @doc "Authorizes a normalized socket context for a closed capability."
  def authorize(socket, capability) when capability in @capabilities do
    case Auth.fetch_context(socket) do
      {:ok, context} -> authorize_context(context, capability)
      :error -> {:error, :unauthenticated}
    end
  end

  def authorize(_socket, _capability), do: {:error, :forbidden}

  @doc "Authorizes a normalized context before a service or catalog action."
  def authorize_context(context, capability) when capability in @capabilities,
    do: Policy.authorize(context, capability)

  def authorize_context(_context, _capability), do: {:error, :forbidden}

  @doc "Attaches event, info, and async guards and schedules OIDC expiry."
  def attach(socket, capability) when capability in @capabilities do
    if lifecycle_socket?(socket) do
      socket =
        socket
        |> LiveView.attach_hook({__MODULE__, capability, :event}, :handle_event, fn _event,
                                                                                    _params,
                                                                                    socket ->
          lifecycle_result(socket, capability)
        end)
        |> LiveView.attach_hook({__MODULE__, capability, :info}, :handle_info, fn message,
                                                                                  socket ->
          info_lifecycle_result(message, socket, capability)
        end)
        |> LiveView.attach_hook({__MODULE__, capability, :async}, :handle_async, fn _key,
                                                                                    _result,
                                                                                    socket ->
          lifecycle_result(socket, capability)
        end)

      if capability == :web_access, do: schedule_expiry(socket), else: socket
    else
      socket
    end
  end

  def attach(socket, _capability), do: socket

  @doc "Revokes connected static sockets when the credential-derived marker rotates."
  def attach_static(socket, marker) when is_binary(marker) do
    if lifecycle_socket?(socket) do
      socket
      |> LiveView.attach_hook({__MODULE__, :static, :event}, :handle_event, fn _event,
                                                                               _params,
                                                                               socket ->
        static_lifecycle_result(socket, marker)
      end)
      |> LiveView.attach_hook({__MODULE__, :static, :info}, :handle_info, fn message, socket ->
        static_info_result(message, socket, marker)
      end)
      |> LiveView.attach_hook({__MODULE__, :static, :async}, :handle_async, fn _key,
                                                                               _result,
                                                                               socket ->
        static_lifecycle_result(socket, marker)
      end)
      |> schedule_static_check()
    else
      socket
    end
  end

  def attach_static(socket, _marker), do: socket

  @doc "Returns a generic socket denial without exposing claims or roles."
  def deny_event(socket, capability) do
    case authorize(socket, capability) do
      :ok -> :ok
      {:error, reason} -> {:error, reason, redirect(socket, reason)}
    end
  end

  @doc "Guards a direct event handler before parsing input or doing work."
  def event(socket, capability), do: deny_event(socket, capability)

  defp lifecycle_result(socket, capability) do
    case authorize(socket, capability) do
      :ok -> {:cont, socket}
      {:error, reason} -> {:halt, redirect(socket, reason)}
    end
  end

  defp info_lifecycle_result(:smolquery_auth_expiry, socket, :web_access) do
    case authorize(socket, :web_access) do
      :ok -> {:halt, schedule_expiry(socket)}
      {:error, reason} -> {:halt, redirect(socket, reason)}
    end
  end

  defp info_lifecycle_result(_message, socket, capability),
    do: lifecycle_result(socket, capability)

  defp schedule_expiry(socket) do
    case Auth.fetch_context(socket) do
      {:ok, %{expires_at: expires_at}} when is_integer(expires_at) ->
        remaining = max(expires_at * 1_000 - System.system_time(:millisecond), 0)
        Process.send_after(self(), :smolquery_auth_expiry, min(remaining, @max_expiry_timer_ms))
        socket

      _ ->
        socket
    end
  end

  defp static_info_result(:smolquery_static_auth_check, socket, marker) do
    case static_marker_current?(marker) do
      true -> {:halt, schedule_static_check(socket)}
      false -> {:halt, LiveView.redirect(socket, to: "/")}
    end
  end

  defp static_info_result(_message, socket, marker),
    do: static_lifecycle_result(socket, marker)

  defp static_lifecycle_result(socket, marker) do
    if static_marker_current?(marker),
      do: {:cont, socket},
      else: {:halt, LiveView.redirect(socket, to: "/")}
  end

  defp static_marker_current?(marker),
    do: match?({:ok, %{auth_mode: :static, session_marker: ^marker}}, Runtime.fetch(SmolqueryWeb))

  defp schedule_static_check(socket) do
    Process.send_after(self(), :smolquery_static_auth_check, @static_check_ms)
    socket
  end

  defp redirect(socket, :unauthenticated),
    do: LiveView.redirect(socket, to: "/auth/login")

  defp redirect(socket, :forbidden),
    do: LiveView.redirect(socket, to: "/cluster")

  defp lifecycle_socket?(%LiveView.Socket{private: private}),
    do: is_map(private) and Map.has_key?(private, :lifecycle)

  defp lifecycle_socket?(_socket), do: false
end
