defmodule Smolquery.PeerSocket do
  @moduledoc """
  TCP options that bound how long a socket to a peer node can outlive the peer
  (T-614).

  A pod killed without closing its sockets leaves every connection to it
  pointed at an IP that no longer answers. Linux keeps such a socket until it
  gives up retransmitting, `tcp_retries2` = 15, about 15.4 minutes, and an idle
  socket with no keepalive is never checked at all. A pooled HTTP connection
  in that state stays in its pool, and each request that draws it waits out
  its whole receive timeout before the pool lets it go. On the sandbox on
  2026-10-02 that was a failed seal about once a minute after a buffer pod
  was replaced, one per stale connection.

  `tcp_options/0` sets two bounds:

    * **Keepalive** probes an idle socket after `keepalive_idle_s` (5) and
      every `keepalive_interval_s` (5) after that, and the kernel closes it
      after `keepalive_count` (2) unanswered probes: about 15 s, the
      timers gen_rpc already sets on the sockets it accepts. A closed idle
      socket leaves its pool before a request can draw it.
    * **`TCP_USER_TIMEOUT`** closes a socket whose sent data has gone
      unacknowledged for `user_timeout_ms` (20,000). A request already
      in flight on a dead socket fails then, not after its receive timeout.

  Both act only on a peer that has stopped answering at the TCP level. A slow
  peer still ACKs, so neither bound cuts a slow response short.

  The keepalive timers and the user timeout are Linux socket options set
  through `{:raw, ...}`. Elsewhere only `keepalive: true` is set, with the
  operating system's own timers.

      config :smolquery, Smolquery.PeerSocket,
        user_timeout_ms: 20_000,
        keepalive_idle_s: 5,
        keepalive_interval_s: 5,
        keepalive_count: 2
  """

  @ipproto_tcp 6
  @tcp_keepidle 4
  @tcp_keepintvl 5
  @tcp_keepcnt 6
  @tcp_user_timeout 18

  @defaults [
    user_timeout_ms: 20_000,
    keepalive_idle_s: 5,
    keepalive_interval_s: 5,
    keepalive_count: 2
  ]

  @doc """
  The `:gen_tcp` options for a socket to a peer node, from this node's
  configuration.
  """
  @spec tcp_options() :: keyword()
  def tcp_options, do: tcp_options(config(), :os.type())

  @doc """
  The `:gen_tcp` options for `config` on the operating system `os_type`, as
  `:os.type/0` names it.
  """
  @spec tcp_options(keyword(), {atom(), atom()}) :: keyword()
  def tcp_options(config, {:unix, :linux}) do
    [
      keepalive: true,
      raw: {@ipproto_tcp, @tcp_keepidle, int(config[:keepalive_idle_s])},
      raw: {@ipproto_tcp, @tcp_keepintvl, int(config[:keepalive_interval_s])},
      raw: {@ipproto_tcp, @tcp_keepcnt, int(config[:keepalive_count])},
      raw: {@ipproto_tcp, @tcp_user_timeout, int(config[:user_timeout_ms])}
    ]
  end

  def tcp_options(_config, _os_type), do: [keepalive: true]

  @doc """
  This node's settings, each defaulted.
  """
  @spec config() :: keyword()
  def config, do: Keyword.merge(@defaults, Application.get_env(:smolquery, __MODULE__, []))

  defp int(value), do: <<value::32-native>>
end
