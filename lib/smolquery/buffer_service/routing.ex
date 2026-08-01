defmodule Smolquery.BufferService.Routing do
  @moduledoc """
  Where a table's calls go, resolvable on a node that runs no buffer at all.

  A query node holds no hot tier and starts no buffer subtree, yet it has to know
  which buffer node owns each table it reads. So ownership cannot live only in the
  buffer service's own runtime — it has to be answerable from configuration alone.

  Two sources, in order:

    * a published `Smolquery.BufferService.Runtime`, on nodes running the `:buffer`
      role — that is authoritative, and keeps a test's per-instance ring in step
      with the buffer it started
    * otherwise application configuration, cached in `:persistent_term` so the
      ring is built once rather than per call

  Ring changes at runtime are Milestone 8; until then a resolved routing is
  immutable, which is what makes caching it safe.
  """

  alias Smolquery.BufferService.Ring
  alias Smolquery.BufferService.Runtime
  alias Smolquery.BufferService.Transport

  @enforce_keys [:name, :ring, :remote_transport, :write_timeout_ms, :control_timeout_ms]
  defstruct [:name, :ring, :remote_transport, :write_timeout_ms, :control_timeout_ms]

  @type t :: %__MODULE__{
          name: atom(),
          ring: Ring.t(),
          remote_transport: module(),
          write_timeout_ms: timeout(),
          control_timeout_ms: timeout()
        }

  @default_control_timeout_ms 15_000
  @default_write_timeout_ms 15_000

  @doc """
  The routing for an instance.
  """
  @spec resolve(atom()) :: t()
  def resolve(name) do
    case Runtime.fetch(name) do
      {:ok, runtime} -> from_runtime(runtime)
      :error -> cached(name)
    end
  end

  @doc """
  The transport reaching `node`, and the channel a given operation belongs on.

  Bulk and control are deliberately separate connections; see
  `Smolquery.BufferService.Transport`.
  """
  @spec transport(t(), node()) :: module()
  def transport(%__MODULE__{} = routing, node) do
    if node == node(), do: Transport.Local, else: routing.remote_transport
  end

  @doc """
  The node owning `table_ref`.
  """
  @spec owner(t(), Smolquery.Segments.Store.table_ref()) :: node()
  def owner(%__MODULE__{ring: ring}, table_ref), do: Ring.owner(ring, table_ref)

  @doc """
  Drops a cached routing, so the next resolve rebuilds it from configuration.
  """
  @spec forget(atom()) :: boolean()
  def forget(name), do: :persistent_term.erase(key(name))

  defp from_runtime(%Runtime{} = runtime) do
    %__MODULE__{
      name: runtime.name,
      ring: runtime.ring,
      remote_transport: remote_transport(),
      write_timeout_ms: runtime.write_timeout_ms,
      control_timeout_ms: runtime.control_timeout_ms
    }
  end

  defp cached(name) do
    :persistent_term.get(key(name))
  rescue
    ArgumentError ->
      routing = build(name)
      :persistent_term.put(key(name), routing)

      routing
  end

  defp build(name) do
    config = Application.get_env(:smolquery, Smolquery.BufferService, [])

    %__MODULE__{
      name: name,
      ring: Ring.new!(Keyword.get(config, :ring, [node()])),
      remote_transport: remote_transport(),
      write_timeout_ms: Keyword.get(config, :write_timeout_ms, @default_write_timeout_ms),
      control_timeout_ms: Keyword.get(config, :control_timeout_ms, @default_control_timeout_ms)
    }
  end

  defp remote_transport do
    :smolquery
    |> Application.get_env(Smolquery.BufferService, [])
    |> Keyword.get(:remote_transport, Transport.GenRpc)
  end

  defp key(name), do: {__MODULE__, name}
end
