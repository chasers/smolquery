defmodule Smolquery.Engine.CallExited do
  @moduledoc """
  An engine call that exited instead of replying, caught and made a value.

  A `GenServer.call` to an engine connection has two failure shapes, and only
  one of them is an `{:error, _}` reply. The other is an exit raised in the
  caller: the call timed out, or the connection died before replying. The
  connection serializes its queries, so a timeout does not even mean the
  engine is broken — a `DROP TABLE` queued behind another merge's five-minute
  `COPY` times out on a perfectly healthy connection.

  `Smolquery.Engine.try_query/4` catches that exit and returns it as this
  exception, for callers that must outlive it. It carries the exit reason
  with the `GenServer.call` mfa stripped: the SQL and parameters in that
  tuple can be large, and the reason is what the log line needs.
  """

  defexception [:reason]

  @type t :: %__MODULE__{reason: term()}

  @doc """
  Wraps an exit reason, stripping the `GenServer.call` mfa a call exit carries.
  """
  @spec new(term()) :: t()
  def new({reason, {GenServer, :call, _args}}), do: %__MODULE__{reason: reason}
  def new(reason), do: %__MODULE__{reason: reason}

  @impl true
  def message(%__MODULE__{reason: reason}) do
    "engine call exited before replying: #{inspect(reason)}"
  end
end
