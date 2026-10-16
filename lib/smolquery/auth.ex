defmodule Smolquery.Auth do
  @moduledoc """
  Attachment seam for an authenticated `Smolquery.Auth.Context`.

  The same stable assign key is used by Plug connections and LiveView sockets.
  Assigning a value that is not a context raises `ArgumentError`; fetching a
  missing or malformed assign returns `:error`. Assignment and fetching prove
  structure only; trusted adapters establish authentication provenance before
  assignment, and policy authorization checks expiry and capability access.
  """

  alias Phoenix.LiveView.Socket
  alias Smolquery.Auth.Context

  @assign_key :smolquery_auth_context

  @doc """
  Returns the stable assign key used for auth contexts.
  """
  @spec assign_key() :: :smolquery_auth_context
  def assign_key, do: @assign_key

  @doc """
  Assigns a context to a Plug connection or LiveView socket.
  """
  @spec assign_context(Plug.Conn.t() | Socket.t(), Context.t()) ::
          Plug.Conn.t() | Socket.t()
  def assign_context(%Plug.Conn{} = conn, %Context{} = context),
    do: assign_valid_context(conn, context)

  def assign_context(%Socket{} = socket, %Context{} = context),
    do: assign_valid_context(socket, context)

  def assign_context(_target, _context),
    do: raise(ArgumentError, "expected a Smolquery.Auth.Context")

  @doc """
  Fetches an assigned context from a Plug connection or LiveView socket.

  Assignment checks are structural only; policy authorization checks expiry and
  capability access.
  """
  @spec fetch_context(Plug.Conn.t() | Socket.t()) :: {:ok, Context.t()} | :error
  def fetch_context(%Plug.Conn{assigns: assigns}), do: fetch_assign(assigns)
  def fetch_context(%Socket{assigns: assigns}), do: fetch_assign(assigns)
  def fetch_context(_target), do: :error

  defp fetch_assign(%{@assign_key => %Context{} = context}) do
    if Context.well_formed?(context), do: {:ok, context}, else: :error
  end

  defp fetch_assign(_assigns), do: :error

  defp assign_valid_context(target, context) do
    if Context.well_formed?(context) do
      do_assign(target, context)
    else
      raise ArgumentError, "expected a well-formed Smolquery.Auth.Context"
    end
  end

  defp do_assign(%Plug.Conn{} = conn, context), do: Plug.Conn.assign(conn, @assign_key, context)

  defp do_assign(%Socket{} = socket, context),
    do: Phoenix.Component.assign(socket, @assign_key, context)
end
