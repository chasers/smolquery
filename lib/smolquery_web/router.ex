defmodule SmolqueryWeb.Router do
  @moduledoc """
  The UI's routes, all of them behind `SmolqueryWeb.Auth`.

  The pipeline's order is the contract: `:fetch_session` runs before the auth
  plug, because a successful check writes the session marker that
  `live_session` then requires of the socket. Both layers are needed — the
  plug guards the request, the `on_mount` hook guards the upgrade the plug
  never sees.
  """

  use SmolqueryWeb, :router

  pipeline :browser do
    plug :accepts, ["html"]
    plug :fetch_session
    plug SmolqueryWeb.Auth
    plug :fetch_live_flash
    plug :put_root_layout, html: {SmolqueryWeb.Layouts, :root}
    plug :protect_from_forgery
    plug :put_secure_browser_headers
  end

  scope "/", SmolqueryWeb do
    pipe_through :browser

    live_session :authenticated, on_mount: {SmolqueryWeb.Auth, :require_authenticated} do
      live "/", TableLive.Index, :index
      live "/tables", TableLive.Index, :index
      live "/tables/:dataset/:table", TableLive.Show, :show
      live "/query", QueryLive.Index, :index
      live "/cluster", ClusterLive.Index, :index
    end
  end
end
