defmodule SmolqueryWeb.Router do
  @moduledoc """
  The UI's routes. `SmolqueryWeb.Auth` guards all of them.

  The pipeline's order matters. `:fetch_session` runs before the auth plug,
  because a successful check writes the session marker. `live_session` then
  requires that marker on the socket. Both layers are needed. The plug guards
  the HTTP request. The plug does not see the websocket upgrade, so the
  `on_mount` hook guards that.
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
