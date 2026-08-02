defmodule SmolqueryWeb.Router do
  use SmolqueryWeb, :router

  pipeline :browser do
    plug :accepts, ["html"]
    plug :fetch_session
    plug :fetch_live_flash
    plug :put_root_layout, html: {SmolqueryWeb.Layouts, :root}
    plug :protect_from_forgery
    plug :put_secure_browser_headers
  end

  scope "/", SmolqueryWeb do
    pipe_through :browser

    live "/", TableLive.Index, :index
    live "/tables", TableLive.Index, :index
    live "/tables/:dataset/:table", TableLive.Show, :show
    live "/query", QueryLive.Index, :index
    live "/cluster", ClusterLive.Index, :index
  end
end
