defmodule SmolqueryApi.ErrorJSON do
  @moduledoc """
  What Phoenix renders when a route raises — the envelope, naming nothing.

  Expected failures never reach here: handlers answer through
  `SmolqueryApi.Errors`, parse failures through `SmolqueryApi.Parsers`, and
  the router's catch-all owns 404. This is only the backstop for a genuine
  crash, so it says exactly what the Plug.Router's `handle_errors` fallback
  said: internal error, no internals.
  """

  def render(_template, _assigns) do
    %{"error" => %{"code" => 500, "status" => "INTERNAL", "message" => "internal error"}}
  end
end
