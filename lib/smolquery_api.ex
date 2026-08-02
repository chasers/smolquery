defmodule SmolqueryApi do
  @moduledoc """
  The entrypoint for defining the API's Phoenix pieces — the same role
  `SmolqueryWeb` plays for the UI, minus everything HTML.

  Controllers here never render views: every action answers through
  `SmolqueryApi.Json.send_json/3` or `SmolqueryApi.Errors`, so the envelope
  stays the single JSON surface the API has spoken since it was a
  `Plug.Router`.
  """

  @doc false
  def controller do
    quote do
      use Phoenix.Controller, formats: []

      import Plug.Conn
    end
  end

  defmacro __using__(which) when is_atom(which) do
    apply(__MODULE__, which, [])
  end
end
