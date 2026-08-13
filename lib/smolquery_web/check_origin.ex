defmodule SmolqueryWeb.CheckOrigin do
  @moduledoc """
  Parses `SMOLQUERY_WEB_CHECK_ORIGIN` into the endpoint's `check_origin` value.

  `true` and `false` (any case) become booleans. Any other value is a
  comma-separated origin list. Each entry must carry a host, which means a
  scheme (`https://ui.example.com`) or a leading `//` (`//ui.example.com`,
  `//*.example.com`).

  Phoenix parses a bare hostname as a path and raises on the first websocket
  connect — far from the configuration mistake. This parser raises at boot
  instead, the same contract as `Smolquery.Roles.parse!/1`.
  """

  @spec parse!(String.t()) :: boolean() | [String.t()]
  def parse!(value) do
    case value |> String.trim() |> String.downcase() do
      "true" -> true
      "false" -> false
      _origins -> value |> String.split(",", trim: true) |> Enum.map(&parse_origin!/1)
    end
  end

  defp parse_origin!(entry) do
    origin = String.trim(entry)

    case URI.parse(origin) do
      %URI{host: host} when is_binary(host) and host != "" ->
        origin

      _no_host ->
        raise ArgumentError,
              "SMOLQUERY_WEB_CHECK_ORIGIN entry #{inspect(origin)} has no host: " <>
                "write a scheme or a leading //, e.g. https://ui.example.com or " <>
                "//ui.example.com, or set the whole variable to true or false"
    end
  end
end
