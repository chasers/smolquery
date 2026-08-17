defmodule SmolqueryWeb.Waterfall do
  @moduledoc """
  A trace's spans as a waterfall: one row per span, positioned by time.

  Renders `Smolquery.QueryService.Trace` spans with plain positioned divs —
  each bar's offset is the span's start relative to the trace, its width the
  span's share of the trace's total wall clock. A span too short to see at
  that scale keeps a minimum width, because an invisible phase reads as a
  missing one.
  """

  use Phoenix.Component

  @minimum_width_percent 0.5

  attr :id, :string, required: true
  attr :spans, :list, required: true

  def waterfall(assigns) do
    assigns = assign(assigns, :total, total_us(assigns.spans))

    ~H"""
    <div id={@id} class="space-y-1">
      <div
        :for={span <- @spans}
        class="flex items-center gap-2 text-xs font-mono"
        title={row_title(span)}
      >
        <span class="w-32 shrink-0 text-right opacity-70">{span.name}</span>
        <div class="relative h-4 grow rounded bg-base-200">
          <div class="absolute inset-y-0 rounded bg-primary" style={bar_style(span, @total)}></div>
        </div>
        <span class="w-20 shrink-0 opacity-70">{duration_label(span.duration_us)}</span>
      </div>
    </div>
    """
  end

  defp total_us(spans) do
    spans
    |> Enum.map(&(&1.start_us + &1.duration_us))
    |> Enum.max(fn -> 1 end)
    |> max(1)
  end

  defp bar_style(span, total) do
    left = min(span.start_us / total * 100, 100 - @minimum_width_percent)
    width = max(min(span.duration_us / total * 100, 100 - left), @minimum_width_percent)

    "left: #{Float.round(left, 3)}%; width: #{Float.round(width, 3)}%"
  end

  defp row_title(%{meta: meta} = span) when map_size(meta) > 0 do
    detail = Enum.map_join(meta, ", ", fn {key, value} -> "#{key}: #{value}" end)

    "#{span.name} (#{detail})"
  end

  defp row_title(span), do: to_string(span.name)

  @doc """
  A human-readable duration from microseconds.

  Public because it is the UI's one duration format: the waterfall and the
  lifecycle card (T-295) label durations through the same function, so the
  two cannot drift.
  """
  @spec duration_label(non_neg_integer()) :: String.t()
  def duration_label(us) when us >= 1_000_000, do: "#{Float.round(us / 1_000_000, 2)} s"
  def duration_label(us) when us >= 1_000, do: "#{Float.round(us / 1_000, 1)} ms"
  def duration_label(us), do: "#{us} µs"
end
