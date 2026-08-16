defmodule SmolqueryWeb.WaterfallTest do
  use ExUnit.Case, async: true

  import Phoenix.LiveViewTest

  alias SmolqueryWeb.Waterfall

  defp spans do
    [
      %{name: :serialize, start_us: 0, duration_us: 250, meta: %{}},
      %{name: :manifest_fetch, start_us: 250, duration_us: 750, meta: %{url: "http://n1:4321"}}
    ]
  end

  test "renders one positioned bar per span" do
    html = render_component(&Waterfall.waterfall/1, id: "trace", spans: spans())

    assert html =~ "serialize"
    assert html =~ "manifest_fetch"
    assert html =~ "left: 0.0%"
    assert html =~ "left: 25.0%"
    assert html =~ "width: 75.0%"
  end

  test "meta lands in the row title" do
    html = render_component(&Waterfall.waterfall/1, id: "trace", spans: spans())

    assert html =~ "manifest_fetch (url: http://n1:4321)"
  end

  test "a zero-length span still gets a visible bar" do
    html =
      render_component(&Waterfall.waterfall/1,
        id: "trace",
        spans: [%{name: :build, start_us: 0, duration_us: 0, meta: %{}}]
      )

    assert html =~ "width: 0.5%"
  end

  test "an empty trace renders an empty shell, not a crash" do
    html = render_component(&Waterfall.waterfall/1, id: "trace", spans: [])

    assert html =~ ~s|id="trace"|
  end
end
