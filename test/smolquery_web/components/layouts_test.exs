defmodule SmolqueryWeb.LayoutsTest do
  use ExUnit.Case, async: true

  import Phoenix.Component
  import Phoenix.LiveViewTest
  import SmolqueryWeb.Layouts

  describe "flash_group/1" do
    test "both connection banners say the page is reconnecting, and nothing went wrong" do
      assigns = %{flash: %{}}

      html =
        rendered_to_string(~H"""
        <.flash_group flash={@flash} />
        """)

      assert html =~ ~s|id="client-error"|
      assert html =~ ~s|id="server-error"|
      assert [_before, _client, _server] = String.split(html, "Reconnecting")
      refute html =~ "Something went wrong"
      refute html =~ "can't find the internet"
    end
  end
end
