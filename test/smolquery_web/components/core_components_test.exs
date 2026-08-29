defmodule SmolqueryWeb.CoreComponentsTest do
  use ExUnit.Case, async: true

  import Phoenix.Component
  import Phoenix.LiveViewTest
  import SmolqueryWeb.CoreComponents

  describe "modal/1" do
    test "titles itself, closes on the button and on Escape, and keeps the backdrop inert" do
      assigns = %{}

      html =
        rendered_to_string(~H"""
        <.modal id="thing-modal" title="New thing" on_close="cancel">body</.modal>
        """)

      assert html =~ ~s|aria-labelledby="thing-modal-title"|
      assert html =~ ~s|id="thing-modal-title"|
      assert html =~ "New thing"
      assert html =~ "body"
      assert html =~ ~s|phx-window-keydown="cancel"|
      assert html =~ ~s|phx-key="Escape"|
      assert html =~ ~s|aria-label="Close"|
      assert html =~ ~s|phx-click="cancel"|
      refute html =~ ~s|modal-backdrop" phx-click|
    end
  end
end
