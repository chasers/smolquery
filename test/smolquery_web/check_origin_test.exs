defmodule SmolqueryWeb.CheckOriginTest do
  use ExUnit.Case, async: true

  alias SmolqueryWeb.CheckOrigin

  describe "parse!/1" do
    test "accepts true and false in any case" do
      assert CheckOrigin.parse!("false") == false
      assert CheckOrigin.parse!(" FALSE ") == false
      assert CheckOrigin.parse!("true") == true
      assert CheckOrigin.parse!("True") == true
    end

    test "splits a list and trims its entries" do
      assert CheckOrigin.parse!("https://ui.example.com, //other.example.com") ==
               ["https://ui.example.com", "//other.example.com"]
    end

    test "accepts a wildcard subdomain" do
      assert CheckOrigin.parse!("//*.example.com") == ["//*.example.com"]
    end

    test "rejects a bare hostname at boot" do
      assert_raise ArgumentError, ~r/has no host/, fn ->
        CheckOrigin.parse!("ui.example.com")
      end
    end

    test "rejects a value that is neither a boolean nor an origin" do
      assert_raise ArgumentError, ~r/has no host/, fn ->
        CheckOrigin.parse!("0")
      end
    end
  end
end
