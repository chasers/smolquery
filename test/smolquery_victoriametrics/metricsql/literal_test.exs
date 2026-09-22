defmodule SmolqueryVictoriaMetrics.MetricsQL.LiteralTest do
  use ExUnit.Case, async: true

  alias SmolqueryVictoriaMetrics.MetricsQL.Literal

  describe "number/1" do
    test "reads decimals, multipliers, base prefixes and the specials" do
      assert Literal.number("12") == {:ok, 12.0}
      assert Literal.number("123.") == {:ok, 123.0}
      assert Literal.number(".5") == {:ok, 0.5}
      assert Literal.number("12.e+4") == {:ok, 1.2e5}
      assert Literal.number("1_2_34.5") == {:ok, 1234.5}
      assert Literal.number("12Ki") == {:ok, 12_288.0}
      assert Literal.number("3M") == {:ok, 3.0e6}
      assert Literal.number("1.23Gb") == {:ok, 1.23e9}
      assert Literal.number("2TiB") == {:ok, 2.0 * 1024 ** 4}
      assert Literal.number("0x3B") == {:ok, 59.0}
      assert Literal.number("073") == {:ok, 59.0}
      assert Literal.number("0o12") == {:ok, 10.0}
      assert Literal.number("0b1011") == {:ok, 11.0}
      assert Literal.number("InF") == {:ok, :inf}
      assert Literal.number("nan") == {:ok, :nan}
      assert Literal.number("1e400") == {:ok, :inf}
      assert Literal.number("1e308k") == {:ok, :inf}
    end

    test "refuses what Go's strconv refuses" do
      for text <- ["08", "1__2", "1_", "0x", "012Ki", "0x1g"] do
        assert {:error, "cannot parse number " <> _quoted} = Literal.number(text), text
      end
    end
  end

  test "decimal/2 reads a Go float without a multiplier" do
    assert Literal.decimal("2e-3") == {:ok, 0.002}
    assert {:error, _message} = Literal.decimal(".")
  end

  describe "string/1" do
    test "reads Go escapes in double quotes" do
      assert Literal.string(~S|"a\nb\x41\101é\U0001F600\\\""|) == {:ok, "a\nbAAé😀\\\""}
      assert Literal.string(~S|"\xff"|) == {:ok, <<0xFF>>}
    end

    test "reads single quotes with \\' and a bare double quote" do
      assert Literal.string(~S|'foo\'bar"BAZ'|) == {:ok, ~S|foo'bar"BAZ|}
    end

    test "reads backquotes raw, without carriage returns" do
      assert Literal.string("`a\\n\r\"b`") == {:ok, ~S|a\n"b|}
    end

    test "refuses unknown escapes, raw newlines and stray quotes" do
      for token <- [~S|"a\q"|, "\"a\nb\"", ~S|"\'"|, ~S|'a\"b'|, "`a\\`b`", ~S|"\uD800"|] do
        assert {:error, "cannot parse string literal " <> _token} = Literal.string(token), token
      end
    end
  end

  test "quote_string/1 writes Go's strconv.Quote" do
    assert Literal.quote_string("a\"b\\c") == ~S|"a\"b\\c"|
    assert Literal.quote_string("\n\t\r\a\b\f\v") == ~S|"\n\t\r\a\b\f\v"|
    assert Literal.quote_string(<<1, 0x7F, 0xFF>>) == ~S|"\x01\x7f\xff"|
    assert Literal.quote_string("\u00e9\u0085" <> <<0xC2, 0xA0>>) == ~S|"é\u0085\u00a0"|
    assert Literal.quote_string("\u{E0001}") == ~S|"\U000e0001"|
  end

  test "unescape_ident/1 replaces every escape" do
    assert Literal.unescape_ident(~S|\x2E\x2ef\oo|) == "..foo"
    assert Literal.unescape_ident(~S|aé\-b|) == "aé-b"
    assert Literal.unescape_ident(~S|a\xZZ|) == ~S|a\xZZ|
    assert Literal.unescape_ident("tail\\") == "tail\\"
  end

  test "escape_ident/1 escapes what may not stand in an identifier" do
    assert Literal.escape_ident("..foo") == ~S|\..foo|
    assert Literal.escape_ident("3foo") == ~S|\3foo|
    assert Literal.escape_ident("a b-c") == ~S|a\ b\-c|
    assert Literal.escape_ident("温度:x.y") == "温度:x.y"
    assert Literal.escape_ident("a\u0001") == ~S|a\x01|
  end
end
