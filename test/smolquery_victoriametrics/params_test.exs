defmodule SmolqueryVictoriaMetrics.ParamsTest do
  use ExUnit.Case, async: true

  import Plug.Conn, only: [put_req_header: 3]
  import Plug.Test, only: [conn: 3]

  alias SmolqueryVictoriaMetrics.Params

  doctest Params

  describe "time/3, as VictoriaMetrics' GetTime reads it" do
    test "Unix seconds, whole or with a fraction, kept to the millisecond" do
      for {text, ms} <- [
            {"1562529662.324", 1_562_529_662_324},
            {"1223372036.855", 1_223_372_036_855},
            {"1695000000", 1_695_000_000_000},
            {"1695000000.5", 1_695_000_000_500},
            {"1695000000.12345", 1_695_000_000_123},
            {"1.5e9", 1_500_000_000_000}
          ] do
        assert Params.time(%{"t" => text}, "t", 0) == {:ok, ms}, text
      end
    end

    test "RFC 3339" do
      assert Params.time(%{"t" => "2020-02-21T16:07:49.433Z"}, "t", 0) == {:ok, 1_582_301_269_433}

      assert Params.time(%{"t" => "2019-07-07T20:47:40+03:00"}, "t", 0) ==
               {:ok, 1_562_521_660_000}
    end

    test "before the epoch is the epoch" do
      assert Params.time(%{"t" => "-9223372036.854"}, "t", 0) == {:ok, 0}
    end

    test "missing is the default, rounded down to the second" do
      assert Params.time(%{}, "t", 123_456) == {:ok, 123_000}
      assert Params.time(%{"t" => ""}, "t", 123_456) == {:ok, 123_000}
    end

    test "anything else is refused" do
      for text <- ["foo", "foo1", "1245-5", "2022-x7", "2022-02-02Tx7"] do
        assert {:error, message} = Params.time(%{"t" => text}, "t", 0), text
        assert message =~ "cannot parse t=#{text}"
      end
    end

    test "seconds past what a double of milliseconds holds are refused, not raised" do
      for text <- ["1e308", "-1e308", "1.7976931348623157e308"] do
        assert {:error, message} = Params.time(%{"t" => text}, "t", 0), text
        assert message =~ "out of the range"
      end
    end
  end

  describe "duration/3, as VictoriaMetrics' GetDuration reads it" do
    test "seconds as a number, or a duration" do
      for {text, ms} <- [
            {"15", 15_000},
            {"0.5", 500},
            {"15s", 15_000},
            {"1m30s", 90_000},
            {"1h", 3_600_000},
            {"250ms", 250}
          ] do
        assert Params.duration(%{"step" => text}, "step", nil) == {:ok, ms}, text
      end
    end

    test "missing or undefined is the default" do
      assert Params.duration(%{}, "step", 300_000) == {:ok, 300_000}
      assert Params.duration(%{"step" => "undefined"}, "step", 7) == {:ok, 7}
    end

    test "zero, negative, past 100 years, or unreadable is refused" do
      for text <- ["0", "-1", "-5s", "200y", "1e308", "-1e308"] do
        assert {:error, message} = Params.duration(%{"step" => text}, "step", nil), text
        assert message =~ "is out of allowed range"
      end

      assert {:error, "cannot parse step=\"soon\""} =
               Params.duration(%{"step" => "soon"}, "step", nil)
    end
  end

  describe "timeout/2" do
    test "held to the ceiling, which is also the default" do
      assert Params.timeout(%{"timeout" => "1000h"}, 30_000) == {:ok, 30_000}
      assert Params.timeout(%{"timeout" => "250ms"}, 30_000) == {:ok, 250}
      assert Params.timeout(%{"timeout" => "undefined"}, 30_000) == {:ok, 30_000}
      assert {:error, _message} = Params.timeout(%{"timeout" => "1e308"}, 30_000)
    end
  end

  describe "read/1, as Go's ParseForm gathers r.Form" do
    test "the URL's pairs in order, repeats kept" do
      conn = conn(:get, "/x?match[]=a&match=b&match%5B%5D=c&limit=1", nil)

      assert {:ok, pairs, _conn} = Params.read(conn)
      assert pairs == [{"match[]", "a"}, {"match", "b"}, {"match[]", "c"}, {"limit", "1"}]
    end

    test "a form-encoded POST body's pairs come first" do
      conn =
        conn(:post, "/x?limit=1&match[]=url", "match%5B%5D=up&limit=2")
        |> put_req_header("content-type", "application/x-www-form-urlencoded")

      assert {:ok, pairs, _conn} = Params.read(conn)
      assert pairs == [{"match[]", "up"}, {"limit", "2"}, {"limit", "1"}, {"match[]", "url"}]
      assert Params.values(pairs) == %{"match[]" => "up", "limit" => "2"}
      assert Params.all(pairs, ["match[]"]) == ["up", "url"]
    end

    test "a POST body of another type is not read" do
      conn =
        conn(:post, "/x?a=1", "a=2")
        |> put_req_header("content-type", "application/json")

      assert {:ok, [{"a", "1"}], _conn} = Params.read(conn)
    end

    test "a body past 1 MiB is bad data" do
      conn =
        conn(:post, "/x", String.duplicate("a", 1_048_577))
        |> put_req_header("content-type", "application/x-www-form-urlencoded")

      assert {:error, {:bad_data, "the request body is too large"}, %Plug.Conn{}} =
               Params.read(conn)
    end
  end

  describe "int/2, as GetInt reads it" do
    test "missing or empty is 0, an integer is read with its sign" do
      assert Params.int(%{}, "limit") == {:ok, 0}
      assert Params.int(%{"limit" => ""}, "limit") == {:ok, 0}
      assert Params.int(%{"limit" => "10"}, "limit") == {:ok, 10}
      assert Params.int(%{"limit" => "-3"}, "limit") == {:ok, -3}
    end

    test "anything else is refused" do
      for text <- ["ten", "1.5", "10x"] do
        assert Params.int(%{"limit" => text}, "limit") ==
                 {:error, "cannot parse integer \"limit\"=#{inspect(text)}"}
      end
    end
  end
end
