defmodule Smolquery.StatTextTest do
  use ExUnit.Case, async: true

  alias Smolquery.StatText

  doctest StatText

  describe "date/1" do
    test "reads four to seven year digits" do
      assert StatText.date("2024-02-29") == ~D[2024-02-29]
      assert StatText.date("0001-01-01") == ~D[0001-01-01]
      assert StatText.date("10000-01-01") == Date.new!(10_000, 1, 1)
      assert StatText.date("5881580-07-10") == Date.new!(5_881_580, 7, 10)
    end

    test "is nil for what has no Date form here" do
      for text <- [
            "infinity",
            "-infinity",
            "0044-03-15 (BC)",
            "-0044-03-15",
            "2023-02-29",
            "24-01-01",
            "12345678-01-01",
            "2024-01-01 00:00:00",
            ""
          ] do
        assert StatText.date(text) == nil, text
      end
    end
  end

  describe "naive_datetime/2" do
    test "reads a space or a T, at microsecond precision" do
      for text <- ["2024-01-01 10:00:00", "2024-01-01T10:00:00"] do
        assert StatText.naive_datetime(text, :floor) == ~N[2024-01-01 10:00:00.000000]
      end

      assert StatText.naive_datetime("2024-01-01 10:00:00.5", :floor) ==
               ~N[2024-01-01 10:00:00.500000]
    end

    test "reads years past 9999 up to DuckDB's last timestamp" do
      assert StatText.naive_datetime("10000-01-01 00:00:00", :floor) ==
               NaiveDateTime.new!(10_000, 1, 1, 0, 0, 0, {0, 6})

      assert StatText.naive_datetime("294247-01-10 04:00:54.775806", :ceil) ==
               NaiveDateTime.new!(294_247, 1, 10, 4, 0, 54, {775_806, 6})
    end

    test "rounds a nanosecond fraction outward" do
      assert StatText.naive_datetime("1969-12-31 23:59:59.999999999", :floor) ==
               ~N[1969-12-31 23:59:59.999999]

      assert StatText.naive_datetime("1969-12-31 23:59:59.999999999", :ceil) ==
               ~N[1970-01-01 00:00:00.000000]

      assert StatText.naive_datetime("10000-01-01 00:00:00.000000001", :ceil) ==
               NaiveDateTime.new!(10_000, 1, 1, 0, 0, 0, {1, 6})

      assert StatText.naive_datetime("2024-01-01 00:00:00.123456", :ceil) ==
               ~N[2024-01-01 00:00:00.123456]
    end

    test "reads the hot manifest's encoding as NaiveDateTime.from_iso8601/1 did" do
      for datetime <- [
            ~N[2026-07-31 12:00:01.000000],
            ~N[1960-06-01 00:00:00.500000],
            ~N[0001-01-01 00:00:00.000000],
            ~N[9999-12-31 23:59:59.999999]
          ] do
        text = NaiveDateTime.to_iso8601(datetime)

        assert StatText.naive_datetime(text, :floor) == datetime
        assert {:ok, ^datetime} = NaiveDateTime.from_iso8601(text)
      end
    end

    test "is nil for what has no NaiveDateTime form here" do
      for text <- [
            "infinity",
            "-infinity",
            "0044-03-15 10:00:00 (BC)",
            "-0044-03-15 10:00:00",
            "2024-01-01 24:00:00",
            "2024-01-01 10:00:00.1234567890",
            "2024-01-01 10:00:00Z",
            "2024-01-01"
          ] do
        assert StatText.naive_datetime(text, :floor) == nil, text
      end
    end
  end
end
