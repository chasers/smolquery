defmodule Smolquery.Engine.ParamsTest do
  use ExUnit.Case, async: true

  alias Smolquery.Engine.Params

  describe "normalize/1" do
    test "binds a NaiveDateTime as a TIMESTAMP without a timezone" do
      assert [%Adbc.Column{field: field}] = Params.normalize([~N[2026-07-31 12:00:00]])

      assert field.type == {:timestamp, :microseconds, ""}
    end

    test "keeps microsecond precision" do
      assert [column] = Params.normalize([~N[2026-07-31 12:00:00.123456]])

      assert Adbc.Column.to_list(column) == [~N[2026-07-31 12:00:00.123456]]
    end

    test "binds a UTC DateTime as the same TIMESTAMP" do
      utc = DateTime.new!(~D[2026-07-31], ~T[12:00:00.123456], "Etc/UTC")

      assert [%Adbc.Column{field: field} = column] = Params.normalize([utc])
      assert field.type == {:timestamp, :microseconds, ""}
      assert Adbc.Column.to_list(column) == [~N[2026-07-31 12:00:00.123456]]
    end

    test "shifts an offset DateTime to UTC rather than dropping the offset" do
      offset = %DateTime{
        year: 2026,
        month: 7,
        day: 31,
        hour: 12,
        minute: 0,
        second: 0,
        microsecond: {0, 6},
        time_zone: "Etc/GMT-2",
        zone_abbr: "+02",
        utc_offset: 7200,
        std_offset: 0
      }

      assert [column] = Params.normalize([offset])
      assert Adbc.Column.to_list(column) == [~N[2026-07-31 10:00:00.000000]]
    end

    test "leaves types ADBC already infers correctly alone" do
      params = [1, 1.5, "text", true, nil, ~D[2026-07-31], Decimal.new("1.25")]

      assert Params.normalize(params) == params
    end

    test "preserves position and count" do
      assert [1, %Adbc.Column{}, "x"] = Params.normalize([1, ~N[2026-07-31 12:00:00], "x"])
    end

    test "accepts an empty list" do
      assert Params.normalize([]) == []
    end
  end
end
