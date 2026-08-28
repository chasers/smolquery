defmodule SmolqueryPg.ProtocolTest do
  use ExUnit.Case, async: true

  alias Smolquery.Test.PgClient
  alias SmolqueryPg.Protocol

  describe "decode_startup/1" do
    test "reads a startup packet's parameters" do
      packet = IO.iodata_to_binary(PgClient.startup([{"user", "u"}, {"database", "d"}]))

      assert {:ok, {:startup, %{"user" => "u", "database" => "d"}}, <<>>} =
               Protocol.decode_startup(packet)
    end

    test "recognises the three request codes" do
      assert {:ok, :ssl_request, "rest"} =
               Protocol.decode_startup(<<8::32, 80_877_103::32, "rest">>)

      assert {:ok, :gssenc_request, <<>>} = Protocol.decode_startup(<<8::32, 80_877_104::32>>)

      assert {:ok, {:cancel_request, 7, 9}, <<>>} =
               Protocol.decode_startup(<<16::32, 80_877_102::32, 7::32, 9::32>>)
    end

    test "waits for a whole packet, and refuses an absurd length" do
      assert :incomplete = Protocol.decode_startup(<<30::32, 196_608::32, "user">>)
      assert :incomplete = Protocol.decode_startup(<<1, 2>>)

      assert {:error, {:message_too_large, _length}} =
               Protocol.decode_startup(<<1_000_000_000::32, 196_608::32>>)

      assert {:error, {:unsupported_protocol, 2, 0}} =
               Protocol.decode_startup(<<8::32, 131_072::32>>)
    end
  end

  describe "decode/1" do
    test "reads a query and leaves the rest of the buffer" do
      buffer = IO.iodata_to_binary([PgClient.frame(?Q, ["SELECT 1", 0]), PgClient.frame(?X, [])])

      assert {:ok, {:query, "SELECT 1"}, rest} = Protocol.decode(buffer)
      assert {:ok, :terminate, <<>>} = Protocol.decode(rest)
    end

    test "answers :incomplete until the body has arrived" do
      assert :incomplete = Protocol.decode(<<?Q, 20::32, "SEL">>)
    end

    test "names a message it does not know" do
      assert {:ok, {:unknown, ?F, "body"}, <<>>} =
               Protocol.decode(IO.iodata_to_binary(PgClient.frame(?F, "body")))
    end
  end

  describe "encoders" do
    test "frame a row description and a data row" do
      description =
        [%{name: "n", oid: 20, typlen: 8, typmod: -1, format: 0}]
        |> Protocol.row_description()
        |> IO.iodata_to_binary()

      assert <<?T, 26::32, 1::16, "n", 0, 0::32, 0::16, 20::32, 8::16, -1::32-signed, 0::16>> =
               description

      row = ["42", nil] |> Protocol.data_row() |> IO.iodata_to_binary()

      assert <<?D, 16::32, 2::16, 2::32, "42", -1::32-signed>> = row
    end

    test "an error carries severity, code, and message fields" do
      error = Protocol.error_response("42601", "syntax error") |> IO.iodata_to_binary()
      <<?E, _length::32, body::binary>> = error

      assert PgClient.fields(body) == %{
               "S" => "ERROR",
               "V" => "ERROR",
               "C" => "42601",
               "M" => "syntax error"
             }
    end

    test "ready for query carries the transaction status" do
      assert IO.iodata_to_binary(Protocol.ready_for_query(:idle)) == <<?Z, 5::32, ?I>>
      assert IO.iodata_to_binary(Protocol.ready_for_query(:transaction)) == <<?Z, 5::32, ?T>>
      assert IO.iodata_to_binary(Protocol.ready_for_query(:failed)) == <<?Z, 5::32, ?E>>
    end
  end
end

defmodule SmolqueryPg.ProtocolExtendedTest do
  use ExUnit.Case, async: true

  alias Smolquery.Test.PgClient
  alias SmolqueryPg.Protocol

  test "decodes the extended protocol's messages" do
    parse = IO.iodata_to_binary(PgClient.parse("s1", "SELECT $1", [20]))
    assert {:ok, {:parse, "s1", "SELECT $1", [20]}, <<>>} = Protocol.decode(parse)

    bind = IO.iodata_to_binary(PgClient.bind("p1", "s1", [{20, 1, <<1::64>>}, {25, 0, nil}], [1]))

    assert {:ok, {:bind, "p1", "s1", [1, 0], [<<1::64>>, nil], [1]}, <<>>} =
             Protocol.decode(bind)

    describe = IO.iodata_to_binary(PgClient.describe(?S, "s1"))
    assert {:ok, {:describe, :statement, "s1"}, <<>>} = Protocol.decode(describe)

    execute = IO.iodata_to_binary(PgClient.execute("p1", 50))
    assert {:ok, {:execute, "p1", 50}, <<>>} = Protocol.decode(execute)

    close = IO.iodata_to_binary(PgClient.frame(?C, [?P, "p1", 0]))
    assert {:ok, {:close, :portal, "p1"}, <<>>} = Protocol.decode(close)

    assert {:ok, :flush, <<>>} = Protocol.decode(IO.iodata_to_binary(PgClient.frame(?H, [])))
  end

  test "encodes the bodiless answers and a parameter description" do
    assert IO.iodata_to_binary(Protocol.parse_complete()) == <<?1, 4::32>>
    assert IO.iodata_to_binary(Protocol.portal_suspended()) == <<?s, 4::32>>
    assert IO.iodata_to_binary(Protocol.no_data()) == <<?n, 4::32>>

    assert IO.iodata_to_binary(Protocol.parameter_description([20, 25])) ==
             <<?t, 14::32, 2::16, 20::32, 25::32>>
  end
end
