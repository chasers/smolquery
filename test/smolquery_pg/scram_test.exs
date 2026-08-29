defmodule SmolqueryPg.ScramTest do
  use ExUnit.Case, async: true

  alias SmolqueryPg.Scram

  @password "scram-test-password"

  test "the verifier derives once and both GS2 headers are accepted" do
    verifier = Scram.verifier(@password)

    assert byte_size(verifier.salt) == 16
    assert verifier.iterations == 4096
    assert byte_size(verifier.stored_key) == 32
    assert byte_size(verifier.server_key) == 32

    assert {:ok, _first, _state} = Scram.server_first("n,,n=,r=abc", verifier)
    assert {:ok, _first, _state} = Scram.server_first("y,,n=,r=abc", verifier)
  end

  test "a malformed client-first is named, not crashed on" do
    verifier = Scram.verifier(@password)

    assert {:error, "malformed SCRAM GS2 header"} = Scram.server_first("garbage", verifier)

    assert {:error, "malformed SCRAM client-first message"} =
             Scram.server_first("n,,n=", verifier)
  end

  test "a malformed client-final and a wrong-size proof are refused" do
    {:ok, _first, state} = Scram.server_first("n,,n=,r=abc", Scram.verifier(@password))

    assert {:error, "malformed SCRAM client-final message"} = Scram.server_final("junk", state)

    assert {:error, "malformed SCRAM proof"} =
             Scram.server_final(
               "c=biws,r=" <> state.nonce <> ",p=" <> Base.encode64("short"),
               state
             )
  end

  test "attributes/1 reads key=value pairs and skips what is not one" do
    assert Scram.attributes("r=abc,s=ZZ,i=4096,,x") == %{"r" => "abc", "s" => "ZZ", "i" => "4096"}
  end
end
