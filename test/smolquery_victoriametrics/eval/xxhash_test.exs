defmodule SmolqueryVictoriaMetrics.Eval.XXHashTest do
  use ExUnit.Case, async: true

  alias SmolqueryVictoriaMetrics.Eval.XXHash

  test "hash/1 answers the XXH64 reference values, seed 0" do
    assert XXHash.hash("") == 0xEF46DB3751D8E999
    assert XXHash.hash("a") == 0xD24EC4F1A98C6E5B
    assert XXHash.hash("abc") == 0x44BC2CF5AD770999
    assert XXHash.hash("Nobody inspects the spammish repetition") == 0xFBCEA83C8A378BF1
  end

  test "hash/1 reads inputs past one 32-byte stripe" do
    assert XXHash.hash(String.duplicate("x", 100)) == 0x92F0DE5A88A3C094
  end
end
