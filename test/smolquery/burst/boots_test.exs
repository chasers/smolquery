defmodule Smolquery.Burst.BootsTest do
  use ExUnit.Case, async: false

  alias Smolquery.Burst.Boots

  describe "without the table" do
    test "a start is accepted and an end has no duration" do
      refute Process.whereis(Boots)
      pid = spawn(fn -> :ok end)

      assert Boots.started(pid) == :ok
      assert Boots.finished(pid) == nil
    end
  end

  describe "with the table" do
    setup do
      start_supervised!(Boots)
      :ok
    end

    test "times a boot across the two processes the hooks run in" do
      booter = spawn(fn -> :ok end)
      parent = self()

      spawn(fn ->
        Boots.started(booter)
        send(parent, :started)
      end)

      assert_receive :started
      Process.sleep(20)

      duration = Boots.finished(booter)
      assert is_integer(duration) and duration >= 20
      assert Boots.finished(booter) == nil
    end
  end
end
