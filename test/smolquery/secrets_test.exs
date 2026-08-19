defmodule Smolquery.SecretsTest do
  use ExUnit.Case, async: false

  alias Smolquery.Secrets

  setup do
    previous = Application.get_env(:smolquery, :credential_key)
    Application.put_env(:smolquery, :credential_key, Base.encode64(:crypto.strong_rand_bytes(32)))

    on_exit(fn ->
      if previous do
        Application.put_env(:smolquery, :credential_key, previous)
      else
        Application.delete_env(:smolquery, :credential_key)
      end
    end)

    :ok
  end

  describe "seal/1 and open/1" do
    test "a sealed password opens back to itself" do
      assert {:ok, sealed} = Secrets.seal("hunter2")
      assert {:ok, "hunter2"} = Secrets.open(sealed)
    end

    test "the sealed form carries no plaintext" do
      assert {:ok, sealed} = Secrets.seal("hunter2")

      refute sealed =~ "hunter2"
      assert String.starts_with?(sealed, "v1.")
    end

    test "sealing the same password twice gives different ciphertext" do
      assert {:ok, first} = Secrets.seal("hunter2")
      assert {:ok, second} = Secrets.seal("hunter2")

      refute first == second
      assert {:ok, "hunter2"} = Secrets.open(first)
      assert {:ok, "hunter2"} = Secrets.open(second)
    end

    test "an empty password round-trips" do
      assert {:ok, sealed} = Secrets.seal("")
      assert {:ok, ""} = Secrets.open(sealed)
    end

    test "a password with multibyte characters round-trips" do
      assert {:ok, sealed} = Secrets.seal("pässwörd–ok")
      assert {:ok, "pässwörd–ok"} = Secrets.open(sealed)
    end
  end

  describe "open/1 refuses what it cannot authenticate" do
    test "a tampered ciphertext does not open" do
      assert {:ok, sealed} = Secrets.seal("hunter2")
      [version, iv, tag, ciphertext] = String.split(sealed, ".")
      tampered = Enum.join([version, iv, tag, flip(ciphertext)], ".")

      assert Secrets.open(tampered) == {:error, :invalid_secret}
    end

    test "a tampered tag does not open" do
      assert {:ok, sealed} = Secrets.seal("hunter2")
      [version, iv, tag, ciphertext] = String.split(sealed, ".")

      assert Secrets.open(Enum.join([version, iv, flip(tag), ciphertext], ".")) ==
               {:error, :invalid_secret}
    end

    test "a value sealed under another key does not open" do
      assert {:ok, sealed} = Secrets.seal("hunter2")

      Application.put_env(
        :smolquery,
        :credential_key,
        Base.encode64(:crypto.strong_rand_bytes(32))
      )

      assert Secrets.open(sealed) == {:error, :invalid_secret}
    end

    test "an unknown version is refused" do
      assert {:ok, sealed} = Secrets.seal("hunter2")
      ["v1" | rest] = String.split(sealed, ".")

      assert Secrets.open(Enum.join(["v2" | rest], ".")) == {:error, :invalid_secret}
    end

    test "malformed input is refused, not a crash" do
      for bad <- ["", "nonsense", "v1.", "v1.a.b", "v1.!!.!!.!!"] do
        assert Secrets.open(bad) == {:error, :invalid_secret}
      end
    end
  end

  describe "without a usable key" do
    test "configured?/0 is false and both operations refuse" do
      Application.delete_env(:smolquery, :credential_key)

      refute Secrets.configured?()
      assert Secrets.seal("hunter2") == {:error, :no_credential_key}
      assert Secrets.open("v1.a.b.c") == {:error, :no_credential_key}
    end

    test "a key of the wrong length counts as no key" do
      Application.put_env(:smolquery, :credential_key, Base.encode64(<<1, 2, 3>>))

      refute Secrets.configured?()
      assert Secrets.seal("hunter2") == {:error, :no_credential_key}
    end

    test "a key that is not base64 counts as no key" do
      Application.put_env(:smolquery, :credential_key, "not base64 at all !!")

      refute Secrets.configured?()
    end
  end

  describe "configured?/0" do
    test "is true for a 32-byte base64 key" do
      assert Secrets.configured?()
    end
  end

  defp flip(<<first::binary-size(1), rest::binary>>) do
    replacement = if first == "A", do: "B", else: "A"
    replacement <> rest
  end
end
