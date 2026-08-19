defmodule Smolquery.Secrets do
  @moduledoc """
  Authenticated encryption for the credentials the catalog stores (T-322).

  A federated connection needs a password the query path can replay, so the
  catalog must hold something reversible — a hash would not do. What it must
  not hold is plaintext: the catalog's metadata database is a Postgres a
  deployment already replicates, backs up, and hands to operators, and a
  password sitting in a side table is a password in every one of those copies.

  So the password is sealed here before it reaches the catalog and opened only
  when a job engine builds its `ATTACH`. The key lives in the environment
  (`SMOLQUERY_CREDENTIAL_KEY`), never in the catalog. Losing the key
  invalidates every stored connection, which is the intended failure: the
  operator re-enters passwords rather than the catalog holding enough to
  reconstruct them.

  ## The construction

  AES-256-GCM, a fresh 12-byte IV per seal, and a 16-byte tag. GCM rather than
  CBC because the tag authenticates: a tampered ciphertext fails to open
  instead of decrypting to garbage that then travels into a connection string.

  The sealed form is `v1.<iv>.<tag>.<ciphertext>`, each part
  URL-safe base64 without padding. The version prefix is what makes key
  rotation a decode concern rather than a migration — a `v2` reader can accept
  both while it re-seals.

  `@aad` is bound into the tag, so a sealed value lifted out of this table
  cannot be presented to a future caller that seals something else with the
  same key.

  ## Why a missing key is not an error here

  `configured?/0` answers whether a key exists without raising, because the
  boot check needs the question answered on a node that may legitimately have
  no key: a deployment that never registers a connection is never asked for
  one. `seal/1` and `open/1` raise nothing either — they return
  `{:error, :no_credential_key}`, so the API answers a misconfigured node with
  a clear refusal rather than a 500 from a crash.
  """

  @version "v1"
  @aad "smolquery.connection.v1"
  @iv_bytes 12
  @tag_bytes 16
  @key_bytes 32

  @doc """
  Whether this node holds a usable credential key.

  False for both a missing key and one that is not #{@key_bytes} bytes after
  base64 decoding, because neither can seal anything and the difference does
  not change what a caller does.
  """
  @spec configured?() :: boolean()
  def configured? do
    match?({:ok, _key}, key())
  end

  @doc """
  Seals `plaintext` into the storable form.

  Returns `{:error, :no_credential_key}` when this node holds no usable key.
  """
  @spec seal(String.t()) :: {:ok, String.t()} | {:error, :no_credential_key}
  def seal(plaintext) when is_binary(plaintext) do
    with {:ok, key} <- key() do
      iv = :crypto.strong_rand_bytes(@iv_bytes)

      {ciphertext, tag} =
        :crypto.crypto_one_time_aead(:aes_256_gcm, key, iv, plaintext, @aad, @tag_bytes, true)

      {:ok, Enum.map_join([@version, iv, tag, ciphertext], ".", &encode/1)}
    end
  end

  @doc """
  Opens a value `seal/1` produced.

  `{:error, :invalid_secret}` covers every way a stored value can fail to
  open — an unknown version, a malformed part, a wrong key, a tampered
  ciphertext — because none of them is separately actionable and telling them
  apart would describe the key to a caller holding the ciphertext.
  """
  @spec open(String.t()) :: {:ok, String.t()} | {:error, :no_credential_key | :invalid_secret}
  def open(sealed) when is_binary(sealed) do
    with {:ok, key} <- key(),
         {:ok, iv, tag, ciphertext} <- parts(sealed) do
      case :crypto.crypto_one_time_aead(:aes_256_gcm, key, iv, ciphertext, @aad, tag, false) do
        plaintext when is_binary(plaintext) -> {:ok, plaintext}
        _error -> {:error, :invalid_secret}
      end
    end
  end

  defp parts(sealed) do
    with [@version, iv, tag, ciphertext] <- String.split(sealed, ".", parts: 4),
         {:ok, iv} <- decode(iv, @iv_bytes),
         {:ok, tag} <- decode(tag, @tag_bytes),
         {:ok, ciphertext} <- Base.url_decode64(ciphertext, padding: false) do
      {:ok, iv, tag, ciphertext}
    else
      _malformed -> {:error, :invalid_secret}
    end
  end

  defp decode(value, bytes) do
    case Base.url_decode64(value, padding: false) do
      {:ok, decoded} when byte_size(decoded) == bytes -> {:ok, decoded}
      _wrong -> :error
    end
  end

  defp encode(@version), do: @version
  defp encode(binary), do: Base.url_encode64(binary, padding: false)

  defp key do
    with value when is_binary(value) <- Application.get_env(:smolquery, :credential_key),
         {:ok, key} <- Base.decode64(value),
         @key_bytes <- byte_size(key) do
      {:ok, key}
    else
      _absent_or_wrong_size -> {:error, :no_credential_key}
    end
  end
end
