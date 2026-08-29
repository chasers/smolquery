defmodule SmolqueryPg.Scram do
  @moduledoc """
  The server side of SCRAM-SHA-256 (RFC 5802 / RFC 7677), as Postgres
  speaks it (PL-58 layer 5).

  The exchange replaces the cleartext password message: the client proves
  it knows the password without sending it, and the server proves it holds
  the password's verifier. Three messages cross the wire:

  1. `AuthenticationSASL` offers the mechanism; the client's
     `SASLInitialResponse` carries `client-first`: a GS2 header and the
     client nonce.
  2. `AuthenticationSASLContinue` answers `server-first`: the combined
     nonce, a fresh salt, and the iteration count. The client's
     `SASLResponse` carries `client-final`: the channel-binding echo, the
     combined nonce back, and the proof.
  3. The proof verifies against the salted password; the server answers
     its own signature in `AuthenticationSASLFinal`.

  The salt is per-connection: the server holds the password itself (it is
  the API key), so nothing needs a stored, fixed verifier. Only the base
  `SCRAM-SHA-256` mechanism is offered — no channel binding — and a client
  that requires binding (`p=...`) is refused.
  """

  @mechanism "SCRAM-SHA-256"
  @iterations 4096
  @salt_bytes 16
  @nonce_bytes 18
  @key_length 32

  @type t :: %{
          password: String.t(),
          nonce: String.t(),
          salt: binary(),
          client_first_bare: String.t(),
          server_first: String.t()
        }

  @doc """
  The mechanism this server offers.
  """
  @spec mechanism() :: String.t()
  def mechanism, do: @mechanism

  @doc """
  Reads `client-first`, answers `server-first` and the exchange state.
  """
  @spec server_first(String.t(), String.t()) :: {:ok, String.t(), t()} | {:error, String.t()}
  def server_first(client_first, password) do
    with {:ok, bare} <- strip_gs2(client_first),
         %{"r" => client_nonce} <- attributes(bare) do
      nonce = client_nonce <> Base.encode64(:crypto.strong_rand_bytes(@nonce_bytes))
      salt = :crypto.strong_rand_bytes(@salt_bytes)
      server_first = "r=#{nonce},s=#{Base.encode64(salt)},i=#{@iterations}"

      {:ok, server_first,
       %{
         password: password,
         nonce: nonce,
         salt: salt,
         client_first_bare: bare,
         server_first: server_first
       }}
    else
      {:error, reason} -> {:error, reason}
      _missing_nonce -> {:error, "malformed SCRAM client-first message"}
    end
  end

  @doc """
  Verifies `client-final` and answers `server-final` (the server
  signature), or the refusal.
  """
  @spec server_final(String.t(), t()) :: {:ok, String.t()} | {:error, String.t()}
  def server_final(client_final, state) do
    with %{"r" => nonce, "p" => proof} <- attributes(client_final),
         true <- nonce == state.nonce or {:error, "SCRAM nonce mismatch"},
         {:ok, proof} <- decode_proof(proof) do
      verify(client_final, proof, state)
    else
      {:error, reason} -> {:error, reason}
      _malformed -> {:error, "malformed SCRAM client-final message"}
    end
  end

  defp verify(client_final, proof, state) do
    without_proof = client_final |> String.split(",p=", parts: 2) |> hd()

    auth_message =
      Enum.join([state.client_first_bare, state.server_first, without_proof], ",")

    salted = :crypto.pbkdf2_hmac(:sha256, state.password, state.salt, @iterations, @key_length)
    client_key = :crypto.mac(:hmac, :sha256, salted, "Client Key")
    stored_key = :crypto.hash(:sha256, client_key)
    client_signature = :crypto.mac(:hmac, :sha256, stored_key, auth_message)
    recovered_key = :crypto.exor(proof, client_signature)

    if Plug.Crypto.secure_compare(:crypto.hash(:sha256, recovered_key), stored_key) do
      server_key = :crypto.mac(:hmac, :sha256, salted, "Server Key")
      server_signature = :crypto.mac(:hmac, :sha256, server_key, auth_message)

      {:ok, "v=" <> Base.encode64(server_signature)}
    else
      {:error, "password authentication failed"}
    end
  end

  defp decode_proof(proof) do
    case Base.decode64(proof) do
      {:ok, decoded} when byte_size(decoded) == @key_length -> {:ok, decoded}
      _invalid -> {:error, "malformed SCRAM proof"}
    end
  end

  defp strip_gs2("n,," <> bare), do: {:ok, bare}
  defp strip_gs2("y,," <> bare), do: {:ok, bare}

  defp strip_gs2("p=" <> _rest),
    do: {:error, "channel binding is not supported; use SCRAM-SHA-256 without binding"}

  defp strip_gs2(_other), do: {:error, "malformed SCRAM GS2 header"}

  defp attributes(message) do
    for part <- String.split(message, ","), part != "", into: %{} do
      case part do
        <<key, ?=, value::binary>> -> {<<key>>, value}
        other -> {other, ""}
      end
    end
  end
end
