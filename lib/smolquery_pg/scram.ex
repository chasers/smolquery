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

  The expensive derivation happens once, at boot: `verifier/1` salts the
  password (PBKDF2, 4096 iterations) under a per-boot random salt and
  keeps the stored key and server key. Per-connection work is then four
  HMACs — a reconnect storm cannot buy CPU with garbage proofs, which a
  per-connection salt would hand any unauthenticated caller. The client
  never notices: it uses whatever salt `server-first` names.

  The password is normalized to NFKC before the derivation — the core of
  SASLprep (RFC 4013), which libpq applies client-side before computing
  its proof; without it a correct non-ASCII password in another Unicode
  form would be refused. The prohibited-character tables are not
  enforced.

  Only the base `SCRAM-SHA-256` mechanism is offered — no channel
  binding — and a client that requires binding (`p=...`) is refused. The
  `c=` echo in `client-final` is verified against the GS2 header the
  client actually sent, as RFC 5802 requires.
  """

  @mechanism "SCRAM-SHA-256"
  @iterations 4096
  @salt_bytes 16
  @nonce_bytes 18
  @key_length 32

  @type verifier :: %{
          salt: binary(),
          iterations: pos_integer(),
          stored_key: binary(),
          server_key: binary()
        }

  @type t :: %{
          verifier: verifier(),
          nonce: String.t(),
          gs2: String.t(),
          client_first_bare: String.t(),
          server_first: String.t()
        }

  @doc """
  The mechanism this server offers.
  """
  @spec mechanism() :: String.t()
  def mechanism, do: @mechanism

  @doc """
  The boot-time verifier for `password`: a fresh salt, and the keys the
  per-connection exchange needs.
  """
  @spec verifier(String.t()) :: verifier()
  def verifier(password) do
    normalized = :unicode.characters_to_nfkc_binary(password)
    salt = :crypto.strong_rand_bytes(@salt_bytes)
    salted = :crypto.pbkdf2_hmac(:sha256, normalized, salt, @iterations, @key_length)
    client_key = :crypto.mac(:hmac, :sha256, salted, "Client Key")

    %{
      salt: salt,
      iterations: @iterations,
      stored_key: :crypto.hash(:sha256, client_key),
      server_key: :crypto.mac(:hmac, :sha256, salted, "Server Key")
    }
  end

  @doc """
  Reads `client-first`, answers `server-first` and the exchange state.
  """
  @spec server_first(String.t(), verifier()) :: {:ok, String.t(), t()} | {:error, String.t()}
  def server_first(client_first, verifier) do
    with {:ok, gs2, bare} <- strip_gs2(client_first),
         %{"r" => client_nonce} <- attributes(bare) do
      nonce = client_nonce <> Base.encode64(:crypto.strong_rand_bytes(@nonce_bytes))

      server_first =
        "r=#{nonce},s=#{Base.encode64(verifier.salt)},i=#{verifier.iterations}"

      {:ok, server_first,
       %{
         verifier: verifier,
         nonce: nonce,
         gs2: gs2,
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
    with %{"r" => nonce, "p" => proof, "c" => binding} <- attributes(client_final),
         true <- nonce == state.nonce or {:error, "SCRAM nonce mismatch"},
         true <-
           binding == Base.encode64(state.gs2) or
             {:error, "SCRAM channel-binding echo does not match the GS2 header"},
         {:ok, proof} <- decode_proof(proof) do
      verify(client_final, proof, state)
    else
      {:error, reason} -> {:error, reason}
      _malformed -> {:error, "malformed SCRAM client-final message"}
    end
  end

  defp verify(client_final, proof, state) do
    %{stored_key: stored_key, server_key: server_key} = state.verifier
    without_proof = client_final |> String.split(",p=", parts: 2) |> hd()

    auth_message =
      Enum.join([state.client_first_bare, state.server_first, without_proof], ",")

    client_signature = :crypto.mac(:hmac, :sha256, stored_key, auth_message)
    recovered_key = :crypto.exor(proof, client_signature)

    if Plug.Crypto.secure_compare(:crypto.hash(:sha256, recovered_key), stored_key) do
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

  defp strip_gs2("n,," <> bare), do: {:ok, "n,,", bare}
  defp strip_gs2("y,," <> bare), do: {:ok, "y,,", bare}

  defp strip_gs2("p=" <> _rest),
    do: {:error, "channel binding is not supported; use SCRAM-SHA-256 without binding"}

  defp strip_gs2(_other), do: {:error, "malformed SCRAM GS2 header"}

  @doc """
  The `key=value` attributes of a SCRAM message. Public so a test speaks
  the same parser the server does.
  """
  @spec attributes(String.t()) :: %{String.t() => String.t()}
  def attributes(message) do
    for <<key, ?=, value::binary>> <- String.split(message, ","), into: %{} do
      {<<key>>, value}
    end
  end
end
