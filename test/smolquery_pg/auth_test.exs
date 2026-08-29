defmodule SmolqueryPg.AuthTest do
  @moduledoc """
  SCRAM-SHA-256 and the TLS upgrade (PL-58 layer 5).
  """

  use ExUnit.Case, async: false

  alias Smolquery.QueryService
  alias Smolquery.Test.FixedCatalog
  alias Smolquery.Test.MapCatalog
  alias Smolquery.Test.PgClient
  alias SmolqueryPg.Runtime
  alias SmolqueryPg.Scram

  @password "auth-test-password"
  @cert Path.expand("../support/fixtures/pg_tls_cert.pem", __DIR__)
  @key Path.expand("../support/fixtures/pg_tls_key.pem", __DIR__)

  defp start_edge(opts) do
    unique = :erlang.unique_integer([:positive])
    query = :"pg_auth_query_#{unique}"
    pg = :"pg_auth_edge_#{unique}"

    start_supervised!(
      {QueryService.Supervisor,
       name: query, catalog: FixedCatalog.new(%{snapshot: 1, schemas: %{}, segments: %{}})},
      id: query
    )

    on_exit(fn -> QueryService.Runtime.delete(query) end)

    start_supervised!(
      {SmolqueryPg.Supervisor,
       Keyword.merge(
         [name: pg, password: @password, query_name: query, port: 0, catalog: MapCatalog.new()],
         opts
       )},
      id: pg
    )

    on_exit(fn -> Runtime.delete(pg) end)

    {:ok, {_ip, port}} = SmolqueryPg.Supervisor.bound(pg)

    port
  end

  describe "SCRAM-SHA-256 (the default)" do
    test "authenticates without the password crossing the wire" do
      port = start_edge([])

      {:ok, socket, params} = PgClient.connect(port, password: @password)

      assert params["server_version"] == "14.10"
      assert %{results: [%{rows: [["1"]]}]} = PgClient.query(socket, "SELECT 1")
    end

    test "refuses a wrong password with 28P01" do
      port = start_edge([])

      assert {:error, %{"C" => "28P01"}} = PgClient.connect(port, password: "wrong")
    end

    test "the exchange verifies both directions" do
      {:ok, server_first, state} =
        Scram.server_first("n,,n=,r=clientnonce", Scram.verifier(@password))

      assert server_first =~ "r=clientnonce"
      assert %{"r" => nonce, "s" => salt, "i" => "4096"} = scram_map(server_first)
      assert String.starts_with?(nonce, "clientnonce")

      {:ok, decoded_salt} = Base.decode64(salt)
      salted = :crypto.pbkdf2_hmac(:sha256, @password, decoded_salt, 4096, 32)
      client_key = :crypto.mac(:hmac, :sha256, salted, "Client Key")
      stored = :crypto.hash(:sha256, client_key)
      without_proof = "c=biws,r=" <> nonce
      auth_message = Enum.join(["n=,r=clientnonce", server_first, without_proof], ",")
      signature = :crypto.mac(:hmac, :sha256, stored, auth_message)
      proof = Base.encode64(:crypto.exor(client_key, signature))

      assert {:ok, "v=" <> server_signature} =
               Scram.server_final(without_proof <> ",p=" <> proof, state)

      server_key = :crypto.mac(:hmac, :sha256, salted, "Server Key")

      assert Base.decode64!(server_signature) ==
               :crypto.mac(:hmac, :sha256, server_key, auth_message)

      assert {:error, "SCRAM nonce mismatch"} =
               Scram.server_final("c=biws,r=other,p=" <> proof, state)

      assert {:error, "SCRAM channel-binding echo does not match the GS2 header" <> _rest} =
               Scram.server_final("c=eSws,r=" <> nonce <> ",p=" <> proof, state)

      assert {:error, "password authentication failed"} =
               Scram.server_final(
                 without_proof <> ",p=" <> Base.encode64(:binary.copy(<<0>>, 32)),
                 state
               )
    end

    test "a channel-binding-required client is refused" do
      assert {:error, message} =
               Scram.server_first("p=tls-server-end-point,,n=,r=x", Scram.verifier(@password))

      assert message =~ "channel binding"
    end

    test "a decomposed-Unicode password authenticates: the server normalizes to NFKC" do
      decomposed = "cafe\u0301!"
      composed = :unicode.characters_to_nfkc_binary(decomposed)
      port = start_edge(password: decomposed)

      assert {:ok, _socket, _params} = PgClient.connect(port, password: composed)
    end

    test "cleartext mode still answers a legacy client" do
      port = start_edge(auth: :cleartext)

      {:ok, socket, _params} = PgClient.connect(port, password: @password)

      assert %{results: [%{rows: [["1"]]}]} = PgClient.query(socket, "SELECT 1")
      assert {:error, %{"C" => "28P01"}} = PgClient.connect(port, password: "wrong")
    end

    test "an unauthenticated connection is closed at the auth deadline" do
      port = start_edge(auth_timeout_ms: 200)

      {:ok, socket} = :gen_tcp.connect(~c"127.0.0.1", port, [:binary, active: false])

      assert {:error, :closed} = :gen_tcp.recv(socket, 0, 5_000)
    end
  end

  describe "TLS" do
    test "SSLRequest upgrades the connection and queries run over it" do
      port = start_edge(tls_cert: @cert, tls_key: @key)

      {:ok, tcp} = :gen_tcp.connect(~c"127.0.0.1", port, [:binary, active: false])
      :ok = :gen_tcp.send(tcp, <<8::32, 80_877_103::32>>)
      assert {:ok, "S"} = :gen_tcp.recv(tcp, 1, 5_000)

      {:ok, tls} = :ssl.connect(tcp, [verify: :verify_none, active: false], 5_000)

      :ok = :ssl.send(tls, PgClient.startup([{"user", "u"}, {"database", "d"}]))
      assert {:ok, <<?R, _length::32, 10::32, _rest::binary>>} = :ssl.recv(tls, 0, 5_000)
    end

    test "without a certificate the edge declines and stays plaintext" do
      port = start_edge([])

      {:ok, tcp} = :gen_tcp.connect(~c"127.0.0.1", port, [:binary, active: false])
      :ok = :gen_tcp.send(tcp, <<8::32, 80_877_103::32>>)
      assert {:ok, "N"} = :gen_tcp.recv(tcp, 1, 5_000)
    end

    test "a certificate without a key refuses to boot" do
      assert_raise ArgumentError, ~r/tls_cert and tls_key together/, fn ->
        SmolqueryPg.Runtime.new(password: @password, tls_cert: @cert)
      end
    end

    test "an unreadable or empty certificate file refuses to boot" do
      assert_raise ArgumentError, ~r/cannot read its tls_cert/, fn ->
        SmolqueryPg.Runtime.new(password: @password, tls_cert: "/nope.pem", tls_key: @key)
      end

      empty = Path.join(System.tmp_dir!(), "empty-#{System.unique_integer([:positive])}.pem")
      File.write!(empty, "not pem")

      assert_raise ArgumentError, ~r/holds no PEM entries/, fn ->
        SmolqueryPg.Runtime.new(password: @password, tls_cert: empty, tls_key: @key)
      end
    end

    test "a second SSLRequest inside the TLS session is refused" do
      port = start_edge(tls_cert: @cert, tls_key: @key)

      {:ok, tcp} = :gen_tcp.connect(~c"127.0.0.1", port, [:binary, active: false])
      :ok = :gen_tcp.send(tcp, <<8::32, 80_877_103::32>>)
      assert {:ok, "S"} = :gen_tcp.recv(tcp, 1, 5_000)
      {:ok, tls} = :ssl.connect(tcp, [verify: :verify_none, active: false], 5_000)

      :ok = :ssl.send(tls, <<8::32, 80_877_103::32>>)
      assert {:ok, <<?E, _length::32, body::binary>>} = :ssl.recv(tls, 0, 5_000)
      assert %{"C" => "08P01"} = PgClient.fields(body)
    end

    test "bytes pipelined behind an SSLRequest are refused, never carried across the upgrade" do
      port = start_edge(tls_cert: @cert, tls_key: @key)

      {:ok, tcp} = :gen_tcp.connect(~c"127.0.0.1", port, [:binary, active: false])
      startup = IO.iodata_to_binary(PgClient.startup([{"user", "mitm"}]))
      :ok = :gen_tcp.send(tcp, <<8::32, 80_877_103::32>> <> startup)

      assert {:ok, <<?E, _length::32, body::binary>>} = :gen_tcp.recv(tcp, 0, 5_000)
      assert %{"C" => "08P01", "M" => message} = PgClient.fields(body)
      assert message =~ "after SSLRequest"
    end
  end

  defp scram_map(message) do
    for part <- String.split(message, ","), into: %{} do
      <<key, ?=, value::binary>> = part
      {<<key>>, value}
    end
  end
end
