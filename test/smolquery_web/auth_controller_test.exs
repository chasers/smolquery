defmodule SmolqueryWeb.AuthControllerTest do
  use SmolqueryWeb.ConnCase, async: false

  alias Smolquery.Auth.OIDC.Provider
  alias Smolquery.{Catalog, Schema}
  alias Smolquery.Test.MapCatalog
  alias SmolqueryWeb.{OIDC, Runtime, Session}

  @private_key JOSE.JWK.generate_key({:rsa, 2048})
  @public_key JOSE.JWK.to_map(JOSE.JWK.to_public(@private_key))
              |> elem(1)
              |> Map.put("kid", "web-key")
  @jwks %{"keys" => [@public_key]}
  @metadata %{
    "issuer" => "https://issuer.example",
    "authorization_endpoint" => "https://issuer.example/authorize",
    "token_endpoint" => "https://issuer.example/token",
    "jwks_uri" => "https://issuer.example/keys",
    "id_token_signing_alg_values_supported" => ["RS256"]
  }
  @all_capabilities [:web_access, :query, :ingest, :catalog_manage, :platform_operate]

  setup do
    {:ok, token_response} = Agent.start_link(fn -> nil end)
    test = self()

    token_client = fn url, options ->
      send(test, {:token_request, url, options})
      Agent.get(token_response, & &1)
    end

    runtime =
      Runtime.new(
        name: SmolqueryWeb,
        catalog: MapCatalog.new(),
        auth_mode: :oidc,
        secret_key_base: String.duplicate("s", 64),
        oidc_http_client: token_client,
        oidc: [
          issuer: "https://issuer.example",
          web_client_id: "web-client",
          web_client_auth_method: :none,
          web_origin: "https://ui.example",
          web_redirect_uri: "https://ui.example/auth/callback",
          claim_capabilities: %{
            "roles" => %{
              "operator" => @all_capabilities,
              "analyst" => [:web_access, :query],
              "reader" => [:web_access]
            }
          }
        ]
      )

    Runtime.put(runtime)
    on_exit(fn -> Runtime.delete(SmolqueryWeb) end)

    start_supervised!(
      {Provider,
       name: SmolqueryWeb.OIDCProvider,
       config: runtime.oidc,
       http_client: fn url, _options ->
         case url do
           "https://issuer.example/.well-known/openid-configuration" -> response(@metadata)
           "https://issuer.example/keys" -> response(@jwks)
         end
       end}
    )

    start_supervised!({Phoenix.PubSub, name: Smolquery.PubSub})
    start_supervised!(SmolqueryWeb.Endpoint)

    %{runtime: runtime, token_response: token_response}
  end

  test "login and callback create only an encrypted minimal renewed identity session", %{
    token_response: token_response
  } do
    login = get(build_conn(), ~p"/auth/login")
    assert login.status == 302
    assert [location] = get_resp_header(login, "location")
    assert String.starts_with?(location, "https://issuer.example/authorize?")
    assert get_resp_header(login, "cache-control") == ["no-store"]

    encoded_transaction = get_session(login, OIDC.transaction_key())
    assert {:ok, transaction} = OIDC.decode_transaction(encoded_transaction)
    login_cookie = login |> get_resp_header("set-cookie") |> Enum.join("\n")
    refute login_cookie =~ transaction.state
    refute login_cookie =~ transaction.nonce
    refute login_cookie =~ transaction.verifier

    Agent.update(token_response, fn _ -> token_response(transaction.nonce, "operator") end)

    callback =
      login
      |> recycle()
      |> get(~p"/auth/callback?state=#{transaction.state}&code=authorization-code")

    assert callback.status == 302
    assert get_resp_header(callback, "location") == ["/"]
    assert get_resp_header(callback, "cache-control") == ["no-store"]
    assert get_session(callback, OIDC.transaction_key()) == nil

    identity = get_session(callback, Session.key())
    assert identity["sub"] == "subject-1"

    assert Enum.sort(identity["capabilities"]) ==
             Enum.sort(Enum.map(@all_capabilities, &to_string/1))

    refute Map.has_key?(identity, "id_token")
    refute Map.has_key?(identity, "access_token")

    callback_cookie = callback |> get_resp_header("set-cookie") |> Enum.join("\n")
    refute callback_cookie =~ "subject-1"
    refute callback_cookie =~ "authorization-code"
    refute callback_cookie =~ "access-token"
    assert_receive {:token_request, "https://issuer.example/token", _options}
  end

  test "callback preserves the accepted expiration-skew boundary", %{
    runtime: runtime,
    token_response: token_response
  } do
    login = get(build_conn(), ~p"/auth/login")
    {:ok, transaction} = OIDC.decode_transaction(get_session(login, OIDC.transaction_key()))
    raw_expiry = System.system_time(:second) - 1

    Agent.update(token_response, fn _ ->
      token_response(transaction.nonce, "operator", raw_expiry)
    end)

    callback =
      login
      |> recycle()
      |> get(~p"/auth/callback?state=#{transaction.state}&code=authorization-code")

    assert callback.status == 302
    assert get_session(callback, Session.key())["exp"] == raw_expiry + runtime.oidc.clock_skew
  end

  test "state mismatch is generic, preserves unrelated transactions, and never exchanges", %{
    conn: _conn
  } do
    login = get(build_conn(), ~p"/auth/login")
    transaction_cookie = get_session(login, OIDC.transaction_key())

    callback =
      login
      |> recycle()
      |> get(~p"/auth/callback?state=wrong-state&code=authorization-code")

    assert callback.status == 400
    assert callback.resp_body == "authentication failed"
    assert get_session(callback, OIDC.transaction_key()) == transaction_cookie
    refute_received {:token_request, _url, _options}
  end

  test "provider error callback consumes its matching transaction without exchange" do
    login = get(build_conn(), ~p"/auth/login")
    {:ok, transaction} = OIDC.decode_transaction(get_session(login, OIDC.transaction_key()))

    callback =
      login
      |> recycle()
      |> get(~p"/auth/callback?state=#{transaction.state}&error=access_denied")

    assert callback.status == 400
    assert callback.resp_body == "authentication failed"
    assert get_session(callback, OIDC.transaction_key()) == nil
    refute_received {:token_request, _url, _options}
  end

  test "concurrent login callbacks consume only their matching transactions", %{
    token_response: token_response
  } do
    first_login = get(build_conn(), ~p"/auth/login")
    {:ok, first} = OIDC.decode_transaction(get_session(first_login, OIDC.transaction_key()))

    second_login = first_login |> recycle() |> get(~p"/auth/login")
    [second_location] = get_resp_header(second_login, "location")
    second_params = second_location |> URI.parse() |> Map.fetch!(:query) |> URI.decode_query()

    Agent.update(token_response, fn _ -> token_response(first.nonce, "operator") end)

    first_callback =
      second_login
      |> recycle()
      |> get(~p"/auth/callback?state=#{first.state}&code=first-code")

    assert first_callback.status == 302

    assert {:ok, second} =
             OIDC.decode_transaction(get_session(first_callback, OIDC.transaction_key()))

    assert second.state == second_params["state"]

    Agent.update(token_response, fn _ -> token_response(second.nonce, "operator") end)

    second_callback =
      first_callback
      |> recycle()
      |> get(~p"/auth/callback?state=#{second.state}&code=second-code")

    assert second_callback.status == 302
    assert get_session(second_callback, OIDC.transaction_key()) == nil
  end

  test "callback admits a valid web_access-only identity", %{
    token_response: token_response
  } do
    login = get(build_conn(), ~p"/auth/login")
    {:ok, transaction} = OIDC.decode_transaction(get_session(login, OIDC.transaction_key()))
    Agent.update(token_response, fn _ -> token_response(transaction.nonce, "reader") end)

    callback =
      login
      |> recycle()
      |> get(~p"/auth/callback?state=#{transaction.state}&code=authorization-code")

    assert callback.status == 302
    assert get_resp_header(callback, "location") == ["/cluster"]
    assert get_session(callback, Session.key())["capabilities"] == ["web_access"]
    assert {:ok, cluster_live, html} = live(recycle(callback), ~p"/cluster")
    assert html =~ "Cluster"
    refute has_element?(cluster_live, "button", "Kill")

    assert {:error, {:redirect, %{to: "/cluster"}}} = live(recycle(callback), ~p"/query")
  end

  test "an active socket expires in place and the stale cookie cannot reconnect", %{
    runtime: runtime,
    token_response: token_response
  } do
    login = get(build_conn(), ~p"/auth/login")
    {:ok, transaction} = OIDC.decode_transaction(get_session(login, OIDC.transaction_key()))
    expires_at = System.system_time(:second) + 2 - runtime.oidc.clock_skew

    Agent.update(token_response, fn _ ->
      token_response(transaction.nonce, "reader", expires_at)
    end)

    callback =
      login
      |> recycle()
      |> get(~p"/auth/callback?state=#{transaction.state}&code=authorization-code")

    assert {:ok, cluster_live, _html} = live(recycle(callback), ~p"/cluster")
    assert_redirect(cluster_live, "/auth/login", 3_000)

    assert {:error, {:redirect, %{to: "/auth/login"}}} =
             live(recycle(callback), ~p"/cluster")
  end

  test "query-only browser sessions mount read routes but cannot forge mutations or fleet actions",
       %{
         runtime: runtime,
         token_response: token_response
       } do
    :ok = Catalog.create_dataset(runtime.catalog, "analytics")

    :ok =
      Catalog.create_table(
        runtime.catalog,
        {"analytics", "events"},
        Schema.new!([{"ts", :timestamp}])
      )

    callback = callback_as(token_response, "analyst")

    assert {:ok, query_live, _html} = live(recycle(callback), ~p"/query")
    assert render(query_live) =~ "Query"

    assert {:ok, tables_live, tables_html} = live(recycle(callback), ~p"/tables")
    assert tables_html =~ "analytics"
    refute has_element?(tables_live, "#create-dataset")
    refute has_element?(tables_live, "#create-table")

    render_submit(tables_live, "create_dataset", %{"dataset" => %{"name" => "forged"}})
    assert_redirect(tables_live, "/cluster")
    assert Catalog.list_datasets(runtime.catalog) == {:ok, ["analytics"]}

    assert {:ok, tables_live, _html} = live(recycle(callback), ~p"/tables")

    render_submit(tables_live, "create_table", %{
      "table" => %{
        "dataset" => "analytics",
        "name" => "forged",
        "fields" => %{"0" => %{"name" => "id", "type" => "INT64", "nullable" => "false"}}
      }
    })

    assert_redirect(tables_live, "/cluster")
    assert {:error, _reason} = Catalog.table_schema(runtime.catalog, {"analytics", "forged"})

    assert {:ok, show_live, _html} = live(recycle(callback), ~p"/tables/analytics/events")
    refute has_element?(show_live, "#retention")

    render_submit(show_live, "save_retention", %{
      "retention" => %{"column" => "ts", "ttl_ms" => "1000"}
    })

    assert_redirect(show_live, "/cluster")
    assert Catalog.retention(runtime.catalog, {"analytics", "events"}) == {:ok, nil}

    :ok =
      Catalog.put_retention(runtime.catalog, {"analytics", "events"}, %{
        column: "ts",
        ttl_ms: 1_000
      })

    assert {:ok, show_live, _html} = live(recycle(callback), ~p"/tables/analytics/events")
    render_click(show_live, "clear_retention")
    assert_redirect(show_live, "/cluster")

    assert Catalog.retention(runtime.catalog, {"analytics", "events"}) ==
             {:ok, %{column: "ts", ttl_ms: 1_000}}

    for event <- ["kill", "restart", "drain"] do
      assert {:ok, cluster_live, _html} = live(recycle(callback), ~p"/cluster")
      refute has_element?(cluster_live, "button", "Kill")
      refute has_element?(cluster_live, "button", "Restart")
      refute has_element?(cluster_live, "button", "Drain")

      params = if event == "drain", do: %{"node" => "forged@node"}, else: %{"pod" => "forged-pod"}
      render_click(cluster_live, event, params)
      assert_redirect(cluster_live, "/cluster")
    end
  end

  defp callback_as(token_response, role) do
    login = get(build_conn(), ~p"/auth/login")
    {:ok, transaction} = OIDC.decode_transaction(get_session(login, OIDC.transaction_key()))
    Agent.update(token_response, fn _ -> token_response(transaction.nonce, role) end)

    login
    |> recycle()
    |> get(~p"/auth/callback?state=#{transaction.state}&code=authorization-code")
  end

  defp token_response(nonce, role) do
    token_response(nonce, role, System.system_time(:second) + 300)
  end

  defp token_response(nonce, role, expires_at) do
    now = System.system_time(:second)

    claims = %{
      "iss" => "https://issuer.example",
      "aud" => "web-client",
      "sub" => "subject-1",
      "exp" => expires_at,
      "iat" => now,
      "nonce" => nonce,
      "roles" => [role]
    }

    id_token =
      JOSE.JWT.sign(
        @private_key,
        JOSE.JWS.from_map(%{"alg" => "RS256", "kid" => "web-key"}),
        claims
      )
      |> JOSE.JWS.compact()
      |> elem(1)

    response(%{"id_token" => id_token, "access_token" => "access-token"})
  end

  defp response(body) do
    {:ok,
     %Req.Response{
       status: 200,
       headers: %{"content-type" => ["application/json"]},
       body: JSON.encode!(body)
     }}
  end
end
