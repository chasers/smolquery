defmodule Smolquery.Segments.Store.S3ChainTest do
  @moduledoc """
  `Store.S3` in credential-chain mode, driven through a real `Req` request.

  The T-240 rollout shipped the chain as a lazy `{module, function, args}`
  tuple in `:aws_sigv4`, and every seal upload crashed in production:
  `ReqS3.handle_s3_url/1` reads the signing options eagerly with
  `Access.get/3`, which no MFA tuple implements. The unit suite only ever
  asserted the DuckDB side of the chain, so nothing here ran a request.
  This suite exists to keep the Elixir side honest — a chain-mode store
  must resolve credentials before `req_s3` sees them.

  Env-provider credentials plus a stub listing endpoint stand in for Pod
  Identity: the crash happened before any network I/O, so any resolvable
  chain reproduces it.
  """

  use ExUnit.Case, async: false

  alias Smolquery.Segments.Store
  alias Smolquery.Segments.Store.S3

  defmodule ListingStub do
    @moduledoc false

    @behaviour Plug

    import Plug.Conn

    @listing """
    <?xml version="1.0" encoding="UTF-8"?>
    <ListBucketResult>
      <IsTruncated>false</IsTruncated>
      <Contents><Key>analytics/events/01K0.parquet</Key></Contents>
    </ListBucketResult>
    """

    @impl Plug
    def init(opts), do: opts

    @impl Plug
    def call(conn, _opts) do
      conn
      |> put_resp_content_type("application/xml")
      |> send_resp(200, @listing)
    end
  end

  setup context do
    System.put_env("AWS_ACCESS_KEY_ID", "AKIAFROMENV")
    System.put_env("AWS_SECRET_ACCESS_KEY", "env-secret")

    on_exit(fn ->
      System.delete_env("AWS_ACCESS_KEY_ID")
      System.delete_env("AWS_SECRET_ACCESS_KEY")
      :aws_credentials.force_credentials_refresh()
    end)

    server = start_supervised!({Bandit, plug: ListingStub, port: 0, startup_log: false})
    {:ok, {_address, port}} = ThousandIsland.listener_info(server)

    store =
      S3.new(
        bucket: "sealed",
        staging_dir: Path.join(context.tmp_dir, "staging"),
        endpoint: "http://127.0.0.1:#{port}"
      )

    :aws_credentials.force_credentials_refresh()

    %{store: store}
  end

  @moduletag :tmp_dir

  test "a chain-mode store signs req_s3 requests with resolved credentials", %{store: store} do
    assert Store.list(store, "analytics/events") == {:ok, ["analytics/events/01K0.parquet"]}
  end
end
