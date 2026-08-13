defmodule Smolquery.Segments.Store.S3Test do
  use ExUnit.Case, async: true

  alias Smolquery.Segments.Store
  alias Smolquery.Segments.Store.S3

  describe "new/1" do
    test "builds a store value pairing the S3 impl with its config" do
      assert %Store{impl: S3, config: %S3{bucket: "sealed"}} =
               S3.new(
                 bucket: "sealed",
                 access_key_id: "id",
                 secret_access_key: "secret",
                 staging_dir: "/tmp/staging"
               )
    end

    test "shared?/1 is true — an s3:// location is readable from any node" do
      store =
        S3.new(
          bucket: "sealed",
          access_key_id: "id",
          secret_access_key: "secret",
          staging_dir: "/tmp/staging"
        )

      assert Store.shared?(store)
    end

    test "location/2 is an s3:// URL" do
      store =
        S3.new(
          bucket: "sealed",
          access_key_id: "id",
          secret_access_key: "secret",
          staging_dir: "/tmp/staging"
        )

      assert Store.location(store, "analytics/events/01K0.parquet") ==
               "s3://sealed/analytics/events/01K0.parquet"
    end
  end

  describe "create_secret_statement/1" do
    test "targets real AWS S3 when no endpoint is configured" do
      %Store{config: config} =
        S3.new(
          bucket: "sealed",
          access_key_id: "AKIA...",
          secret_access_key: "shh",
          staging_dir: "/tmp/staging",
          region: "us-west-2"
        )

      statement = S3.create_secret_statement(config)

      assert statement =~ "CREATE SECRET IF NOT EXISTS smolquery_sealed_tier"
      assert statement =~ "TYPE S3"
      assert statement =~ "KEY_ID 'AKIA...'"
      assert statement =~ "SECRET 'shh'"
      assert statement =~ "REGION 'us-west-2'"
      assert statement =~ "SCOPE 's3://sealed'"
      refute statement =~ "ENDPOINT"
      refute statement =~ "URL_STYLE"
    end

    test "adds an endpoint, path-style addressing, and USE_SSL for a self-hosted store" do
      %Store{config: config} =
        S3.new(
          bucket: "sealed",
          access_key_id: "id",
          secret_access_key: "secret",
          staging_dir: "/tmp/staging",
          endpoint: "http://minio:9000"
        )

      statement = S3.create_secret_statement(config)

      assert statement =~ "ENDPOINT 'minio:9000'"
      assert statement =~ "URL_STYLE 'path'"
      assert statement =~ "USE_SSL false"
    end

    test "USE_SSL true for an https endpoint" do
      %Store{config: config} =
        S3.new(
          bucket: "sealed",
          access_key_id: "id",
          secret_access_key: "secret",
          staging_dir: "/tmp/staging",
          endpoint: "https://s3.example.com"
        )

      statement = S3.create_secret_statement(config)

      assert statement =~ "USE_SSL true"
    end

    test "an explicit :url_style overrides the endpoint-implied default" do
      %Store{config: config} =
        S3.new(
          bucket: "sealed",
          access_key_id: "id",
          secret_access_key: "secret",
          staging_dir: "/tmp/staging",
          endpoint: "https://s3.example.com",
          url_style: "vhost"
        )

      statement = S3.create_secret_statement(config)

      assert statement =~ "URL_STYLE 'vhost'"
    end

    test "delegates to DuckDB's credential chain when no static keys are configured" do
      %Store{config: config} =
        S3.new(bucket: "sealed", staging_dir: "/tmp/staging", region: "us-west-2")

      statement = S3.create_secret_statement(config)

      assert statement =~ "CREATE SECRET IF NOT EXISTS smolquery_sealed_tier"
      assert statement =~ "TYPE S3"
      assert statement =~ "PROVIDER credential_chain"
      assert statement =~ "REGION 'us-west-2'"
      assert statement =~ "SCOPE 's3://sealed'"
      refute statement =~ "KEY_ID"
      refute statement =~ "SECRET '"
    end

    test "refreshes chain credentials, so a long-lived engine outlives their expiry" do
      %Store{config: config} = S3.new(bucket: "sealed", staging_dir: "/tmp/staging")

      assert S3.create_secret_statement(config) =~ "REFRESH auto"
    end
  end

  describe "credential mode" do
    test "static keys need no extra extension, the chain needs aws" do
      %Store{config: static} =
        S3.new(
          bucket: "sealed",
          access_key_id: "id",
          secret_access_key: "secret",
          staging_dir: "/tmp/staging"
        )

      %Store{config: chain} = S3.new(bucket: "sealed", staging_dir: "/tmp/staging")

      refute S3.credential_chain?(static)
      assert S3.required_extensions(static) == []

      assert S3.credential_chain?(chain)
      assert S3.required_extensions(chain) == [:aws]
    end

    test "rejects an access key without its secret" do
      assert_raise ArgumentError, ~r/SMOLQUERY_S3_SECRET_ACCESS_KEY/, fn ->
        S3.new(bucket: "sealed", access_key_id: "id", staging_dir: "/tmp/staging")
      end
    end

    test "rejects a secret without its access key" do
      assert_raise ArgumentError, ~r/SMOLQUERY_S3_ACCESS_KEY_ID/, fn ->
        S3.new(bucket: "sealed", secret_access_key: "secret", staging_dir: "/tmp/staging")
      end
    end
  end
end
