defmodule Smolquery.AwsCredentialsTest do
  use ExUnit.Case, async: false

  alias Smolquery.AwsCredentials

  @req_sigv4_keys [:access_key_id, :secret_access_key, :token, :service, :region, :datetime]

  describe "ensure_started/0" do
    test "starts the chain, and a second call is satisfied by the first" do
      assert AwsCredentials.ensure_started() == :ok
      assert AwsCredentials.ensure_started() == :ok
      assert List.keymember?(Application.started_applications(), :aws_credentials, 0)
    end
  end

  describe "options_from/2" do
    test "carries the session token a role credential arrives with" do
      options = AwsCredentials.options_from(pod_identity_credentials(), "us-west-2")

      assert options[:access_key_id] == "ASIAEXAMPLE"
      assert options[:secret_access_key] == "shh"
      assert options[:token] == "session-token"
    end

    test "takes the region from the store, which Pod Identity never supplies" do
      refute Map.has_key?(pod_identity_credentials(), :region)

      options = AwsCredentials.options_from(pod_identity_credentials(), "eu-central-1")

      assert options[:region] == "eu-central-1"
    end

    test "omits the token for a provider that issues none" do
      credentials = %{
        credential_provider: :aws_credentials_env,
        access_key_id: "AKIAEXAMPLE",
        secret_access_key: "shh"
      }

      options = AwsCredentials.options_from(credentials, "us-east-1")

      refute Keyword.has_key?(options, :token)
      assert options[:access_key_id] == "AKIAEXAMPLE"
    end

    test "drops credential_provider, which Req would reject as an unknown option" do
      keys =
        pod_identity_credentials()
        |> AwsCredentials.options_from("us-east-1")
        |> Keyword.keys()

      refute :credential_provider in keys
      assert Enum.all?(keys, &(&1 in @req_sigv4_keys))
    end

    test "names the static-key variables when the chain resolved nothing" do
      assert_raise RuntimeError, ~r/SMOLQUERY_S3_ACCESS_KEY_ID/, fn ->
        AwsCredentials.options_from(:undefined, "us-east-1")
      end
    end
  end

  describe "sigv4_options/1 against the running chain" do
    test "signs with whatever the chain currently holds" do
      AwsCredentials.ensure_started()

      System.put_env("AWS_ACCESS_KEY_ID", "AKIAFROMENV")
      System.put_env("AWS_SECRET_ACCESS_KEY", "env-secret")

      on_exit(fn ->
        System.delete_env("AWS_ACCESS_KEY_ID")
        System.delete_env("AWS_SECRET_ACCESS_KEY")
        :aws_credentials.force_credentials_refresh()
      end)

      :aws_credentials.force_credentials_refresh()

      options = AwsCredentials.sigv4_options("us-west-2")

      assert options[:access_key_id] == "AKIAFROMENV"
      assert options[:secret_access_key] == "env-secret"
      assert options[:region] == "us-west-2"
    end
  end

  defp pod_identity_credentials do
    %{
      credential_provider: :aws_credentials_eks,
      access_key_id: "ASIAEXAMPLE",
      secret_access_key: "shh",
      token: "session-token"
    }
  end
end
