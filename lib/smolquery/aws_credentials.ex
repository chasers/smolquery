defmodule Smolquery.AwsCredentials do
  @moduledoc """
  The AWS default credential chain, for deployments that cannot hold static
  S3 keys.

  Static keys force an IAM-user principal. An organization SCP that denies
  `s3:*` for IAM users cannot be overridden by any IAM policy, so such a
  deployment can only reach S3 through role credentials — EKS Pod Identity
  delivers those as rotating temporary credentials on the container
  credentials endpoint. `Smolquery.Segments.Store.S3` resolves through here
  when no static keys are configured (T-240).

  ## Why the application starts lazily

  `:aws_credentials` fetches at boot and, when nothing answers, logs one
  error per provider and retries on a timer forever. Every deployment that
  does hold static keys — MinIO, dev, the test suite — would pay that noise
  for a chain it never consults. So the dependency is `runtime: false` and
  the release loads it without starting it (`mix.exs`); `ensure_started/0`
  starts it only once a store actually runs in chain mode.

  ## The provider order is the library's own

  `:aws_credentials` 1.1 resolves in the order env, profile, ECS, Pod
  Identity, web identity, EC2 — both pod-scoped identities ahead of instance
  metadata. That ordering is what this module depends on: a node whose IMDS
  answers pods would otherwise hand back the *node's* role and silently
  authenticate as the wrong principal. The 0.3 line has no Pod Identity and
  no web-identity provider at all, so on it a pod falls through to instance
  metadata; do not pin back to it.
  """

  @doc """
  Starts `:aws_credentials`, unless it is already running.

  Callers run this when the store is built, so the first sealed-segment
  write does not pay for the initial resolution, and so a deployment that
  cannot reach its credential source logs that at boot rather than in the
  middle of a seal.

  Starting still succeeds when nothing answers: the library keeps
  `fail_if_unavailable` false, holds `:undefined`, and retries on a timer,
  so an AWS blip during a pod restart resolves itself instead of becoming a
  crash loop. `sigv4_options/1` is where an unresolved chain turns into an
  error, at the point a caller actually needs credentials.
  """
  @spec ensure_started() :: :ok
  def ensure_started do
    case Application.ensure_all_started(:aws_credentials) do
      {:ok, _apps} ->
        :ok

      {:error, reason} ->
        raise "could not start the AWS credential chain: #{inspect(reason)}"
    end
  end

  @doc """
  Signing options for `Req`'s `:aws_sigv4`, resolved fresh on every call.

  The options must be a resolved list, not a lazy `{module, function, args}`
  tuple: `ReqS3.handle_s3_url/1` reads them eagerly with `Access.get/3` when
  it rewrites an `s3://` URL, before `Req`'s own sigv4 step would evaluate
  an MFA. Rotation still needs no restart, because callers build a request
  per operation and call this at build time — `:aws_credentials` refreshes
  in the background ahead of expiry, and each call reads whatever it
  currently holds.

  `region` comes from the store rather than the chain, because the providers
  that serve Pod Identity return no region at all.
  """
  @spec sigv4_options(String.t()) :: keyword()
  def sigv4_options(region) do
    :aws_credentials.get_credentials()
    |> options_from(region)
  end

  @doc """
  The signing options for one already-resolved credentials map.

  Split out from `sigv4_options/1` so the mapping is reachable without a
  running credential chain. It builds the options explicitly rather than
  passing the map through: some providers resolve a `:region` of their own,
  and requests must be signed for the store's region — the bucket's — not
  the credential source's.
  """
  @spec options_from(:undefined | map(), String.t()) :: keyword()
  def options_from(:undefined, _region) do
    raise "the AWS credential chain resolved no credentials; " <>
            "set SMOLQUERY_S3_ACCESS_KEY_ID and SMOLQUERY_S3_SECRET_ACCESS_KEY " <>
            "for a store that needs static keys"
  end

  def options_from(
        %{access_key_id: access_key_id, secret_access_key: secret_access_key} = credentials,
        region
      ) do
    [
      access_key_id: access_key_id,
      secret_access_key: secret_access_key,
      region: region
    ] ++ token(credentials)
  end

  defp token(%{token: token}) when is_binary(token), do: [token: token]
  defp token(_credentials), do: []
end
