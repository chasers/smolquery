defmodule Smolquery.Auth.Principal do
  @moduledoc """
  Provider-neutral identity for an authenticated actor.

  An OIDC principal's stable identity is `{issuer, subject}`. Its `id` is the
  versioned, namespaced SHA-256 digest of the exact bytes of those values,
  encoded with their lengths to avoid delimiter collisions:

      "oidc:v1:" <> Base.url_encode64(:crypto.hash(:sha256, <<byte_size(issuer)::unsigned-32, issuer::binary, byte_size(subject)::unsigned-32, subject::binary>>), padding: false)

  The digest is opaque and does not contain display metadata. `display_name`
  and `client_id` are selected descriptive values only; raw tokens and claims
  are deliberately not represented here.

  Local principals are for credentials managed by smolquery. Their identity is
  supplied by the caller and must not use the `:oidc` authentication method.
  """

  @authn [:oidc, :api_key, :basic]
  @kinds [:user, :service]
  @option_keys [:display_name, :client_id, :expires_at]
  @oidc_prefix "oidc:v1:"

  @enforce_keys [:id, :authn, :kind]
  defstruct [
    :id,
    :authn,
    :kind,
    :issuer,
    :subject,
    :display_name,
    :client_id,
    :expires_at
  ]

  @type authn :: :oidc | :api_key | :basic
  @type kind :: :user | :service
  @type t :: %__MODULE__{
          id: String.t(),
          authn: authn(),
          kind: kind(),
          issuer: String.t() | nil,
          subject: String.t() | nil,
          display_name: String.t() | nil,
          client_id: String.t() | nil,
          expires_at: non_neg_integer() | nil
        }

  @doc """
  Builds an OIDC principal from an exact issuer and subject.

  Supported options are `:display_name`, `:client_id`, and `:expires_at`.
  """
  @spec oidc(String.t(), String.t(), kind(), keyword()) :: {:ok, t()} | {:error, term()}
  def oidc(issuer, subject, kind, opts \\ []) do
    with :ok <- validate_string(issuer, :issuer),
         :ok <- validate_string(subject, :subject),
         :ok <- validate_kind(kind),
         {:ok, attributes} <- options(opts) do
      {:ok,
       %__MODULE__{
         id: oidc_id(issuer, subject),
         authn: :oidc,
         kind: kind,
         issuer: issuer,
         subject: subject,
         display_name: attributes.display_name,
         client_id: attributes.client_id,
         expires_at: attributes.expires_at
       }}
    end
  end

  @doc """
  Builds a principal for a smolquery-managed credential.

  The `authn` value must be `:api_key` or `:basic`; OIDC identities must use
  `oidc/4` so their stable identity cannot be caller-selected.
  """
  @spec local(String.t(), authn(), kind(), keyword()) :: {:ok, t()} | {:error, term()}
  def local(id, authn, kind, opts \\ []) do
    with :ok <- validate_string(id, :id),
         :ok <- validate_local_authn(authn),
         :ok <- validate_kind(kind),
         {:ok, attributes} <- options(opts) do
      {:ok,
       %__MODULE__{
         id: id,
         authn: authn,
         kind: kind,
         issuer: nil,
         subject: nil,
         display_name: attributes.display_name,
         client_id: attributes.client_id,
         expires_at: attributes.expires_at
       }}
    end
  end

  @doc """
  Reports whether a term is a valid principal produced by this module.
  """
  @spec valid?(term()) :: boolean()
  def valid?(%__MODULE__{} = principal) do
    valid_string?(principal.id) and
      principal.authn in @authn and
      principal.kind in @kinds and
      valid_identity?(principal) and
      valid_optional_string?(principal.display_name) and
      valid_optional_string?(principal.client_id) and
      valid_expiry?(principal.expires_at)
  end

  def valid?(_term), do: false

  defp valid_identity?(%__MODULE__{id: id, authn: :oidc, issuer: issuer, subject: subject}) do
    valid_string?(issuer) and valid_string?(subject) and id == oidc_id(issuer, subject)
  end

  defp valid_identity?(%__MODULE__{authn: authn, issuer: nil, subject: nil})
       when authn in [:api_key, :basic],
       do: true

  defp valid_identity?(_principal), do: false

  defp options(opts) when is_list(opts) do
    cond do
      not Keyword.keyword?(opts) -> {:error, :invalid_options}
      duplicate_keys?(opts) -> {:error, :invalid_options}
      true -> Enum.reduce_while(opts, {:ok, defaults()}, &reduce_option/2)
    end
  end

  defp options(_opts), do: {:error, :invalid_options}

  defp reduce_option({key, value}, {:ok, attributes}) when key in @option_keys do
    case validate_option(key, value) do
      :ok -> {:cont, {:ok, Map.put(attributes, key, value)}}
      {:error, _reason} = error -> {:halt, error}
    end
  end

  defp reduce_option({key, _value}, _acc), do: {:halt, {:error, {:unknown_option, key}}}

  defp defaults do
    %{display_name: nil, client_id: nil, expires_at: nil}
  end

  defp duplicate_keys?(opts) do
    keys = Keyword.keys(opts)
    length(keys) != length(Enum.uniq(keys))
  end

  defp validate_option(key, value) when key in [:display_name, :client_id] do
    if is_nil(value) or valid_string?(value) do
      :ok
    else
      {:error, {:invalid_option, key}}
    end
  end

  defp validate_option(:expires_at, value) do
    if is_nil(value) or (is_integer(value) and value >= 0) do
      :ok
    else
      {:error, {:invalid_option, :expires_at}}
    end
  end

  defp validate_string(value, field) do
    if valid_string?(value), do: :ok, else: {:error, {:invalid, field}}
  end

  defp valid_string?(value), do: is_binary(value) and byte_size(value) > 0

  defp valid_optional_string?(nil), do: true
  defp valid_optional_string?(value), do: valid_string?(value)

  defp valid_expiry?(nil), do: true
  defp valid_expiry?(value), do: is_integer(value) and value >= 0

  defp validate_kind(kind) when kind in @kinds, do: :ok
  defp validate_kind(_kind), do: {:error, :invalid_kind}

  defp validate_local_authn(authn) when authn in [:api_key, :basic], do: :ok
  defp validate_local_authn(:oidc), do: {:error, :oidc_requires_oidc_constructor}
  defp validate_local_authn(_authn), do: {:error, :invalid_authn}

  defp oidc_id(issuer, subject) do
    encoded =
      <<byte_size(issuer)::unsigned-32, issuer::binary, byte_size(subject)::unsigned-32,
        subject::binary>>

    @oidc_prefix <> Base.url_encode64(:crypto.hash(:sha256, encoded), padding: false)
  end
end
