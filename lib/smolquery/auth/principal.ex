defmodule Smolquery.Auth.Principal do
  @moduledoc """
  Provider-neutral identity for an authenticated actor.

  An OIDC principal's stable identity is `{issuer, subject}`. Its `id` is the
  versioned, namespaced SHA-256 digest of the exact bytes of those values,
  encoded with their lengths to avoid delimiter collisions:

      "oidc:v1:" <> Base.url_encode64(:crypto.hash(:sha256, <<byte_size(issuer)::unsigned-big-32, issuer::binary, byte_size(subject)::unsigned-big-32, subject::binary>>), padding: false)

  Local principal source keys are stable, non-secret credential identifiers.
  The constructor derives their final IDs in authn-specific, versioned
  namespaces. Derived IDs are opaque stable pseudonymous identifiers, not
  anonymous or non-PII values. The source key is not retained in the struct.
  Local namespaces cannot collide with OIDC or with one another.

  Every identity string is framed by its unsigned big-endian 32-bit byte
  length. Digests are SHA-256 values encoded with URL-safe Base64 without
  padding. Smolquery treats exact differing `{issuer, subject}` pairs as
  distinct identities, including pairwise subjects across clients or sectors;
  the derived IDs rely on SHA-256 collision resistance, and mutable display
  claims must never merge identities. Display metadata is descriptive only, and raw
  tokens and claims are deliberately not represented here.

  Structural checks prove shape only, not authentication provenance. Only
  trusted authenticators and mappers may construct principals; client input
  must not be decoded directly into structs or capabilities.
  """

  @authn [:oidc, :api_key, :basic]
  @local_authn [:api_key, :basic]
  @kinds [:user, :service]
  @option_keys [:display_name, :client_id]
  @oidc_prefix "oidc:v1:"
  @local_prefixes %{api_key: "api_key:v1:", basic: "basic:v1:"}
  @max_frame_size 4_294_967_295
  @digest_size 32
  @encoded_digest_size 43

  @enforce_keys [:id, :authn, :kind]
  defstruct [:id, :authn, :kind, :issuer, :subject, :display_name, :client_id]

  @type authn :: :oidc | local_authn()
  @type local_authn :: :api_key | :basic
  @type kind :: :user | :service
  @type t :: %__MODULE__{
          id: String.t(),
          authn: authn(),
          kind: kind(),
          issuer: String.t() | nil,
          subject: String.t() | nil,
          display_name: String.t() | nil,
          client_id: String.t() | nil
        }

  @doc """
  Builds an OIDC principal from an exact issuer and subject.

  Supported options are `:display_name` and `:client_id`.
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
         client_id: attributes.client_id
       }}
    end
  end

  @doc """
  Builds a principal for a smolquery-managed credential.

  `source_key` is a stable, non-secret source key. The constructor derives an
  opaque ID from it in an authn-specific namespace. OIDC identities must use
  `oidc/4` so their stable identity cannot be caller-selected.
  """
  @spec local(String.t(), local_authn(), kind(), keyword()) :: {:ok, t()} | {:error, term()}
  def local(source_key, authn, kind, opts \\ []) do
    with :ok <- validate_string(source_key, :source_key),
         :ok <- validate_local_authn(authn),
         :ok <- validate_kind(kind),
         {:ok, attributes} <- options(opts) do
      {:ok,
       %__MODULE__{
         id: local_id(source_key, authn),
         authn: authn,
         kind: kind,
         issuer: nil,
         subject: nil,
         display_name: attributes.display_name,
         client_id: attributes.client_id
       }}
    end
  end

  @doc """
  Reports whether a term has the structure of a principal produced by this
  module. This does not prove authentication provenance.
  """
  @spec well_formed?(term()) :: boolean()
  def well_formed?(%__MODULE__{
        id: id,
        authn: authn,
        kind: kind,
        issuer: issuer,
        subject: subject,
        display_name: display_name,
        client_id: client_id
      }) do
    valid_string?(id) and
      authn in @authn and
      kind in @kinds and
      valid_identity?(id, authn, issuer, subject) and
      valid_optional_string?(display_name) and
      valid_optional_string?(client_id)
  end

  def well_formed?(_term), do: false

  defp valid_identity?(id, :oidc, issuer, subject) do
    valid_string?(issuer) and valid_string?(subject) and id == oidc_id(issuer, subject)
  end

  defp valid_identity?(id, authn, nil, nil) when authn in @local_authn do
    local_id_shape?(id, authn)
  end

  defp valid_identity?(_id, _authn, _issuer, _subject), do: false

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

  defp defaults, do: %{display_name: nil, client_id: nil}

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

  defp validate_string(value, field) do
    if valid_string?(value), do: :ok, else: {:error, {:invalid, field}}
  end

  defp valid_string?(value),
    do: is_binary(value) and byte_size(value) > 0 and byte_size(value) <= @max_frame_size

  defp valid_optional_string?(nil), do: true
  defp valid_optional_string?(value), do: valid_string?(value)

  defp validate_kind(kind) when kind in @kinds, do: :ok
  defp validate_kind(_kind), do: {:error, :invalid_kind}

  defp validate_local_authn(authn) when authn in @local_authn, do: :ok
  defp validate_local_authn(:oidc), do: {:error, :oidc_requires_oidc_constructor}
  defp validate_local_authn(_authn), do: {:error, :invalid_authn}

  defp oidc_id(issuer, subject) do
    encoded = frame(issuer, subject)
    @oidc_prefix <> digest(encoded)
  end

  defp local_id(source_key, authn) do
    namespace = Map.fetch!(@local_prefixes, authn)
    namespace <> digest(frame(namespace, source_key))
  end

  defp frame(first, second) do
    <<byte_size(first)::unsigned-big-32, first::binary, byte_size(second)::unsigned-big-32,
      second::binary>>
  end

  defp digest(bytes), do: Base.url_encode64(:crypto.hash(:sha256, bytes), padding: false)

  defp local_id_shape?(id, authn) do
    prefix = Map.fetch!(@local_prefixes, authn)
    prefix_size = byte_size(prefix)

    case id do
      <<candidate::binary-size(^prefix_size), digest_part::binary>> when candidate == prefix ->
        canonical_digest?(digest_part)

      _other ->
        false
    end
  end

  defp canonical_digest?(digest_part) when byte_size(digest_part) == @encoded_digest_size do
    case Base.url_decode64(digest_part, padding: false) do
      {:ok, <<_::binary-size(@digest_size)>> = decoded} ->
        Base.url_encode64(decoded, padding: false) == digest_part

      :error ->
        false
    end
  end

  defp canonical_digest?(_digest_part), do: false
end
