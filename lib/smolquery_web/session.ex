defmodule SmolqueryWeb.Session do
  @moduledoc "Minimal trusted browser identity serialization and reconstruction."

  alias Smolquery.Auth.Context
  alias Smolquery.Auth.Principal

  @key "smolquery_identity"
  @version 1
  @max_issuer 512
  @max_subject 512
  @max_optional 256
  @max_serialized 2_048

  def key, do: @key

  @doc "Serializes only bounded normalized identity data, never provider tokens or claims."
  def encode(%Context{principal: principal, capabilities: capabilities, expires_at: expires_at}) do
    value = %{
      "v" => @version,
      "iss" => principal.issuer,
      "sub" => principal.subject,
      "display_name" => principal.display_name,
      "client_id" => principal.client_id,
      "capabilities" => capabilities |> MapSet.to_list() |> Enum.map(&Atom.to_string/1),
      "exp" => expires_at
    }

    with true <- valid_string?(value["iss"], @max_issuer),
         true <- valid_string?(value["sub"], @max_subject),
         true <- valid_optional?(value["display_name"]),
         true <- valid_optional?(value["client_id"]),
         encoded <- JSON.encode!(value),
         true <- byte_size(encoded) <= @max_serialized do
      {:ok, value}
    else
      _failure -> :error
    end
  end

  def encode(_context), do: :error

  @doc "Reconstructs through trusted constructors and checks the current expiry."
  def decode(
        %{
          "v" => @version,
          "iss" => issuer,
          "sub" => subject,
          "capabilities" => capabilities,
          "exp" => expires_at
        } = value
      ) do
    with true <- valid_string?(issuer, @max_issuer),
         true <- valid_string?(subject, @max_subject),
         true <- valid_optional?(value["display_name"]),
         true <- valid_optional?(value["client_id"]),
         encoded <- JSON.encode!(value),
         true <- byte_size(encoded) <= @max_serialized,
         true <- is_integer(expires_at) and expires_at >= 0,
         {:ok, capabilities} <- parse_capabilities(capabilities),
         {:ok, principal} <- Principal.oidc(issuer, subject, :user, options(value)),
         {:ok, context} <- Context.single_tenant(principal, capabilities, expires_at: expires_at),
         true <- Context.active?(context, System.system_time(:second)) do
      {:ok, context}
    else
      _failure -> :error
    end
  end

  def decode(_value), do: :error

  defp options(value) do
    [display_name: value["display_name"], client_id: value["client_id"]]
  end

  defp parse_capabilities(capabilities) when is_list(capabilities) do
    capabilities = Enum.map(capabilities, &parse_capability/1)

    if Enum.all?(capabilities, &match?({:ok, _}, &1)),
      do: {:ok, Enum.map(capabilities, &elem(&1, 1))},
      else: :error
  end

  defp parse_capabilities(_capabilities), do: :error

  defp parse_capability(value)
       when value in ["web_access", "query", "ingest", "catalog_manage", "platform_operate"],
       do: {:ok, String.to_existing_atom(value)}

  defp parse_capability(_value), do: :error

  defp valid_optional?(nil), do: true
  defp valid_optional?(value), do: valid_string?(value, @max_optional)

  defp valid_string?(value, max),
    do: is_binary(value) and byte_size(value) > 0 and byte_size(value) <= max
end
