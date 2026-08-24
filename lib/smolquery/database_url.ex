defmodule Smolquery.DatabaseUrl do
  @moduledoc """
  `CATALOG_DATABASE_URL`, split into the two forms its consumers need.

  One URL configures both node discovery (`Smolquery.Cluster`, a Postgrex
  keyword list) and the catalog's own metadata connection
  (`Smolquery.Catalog.DuckLake`, a libpq `key=value` string) — see PL-11 D1.
  Both splits share the parsing pitfalls this module exists to get right
  once:

    * `URI.parse/1` does not percent-decode userinfo, and a password
      containing `@`, `:`, `/`, or `%` *must* be percent-encoded to appear in
      a URL at all — passing the still-encoded form to Postgres fails
      authentication with no hint why. `parse!/1` decodes each component
      (`URI.decode/1`, not `decode_www_form/1`: `+` is a literal plus in
      userinfo, not a space).
    * libpq `key=value` values end at whitespace and treat quotes
      structurally, so a raw password lands unparseable (a quote) or worse,
      injects keys (a space). `libpq_metadata/1` single-quotes every value
      with `\\` and `'` backslash-escaped, per libpq's quoting rules.
  """

  @type parts :: %{
          hostname: String.t(),
          port: :inet.port_number(),
          username: String.t(),
          password: String.t(),
          database: String.t()
        }

  @doc """
  The URL's decoded components, defaulting like the `postgres://` scheme
  does: port 5432, user `postgres`, database `smolquery`.

  Raises on a URL without a host — failing the boot beats discovering a
  half-configured cluster later. Query parameters (`?sslmode=...`) are not
  supported and raise rather than being silently dropped.
  """
  @spec parse!(String.t()) :: parts()
  def parse!(url) do
    uri = URI.parse(url)

    if uri.host in [nil, ""] do
      raise ArgumentError, "CATALOG_DATABASE_URL has no host: #{inspect(url)}"
    end

    if uri.query not in [nil, ""] do
      raise ArgumentError,
            "CATALOG_DATABASE_URL query parameters are not supported: ?#{uri.query}"
    end

    [username | password] = String.split(uri.userinfo || "postgres", ":", parts: 2)

    %{
      hostname: uri.host,
      port: uri.port || 5432,
      username: URI.decode(username),
      password: URI.decode(List.first(password) || ""),
      database: uri.path |> default_path() |> String.trim_leading("/") |> URI.decode()
    }
  end

  @doc """
  The `postgres:` metadata string `Smolquery.Catalog.DuckLake` attaches
  through, every value libpq-quoted.

  An `:sslmode` in `parts` is carried through; the URL parser never sets
  one, but a dataset's own catalog (`Smolquery.Catalog.Dataset`) always does.
  """
  @spec libpq_metadata(parts() | map()) :: String.t()
  def libpq_metadata(parts) do
    values =
      [
        dbname: parts.database,
        host: parts.hostname,
        port: Integer.to_string(parts.port),
        user: parts.username,
        password: parts.password,
        sslmode: Map.get(parts, :sslmode)
      ]
      |> Enum.reject(fn {_key, value} -> is_nil(value) end)

    "postgres:" <>
      Enum.map_join(values, " ", fn {key, value} -> "#{key}=#{quote_value(value)}" end)
  end

  defp default_path(path) when path in [nil, ""], do: "/smolquery"
  defp default_path(path), do: path

  defp quote_value(value) do
    escaped = value |> String.replace("\\", "\\\\") |> String.replace("'", "\\'")

    "'" <> escaped <> "'"
  end
end
