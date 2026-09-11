defmodule Smolquery do
  @moduledoc """
  An open source BigQuery alternative on DuckDB and Elixir.

  One OTP application containing four services — `Smolquery.IngestService`,
  `Smolquery.BufferService`, `Smolquery.StorageService`, and
  `Smolquery.QueryService` — around immutable Parquet segments and a DuckLake
  catalog. Which services a node runs is role configuration; see
  `Smolquery.Roles`.
  """

  @doc """
  The running application version.

  ## Examples

      iex> Smolquery.version() =~ ~r/^\\d+\\.\\d+\\.\\d+/
      true

  """
  @spec version() :: String.t()
  def version do
    :smolquery |> Application.spec(:vsn) |> to_string()
  end

  @doc """
  The git commit this node was built from, or `nil` when the build did not
  say.

  Every push to `main` publishes an image tagged with its commit, and most
  of them do not bump `mix.exs`'s version, so the version alone cannot tell
  two deploys apart. The image build bakes the commit in as
  `SMOLQUERY_GIT_SHA` (`Dockerfile`, `.github/workflows/release.yml`), and
  `config/runtime.exs` reads it into `:git_sha`. A dev checkout has no such
  setting and answers `nil`.
  """
  @spec git_sha() :: String.t() | nil
  def git_sha, do: Application.get_env(:smolquery, :git_sha)

  @doc """
  What this node is running: its version and the commit it was built from
  (T-465). The cluster page reads this from every node over RPC.
  """
  @spec build() :: %{version: String.t(), sha: String.t() | nil}
  def build, do: %{version: version(), sha: git_sha()}
end
