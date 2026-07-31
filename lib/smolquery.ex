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
end
