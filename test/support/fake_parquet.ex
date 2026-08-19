defmodule Smolquery.Test.FakeParquet do
  @moduledoc """
  Bytes that pass `Smolquery.Segments.Store.validate_parquet/1` without being a
  real Parquet file — a `PAR1` magic marker at each end around a 4-byte footer
  length, the structural minimum a Parquet file carries, for a store test that
  cares about `put/3`'s write path, not about Parquet itself.
  """

  @spec bytes(String.t()) :: binary()
  def bytes(body \\ ""), do: "PAR1" <> body <> <<byte_size(body)::little-32>> <> "PAR1"
end
