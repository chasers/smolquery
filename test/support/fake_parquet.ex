defmodule Smolquery.Test.FakeParquet do
  @moduledoc """
  Bytes that pass `Smolquery.Segments.Store.validate_parquet/1` without being a
  real Parquet file — a `PAR1` magic marker at each end, for a store test that
  cares about `put/3`'s write path, not about Parquet itself.
  """

  @spec bytes(String.t()) :: binary()
  def bytes(body \\ ""), do: "PAR1" <> body <> "PAR1"
end
