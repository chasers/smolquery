defmodule Smolquery.EngineSecrets do
  @moduledoc """
  Bootstrap `CREATE SECRET` statements an engine needs before it can read
  what its configuration points at.

  `StorageService`'s own engine (compaction re-reads sealed segments to
  re-merge them) and `QueryService`'s per-job engines (every query touching
  either tier) both need the same two secrets — the hot tier's internal
  auth header, and the sealed tier's S3 credentials when it lives there
  (Milestone 8 L3) — so this is the one place that builds them, rather than
  each service carrying its own copy of "is httpfs enabled" / "is the store
  S3-backed".
  """

  alias Smolquery.InternalSecret
  alias Smolquery.Segments.Store

  @doc """
  The hot tier's `CREATE SECRET`, if `engine_extensions` loads `httpfs` —
  otherwise `[]`, since nothing here reads the hot tier over HTTP at all.
  """
  @spec hot_tier([atom() | String.t()], String.t()) :: [String.t()]
  def hot_tier(engine_extensions, buffer_base_url) do
    if :httpfs in engine_extensions do
      [InternalSecret.create_secret_statement(buffer_base_url)]
    else
      []
    end
  end

  @doc """
  The sealed tier's `CREATE SECRET`, if `store` is `Segments.Store.S3` —
  otherwise `[]` (a local store needs no engine credential to read its own
  disk).
  """
  @spec sealed_tier(Store.t() | nil) :: [String.t()]
  def sealed_tier(%Store{impl: Store.S3, config: config}),
    do: [Store.S3.create_secret_statement(config)]

  def sealed_tier(_store), do: []
end
