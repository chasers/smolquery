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

  alias Smolquery.Cluster
  alias Smolquery.InternalSecret
  alias Smolquery.Segments.Store

  @doc """
  The hot tier's `CREATE SECRET`, if `engine_extensions` loads `httpfs` —
  otherwise `[]`, since nothing here reads the hot tier over HTTP at all.

  Single-node, the secret is scoped to `buffer_base_url` — the one address
  hot-tier bytes can come from. Clustered, plans read micro-segments at
  per-owner URLs derived from node names (Milestone 8 L5), which no static
  scope can enumerate up front; the scope widens to `http://` so the header
  reaches every buffer node's `HotServer`. That widening leaks nothing new:
  the header is one shared internal secret either way, and the query path's
  lockdown (`allowed_paths`) still pins exactly which URLs an engine may
  touch.
  """
  @spec hot_tier([atom() | String.t()], String.t()) :: [String.t()]
  def hot_tier(engine_extensions, buffer_base_url) do
    if :httpfs in engine_extensions do
      [InternalSecret.create_secret_statement(hot_tier_scope(buffer_base_url))]
    else
      []
    end
  end

  defp hot_tier_scope(buffer_base_url) do
    if Cluster.enabled?(), do: "http://", else: buffer_base_url
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

  @doc """
  Extensions an engine must load before `sealed_tier/1`'s statement will run,
  merged into whatever the service already loads.

  A sealed tier authenticating through the AWS credential chain needs the
  `aws` extension for the `credential_chain` provider; static keys and local
  stores need nothing beyond the service's own list.
  """
  @spec sealed_tier_extensions(Store.t() | nil, [atom() | String.t()]) :: [atom() | String.t()]
  def sealed_tier_extensions(%Store{impl: Store.S3, config: config}, extensions),
    do: Enum.uniq(extensions ++ Store.S3.required_extensions(config))

  def sealed_tier_extensions(_store, extensions), do: extensions
end
