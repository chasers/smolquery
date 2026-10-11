defmodule Smolquery.Catalog.DuckLake.Format do
  @moduledoc """
  The DuckLake catalog format smolquery's own SQL against DuckLake's tables
  is written for, and the checks that keep that SQL off any other (T-638).

  DuckLake records its format in `ducklake_metadata` under the key
  `version`, and an extension refuses at `ATTACH` a catalog newer than its
  own. Two paths never attach: `Smolquery.Catalog.DuckLake.Swap` writes
  DuckLake's rows itself, as DuckLake 1.0 commits them, and
  `Smolquery.Catalog.DuckLake.Reader` reads them over Postgrex. An engine
  attached before a migration keeps the format it attached with. A migrated
  catalog would take the swap's rows without complaint and read them by
  another format's rules, so the swap checks the format inside its own
  transaction, and the reader checks it when its pool starts.

  ## The guard

  `guard_statements/2` heads a metadata transaction. On Postgres it first
  takes the version row `FOR SHARE`. DuckLake migrates in one metadata
  transaction whose last statement updates that row, so a migration either
  commits before the guard reads the row, and the guard fails, or waits for
  the swap to commit; a migration already holding a table the swap writes
  deadlocks with it, and Postgres aborts one of the two. The check itself is
  a cast that fails unless the version is `version/0`, the one way to fail a
  statement that Postgres and DuckDB share; `mismatch/1` reads the version
  back out of the error. The marker it reads by is split in the SQL, since a
  Postgres error through DuckDB quotes the statement that raised it.
  """

  @version "1.0"
  @marker "smolquery catalog format"

  @doc """
  The DuckLake catalog format this code is written for.
  """
  @spec version() :: String.t()
  def version, do: @version

  @doc """
  The query answering the catalog's format version from the metadata tables
  under `prefix`: one row, or none for a database DuckLake has not made.
  """
  @spec version_sql(String.t()) :: String.t()
  def version_sql(prefix),
    do: "SELECT value FROM #{prefix}.ducklake_metadata WHERE key = 'version'"

  @doc """
  `:ok` when `found` is `version/0`, and the error naming both otherwise.
  """
  @spec check(String.t() | nil) :: :ok | {:error, {:catalog_format, String.t() | nil, String.t()}}
  def check(@version), do: :ok
  def check(found), do: {:error, {:catalog_format, found, @version}}

  @doc """
  The statements that fail a metadata transaction unless the catalog under
  `prefix` is at `version/0`. With `lock: true`, for Postgres, the version
  row is first held `FOR SHARE` until the transaction ends.
  """
  @spec guard_statements(String.t(), keyword()) :: [String.t()]
  def guard_statements(prefix, opts \\ []) do
    lock =
      if Keyword.get(opts, :lock, false),
        do: ["SELECT 1 FROM #{prefix}.ducklake_metadata WHERE key = 'version' FOR SHARE"],
        else: []

    lock ++
      [
        "SELECT CAST(CASE WHEN value = '#{@version}' THEN '0' " <>
          "ELSE '#{@marker} ' || '[' || value || ']' END AS INTEGER) " <>
          "FROM #{prefix}.ducklake_metadata WHERE key = 'version'"
      ]
  end

  @doc """
  The error `check/1` gives for the version a failed guard statement found,
  or `:error` when `error` is not a guard's.
  """
  @spec mismatch(term()) :: {:ok, {:catalog_format, String.t(), String.t()}} | :error
  def mismatch(%{__exception__: true} = error) do
    case Regex.run(~r/#{@marker} \[([^\]]*)\]/, Exception.message(error)) do
      [_match, found] -> {:ok, {:catalog_format, found, @version}}
      nil -> :error
    end
  end

  def mismatch(_error), do: :error
end
