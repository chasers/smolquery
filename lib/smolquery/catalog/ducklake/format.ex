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
  transaction whose last statement updates that row, so the lock orders the
  swap against that update. A migration that commits first fails the guard:
  the postgres extension runs the batch `REPEATABLE READ`, so a swap that
  waited on the row fails to serialize, retries, and then fails the guard. A
  migration that comes second waits for the swap to commit.

  That orders the swap against the version update, not against everything
  the migration did before it. Every DuckLake migration so far alters tables
  the swap writes, so the two take conflicting locks and Postgres aborts one;
  a migration that only read those tables and then waited on the version
  row would commit without the swap's rows. A format whose migration works
  that way needs the swap to wait on the migration itself.

  The check itself is a cast that fails unless the version is `version/0`,
  the one way to fail a statement that Postgres and DuckDB share;
  `mismatch/1` reads the version back out of the error. The marker it reads
  by is split in the SQL, since a Postgres error through DuckDB quotes the
  statement that raised it.
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
  The version in the rows `version_sql/1` answered, or `nil` when there are
  none.
  """
  @spec version_in([[String.t()]]) :: String.t() | nil
  def version_in(rows), do: Enum.find_value(rows, fn [version] -> version end)

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
