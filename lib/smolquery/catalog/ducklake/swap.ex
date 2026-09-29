defmodule Smolquery.Catalog.DuckLake.Swap do
  @moduledoc """
  The compaction swap written at DuckLake's metadata level: retire a group's
  files and register the merged one in one metadata transaction, recorded as
  a compaction rather than a delete (T-600).

  `Smolquery.Catalog.DuckLake.replace_segments/4` used to swap through a
  `DELETE ... WHERE filename IN (...)` and `ducklake_add_data_files` in one
  DuckLake transaction. DuckLake records such a transaction as
  `deleted_from_table`, and refuses to commit a delete from a table another
  transaction inserted into meanwhile, since a predicate delete could have
  matched the new rows. A seal is such an insert, so on a table that seals
  every 20 s and a `DELETE` that stayed open 29 s, nearly every swap lost
  (T-595, T-600). DuckLake's own compaction records `merge_adjacent`
  instead, which a concurrent insert commits through and a concurrent delete
  still conflicts with. No DuckLake function swaps files smolquery merged
  itself, so this module writes what DuckLake would.

  ## Staged, then moved

  The merged file is first registered by `ducklake_add_data_files` into a
  twin table in a hidden schema (`stage_schema/0`). That is an insert into
  another table, so it conflicts with nothing, and it has DuckLake read the
  footer and write the file's column statistics and name mapping by its own
  rules: nothing here formats a statistic, so nothing here can prune a query
  wrongly. The move then rewrites those rows onto the real table, with the
  twin's column ids remapped by column path, since a table that dropped and
  re-added a column numbers it differently from a fresh twin.

  ## One metadata transaction, guarded by the snapshot key

  Every DuckLake commit inserts `snapshot_id = latest + 1`, so a writer that
  read snapshot `L` and inserts `L + 1` fails on the snapshot key when anyone
  committed in between. `statements/2` puts that insert first, then:

    * `end_snapshot = L + 1` on the inputs, by `data_file_id`;
    * the staged `ducklake_data_file` row moved to the table, at
      `begin_snapshot = L + 1` and `row_id_start` = the table's `next_row_id`,
      with a name mapping in the table's column ids;
    * its `ducklake_file_column_stats` and `ducklake_file_variant_stats`
      moved with it;
    * the table's `next_row_id` advanced past the moved file's new row ids.
      Its `record_count` and `file_size_bytes` stay as they were, as
      DuckLake's own `ducklake_merge_adjacent_files` leaves them: a
      compaction keeps the table's rows, and growing them by each merged
      file would count every compacted row once per compaction;
    * `merge_adjacent:<table_id>` in `ducklake_snapshot_changes`, so a
      concurrent DuckLake delete from the table still fails.

  Staged files an interrupted swap left behind are retired in the same
  transaction once older than the caller's cutoff.

  When the key collides, `rebase?/3` reads what committed since `L`: inserts
  anywhere, and anything naming another table, leave every read but the
  latest snapshot and `next_row_id` valid, so only those are read again, the
  way DuckLake retries its own commits. Anything else redoes the swap.
  """

  alias Smolquery.Identifier

  @stage_schema "__smolquery_stage"

  @typedoc "A `ducklake_column` row: `{column_id, column_name, parent_column, column_type}`."
  @type column :: {integer(), String.t(), integer() | nil, String.t()}

  @typedoc """
  A `ducklake_name_mapping` row: `{column_id, source_name, target_field_id,
  parent_column, is_partition}`.
  """
  @type name_row :: {integer(), String.t(), integer(), integer() | nil, boolean()}

  @typedoc "A name mapping: its type and its rows."
  @type mapping :: %{type: String.t(), rows: [name_row()]}

  @typedoc "What the move writes, every id read at `snapshot`."
  @type plan :: %{
          snapshot: %{
            id: integer(),
            schema_version: integer(),
            next_catalog_id: integer(),
            next_file_id: integer()
          },
          table_id: integer(),
          stage_table_id: integer(),
          next_row_id: integer(),
          staged: %{data_file_id: integer(), rows: integer()},
          retire: [integer()],
          abandoned: [integer()],
          column_ids: %{integer() => integer()},
          mapping: {:existing, integer() | nil} | {:new, mapping()}
        }

  @doc "The hidden schema twin tables live in."
  @spec stage_schema() :: String.t()
  def stage_schema, do: @stage_schema

  @doc "The twin table's name for a table id."
  @spec stage_table(integer()) :: String.t()
  def stage_table(table_id) when is_integer(table_id), do: "t#{table_id}"

  @doc """
  Whether a twin's columns still match its table's, by path and type. A twin
  that drifted, because the table gained, lost or retyped a column, is
  recreated before anything is staged into it.
  """
  @spec matches?([column()], [column()]) :: boolean()
  def matches?(stage_columns, table_columns),
    do: typed_paths(stage_columns) == typed_paths(table_columns)

  defp typed_paths(columns) do
    paths = paths(columns)

    columns
    |> Enum.map(fn {id, _name, _parent, type} -> {Map.fetch!(paths, id), type} end)
    |> Enum.sort()
  end

  @doc """
  The twin's column ids mapped to the table's, by column path.

  `{:error, {:unmapped_columns, paths}}` names any twin column the table has
  no column for. `matches?/2` rules that out before staging, so this is the
  check that a drift between the two did not slip past it.
  """
  @spec column_ids([column()], [column()]) ::
          {:ok, %{integer() => integer()}} | {:error, {:unmapped_columns, [[String.t()]]}}
  def column_ids(stage_columns, table_columns) do
    by_path = Map.new(paths(table_columns), fn {id, path} -> {path, id} end)

    {mapped, unmapped} =
      stage_columns
      |> paths()
      |> Enum.split_with(fn {_id, path} -> Map.has_key?(by_path, path) end)

    case unmapped do
      [] -> {:ok, Map.new(mapped, fn {id, path} -> {id, Map.fetch!(by_path, path)} end)}
      _missing -> {:error, {:unmapped_columns, unmapped |> Enum.map(&elem(&1, 1)) |> Enum.sort()}}
    end
  end

  defp paths(columns) do
    parents = Map.new(columns, fn {id, name, parent, _type} -> {id, {name, parent}} end)

    Map.new(columns, fn {id, _name, _parent, _type} -> {id, path(parents, id, [])} end)
  end

  defp path(_parents, nil, names), do: names

  defp path(parents, id, names) do
    {name, parent} = Map.fetch!(parents, id)

    path(parents, parent, [name | names])
  end

  @doc """
  The mapping the moved file reads through in the table: the twin's mapping
  with its targets in the table's column ids, reusing an identical mapping
  the table already has (`{:existing, id}`), or a new one to write
  (`{:new, mapping}`). A staged file with no mapping keeps none.
  """
  @spec mapping(mapping() | nil, %{integer() => mapping()}, %{integer() => integer()}) ::
          {:existing, integer() | nil} | {:new, mapping()}
  def mapping(nil, _table_mappings, _column_ids), do: {:existing, nil}

  def mapping(%{type: type, rows: rows}, table_mappings, column_ids) do
    translated = %{
      type: type,
      rows:
        rows
        |> Enum.map(fn {column, source, target, parent, partition} ->
          {column, source, Map.fetch!(column_ids, target), parent, partition}
        end)
        |> Enum.sort()
    }

    case Enum.find(table_mappings, fn {_id, candidate} -> sorted(candidate) == translated end) do
      {id, _identical} -> {:existing, id}
      nil -> {:new, translated}
    end
  end

  defp sorted(%{type: type, rows: rows}), do: %{type: type, rows: Enum.sort(rows)}

  @doc """
  The move's statements, in order, against the metadata tables under
  `prefix` (`"__ducklake_metadata_lake"` through DuckDB, `"public"` sent
  straight to Postgres). The snapshot insert comes first, so a stale plan
  fails before it writes anything.
  """
  @spec statements(plan(), String.t()) :: [String.t()]
  def statements(%{} = plan, prefix) do
    snapshot = plan.snapshot.id + 1
    {mapping_id, mapping_statements, next_file_id} = mapping_writes(plan, prefix)
    staged = plan.staged.data_file_id
    table = plan.table_id

    [
      "INSERT INTO #{prefix}.ducklake_snapshot " <>
        "(snapshot_id, snapshot_time, schema_version, next_catalog_id, next_file_id) " <>
        "VALUES (#{snapshot}, now(), #{plan.snapshot.schema_version}, " <>
        "#{plan.snapshot.next_catalog_id}, #{next_file_id})"
    ] ++
      mapping_statements ++
      [
        "UPDATE #{prefix}.ducklake_data_file SET end_snapshot = #{snapshot} " <>
          "WHERE table_id = #{table} AND end_snapshot IS NULL " <>
          "AND data_file_id IN (#{ids(plan.retire)})",
        "UPDATE #{prefix}.ducklake_data_file SET table_id = #{table}, " <>
          "begin_snapshot = #{snapshot}, row_id_start = #{plan.next_row_id}, " <>
          "mapping_id = #{mapping_id} WHERE data_file_id = #{staged}"
      ] ++
      Enum.map(
        ["ducklake_file_column_stats", "ducklake_file_variant_stats"],
        &"UPDATE #{prefix}.#{&1} SET table_id = #{table}, column_id = #{remap(plan.column_ids)} WHERE data_file_id = #{staged}"
      ) ++
      [
        "UPDATE #{prefix}.ducklake_table_stats SET " <>
          "next_row_id = next_row_id + #{plan.staged.rows} WHERE table_id = #{table}"
      ] ++
      abandoned_statements(plan, prefix, snapshot) ++
      [
        "INSERT INTO #{prefix}.ducklake_snapshot_changes " <>
          "(snapshot_id, changes_made, author, commit_message, commit_extra_info) " <>
          "VALUES (#{snapshot}, #{Identifier.sql_string(changes(plan))}, NULL, NULL, NULL)"
      ]
  end

  defp mapping_writes(%{mapping: {:existing, nil}} = plan, _prefix),
    do: {"NULL", [], plan.snapshot.next_file_id}

  defp mapping_writes(%{mapping: {:existing, id}} = plan, _prefix),
    do: {Integer.to_string(id), [], plan.snapshot.next_file_id}

  defp mapping_writes(%{mapping: {:new, %{type: type, rows: rows}}} = plan, prefix) do
    id = plan.snapshot.next_file_id

    values =
      Enum.map_join(rows, ", ", fn {column, source, target, parent, partition} ->
        "(#{id}, #{column}, #{Identifier.sql_string(source)}, #{target}, " <>
          "#{nullable(parent)}, #{partition})"
      end)

    {Integer.to_string(id),
     [
       "INSERT INTO #{prefix}.ducklake_column_mapping (mapping_id, table_id, type) " <>
         "VALUES (#{id}, #{plan.table_id}, #{Identifier.sql_string(type)})",
       "INSERT INTO #{prefix}.ducklake_name_mapping " <>
         "(mapping_id, column_id, source_name, target_field_id, parent_column, is_partition) " <>
         "VALUES #{values}"
     ], id + 1}
  end

  defp abandoned_statements(%{abandoned: []}, _prefix, _snapshot), do: []

  defp abandoned_statements(plan, prefix, snapshot) do
    [
      "UPDATE #{prefix}.ducklake_data_file SET end_snapshot = #{snapshot} " <>
        "WHERE table_id = #{plan.stage_table_id} AND end_snapshot IS NULL " <>
        "AND data_file_id IN (#{ids(plan.abandoned)})"
    ]
  end

  defp changes(%{abandoned: []} = plan), do: "merge_adjacent:#{plan.table_id}"

  defp changes(plan),
    do: "merge_adjacent:#{plan.table_id},deleted_from_table:#{plan.stage_table_id}"

  defp remap(column_ids) do
    whens =
      column_ids
      |> Enum.sort()
      |> Enum.map_join(" ", fn {from, to} -> "WHEN #{from} THEN #{to}" end)

    "CASE column_id #{whens} ELSE column_id END"
  end

  defp ids(ids), do: Enum.map_join(ids, ", ", &Integer.to_string/1)

  defp nullable(nil), do: "NULL"
  defp nullable(value), do: Integer.to_string(value)

  @doc """
  Whether a move that lost the snapshot key can be rewritten over the new
  latest snapshot without redoing its reads: true when every change
  committed since is an insert into some table, or names a table other than
  `table_id` and `stage_table_id` (the twin). An insert leaves the inputs
  live and the staged file where it was; only `next_row_id` and the counters
  move, and those are read again. Anything else, or a change this cannot
  parse, redoes the swap.
  """
  @spec rebase?([String.t()], integer(), integer()) :: boolean()
  def rebase?(changes, table_id, stage_table_id) do
    mine = [Integer.to_string(table_id), Integer.to_string(stage_table_id)]

    changes
    |> Enum.flat_map(&String.split(&1, ","))
    |> Enum.all?(fn change ->
      case String.split(change, ":", parts: 2) do
        ["inserted_into_table", _table] -> true
        [_kind, id] -> Regex.match?(~r/^\d+$/, id) and id not in mine
        _unparsed -> false
      end
    end)
  end

  @doc """
  Whether a failed move lost the snapshot key to a concurrent commit, as
  Postgres and SQLite metadata word a duplicate `snapshot_id`.
  """
  @spec lost_snapshot?(Exception.t() | term()) :: boolean()
  def lost_snapshot?(%{__exception__: true} = error) do
    message = Exception.message(error)

    String.contains?(message, "ducklake_snapshot_pkey") or
      String.contains?(message, "ducklake_snapshot.snapshot_id")
  end

  def lost_snapshot?(_error), do: false
end
