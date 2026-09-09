defmodule Smolquery.QueryService.Views do
  @moduledoc """
  The one place a table's view SQL is rendered.

  `Smolquery.QueryService.Planner` shadows the lake with a view per
  referenced table; `Smolquery.QueryService.Scatter` defines the same view
  name over one shard's files on each worker. Both must project exactly the
  catalog's columns in catalog order — the projection is what keeps a column
  neither tier carries from leaking in — so both render through here, and a
  change to the shape cannot land in one site and not the other (PL-49
  review).

  The projection is also where a column's query type is put on: a column with
  a `Smolquery.Schema.view_cast/1` is cast to it, which is how a `VARIANT`
  column stored as `JSON` reaches a query as a variant — and still will when a
  later table stores it as `VARIANT`.
  """

  alias Smolquery.Identifier
  alias Smolquery.Schema

  @doc """
  A `read_parquet` over `sources` (paths or URLs), union-by-name.
  """
  @spec read_parquet([String.t()]) :: String.t()
  def read_parquet(sources) do
    "read_parquet([" <>
      Enum.map_join(sources, ", ", &Identifier.sql_string/1) <> "], union_by_name := true)"
  end

  @doc """
  A full `SELECT * FROM read_parquet(...)` over `sources`.
  """
  @spec parquet_select([String.t()]) :: String.t()
  def parquet_select(sources), do: "SELECT * FROM " <> read_parquet(sources)

  @doc """
  The read over hot micro-segments (and any other Parquet sources) projected
  onto `schema` **by column id**, as one `UNION ALL BY NAME` (PL-62).

  Each source is a manifest entry as `Smolquery.BufferService.HotClient`
  answers it — at least a `"url"`, and `"field_ids"` when the file was written
  with ids. `read_parquet(..., union_by_name)` unions by *name*, so two files
  that use one name for two different ids cannot share a scan: sources are
  grouped by their `"field_ids"`, each group is projected through
  `Smolquery.Schema.projection_by_id/2` — the input column with the right id,
  whatever it is named, or a typed `NULL` — and the groups are unioned by
  name after projection, when every group already speaks the catalog's names.
  In steady state every file agrees and there is one group, so the SQL is one
  scan, as before.

  A sealed file a shard reads directly carries no ids in its manifest, so
  the worker asks the file (`Smolquery.Segments.FieldIds`) and hands the
  answer here as `"field_ids"`; a sealed file written before ids existed
  carries its `"columns"` and the `"snapshot"` it was registered at instead,
  and is projected *as of* that snapshot (`Smolquery.Schema.projection_as_of/3`):
  a column that began later is `NULL` whatever the file names it — the
  sealed tier's legacy rule, the same one the compactor applies. Sources
  with none of that — a micro-segment written before ids existed — form one
  group read by name, exactly the read every file had before. Groups are
  rendered in a fixed order so the same sources always give the same SQL.
  """
  @spec sources_select(Schema.t(), [map()]) :: String.t()
  def sources_select(%Schema{} = schema, sources) do
    sources
    |> Enum.group_by(&group_key/1)
    |> Enum.sort_by(fn {key, _sources} -> sort_key(key) end)
    |> Enum.map_join(" UNION ALL BY NAME ", fn {key, grouped} ->
      group_select(schema, key, grouped)
    end)
  end

  defp group_key(source) do
    case {Map.get(source, "field_ids"), Map.get(source, "snapshot"), Map.get(source, "columns")} do
      {field_ids, _snapshot, _columns} when is_map(field_ids) ->
        {:ids, field_ids}

      {nil, snapshot, columns} when is_integer(snapshot) and is_list(columns) ->
        {:as_of, snapshot}

      _plain ->
        :by_name
    end
  end

  defp sort_key(:by_name), do: {0, nil}
  defp sort_key({:as_of, snapshot}), do: {1, snapshot}
  defp sort_key({:ids, field_ids}), do: {2, Enum.sort(field_ids)}

  defp group_select(_schema, :by_name, sources), do: parquet_select(urls(sources))

  defp group_select(schema, {:as_of, snapshot}, sources) do
    columns = sources |> Enum.flat_map(& &1["columns"]) |> Enum.uniq()
    {:ok, projection} = Schema.projection_as_of(schema, columns, snapshot)

    "SELECT #{projection} FROM #{read_parquet(urls(sources))}"
  end

  defp group_select(schema, {:ids, field_ids}, sources) do
    {:ok, projection} = Schema.projection_by_id(schema, field_ids)

    "SELECT #{projection} FROM #{read_parquet(urls(sources))}"
  end

  defp urls(sources), do: Enum.map(sources, & &1["url"])

  @doc """
  The statements defining `dataset.table` as `schema`'s columns projected
  over `from_sql`.

  The view replaces one of the same name: the planner's Top-N probe
  (`Smolquery.QueryService.TopN`) defines the table over its candidate
  entries first, and the runner's statements then define it for real.

  A materialized column named in `recompute` is read as
  `coalesce(stored, expression)` over the relation's regular columns: the
  value a file carries when it has one, the expression when it does not
  (PL-61). The two never disagree — every write and rewrite computes the
  same deterministic expression — so a read is exact the moment the column
  exists, before any file is rewritten. `recomputed/2` says which columns
  need it for a given set of sources; one that no source lacks reads the
  stored column plainly, which is what lets DuckDB prune the sealed tier on
  its row-group stats. A `coalesce` is opaque to that pruning, so it is
  rendered only while a file that predates the column is still in the read.
  """
  @spec table_view(Smolquery.Catalog.table_ref(), Schema.t(), String.t(), [String.t()]) ::
          [String.t()]
  def table_view({dataset, table}, schema, from_sql, recompute \\ []) do
    ds = Identifier.quote_name!(dataset)
    t = Identifier.quote_name!(table)
    columns = Enum.map_join(schema.fields, ", ", &column_expression(&1, recompute))

    [
      "CREATE SCHEMA IF NOT EXISTS #{ds}",
      "CREATE OR REPLACE VIEW #{ds}.#{t} AS SELECT #{columns} FROM (#{from_sql})"
    ]
  end

  defp column_expression(%Schema.Field{name: name, type: type} = field, recompute) do
    quoted = Identifier.quote_name!(name)

    cond do
      name in recompute and Schema.materialized?(field) ->
        {:ok, duckdb} = Schema.duckdb_type(type)
        %{expression: expression, canonical: canonical} = field.materialized

        "coalesce(#{quoted}, TRY(CAST((#{canonical || expression}) AS #{duckdb}))) AS #{quoted}"

      match?({:cast, _queried}, Schema.view_cast(type)) ->
        {:cast, queried} = Schema.view_cast(type)
        "#{quoted}::#{queried} AS #{quoted}"

      true ->
        quoted
    end
  end

  @doc """
  The materialized columns some of `sources` may not carry, by name — the
  ones `table_view/4` must read as their expression.

  A source with `"field_ids"` lacks a column when its id is absent. A sealed
  source with `"column_ids"` — the columns the catalog recorded for the file
  when it was registered (`Smolquery.Catalog.segment_files/3`) — lacks a
  column whose id is not among them, which is exact even for a file a seal
  wrote across the `ALTER` and registered after it. A source with only a
  `"snapshot"` lacks a column that began after that snapshot
  (`Smolquery.Schema.Field.since`). A source with none of these, or a column
  whose beginning the catalog does not date, may lack it, and is read as the
  expression to be safe.
  """
  @spec recomputed(Schema.t(), [map()]) :: [String.t()]
  def recomputed(%Schema{} = schema, sources) do
    for %Schema.Field{name: name} = field <- Schema.materialized_fields(schema),
        Enum.any?(sources, &may_lack?(&1, field)),
        do: name
  end

  defp may_lack?(%{"field_ids" => ids}, %Schema.Field{id: id}) when is_map(ids),
    do: id not in Map.values(ids)

  defp may_lack?(%{"column_ids" => ids}, %Schema.Field{id: id})
       when is_list(ids) and is_integer(id),
       do: id not in ids

  defp may_lack?(%{"snapshot" => snapshot}, %Schema.Field{since: since})
       when is_integer(snapshot) and is_integer(since),
       do: snapshot < since

  defp may_lack?(_source, _field), do: true
end
