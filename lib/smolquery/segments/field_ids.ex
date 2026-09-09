defmodule Smolquery.Segments.FieldIds do
  @moduledoc """
  What a Parquet file says about its own columns: their names, and the ids
  they were written under (PL-62).

  Every file smolquery writes stamps the catalog's column ids into the file
  (`Smolquery.Schema.parquet_field_ids/1`), and DuckDB's `parquet_schema()`
  reads them back. A reader that does not hold a file's ids from elsewhere —
  the compactor over sealed files, a distributed shard over the files it was
  handed — asks the file: one `parquet_schema([...])` over a batch of files,
  parsed here into a per-file map of the top-level columns and, when every
  top-level column carries one, their ids.

  `parquet_schema` lists a nested type's children as rows of their own, a
  MAP's key and value included; only the top-level columns are the table's
  columns, so the walk skips each subtree by its `num_children`. A file with
  ids on some columns and not others is read as having none: the writer
  stamps all or nothing, so a partial stamp is not a file this system wrote.
  """

  @typedoc """
  One file's columns: `ids` maps each top-level column name to its id, or is
  `nil` when the file carries none; `columns` is every top-level column name.
  """
  @type description :: %{ids: %{String.t() => pos_integer()} | nil, columns: [String.t()]}

  @doc """
  The query that describes `count` files, bound as `$1..$count`.
  """
  @spec sql(pos_integer()) :: String.t()
  def sql(count) do
    placeholders = Enum.map_join(1..count, ", ", &"$#{&1}")

    "SELECT file_name, name, num_children, field_id FROM parquet_schema([#{placeholders}])"
  end

  @doc """
  The per-file description from the rows `sql/1` answers.
  """
  @spec by_file([[term()]]) :: %{String.t() => description()}
  def by_file(rows) do
    rows
    |> Enum.group_by(&hd/1, &tl/1)
    |> Map.new(fn {file, [[_root, count, _id] | columns]} ->
      {file, describe(top_level(columns, count || 0, []))}
    end)
  end

  @doc """
  The per-file ids alone, as `Smolquery.Schema.projection_by_id/2` takes them.
  """
  @spec ids_by_file([[term()]]) :: %{String.t() => %{String.t() => pos_integer()} | nil}
  def ids_by_file(rows),
    do: rows |> by_file() |> Map.new(fn {file, %{ids: ids}} -> {file, ids} end)

  defp describe(columns) do
    ids =
      if Enum.all?(columns, fn {_name, id} -> is_integer(id) end), do: Map.new(columns), else: nil

    %{ids: ids, columns: columns |> Enum.map(&elem(&1, 0)) |> Enum.reverse()}
  end

  defp top_level(_rows, 0, columns), do: columns

  defp top_level([[name, children, id] | rest], remaining, columns),
    do: rest |> skip_subtree(children || 0) |> top_level(remaining - 1, [{name, id} | columns])

  defp skip_subtree(rows, 0), do: rows

  defp skip_subtree([[_name, children, _id] | rest], remaining),
    do: rest |> skip_subtree(children || 0) |> skip_subtree(remaining - 1)
end
