defmodule Smolquery.CatalogEmulation do
  @moduledoc """
  What an edge needs to emulate the catalog its clients read (PL-58, PL-66).

  The Postgres edge answers `pg_catalog` and the ClickHouse edge answers
  `system.*`, each from a private `Smolquery.Engine` it fills from
  `Smolquery.Catalog`. Both must tell a catalog statement from a user's, and
  both must list every table with its schema. Those two jobs are here, once.

  DuckDB's parser is the only parser: `serialize/2` answers the statement's
  AST (`json_serialize_sql`) and the canonical SQL it deserializes to, and
  `base_tables/1` walks the AST for every table the statement reads.
  """

  alias Smolquery.Catalog
  alias Smolquery.Engine
  alias Smolquery.Engine.CallExited
  alias Smolquery.Identifier
  alias Smolquery.Schema

  @doc """
  Parses `sql` on `engine`, answering its AST and its canonical text.
  """
  @spec serialize(Engine.handle(), String.t()) ::
          {:ok, map(), String.t()} | {:error, term()}
  def serialize(engine, sql) do
    quoted = Identifier.sql_string(sql)

    with {:ok, result} <-
           Engine.query(
             engine,
             "SELECT json_serialize_sql(#{quoted}), " <>
               "CASE WHEN json_extract_string(json_serialize_sql(#{quoted}), '$.error') = 'false' " <>
               "THEN json_deserialize_sql(json_serialize_sql(#{quoted})) END"
           ),
         [[json, canonical]] <- result.rows,
         {:ok, %{"error" => false} = ast} <- JSON.decode(json) do
      {:ok, ast, canonical}
    else
      {:ok, %{"error" => true} = ast} ->
        {:error, {:invalid_query, Map.get(ast, "error_message", "unparseable")}}

      {:error, reason} ->
        {:error, reason}

      _unexpected ->
        {:error, :unparseable}
    end
  end

  @doc """
  Every `BASE_TABLE` node of an AST `serialize/2` answered.
  """
  @spec base_tables(term()) :: [map()]
  def base_tables(%{"type" => "BASE_TABLE"} = node), do: [node | child_tables(node)]
  def base_tables(node) when is_map(node), do: child_tables(node)
  def base_tables(node) when is_list(node), do: Enum.flat_map(node, &base_tables/1)
  def base_tables(_leaf), do: []

  defp child_tables(node), do: Enum.flat_map(Map.values(node), &base_tables/1)

  @doc """
  Every table of `catalog` with its schema.

  A table dropped between the listing and its schema read is left out. A
  catalog that could not be asked at all (`%CallExited{}`, T-464) is an
  error, never an empty list: an emulated catalog with no tables is a wrong
  answer a client acts on, not a failure it can retry.
  """
  @spec listed_tables(Catalog.t()) ::
          {:ok, [{String.t(), String.t(), Schema.t()}]} | {:error, term()}
  def listed_tables(catalog) do
    with {:ok, refs} <- Catalog.tables(catalog) do
      Enum.reduce_while(refs, {:ok, []}, &collect_entry(catalog, &1, &2))
    end
  end

  defp collect_entry(catalog, ref, {:ok, entries}) do
    case table_entry(catalog, ref) do
      {:ok, entry} -> {:cont, {:ok, [entry | entries]}}
      :skip -> {:cont, {:ok, entries}}
      {:error, reason} -> {:halt, {:error, reason}}
    end
  end

  defp table_entry(catalog, {dataset, table} = ref) do
    case Catalog.table_schema(catalog, ref) do
      {:ok, schema} -> {:ok, {dataset, table, schema}}
      {:error, %CallExited{} = exited} -> {:error, exited}
      {:error, _dropped_meanwhile} -> :skip
    end
  end
end
