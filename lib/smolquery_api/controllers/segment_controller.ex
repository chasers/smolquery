defmodule SmolqueryApi.SegmentController do
  @moduledoc """
  The segment routes' logic: dropping a table's sealed segments by path, over
  `Smolquery.Catalog.drop_segments/3`.

  The operator escape hatch a permanently corrupt sealed segment needs
  (T-310): `StorageService.Compactor` quarantines a segment it cannot compact
  rather than fixing it, and nothing else in the system can make the table
  readable again on its own. Dropping formalizes the loss instead — the rows
  the segment held are gone from every future snapshot, and every other
  reader (queries, compaction, table preview) stops failing on it. The file
  itself is untouched; GC reclaims it once no snapshot references it, same as
  any other drop.
  """

  use SmolqueryApi, :controller

  alias Smolquery.Catalog
  alias SmolqueryApi.DatasetController
  alias SmolqueryApi.Errors
  alias SmolqueryApi.Json

  @doc """
  Drops the segments the body names from a table's current snapshot.

  `{"paths": [...]}` — each path is store-relative, the same string a
  segment carries in the catalog (`Catalog.segments/3`, or the `paths` a
  compaction failure logs once a segment is quarantined). A path the table
  does not currently hold is not an error: dropping is idempotent, the same
  as every other retirement in this codebase — an operator retrying a drop
  that already landed gets 200 again, not 404.
  """
  @spec delete(Plug.Conn.t(), map()) :: Plug.Conn.t()
  def delete(conn, %{"dataset" => dataset, "table" => table}) do
    catalog = DatasetController.catalog(conn)
    table_ref = {dataset, table}

    with {:ok, paths} <- paths_from_json(conn.body_params),
         {:ok, _schema} <- Catalog.table_schema(catalog, table_ref),
         {:ok, snapshot} <- Catalog.drop_segments(catalog, table_ref, paths) do
      Json.send_json(conn, 200, %{"dropped" => paths, "snapshot" => snapshot})
    else
      {:error, reason} -> Errors.from_reason(conn, reason)
    end
  end

  defp paths_from_json(%{"paths" => [_ | _] = paths}) do
    if Enum.all?(paths, &is_binary/1) do
      {:ok, paths}
    else
      {:error, {:invalid_param, "paths"}}
    end
  end

  defp paths_from_json(%{"paths" => _paths}), do: {:error, {:invalid_param, "paths"}}
  defp paths_from_json(_body), do: {:error, {:missing_field, "paths"}}
end
