defmodule Smolquery.Engine.HttpfsTest do
  @moduledoc """
  Proves the hot-tier read path: DuckDB reading Parquet segments over HTTP.

  Tagged `:integration` because loading `httpfs` downloads the extension from
  DuckDB's repository on first use.
  """

  use ExUnit.Case, async: false

  alias Smolquery.Engine
  alias Smolquery.Test.SegmentServer

  @moduletag :integration
  @moduletag :tmp_dir

  @engine __MODULE__.Instance

  setup context do
    start_supervised!({Engine, name: @engine, extensions: [:httpfs]})
    server = start_supervised!(SegmentServer.bandit_spec(context.tmp_dir))

    %{base_url: SegmentServer.base_url(server)}
  end

  defp write_segment(dir, name, df) do
    Explorer.DataFrame.to_parquet!(df, Path.join(dir, name))
  end

  test "reads a remote segment", %{tmp_dir: tmp_dir, base_url: base_url} do
    write_segment(tmp_dir, "hot.parquet", Explorer.DataFrame.new(id: [1, 2, 3]))

    result =
      Engine.query!(
        @engine,
        "SELECT count(*) AS n, min(id) AS lo, max(id) AS hi FROM read_parquet($1)",
        ["#{base_url}/hot.parquet"]
      )

    assert result.rows == [[3, 1, 3]]
  end

  test "unions a local sealed segment with a remote hot segment", %{
    tmp_dir: tmp_dir,
    base_url: base_url
  } do
    write_segment(tmp_dir, "sealed.parquet", Explorer.DataFrame.new(id: [1, 2]))
    write_segment(tmp_dir, "hot.parquet", Explorer.DataFrame.new(id: [3]))

    result =
      Engine.query!(
        @engine,
        """
        SELECT 'sealed' AS tier, id FROM read_parquet($1)
        UNION ALL
        SELECT 'hot' AS tier, id FROM read_parquet($2)
        ORDER BY tier, id
        """,
        [Path.join(tmp_dir, "sealed.parquet"), "#{base_url}/hot.parquet"]
      )

    assert result.rows == [["hot", 3], ["sealed", 1], ["sealed", 2]]
  end

  test "pushes a filter into a remote segment", %{tmp_dir: tmp_dir, base_url: base_url} do
    write_segment(tmp_dir, "hot.parquet", Explorer.DataFrame.new(id: Enum.to_list(1..1000)))

    result =
      Engine.query!(@engine, "SELECT id FROM read_parquet($1) WHERE id = 500", [
        "#{base_url}/hot.parquet"
      ])

    assert result.rows == [[500]]
  end

  test "errors on a missing remote segment", %{base_url: base_url} do
    assert {:error, %Adbc.Error{}} =
             Engine.query(@engine, "SELECT * FROM read_parquet($1)", [
               "#{base_url}/absent.parquet"
             ])
  end
end
