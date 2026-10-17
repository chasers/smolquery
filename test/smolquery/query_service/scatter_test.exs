defmodule Smolquery.QueryService.ScatterTest do
  use ExUnit.Case, async: true

  alias Smolquery.QueryService.Runtime
  alias Smolquery.QueryService.Scatter
  alias Smolquery.Test.MapCatalog

  @pool __MODULE__.Pool

  defp runtime(opts) do
    Runtime.new(
      [name: :scatter_targets_test, catalog: MapCatalog.new(), distributed: [local_workers: 2]] ++
        opts
    )
  end

  defp sealed(count), do: for(i <- 1..count, do: %{"url" => "s3://b/f#{i}.parquet", "bytes" => i})

  defp hot(count),
    do: for(i <- 1..count, do: %{"url" => "http://buffer-#{i}:4001/m#{i}", "hot" => true})

  describe "targets/2" do
    test "peers alone when the policy sized no runner" do
      assert [%{index: 0, kind: :peer, node: node}, %{index: 1, kind: :peer, node: node}] =
               Scatter.targets(runtime(burst: [enabled: true, pool: @pool]), 0)

      assert node == node()
    end

    test "the runners follow the peers, indexed on from them" do
      assert [
               %{index: 0, kind: :peer},
               %{index: 1, kind: :peer},
               %{index: 2, kind: :runner, pool: @pool},
               %{index: 3, kind: :runner, pool: @pool},
               %{index: 4, kind: :runner, pool: @pool}
             ] = Scatter.targets(runtime(burst: [enabled: true, pool: @pool]), 3)
    end
  end

  describe "deal/2" do
    setup do
      %{
        targets: [
          %{index: 0, kind: :peer, node: :a},
          %{index: 1, kind: :peer, node: :b},
          %{index: 2, kind: :runner, pool: @pool},
          %{index: 3, kind: :runner, pool: @pool}
        ]
      }
    end

    test "sealed files go round-robin over every target", %{targets: targets} do
      shards = Scatter.deal(targets, sealed(6))

      assert Enum.map(shards, fn {target, files} ->
               {target.index, Enum.map(files, & &1["url"])}
             end) ==
               [
                 {0, ["s3://b/f1.parquet", "s3://b/f5.parquet"]},
                 {1, ["s3://b/f2.parquet", "s3://b/f6.parquet"]},
                 {2, ["s3://b/f3.parquet"]},
                 {3, ["s3://b/f4.parquet"]}
               ]
    end

    test "hot units go to peers only, and every unit is dealt exactly once", %{targets: targets} do
      units = sealed(5) ++ hot(3)
      shards = Scatter.deal(targets, units)

      for {%{kind: :runner}, files} <- shards,
          do: refute(Enum.any?(files, &String.starts_with?(&1["url"], "http")))

      hot_on_peers =
        for {%{kind: :peer}, files} <- shards,
            file <- files,
            String.starts_with?(file["url"], "http"),
            do: file["url"]

      assert Enum.sort(hot_on_peers) == Enum.sort(Enum.map(hot(3), & &1["url"]))

      dealt = for {_target, files} <- shards, file <- files, do: file["url"]
      assert Enum.sort(dealt) == Enum.sort(Enum.map(units, & &1["url"]))
    end

    test "a peer's hot units come before its sealed files", %{targets: targets} do
      [{%{index: 0}, files} | _rest] = Scatter.deal(targets, sealed(4) ++ hot(1))

      assert Enum.map(files, & &1["url"]) == ["http://buffer-1:4001/m1", "s3://b/f1.parquet"]
    end

    test "a target dealt nothing is dropped", %{targets: targets} do
      assert [{%{index: 0}, _files}] = Scatter.deal(targets, sealed(1))
      assert [{%{index: 0}, _}, {%{index: 1}, _}] = Scatter.deal(targets, hot(2))
    end

    test "nothing to deal is no shard", %{targets: targets} do
      assert Scatter.deal(targets, []) == []
    end

    test "hot units with no peer to take them are refused, never dropped" do
      runners = [%{index: 0, kind: :runner, pool: @pool}]

      assert_raise ArgumentError, ~r/hot units need a peer/, fn ->
        Scatter.deal(runners, hot(1))
      end

      assert [{%{index: 0}, [_file]}] = Scatter.deal(runners, sealed(1))
    end
  end
end
