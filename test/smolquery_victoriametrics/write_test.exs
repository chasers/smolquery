defmodule SmolqueryVictoriaMetrics.WriteTest do
  @moduledoc """
  Remote write end to end, through a real buffer and ingest service over a
  `Smolquery.Test.MapCatalog` that starts with no `metrics` dataset, using
  the bodies vmagent v1.152.0 sent (`test/support/fixtures/victoriametrics`).
  """

  use ExUnit.Case, async: true

  import Bitwise
  import Plug.Conn, only: [put_req_header: 3, get_resp_header: 2]
  import Plug.Test

  alias Smolquery.BufferService
  alias Smolquery.Catalog
  alias Smolquery.IngestService
  alias Smolquery.Schema
  alias Smolquery.Test.MapCatalog
  alias SmolqueryVictoriaMetrics.RemoteWrite
  alias SmolqueryVictoriaMetrics.Router
  alias SmolqueryVictoriaMetrics.Runtime
  alias SmolqueryVictoriaMetrics.Write

  @moduletag :tmp_dir

  @password "victoriametrics-test-password"
  @table {"metrics", "samples"}
  @fixtures Path.expand("../support/fixtures/victoriametrics", __DIR__)

  setup context do
    buffer = :"vm_buffer_#{:erlang.unique_integer([:positive])}"

    start_supervised!(
      {BufferService.Supervisor,
       name: buffer, dir: Path.join(context.tmp_dir, "buffer"), flush_max_rows: 1},
      id: buffer
    )

    on_exit(fn -> BufferService.Runtime.delete(buffer) end)

    catalog = MapCatalog.new()
    ingest = :"vm_ingest_#{:erlang.unique_integer([:positive])}"

    start_supervised!(
      {IngestService.Supervisor, name: ingest, catalog: catalog, buffer_name: buffer},
      id: ingest
    )

    on_exit(fn -> IngestService.Runtime.delete(ingest) end)

    name = :"vm_edge_#{:erlang.unique_integer([:positive])}"

    Runtime.put(
      Runtime.new(name: name, password: @password, ingest_name: ingest, catalog: catalog)
    )

    on_exit(fn -> Runtime.delete(name) end)

    handler = "vm-write-test-#{name}"
    test = self()

    :telemetry.attach(
      handler,
      [:smolquery, :victoriametrics, :samples],
      fn _event, measurements, meta, _config -> send(test, {:samples, measurements, meta}) end,
      nil
    )

    on_exit(fn -> :telemetry.detach(handler) end)

    %{name: name, buffer: buffer, catalog: catalog}
  end

  defp fixture(name), do: File.read!(Path.join(@fixtures, name))

  defp post(name, body, headers) do
    Enum.reduce(headers, conn(:post, "/api/v1/write", body), fn {key, value}, conn ->
      put_req_header(conn, key, value)
    end)
    |> put_req_header("authorization", "Bearer " <> @password)
    |> Router.call(name)
  end

  defp remote_write(name, body, encoding),
    do:
      post(name, body, [
        {"content-type", "application/x-protobuf"},
        {"content-encoding", encoding}
      ])

  defp landed(buffer) do
    {:ok, entries} = BufferService.Client.hot_manifest(buffer, @table)
    Enum.sum(Enum.map(entries, & &1.row_count))
  end

  defp decoded(file, encoding) do
    {:ok, decoded} = RemoteWrite.decode_body(fixture(file), encoding, max_bytes: 8_388_608)
    decoded
  end

  defp varint(n) when n < 0x80, do: <<n>>
  defp varint(n), do: <<1::1, n &&& 0x7F::7, varint(n >>> 7)::binary>>

  defp bytes(field, value), do: varint(field <<< 3 ||| 2) <> varint(byte_size(value)) <> value
  defp label(name, value), do: bytes(1, bytes(1, name) <> bytes(2, value))

  defp sample(timestamp, value),
    do: bytes(2, varint(9) <> <<value::float-little-64>> <> varint(16) <> varint(timestamp))

  describe "a vmagent body" do
    for {file, encoding} <- [{"write_zstd.bin", "zstd"}, {"write_snappy.bin", "snappy"}] do
      test "#{file} creates the table on first write and lands every sample", %{
        name: name,
        buffer: buffer,
        catalog: catalog
      } do
        samples =
          unquote(file)
          |> decoded(String.to_existing_atom(unquote(encoding)))
          |> Map.fetch!(:timeseries)
          |> Enum.map(&length(&1.samples))
          |> Enum.sum()

        response = remote_write(name, fixture(unquote(file)), unquote(encoding))

        assert response.status == 204
        assert response.resp_body == ""
        assert samples == 835
        assert landed(buffer) == samples
        assert_receive {:samples, %{count: 835}, %{result: "written"}}

        assert {:ok, schema} = Catalog.table_schema(catalog, @table)
        assert schema.clustering == ["name", "ts"]

        assert Enum.map(schema.fields, &{&1.name, &1.type, &1.nullable}) == [
                 {"name", :string, false},
                 {"series", :int64, false},
                 {"labels", {:map, :string, :string}, true},
                 {"ts", :timestamp, false},
                 {"value", :float64, false}
               ]
      end
    end

    test "the same body twice is written once", %{name: name, buffer: buffer} do
      body = fixture("write_zstd.bin")

      assert remote_write(name, body, "zstd").status == 204
      assert remote_write(name, body, "zstd").status == 204
      assert landed(buffer) == 835
    end

    test "writes into a table that already exists", %{
      name: name,
      buffer: buffer,
      catalog: catalog
    } do
      :ok = Catalog.create_dataset(catalog, "metrics")
      :ok = Catalog.create_table(catalog, @table, Schema.new!(schema_fields()))

      assert remote_write(name, fixture("write_snappy.bin"), "snappy").status == 204
      assert landed(buffer) == 835
      assert {:ok, []} = Catalog.clustering(catalog, @table)
    end

    test "a table whose columns refuse the rows answers 500 and writes nothing", %{
      name: name,
      buffer: buffer,
      catalog: catalog
    } do
      :ok = Catalog.create_dataset(catalog, "metrics")

      :ok =
        Catalog.create_table(
          catalog,
          @table,
          Schema.new!([{"name", :string}, {"ts", :timestamp}, {"value", :float64}])
        )

      response = remote_write(name, fixture("write_zstd.bin"), "zstd")

      assert response.status == 500
      assert %{"status" => "error", "error" => error} = JSON.decode!(response.resp_body)
      assert error =~ "the block was not written: sample 0 refused: unknown column"
      assert landed(buffer) == 0
      assert_receive {:samples, %{count: 835}, %{result: "refused"}}
    end
  end

  defp schema_fields do
    [
      {"name", :string, nullable: false},
      {"series", :int64, nullable: false},
      {"labels", {:map, :string, :string}},
      {"ts", :timestamp, nullable: false},
      {"value", :float64, nullable: false}
    ]
  end

  describe "refusals" do
    test "remote write 2.0 is 415 and names 1.0", %{name: name} do
      response =
        post(name, fixture("write_snappy.bin"), [
          {"content-type", "application/x-protobuf;proto=io.prometheus.write.v2.Request"},
          {"content-encoding", "snappy"}
        ])

      assert response.status == 415
      assert JSON.decode!(response.resp_body)["error"] =~ "remote write 1.0"
    end

    test "a content type other than protobuf is 415", %{name: name} do
      for headers <- [[{"content-type", "application/json"}], []] do
        response = post(name, "{}", headers)

        assert response.status == 415, inspect(headers)
      end
    end

    test "a 1.0 content type with a proto parameter is read", %{name: name, buffer: buffer} do
      response =
        post(name, fixture("write_snappy.bin"), [
          {"content-type", "application/x-protobuf; proto=prometheus.WriteRequest"},
          {"content-encoding", "snappy"}
        ])

      assert response.status == 204
      assert landed(buffer) == 835
    end

    test "an encoding other than snappy, zstd or none is 415", %{name: name} do
      assert remote_write(name, fixture("write_zstd.bin"), "gzip").status == 415
    end

    test "a snappy body that declares more than the limit is 413", %{name: name} do
      response = remote_write(name, varint(1_000_000_000) <> <<0, 0, 0>>, "snappy")

      assert response.status == 413
      assert JSON.decode!(response.resp_body)["error"] =~ "limited to 8000000 bytes"
    end

    test "a body past the limit as sent is 413", %{name: name} do
      runtime = %{elem(Runtime.fetch(name), 1) | max_ndjson_bytes: 100}
      Runtime.put(runtime)

      assert remote_write(name, :binary.copy(<<0>>, 200), "zstd").status == 413
    end

    test "a body that does not decode is 400", %{name: name, buffer: buffer} do
      for {body, encoding} <- [{"garbage", "zstd"}, {"garbage", "snappy"}, {<<0x0A, 10>>, ""}] do
        response = remote_write(name, body, encoding)

        assert response.status == 400, inspect({body, encoding})
        assert %{"errorType" => "bad_data"} = JSON.decode!(response.resp_body)
      end

      assert landed(buffer) == 0
    end

    test "a series with no __name__ is 400", %{name: name} do
      body = bytes(1, label("job", "api") <> sample(1_700_000_000_000, 1.0))
      response = remote_write(name, body, "")

      assert response.status == 400
      assert JSON.decode!(response.resp_body)["error"] =~ "__name__"
    end

    test "a request whose every sample is dropped writes nothing and answers 204", %{
      name: name,
      buffer: buffer,
      catalog: catalog
    } do
      nan = <<0x7FF8_0000_0000_0001::little-64>>

      body =
        bytes(
          1,
          label("__name__", "up") <> bytes(2, varint(9) <> nan <> varint(16) <> varint(5))
        )

      assert remote_write(name, body, "").status == 204
      assert_receive {:samples, %{count: 1}, %{result: "nan"}}
      assert landed(buffer) == 0
      assert {:error, {:unknown_table, @table}} = Catalog.table_schema(catalog, @table)
    end
  end

  describe "rows/1" do
    test "sorts the labels, leaves __name__ out of the map, and fingerprints the sorted set" do
      series = [
        %{name: "up", labels: [{"job", "api"}, {"instance", "a:1"}], samples: [{1_000, 1.0}]},
        %{name: "up", labels: [{"instance", "a:1"}, {"job", "api"}], samples: [{2_000, 0.0}]}
      ]

      assert {:ok, [first, second]} = Write.rows(series)
      assert first["labels"] == %{"instance" => "a:1", "job" => "api"}
      assert Map.keys(first["labels"]) == ["instance", "job"]
      assert first["series"] == second["series"]
      assert first["name"] == "up"
      assert first["ts"] == ~N[1970-01-01 00:00:01.000000]
      assert second["value"] == 0.0
    end

    test "every row of the vmagent body carries its series' sorted labels" do
      %{timeseries: timeseries} = decoded("write_zstd.bin", :zstd)

      assert {:ok, rows} = Write.rows(timeseries)
      assert Enum.count(rows) == 835
      assert Enum.all?(rows, &(&1["labels"]["job"] == "vmagent"))
      refute Enum.any?(rows, &Map.has_key?(&1["labels"], "__name__"))

      assert rows |> MapSet.new(& &1["series"]) |> MapSet.size() == 835
    end

    test "refuses an unnamed series and a timestamp past the calendar" do
      assert Write.rows([%{name: nil, labels: [], samples: [{0, 1.0}]}]) ==
               {:error, :unnamed_series}

      assert Write.rows([%{name: "", labels: [], samples: []}]) == {:error, :unnamed_series}

      assert Write.rows([%{name: "up", labels: [], samples: [{9_223_372_036_854_775_807, 1.0}]}]) ==
               {:error, {:invalid_timestamp, 9_223_372_036_854_775_807}}
    end
  end

  describe "fingerprint/2" do
    test "is the first 8 bytes of the SHA-256 of the length-framed name and sorted labels" do
      encoding =
        <<2::32, "up", 8::32, "instance", 3::32, "a:1", 3::32, "job", 3::32, "api">>

      <<expected::signed-big-64, _rest::binary>> = :crypto.hash(:sha256, encoding)

      assert Write.fingerprint("up", [{"job", "api"}, {"instance", "a:1"}]) == expected
      assert Write.fingerprint("up", [{"instance", "a:1"}, {"job", "api"}]) == expected
    end

    test "tells apart what a plain concatenation would not" do
      refute Write.fingerprint("up", [{"ab", "c"}]) == Write.fingerprint("up", [{"a", "bc"}])
      refute Write.fingerprint("up", []) == Write.fingerprint("u", [{"p", ""}])
    end
  end

  describe "timestamp/1" do
    test "is a microsecond NaiveDateTime the :timestamp validator takes" do
      assert {:ok, ts} = Write.timestamp(1_789_380_000_123)
      assert ts == ~N[2026-09-14 10:00:00.123000]
      assert Schema.value_from_json(:timestamp, ts) == {:ok, ts}
      assert Write.timestamp(-1) == {:ok, ~N[1969-12-31 23:59:59.999000]}
    end
  end

  describe "failure/2" do
    test "a full or overloaded buffer is 429 with retry-after", %{name: name} do
      {:ok, runtime} = Runtime.fetch(name)

      assert {429, "unavailable", _message, 1} = Write.failure({:error, :buffer_full}, runtime)

      assert {429, "unavailable", _message, 3} =
               Write.failure({:error, {:overloaded, 2_500}}, runtime)
    end

    test "an unreachable write path is 503 with retry-after", %{name: name} do
      {:ok, runtime} = Runtime.fetch(name)

      for reason <- [
            :ingest_service_unavailable,
            :buffer_service_unavailable,
            {:badrpc, :timeout},
            :not_owner,
            {:catalog_unavailable, :closed},
            {:create_failed, :closed}
          ] do
        assert {503, "unavailable", _message, seconds} = Write.failure({:error, reason}, runtime)
        assert is_integer(seconds), inspect(reason)
      end
    end

    test "anything else is a 500 the client retries" do
      runtime = Runtime.new(password: "x", catalog: MapCatalog.new())

      assert {500, "internal", message, nil} = Write.failure({:error, :surprise}, runtime)
      assert message =~ ":surprise"
    end

    test "a 429 carries retry-after on the wire" do
      conn =
        SmolqueryVictoriaMetrics.Errors.send_error(
          conn(:post, "/api/v1/write"),
          Write.failure(
            {:error, :buffer_full},
            Runtime.new(password: "x", catalog: MapCatalog.new())
          )
        )

      assert conn.status == 429
      assert get_resp_header(conn, "retry-after") == ["1"]
    end
  end
end
