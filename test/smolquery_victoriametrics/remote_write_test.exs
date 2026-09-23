defmodule SmolqueryVictoriaMetrics.RemoteWriteTest do
  @moduledoc """
  Hand-encoded `WriteRequest` bytes for every field, wire type and refusal,
  and the two bodies vmagent v1.152.0 sent in its own protocol (zstd) and in
  Prometheus remote write 1.0 (snappy); `test/support/fixtures/victoriametrics/README.md`
  says how they were captured.
  """

  use ExUnit.Case, async: true

  import Bitwise

  alias SmolqueryVictoriaMetrics.RemoteWrite

  @fixtures Path.expand("../support/fixtures/victoriametrics", __DIR__)
  @max_double 1.797_693_134_862_315_7e308
  @none %{nan: 0, histograms: 0, exemplars: 0, metadata: 0}

  defp varint(n) when n < 0, do: varint(n + (1 <<< 64))
  defp varint(n) when n < 0x80, do: <<n>>
  defp varint(n), do: <<1::1, n &&& 0x7F::7, varint(n >>> 7)::binary>>

  defp key(field, wire), do: varint(field <<< 3 ||| wire)
  defp bytes(field, value), do: key(field, 2) <> varint(byte_size(value)) <> value
  defp int(field, value), do: key(field, 0) <> varint(value)
  defp fixed64(field, bits), do: key(field, 1) <> <<bits::little-64>>
  defp fixed32(field, bits), do: key(field, 5) <> <<bits::little-32>>

  defp label(name, value), do: bytes(1, bytes(1, name) <> bytes(2, value))
  defp sample(timestamp, value), do: bytes(2, fixed64(1, float_bits(value)) <> int(2, timestamp))
  defp series(fields), do: bytes(1, IO.iodata_to_binary(fields))

  defp float_bits(:nan), do: 0x7FF8_0000_0000_0001
  defp float_bits(:inf), do: 0x7FF0_0000_0000_0000
  defp float_bits(:neg_inf), do: 0xFFF0_0000_0000_0000

  defp float_bits(value) do
    <<bits::64>> = <<value::float-64>>
    bits
  end

  defp snappy_literal(protobuf) when byte_size(protobuf) <= 60,
    do: varint(byte_size(protobuf)) <> <<(byte_size(protobuf) - 1) <<< 2>> <> protobuf

  defp fixture(name), do: File.read!(Path.join(@fixtures, name))

  describe "decode/1" do
    test "an empty request has no series" do
      assert RemoteWrite.decode(<<>>) == {:ok, %{timeseries: [], dropped: @none}}
    end

    test "a second __name__ stays among the labels, for the writer to refuse" do
      protobuf = series([label("__name__", "up"), label("__name__", "down"), label("a", "1")])

      assert {:ok, %{timeseries: [%{name: "up", labels: [{"__name__", "down"}, {"a", "1"}]}]}} =
               RemoteWrite.decode(protobuf)
    end

    test "a series takes its name from __name__ and keeps the other labels in order" do
      body =
        series([
          label("__name__", "http_requests_total"),
          label("job", "api"),
          label("instance", "a:1"),
          sample(1_700_000_000_000, 1.5),
          sample(1_700_000_001_000, 2.0)
        ]) <> series([label("zone", "b"), sample(5, 0.25)])

      assert RemoteWrite.decode(body) ==
               {:ok,
                %{
                  timeseries: [
                    %{
                      name: "http_requests_total",
                      labels: [{"job", "api"}, {"instance", "a:1"}],
                      samples: [{1_700_000_000_000, 1.5}, {1_700_000_001_000, 2.0}]
                    },
                    %{name: nil, labels: [{"zone", "b"}], samples: [{5, 0.25}]}
                  ],
                  dropped: @none
                }}
    end

    test "a negative timestamp is a two's complement int64" do
      assert {:ok, %{timeseries: [%{samples: [{-1_000, 3.0}]}]}} =
               RemoteWrite.decode(series([sample(-1_000, 3.0)]))

      assert byte_size(varint(-1)) == 10
    end

    test "fields a sample or label leaves out take protobuf's defaults" do
      body = series([bytes(1, <<>>), bytes(2, <<>>), bytes(2, int(2, 7))])

      assert {:ok,
              %{timeseries: [%{name: nil, labels: [{"", ""}], samples: [{0, +0.0}, {7, +0.0}]}]}} =
               RemoteWrite.decode(body)
    end

    test "NaN is dropped and counted; the infinities become the largest finite doubles" do
      body =
        series([
          label("__name__", "m"),
          sample(1, :nan),
          sample(2, :inf),
          sample(3, :neg_inf),
          sample(4, -0.5),
          sample(5, :nan)
        ])

      assert {:ok, %{timeseries: [%{samples: samples}], dropped: dropped}} =
               RemoteWrite.decode(body)

      assert samples == [{2, @max_double}, {3, -@max_double}, {4, -0.5}]
      assert dropped == %{@none | nan: 2}
    end

    test "a series whose every sample is NaN is kept with no samples" do
      assert {:ok, %{timeseries: [%{name: "m", samples: []}]}} =
               RemoteWrite.decode(series([label("__name__", "m"), sample(1, :nan)]))
    end

    test "exemplars, histograms and metadata are counted and not decoded" do
      body =
        series([
          label("__name__", "m"),
          bytes(3, "exemplar"),
          bytes(4, "histogram"),
          bytes(4, "h2")
        ]) <>
          bytes(3, "metadata")

      assert {:ok, %{timeseries: [%{name: "m", labels: [], samples: []}], dropped: dropped}} =
               RemoteWrite.decode(body)

      assert dropped == %{nan: 0, histograms: 2, exemplars: 1, metadata: 1}
    end

    test "unknown fields of every wire type are skipped at every level" do
      unknown = int(9, 1 <<< 40) <> fixed64(10, 1) <> bytes(11, "skip me") <> fixed32(12, 7)
      label = bytes(1, unknown <> bytes(1, "__name__") <> unknown <> bytes(2, "m"))
      sample = bytes(2, unknown <> fixed64(1, float_bits(4.0)) <> int(2, 9) <> unknown)
      body = unknown <> series([unknown, label, unknown, sample]) <> unknown

      assert RemoteWrite.decode(body) ==
               {:ok,
                %{timeseries: [%{name: "m", labels: [], samples: [{9, 4.0}]}], dropped: @none}}
    end

    test "a known field with the wrong wire type is skipped" do
      body = int(1, 5) <> series([int(1, 1), fixed32(2, 1), label("__name__", "m")])

      assert {:ok, %{timeseries: [%{name: "m", labels: [], samples: []}]}} =
               RemoteWrite.decode(body)
    end

    test "malformed protobuf is refused with a message a 400 can carry" do
      for {body, fragment} <- [
            {<<0x0A, 10, "short">>, "runs past the end"},
            {<<0x0A>>, "ends inside a varint"},
            {<<0x08>> <> :binary.copy(<<0xFF>>, 10) <> <<1>>, "longer than 10 bytes"},
            {key(1, 3), "group"},
            {key(1, 4), "group"},
            {key(1, 6), "wire type 6"},
            {key(0, 0) <> <<0>>, "number 0"},
            {series([bytes(2, key(1, 1) <> <<1, 2, 3>>)]), "fixed-width"},
            {series([label(<<0xFF>>, "v")]), "label name is not valid UTF-8"},
            {series([label("job", <<0xC3>>)]), "label job is not valid UTF-8"}
          ] do
        assert {:error, {:invalid_write_request, message}} = RemoteWrite.decode(body)
        assert message =~ fragment
      end
    end
  end

  describe "declared_length/2" do
    test "reads what a body declares it inflates to, per encoding" do
      assert RemoteWrite.declared_length(snappy_literal("abc"), :snappy) == {:ok, 3}
      assert RemoteWrite.declared_length("abcd", :identity) == {:ok, 4}

      zstd = fixture("write_zstd.bin")
      {:ok, %{frameContentSize: declared}} = :zstd.get_frame_header(zstd)
      assert RemoteWrite.declared_length(zstd, :zstd) == {:ok, declared}

      assert {:error, {:invalid_snappy, _message}} = RemoteWrite.declared_length(<<>>, :snappy)
    end
  end

  describe "encoding/1" do
    test "reads Content-Encoding" do
      assert RemoteWrite.encoding("snappy") == {:ok, :snappy}
      assert RemoteWrite.encoding(" ZSTD ") == {:ok, :zstd}
      assert RemoteWrite.encoding(nil) == {:ok, :identity}
      assert RemoteWrite.encoding("") == {:ok, :identity}
      assert RemoteWrite.encoding("identity") == {:ok, :identity}
      assert RemoteWrite.encoding("gzip") == {:error, :unsupported_encoding}
      assert RemoteWrite.encoding("snappy, gzip") == {:error, :unsupported_encoding}
    end
  end

  describe "decode_body/3" do
    setup do
      %{protobuf: series([label("__name__", "up"), sample(1, 1.0)])}
    end

    test "an uncompressed body is decoded as it is, within max_bytes", %{protobuf: protobuf} do
      assert {:ok, %{timeseries: [%{name: "up"}]}} =
               RemoteWrite.decode_body(protobuf, :identity, max_bytes: byte_size(protobuf))

      assert RemoteWrite.decode_body(protobuf, :identity, max_bytes: 3) ==
               {:error, {:too_large, byte_size(protobuf), 3}}
    end

    test "a snappy body is held to max_bytes by its preamble", %{protobuf: protobuf} do
      body = snappy_literal(protobuf)

      assert {:ok, %{timeseries: [%{name: "up"}]}} =
               RemoteWrite.decode_body(body, :snappy, max_bytes: byte_size(protobuf))

      assert RemoteWrite.decode_body(body, :snappy, max_bytes: 3) ==
               {:error, {:too_large, byte_size(protobuf), 3}}

      assert {:error, {:invalid_snappy, _message}} =
               RemoteWrite.decode_body(binary_part(body, 0, 5), :snappy, max_bytes: 1000)
    end

    test "a zstd body is held to max_bytes by its frame", %{protobuf: protobuf} do
      body = protobuf |> :zstd.compress() |> IO.iodata_to_binary()

      assert {:ok, %{timeseries: [%{name: "up"}]}} =
               RemoteWrite.decode_body(body, :zstd, max_bytes: byte_size(protobuf))

      assert RemoteWrite.decode_body(body, :zstd, max_bytes: 3) ==
               {:error, {:too_large, byte_size(protobuf), 3}}
    end

    test "a body that inflates to bad protobuf is refused as protobuf" do
      assert {:error, {:invalid_write_request, _message}} =
               RemoteWrite.decode_body(snappy_literal(<<0x0A, 10>>), :snappy, max_bytes: 1000)
    end
  end

  describe "vmagent v1.152.0 bodies" do
    test "headers.json records the encoding each body was sent in" do
      headers = @fixtures |> Path.join("headers.json") |> File.read!() |> JSON.decode!()

      assert %{
               "Content-Encoding" => "zstd",
               "Content-Type" => "application/x-protobuf",
               "X-VictoriaMetrics-Remote-Write-Version" => "1"
             } = headers["write_zstd.bin"]["headers"]

      assert %{
               "Content-Encoding" => "snappy",
               "Content-Type" => "application/x-protobuf",
               "X-Prometheus-Remote-Write-Version" => "0.1.0"
             } = headers["write_snappy.bin"]["headers"]

      for {file, %{"headers" => %{"Content-Encoding" => encoding}}} <- headers do
        assert {:ok, _encoding} = RemoteWrite.encoding(encoding)
        assert file |> fixture() |> byte_size() > 0
      end
    end

    for {file, encoding} <- [{"write_zstd.bin", :zstd}, {"write_snappy.bin", :snappy}] do
      test "#{file} decodes to vmagent's own metrics" do
        assert {:ok, %{timeseries: timeseries, dropped: dropped}} =
                 RemoteWrite.decode_body(fixture(unquote(file)), unquote(encoding),
                   max_bytes: 8_388_608
                 )

        assert dropped == @none
        assert Enum.count_until(timeseries, 101) > 100

        names = MapSet.new(timeseries, & &1.name)
        assert MapSet.member?(names, "vm_app_version")
        assert MapSet.member?(names, "process_cpu_cores_available")
        refute MapSet.member?(names, nil)

        earliest = DateTime.to_unix(~U[2026-01-01 00:00:00Z], :millisecond)
        latest = DateTime.to_unix(~U[2100-01-01 00:00:00Z], :millisecond)

        for %{labels: labels, samples: samples} <- timeseries do
          label_names = Enum.map(labels, &elem(&1, 0))
          assert "job" in label_names and "instance" in label_names
          refute "__name__" in label_names
          assert {"job", "vmagent"} in labels
          assert samples != []

          for {timestamp, value} <- samples do
            assert timestamp > earliest and timestamp < latest
            assert is_float(value)
          end
        end

        version = Enum.find(timeseries, &(&1.name == "vm_app_version"))
        assert {"short_version", "v1.152.0"} in version.labels
      end
    end
  end
end
