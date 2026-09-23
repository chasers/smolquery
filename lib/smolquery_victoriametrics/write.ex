defmodule SmolqueryVictoriaMetrics.Write do
  @moduledoc """
  Prometheus remote write into one smolquery table (PL-70, T-562).

  vmagent, Prometheus and the OpenTelemetry collector push a
  `prometheus.WriteRequest`, snappy- or zstd-compressed, which
  `SmolqueryVictoriaMetrics.RemoteWrite` decodes. Every sample becomes one
  row of the runtime's table, `metrics.samples` by default:

  | column | type | |
  |---|---|---|
  | `name` | `STRING NOT NULL` | the `__name__` label |
  | `series` | `INT64 NOT NULL` | `fingerprint/2` of the name and the other labels |
  | `labels` | `MAP(STRING, STRING)` | every label but `__name__` |
  | `ts` | `TIMESTAMP NOT NULL` | the sample's millisecond timestamp |
  | `value` | `FLOAT64 NOT NULL` | |

  The table is clustered by `name, ts`, and created with its dataset on the
  first write that finds it missing, since a remote-write client has no way
  to create it. A table that already exists is written as it is.

  The rows go through `Smolquery.IngestService.Client.insert/4` all or
  nothing (`skip_invalid_rows: false`), with the SHA-256 of the body as sent
  as the batch id, so a client's retry of a block after a lost answer is
  answered from the original commit instead of written twice.

  `SmolqueryVictoriaMetrics.Router` has already checked the password and
  counted the body as sent against ingest admission by the time this runs.
  Once the body is read, its reservation is resized to the body plus what it
  declares it inflates to (`SmolqueryApi.Admission.resize/2`), or plus
  `max_decoded_bytes` when a zstd frame declares no size, before anything is
  inflated: a compressed block holds tens of times its wire size once it is
  decoded to rows, and admission counts what the node will hold.

  ## Labels

  A label whose value is empty is dropped before anything else, as
  VictoriaMetrics drops it on ingest and as PromQL reads `a=""`: the label
  is absent. What is left must name the series exactly once with
  `__name__`, and no other label name may repeat, as Prometheus refuses a
  repeated label name; either refusal is the whole request's.

  ## Answers

  VictoriaMetrics' own: a 204 with no body once the rows are durable.
  Prometheus and the collector drop a block on any 4xx but 429 and retry a
  429 or a 5xx. vmagent v1.152.0 drops a block only on a 400, 409 or 415 and
  retries every other status forever, with its queue stuck behind the block
  (`SmolqueryVictoriaMetrics.Errors`). So a block that no retry can fix is a
  400 or a 415, and every refusal the client can outwait is a 429 or a 5xx:

    * 400 — the body does not decompress or decode; it inflates, or
      declares it inflates, past `max_decoded_bytes`, as VictoriaMetrics
      answers its `-maxInsertRequestSize`; a series has no `__name__`, or
      repeats a label name; or a timestamp is past what a `TIMESTAMP`
      holds;
    * 413 — the body as sent is past `max_ndjson_bytes`. vmagent's
      compressed blocks are far below it, and every other client drops the
      block;
    * 415 — a `Content-Type` other than `application/x-protobuf`, a `proto`
      parameter other than `prometheus.WriteRequest` (remote write 2.0 among
      them, so the sender falls back to 1.0 as the specification says it
      must), or a `Content-Encoding` other than `snappy`, `zstd` or none;
    * 429 with `retry-after` — the in-flight bytes the edge admits are
      taken, or the buffer is full, overloaded or at its backlog ceiling;
    * 503 with `retry-after` — the ingest or buffer service is not reachable,
      the table's ownership is moving, or the catalog could not answer;
    * 500 — the table refused a row, which with an all-or-nothing write means
      nothing was written; the client must not drop the block silently.

  Every answer is `SmolqueryVictoriaMetrics.Errors`' JSON form.

  ## Samples not written

  `[:smolquery, :victoriametrics, :samples]` is emitted with `%{count: n}`
  and a `result` of `"written"`, `"nan"`, `"histogram"`, `"exemplar"` or
  `"refused"`: rows written, NaN samples dropped (PL-70 D6), native histograms
  and exemplars dropped (PL-70 D7), and rows a refused write did not write.
  The counts are of what each request carried. A retried block that the
  buffer answers from its first commit is counted again: the ingest service
  answers a duplicate with the original acknowledgement, and nothing in it
  tells the two apart.
  """

  import Plug.Conn

  require Logger

  alias Plug.Conn.Utils
  alias Smolquery.BufferService.Backlog
  alias Smolquery.Catalog
  alias Smolquery.IngestService
  alias Smolquery.Schema
  alias SmolqueryApi.Admission
  alias SmolqueryApi.Body
  alias SmolqueryVictoriaMetrics.Errors
  alias SmolqueryVictoriaMetrics.RemoteWrite
  alias SmolqueryVictoriaMetrics.Runtime

  @remote_write_v1 "prometheus.WriteRequest"
  @remote_write_v2 "io.prometheus.write.v2.Request"
  @clustering ["name", "ts"]
  @schema Schema.new!([
            {"name", :string, nullable: false},
            {"series", :int64, nullable: false},
            {"labels", {:map, :string, :string}},
            {"ts", :timestamp, nullable: false},
            {"value", :float64, nullable: false}
          ])

  @type row :: %{String.t() => term()}

  @type rows_error ::
          :unnamed_series
          | {:duplicate_label, String.t(), String.t()}
          | {:invalid_timestamp, integer()}

  @doc """
  Writes the remote-write body of `conn` to the runtime's table and answers.
  """
  @spec call(Plug.Conn.t(), Runtime.t()) :: Plug.Conn.t()
  def call(conn, %Runtime{} = runtime) do
    with :ok <- content_type(get_req_header(conn, "content-type")),
         {:ok, encoding} <- encoding(get_req_header(conn, "content-encoding")),
         {:ok, body, conn} <- Body.read(conn, runtime.max_ndjson_bytes) do
      write_body(conn, runtime, body, encoding)
    else
      {:error, reason, conn} -> answer(conn, reason, runtime)
      {:error, reason} -> answer(conn, reason, runtime)
    end
  end

  defp write_body(conn, runtime, body, encoding) do
    max = runtime.max_decoded_bytes

    with {:ok, inflated} <- inflated_bytes(body, encoding, max),
         :ok <- Admission.resize(conn, byte_size(body) + inflated),
         {:ok, decoded} <- RemoteWrite.decode_body(body, encoding, max_bytes: max),
         {:ok, rows} <- rows(decoded.timeseries),
         {:ok, written} <- write(runtime, rows, batch_id(body)) do
      samples(decoded.dropped, written)
      send_resp(conn, 204, "")
    else
      {:error, {:too_large, bytes, max}} ->
        Logger.warning(
          "remote write refused: a #{byte_size(body)}-byte #{encoding} body inflates to " <>
            "#{bytes} bytes, past max_decoded_bytes #{max}"
        )

        answer(conn, {:too_large, bytes, max}, runtime)

      {:error, reason} ->
        answer(conn, reason, runtime)
    end
  end

  defp inflated_bytes(_body, :identity, _max), do: {:ok, 0}

  defp inflated_bytes(body, encoding, max) do
    case RemoteWrite.declared_length(body, encoding) do
      {:ok, :unknown} -> {:ok, max}
      {:ok, declared} when declared > max -> {:error, {:too_large, declared, max}}
      {:ok, declared} -> {:ok, declared}
      {:error, reason} -> {:error, reason}
    end
  end

  defp answer(conn, {:rows_refused, refused, _errors} = reason, runtime) do
    emit("refused", refused)
    Errors.send_error(conn, failure({:error, reason}, runtime))
  end

  defp answer(conn, reason, runtime),
    do: Errors.send_error(conn, failure({:error, reason}, runtime))

  @doc """
  The rows a decoded request writes, one per sample, in the order sent.

  Labels with an empty value are dropped first, `__name__` among them. A
  series then left with no `__name__`, or with a label name more than once,
  `__name__` included, is refused whole, as VictoriaMetrics and Prometheus
  refuse it, and so is a timestamp outside what a `TIMESTAMP` holds. A series
  with no samples writes nothing.
  """
  @spec rows([RemoteWrite.series()]) :: {:ok, [row()]} | {:error, rows_error()}
  def rows(timeseries) do
    timeseries
    |> Enum.reduce_while({:ok, []}, fn series, {:ok, acc} ->
      case series_rows(series, acc) do
        {:ok, acc} -> {:cont, {:ok, acc}}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
    |> in_order()
  end

  defp in_order({:ok, reversed}), do: {:ok, Enum.reverse(reversed)}
  defp in_order(error), do: error

  defp series_rows(%{samples: samples} = series, acc) do
    with {:ok, name, sorted} <- labels(series) do
      base = %{
        "name" => name,
        "series" => sorted_fingerprint(name, sorted),
        "labels" => Map.new(sorted)
      }

      Enum.reduce_while(samples, {:ok, acc}, &sample_row(base, &1, &2))
    end
  end

  defp sample_row(base, {timestamp_ms, value}, {:ok, acc}) do
    case timestamp(timestamp_ms) do
      {:ok, ts} -> {:cont, {:ok, [Map.merge(base, %{"ts" => ts, "value" => value}) | acc]}}
      :error -> {:halt, {:error, {:invalid_timestamp, timestamp_ms}}}
    end
  end

  defp labels(%{name: name, labels: labels} = series) do
    all = if is_nil(name), do: labels, else: [{"__name__", name} | labels]
    sorted = all |> Enum.reject(&match?({_label, ""}, &1)) |> Enum.sort()

    case repeated(sorted) do
      nil -> named(sorted)
      label -> {:error, {:duplicate_label, describe_series(series), label}}
    end
  end

  defp repeated([{label, _value}, {label, _other} | _rest]), do: label
  defp repeated([_first | rest]), do: repeated(rest)
  defp repeated([]), do: nil

  defp named(sorted) do
    case List.keytake(sorted, "__name__", 0) do
      {{"__name__", name}, rest} -> {:ok, name, rest}
      nil -> {:error, :unnamed_series}
    end
  end

  defp describe_series(%{name: name, labels: labels}) do
    pairs = Enum.map_join(labels, ",", fn {label, value} -> "#{label}=#{inspect(value)}" end)
    "#{name}{#{pairs}}"
  end

  @doc """
  A millisecond Unix timestamp as the `NaiveDateTime`, in UTC with
  microsecond precision, that `Smolquery.Schema.value_from_json/2` takes for a
  `:timestamp` column; `:error` past the range a `DateTime` holds.
  """
  @spec timestamp(integer()) :: {:ok, NaiveDateTime.t()} | :error
  def timestamp(ms) when is_integer(ms) do
    case DateTime.from_unix(ms * 1_000, :microsecond) do
      {:ok, datetime} -> {:ok, DateTime.to_naive(datetime)}
      {:error, _reason} -> :error
    end
  end

  @doc """
  The `series` column: a signed 64-bit fingerprint of a metric name and its
  labels, in any order.

  The labels, `__name__` excluded, are sorted by name and then by value,
  comparing bytes. The name and then each label's name and value, in that
  order, are each framed as a 32-bit big-endian byte length followed by the
  UTF-8 bytes, and the frames are concatenated:

      <<byte_size(name)::32, name, byte_size(k1)::32, k1, byte_size(v1)::32, v1, ...>>

  The fingerprint is the first 8 bytes of that encoding's SHA-256, read as a
  big-endian two's-complement integer. Nothing in it depends on the BEAM, so
  a query can recompute it, in SQL or elsewhere, from a name and labels.
  """
  @spec fingerprint(String.t(), [{String.t(), String.t()}]) :: integer()
  def fingerprint(name, labels), do: sorted_fingerprint(name, Enum.sort(labels))

  defp sorted_fingerprint(name, sorted) do
    frames = [
      frame(name) | Enum.map(sorted, fn {label, value} -> [frame(label), frame(value)] end)
    ]

    <<fingerprint::signed-big-64, _rest::binary>> = :crypto.hash(:sha256, frames)
    fingerprint
  end

  defp frame(text), do: [<<byte_size(text)::unsigned-big-32>>, text]

  defp content_type([type | _rest]) do
    case Utils.content_type(type) do
      {:ok, "application", "x-protobuf", params} ->
        proto(Map.get(params, "proto"))

      _other ->
        {:error, {:unsupported_content_type, type}}
    end
  end

  defp content_type([]), do: {:error, {:unsupported_content_type, nil}}

  defp proto(nil), do: :ok
  defp proto(@remote_write_v1), do: :ok
  defp proto(@remote_write_v2), do: {:error, :remote_write_v2}
  defp proto(other), do: {:error, {:unsupported_proto, other}}

  defp encoding([header | _rest]), do: RemoteWrite.encoding(header)
  defp encoding([]), do: RemoteWrite.encoding(nil)

  defp batch_id(body), do: :sha256 |> :crypto.hash(body) |> Base.encode16(case: :lower)

  defp write(_runtime, [], _batch_id), do: {:ok, 0}

  defp write(runtime, rows, batch_id) do
    case insert(runtime, rows, batch_id) do
      {:error, {missing, _name}} when missing in [:unknown_table, :unknown_dataset] ->
        with :ok <- create(runtime), do: insert(runtime, rows, batch_id)

      result ->
        result
    end
  end

  defp insert(runtime, rows, batch_id) do
    opts = [batch_id: batch_id, skip_invalid_rows: false]

    case IngestService.Client.insert(runtime.ingest_name, runtime.table, rows, opts) do
      {:ok, %{errors: [], inserted: inserted}} -> {:ok, inserted}
      {:ok, %{errors: errors, inserted: inserted}} -> refused(rows, inserted, errors)
      {:error, reason} -> {:error, reason}
    end
  end

  defp refused(rows, inserted, errors),
    do: {:error, {:rows_refused, length(rows) - inserted, errors}}

  defp create(%Runtime{catalog: catalog, table: {dataset, _table} = ref} = runtime) do
    with :ok <- Catalog.create_dataset(catalog, dataset),
         :ok <- Catalog.create_table(catalog, ref, @schema),
         :ok <- Catalog.put_clustering(catalog, ref, @clustering) do
      IngestService.Client.invalidate(runtime.ingest_name, ref)
    else
      {:error, reason} -> {:error, {:create_failed, reason}}
    end
  end

  defp samples(dropped, written) do
    emit("written", written)
    emit("nan", dropped.nan)
    emit("histogram", dropped.histograms)
    emit("exemplar", dropped.exemplars)
  end

  defp emit(_result, 0), do: :ok

  defp emit(result, count),
    do:
      :telemetry.execute([:smolquery, :victoriametrics, :samples], %{count: count}, %{
        result: result
      })

  @doc """
  The answer a failed write takes, as `SmolqueryVictoriaMetrics.Errors`
  sends it.
  """
  @spec failure({:error, term()}, Runtime.t()) :: Errors.t()
  def failure({:error, reason}, runtime), do: describe(reason, runtime)

  defp describe(:remote_write_v2, _runtime),
    do:
      {415, "bad_data",
       "remote write 2.0 (proto=#{@remote_write_v2}) is not read here; send Prometheus remote write 1.0",
       nil}

  defp describe({:unsupported_proto, proto}, _runtime),
    do:
      {415, "bad_data",
       "proto=#{inspect(proto)} is not read here; send Prometheus remote write 1.0 (proto=#{@remote_write_v1})",
       nil}

  defp describe({:unsupported_content_type, type}, _runtime),
    do:
      {415, "bad_data",
       "Content-Type #{inspect(type)} is not read here; send application/x-protobuf", nil}

  defp describe(:unsupported_encoding, _runtime),
    do: {415, "bad_data", "the Content-Encoding is not read here; send snappy, zstd or none", nil}

  defp describe(:too_large, runtime),
    do:
      {413, "bad_data",
       "remote write bodies are limited to #{runtime.max_ndjson_bytes} bytes as sent; send smaller blocks",
       nil}

  defp describe({:too_large, _bytes, max}, _runtime),
    do:
      {400, "bad_data",
       "the block inflates past #{max} bytes, the edge's max_decoded_bytes " <>
         "(SMOLQUERY_VICTORIAMETRICS_MAX_DECODED_BYTES); it is dropped, send smaller blocks", nil}

  defp describe({invalid, message}, _runtime)
       when invalid in [:invalid_snappy, :invalid_zstd, :invalid_write_request],
       do: {400, "bad_data", message, nil}

  defp describe(:unnamed_series, _runtime),
    do: {400, "bad_data", "a series has no __name__ label", nil}

  defp describe({:duplicate_label, series, label}, _runtime),
    do: {400, "bad_data", "series #{series} has more than one label named #{label}", nil}

  defp describe({:invalid_timestamp, ms}, _runtime),
    do: {400, "bad_data", "sample timestamp #{ms} ms is out of range", nil}

  defp describe(:admission_full, _runtime),
    do: {429, "unavailable", "too many write bytes in flight, retry later", 1}

  defp describe(:buffer_full, _runtime),
    do: {429, "unavailable", "buffer full, retry later", 1}

  defp describe({:overloaded, predicted_ms}, _runtime),
    do:
      {429, "unavailable", "write path overloaded, ~#{predicted_ms} ms behind; retry later",
       max(ceil(predicted_ms / 1000), 1)}

  defp describe({:backlog_full, refusal}, _runtime),
    do: {429, "unavailable", Backlog.message(refusal), 5}

  defp describe({:rows_refused, _refused, [%{index: index, errors: [first | _rest]} | _more]}, _),
    do:
      {500, "internal", "the block was not written: sample #{index} refused: #{first.message}",
       nil}

  defp describe({:create_failed, reason}, runtime),
    do: unavailable("the catalog could not create #{table_name(runtime)}: #{inspect(reason)}", 5)

  defp describe({:stale_schema, _ref, _names}, _runtime),
    do: unavailable("the table's columns changed while writing; retry", 1)

  defp describe(reason, _runtime)
       when reason in [:not_owner, :ownership_settling, :ring_config_stale, :draining],
       do: unavailable("table ownership is moving; retry", 1)

  defp describe(reason, _runtime)
       when reason in [:buffer_service_unavailable, :ingest_service_unavailable],
       do: unavailable("the write path is not available here", 5)

  defp describe({rpc, _reason}, _runtime) when rpc in [:badrpc, :badtcp],
    do: unavailable("the owning buffer node could not be reached; retry", 1)

  defp describe({:catalog_unavailable, _reason}, _runtime),
    do: unavailable("the catalog could not confirm the table's columns; retry", 1)

  defp describe(:whole_request_unsupported, _runtime),
    do:
      unavailable(
        "the owning buffer node runs a release that cannot refuse a whole request; finish the rollout",
        5
      )

  defp describe(reason, _runtime),
    do: {500, "internal", "write failed: #{inspect(reason)}", nil}

  defp unavailable(message, retry_after), do: {503, "unavailable", message, retry_after}

  defp table_name(%Runtime{table: {dataset, table}}), do: "#{dataset}.#{table}"
end
